"""Deterministic fixture generator for MarketPulse (WP0).

Single source of truth for the seeded dataset. It emits the historical **raw
NDJSON** seed and *derives* the bronze / silver fixtures from it, so every layer's
fixture is internally consistent: the silver OHLCV actually aggregates the bronze
rows, which are the clean subset of the raw rows. That makes the silver fixture
double as WP2's correctness oracle.

Stdlib only (``json``/``csv``/``random``/``datetime``/``pathlib``) so local
``pytest`` needs no PySpark.

Determinism: a fixed module-level ``SEED`` drives a single ``random.Random``
consumed in a fixed order, and all floats are rounded to fixed precision. Re-running
must produce byte-identical output (``tests/test_fixtures.py`` asserts this):

    python fixtures/generate_fixtures.py
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

# --------------------------------------------------------------------------- #
# Parameters (constants — the knobs that define the seed)
# --------------------------------------------------------------------------- #
SEED = 20260627

# 3 symbols × 60 min comfortably exercises gold's 30-min rolling window and the
# multi-symbol MERGE while staying small enough to commit.
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
BASE_PRICE = {"BTCUSDT": 61000.0, "ETHUSDT": 3400.0, "SOLUSDT": 145.0}
# Per-trade random-walk step and typical trade size, scaled per symbol.
PRICE_STEP = {"BTCUSDT": 25.0, "ETHUSDT": 2.0, "SOLUSDT": 0.15}
QTY_RANGE = {"BTCUSDT": (0.001, 0.05), "ETHUSDT": (0.01, 0.5), "SOLUSDT": (1.0, 40.0)}

START = datetime(2026, 6, 27, 10, 0, 0)  # naive UTC; rendered with a trailing Z
MINUTES = 60
TRADES_PER_MIN = (8, 20)  # randomised inclusive range per symbol per minute

# A fixed ingest timestamp so the bronze fixture's ingest_ts is deterministic.
INGEST_TS = "2026-06-27T10:05:00.000Z"

# Production landing path baked into the bronze _source_file column. The
# suffix-aware path is resolved by 00_setup at load time; the committed fixture
# carries the production string (cosmetic metadata, matches src/config.volume_path).
VOLUME_PATH = "/Volumes/mktpulse/bronze/raw"

# A minute where we inject a >2× volume spike per symbol, so WP3's volume_spike
# (volume > 2× 30-min rolling avg) has a visible positive case (plan risk R4).
SPIKE_MINUTE = 45
SPIKE_FACTOR = 12.0

# Rounding precision (kept identical in the contract test's oracle).
PRICE_DP = 2
QTY_DP = 6
RATIO_DP = 6
VOLATILITY_DP = 6
ROLLING_DP = 6

# Gold signal knobs — kept identical to src/gold.py so the twin reproduces this
# fixture. Order-flow thresholds gate the *strong* momentum reads; a window is a
# spike when volume exceeds VOLUME_SPIKE_FACTOR× its trailing 30-window rolling avg.
BULL_RATIO = 0.6
BEAR_RATIO = 0.4
VOLUME_SPIKE_FACTOR = 2.0
ROLLING_WINDOW_ROWS = 30

# Fixed wall-clock stamp for the gold fixture's non-deterministic generated_at (the
# Spark sink stamps current_timestamp(); the committed fixture carries a constant so
# it stays byte-identical — same trick as INGEST_TS for bronze).
GENERATED_AT = "2026-06-27T10:06:00.000Z"

OUT = Path(__file__).resolve().parent


# --------------------------------------------------------------------------- #
# Internal trade record
# --------------------------------------------------------------------------- #
@dataclass
class Trade:
    """One trade plus the bookkeeping the derivations need.

    ``file_minute`` is the landing minute (which raw file it goes in); ``event_dt``
    is the event time. For late/out-of-order rows the two differ. ``reason`` being
    set marks a deliberately *dirty* row destined for quarantine.
    """

    file_minute: int
    event_dt: datetime | None
    symbol: str | None
    price: float | None
    qty: float | None
    side: str | None
    trade_id: str | None
    reason: str | None = None  # None ⇒ clean; else the quarantine reason

    @property
    def event_ts(self) -> str | None:
        return _iso_ms(self.event_dt) if self.event_dt is not None else None

    @property
    def is_dirty(self) -> bool:
        return self.reason is not None


def _iso_ms(dt: datetime) -> str:
    """ISO-8601 UTC string with millisecond precision and a trailing Z."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _iso_window(dt: datetime) -> str:
    """ISO-8601 UTC string at second precision (used for window boundaries)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _raw_filename(file_minute: int) -> str:
    """One raw file per landing minute: trades_2026-06-27T1000.json …"""
    return "trades_" + (START + timedelta(minutes=file_minute)).strftime("%Y-%m-%dT%H%M") + ".json"


# --------------------------------------------------------------------------- #
# Generation
# --------------------------------------------------------------------------- #
def _generate_trades() -> list[Trade]:
    """Build the full ordered trade list: clean walk + injected dirty/late rows."""
    rng = random.Random(SEED)
    trades: list[Trade] = []
    price = dict(BASE_PRICE)
    seq = {sym: 1_000_001 for sym in SYMBOLS}

    # Minute-major, symbol order — fixes the rng consumption order.
    for minute in range(MINUTES):
        minute_base = START + timedelta(minutes=minute)
        for sym in SYMBOLS:
            n = rng.randint(*TRADES_PER_MIN)
            for _ in range(n):
                # Random walk keeps prices realistic and gives non-trivial OHLC.
                price[sym] = round(price[sym] + rng.uniform(-PRICE_STEP[sym], PRICE_STEP[sym]), PRICE_DP)
                lo, hi = QTY_RANGE[sym]
                qty = rng.uniform(lo, hi)
                if minute == SPIKE_MINUTE:
                    qty *= SPIKE_FACTOR  # inject the volume spike (plan R4)
                event_dt = minute_base + timedelta(
                    seconds=rng.randint(0, 59), milliseconds=rng.randint(0, 999)
                )
                trades.append(
                    Trade(
                        file_minute=minute,
                        event_dt=event_dt,
                        symbol=sym,
                        price=price[sym],
                        qty=round(qty, QTY_DP),
                        side="buy" if rng.random() < 0.5 else "sell",
                        trade_id=f"{sym}-{seq[sym]}",
                    )
                )
                seq[sym] += 1

    trades.extend(_special_rows())
    return trades


def _special_rows() -> list[Trade]:
    """Deterministic, hand-placed dirty + late rows (fixed values, no rng).

    Dirty rows feed WP1's quarantine path and WP5's DQ; late rows exercise WP2's
    watermark. Fixed so tests can assert them precisely.
    """
    rows: list[Trade] = []

    # --- Deliberately dirty rows (null key ⇒ quarantine, never bronze) -------
    # 2× null event_ts
    rows.append(Trade(5, None, "BTCUSDT", 61010.0, 0.012345, "buy", "BTCUSDT-9000001", "null event_ts"))
    rows.append(Trade(12, None, "ETHUSDT", 3402.5, 0.123456, "sell", "ETHUSDT-9000001", "null event_ts"))
    # 2× null symbol
    rows.append(Trade(7, START + timedelta(minutes=7, seconds=10), None, 144.5, 12.5, "buy", "SOLUSDT-9000002", "null symbol"))
    rows.append(Trade(20, START + timedelta(minutes=20, seconds=30), None, 61005.0, 0.02, "sell", "BTCUSDT-9000002", "null symbol"))
    # 2× null trade_id
    rows.append(Trade(15, START + timedelta(minutes=15, seconds=5), "ETHUSDT", 3399.0, 0.2, "buy", None, "null trade_id"))
    rows.append(Trade(33, START + timedelta(minutes=33, seconds=45), "SOLUSDT", 145.5, 8.0, "sell", None, "null trade_id"))

    # --- Late / out-of-order rows (clean — land late but valid) --------------
    # Inside the 2-min watermark: lands in file 30, event time back at 10:29:58.
    rows.append(Trade(30, START + timedelta(minutes=29, seconds=58, milliseconds=120), "BTCUSDT", 61020.0, 0.0150, "buy", "BTCUSDT-9000100"))
    # Beyond the 2-min watermark: lands in file 40, event time back at 10:37:30.
    rows.append(Trade(40, START + timedelta(minutes=37, seconds=30, milliseconds=500), "ETHUSDT", 3405.0, 0.2500, "sell", "ETHUSDT-9000100"))

    return rows


# --------------------------------------------------------------------------- #
# Silver derivation — 1-min tumbling OHLCV over the CLEAN bronze rows
# --------------------------------------------------------------------------- #
@dataclass
class _Agg:
    trades: list[Trade] = field(default_factory=list)


def derive_silver(clean: list[Trade]) -> list[dict]:
    """Aggregate clean bronze rows into 1-min tumbling OHLCV per (window, symbol).

    Late rows are placed in their *true event-time* window (we group by event_dt),
    which is exactly the oracle WP2's watermarked stream must reproduce.
    """
    buckets: dict[tuple[datetime, str], _Agg] = {}
    for t in clean:
        window_start = t.event_dt.replace(second=0, microsecond=0)
        buckets.setdefault((window_start, t.symbol), _Agg()).trades.append(t)

    rows: list[dict] = []
    for (window_start, symbol), agg in sorted(buckets.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        # open = first by event time, close = last; tie-break on trade_id for stability.
        ordered = sorted(agg.trades, key=lambda t: (t.event_dt, t.trade_id))
        prices = [t.price for t in ordered]
        total_qty = sum(t.qty for t in ordered)
        buy_qty = sum(t.qty for t in ordered if t.side == "buy")
        rows.append(
            {
                "window_start": _iso_window(window_start),
                "window_end": _iso_window(window_start + timedelta(minutes=1)),
                "symbol": symbol,
                "open": ordered[0].price,
                "high": round(max(prices), PRICE_DP),
                "low": round(min(prices), PRICE_DP),
                "close": ordered[-1].price,
                "volume": round(total_qty, QTY_DP),
                "trade_count": len(ordered),
                "taker_buy_ratio": round(buy_qty / total_qty, RATIO_DP) if total_qty else 0.0,
            }
        )
    return rows


# --------------------------------------------------------------------------- #
# Gold derivation — business signals over the silver windows (WP3 oracle)
# --------------------------------------------------------------------------- #
def derive_gold(silver_rows: list[dict]) -> list[dict]:
    """Derive gold market-pulse signals from the silver windows.

    Independent re-implementation of ``src.gold.compute_market_pulse`` (the generator
    deliberately reimplements each layer so the committed fixture is an *independent*
    oracle the twin must match). The rolling average is the trailing
    ``ROLLING_WINDOW_ROWS`` windows per symbol, including the current one, so it is
    always defined; ``volume_spike`` compares volume to ``VOLUME_SPIKE_FACTOR``× it.
    """
    by_symbol: dict[str, list[dict]] = {}
    for r in silver_rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    rows: list[dict] = []
    for symbol, srows in by_symbol.items():
        ordered = sorted(srows, key=lambda r: r["window_start"])
        for i, r in enumerate(ordered):
            lo = max(0, i - (ROLLING_WINDOW_ROWS - 1))
            trailing = ordered[lo : i + 1]
            rolling = sum(t["volume"] for t in trailing) / len(trailing)

            if r["close"] > r["open"]:
                direction = "up"
            elif r["close"] < r["open"]:
                direction = "down"
            else:
                direction = "flat"

            if direction == "up":
                momentum = "strong_bullish" if r["taker_buy_ratio"] >= BULL_RATIO else "bullish"
            elif direction == "down":
                momentum = "strong_bearish" if r["taker_buy_ratio"] <= BEAR_RATIO else "bearish"
            else:
                momentum = "neutral"

            rows.append(
                {
                    "window_start": r["window_start"],
                    "symbol": symbol,
                    "candle_direction": direction,
                    "momentum_signal": momentum,
                    "volume_spike": r["volume"] > VOLUME_SPIKE_FACTOR * rolling,
                    "volatility": round((r["high"] - r["low"]) / r["open"], VOLATILITY_DP) if r["open"] else 0.0,
                    "rolling_vol_30m": round(rolling, ROLLING_DP),
                    "generated_at": GENERATED_AT,
                }
            )

    rows.sort(key=lambda d: (d["window_start"], d["symbol"]))
    return rows


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
def _write_raw(trades: list[Trade]) -> int:
    raw_dir = OUT / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for f in raw_dir.glob("*.json"):
        f.unlink()  # clean slate so removed rows never linger (determinism)

    by_minute: dict[int, list[Trade]] = {}
    for t in trades:
        by_minute.setdefault(t.file_minute, []).append(t)

    for minute in range(MINUTES):
        rows = by_minute.get(minute, [])
        path = raw_dir / _raw_filename(minute)
        with path.open("w", encoding="utf-8", newline="\n") as fh:
            for t in rows:
                # Raw carries everything (incl. dirty rows with nulls); quarantine
                # happens downstream in bronze. Clean field names = option A.
                rec = {
                    "event_ts": t.event_ts,
                    "symbol": t.symbol,
                    "price": t.price,
                    "qty": t.qty,
                    "side": t.side,
                    "trade_id": t.trade_id,
                }
                fh.write(json.dumps(rec) + "\n")
    return MINUTES


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # newline="" is required by the csv module; lineterminator="\n" forces LF so the
    # output matches the LF-normalised git blob on every platform (the determinism
    # test would otherwise fail on Linux while passing on Windows). See issue #10.
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def _write_bronze(clean: list[Trade], dirty: list[Trade]) -> None:
    bronze_dir = OUT / "bronze"
    header = ["event_ts", "symbol", "price", "qty", "side", "trade_id", "ingest_ts", "_source_file"]
    rows = [
        [
            t.event_ts, t.symbol, t.price, t.qty, t.side, t.trade_id,
            INGEST_TS, f"{VOLUME_PATH}/{_raw_filename(t.file_minute)}",
        ]
        for t in clean
    ]
    _write_csv(bronze_dir / "trades.csv", header, rows)

    q_header = header + ["_quarantine_reason"]
    q_rows = [
        [
            t.event_ts, t.symbol, t.price, t.qty, t.side, t.trade_id,
            INGEST_TS, f"{VOLUME_PATH}/{_raw_filename(t.file_minute)}", t.reason,
        ]
        for t in dirty
    ]
    _write_csv(bronze_dir / "trades_quarantine.csv", q_header, q_rows)


def _write_silver(silver_rows: list[dict]) -> None:
    header = [
        "window_start", "window_end", "symbol", "open", "high", "low", "close",
        "volume", "trade_count", "taker_buy_ratio",
    ]
    rows = [[r[c] for c in header] for r in silver_rows]
    _write_csv(OUT / "silver" / "trades_1min.csv", header, rows)


def _write_gold(gold_rows: list[dict]) -> None:
    header = [
        "window_start", "symbol", "candle_direction", "momentum_signal",
        "volume_spike", "volatility", "rolling_vol_30m", "generated_at",
    ]
    rows = [
        [
            # Render the boolean lowercase so it round-trips through the contract's
            # BOOLEAN dtype check (and reads like Spark's CSV booleans).
            str(r[c]).lower() if c == "volume_spike" else r[c]
            for c in header
        ]
        for r in gold_rows
    ]
    _write_csv(OUT / "gold" / "market_pulse.csv", header, rows)


def main() -> None:
    trades = _generate_trades()
    clean = [t for t in trades if not t.is_dirty]
    dirty = [t for t in trades if t.is_dirty]

    n_files = _write_raw(trades)
    _write_bronze(clean, dirty)
    silver_rows = derive_silver(clean)
    _write_silver(silver_rows)
    gold_rows = derive_gold(silver_rows)
    _write_gold(gold_rows)

    print(
        f"Generated {len(trades)} raw trades across {n_files} files "
        f"({len(clean)} clean -> bronze, {len(dirty)} dirty -> quarantine, "
        f"{len(silver_rows)} silver windows, {len(gold_rows)} gold rows)."
    )


if __name__ == "__main__":
    main()
