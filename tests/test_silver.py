"""Silver aggregation tests (WP2).

Two layers, both pure-Python so CI needs no Spark:

1. **Correctness oracle** — `src.silver.aggregate_ohlcv` (the pure twin of the Spark
   `to_silver` transform) must reproduce the committed `silver/trades_1min.csv` from
   `bronze/trades.csv`, and must be idempotent under duplicated input (the dedup +
   MERGE acceptance criteria from issue #3).
2. **Streaming-decision guard** — static checks that `notebooks/02_silver.py` keeps
   the headline streaming decisions (watermark, 1-min window, dedup, `foreachBatch`
   + `MERGE` on the contract key, `Trigger.AvailableNow`). These are exactly the
   choices issue #3 says to "comment every" of, so they must not silently regress.
"""

import csv
import math
from datetime import datetime
from pathlib import Path

from src import silver

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "fixtures"
NOTEBOOK = ROOT / "notebooks" / "02_silver.py"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _parse_ts(value: str) -> datetime:
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if "." in value else "%Y-%m-%dT%H:%M:%SZ"
    return datetime.strptime(value, fmt)


def _bronze_rows() -> list[dict]:
    with (FIX / "bronze" / "trades.csv").open(encoding="utf-8", newline="") as fh:
        return [
            {
                "event_ts": _parse_ts(r["event_ts"]),
                "symbol": r["symbol"],
                "price": float(r["price"]),
                "qty": float(r["qty"]),
                "side": r["side"],
                "trade_id": r["trade_id"],
            }
            for r in csv.DictReader(fh)
        ]


def _silver_fixture() -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    with (FIX / "silver" / "trades_1min.csv").open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            out[(r["window_start"], r["symbol"])] = r
    return out


def _iso_window(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# 1. Correctness oracle — the twin reproduces the committed silver fixture
# --------------------------------------------------------------------------- #
def test_aggregate_ohlcv_matches_silver_fixture():
    got = {(_iso_window(r["window_start"]), r["symbol"]): r for r in silver.aggregate_ohlcv(_bronze_rows())}
    expected = _silver_fixture()
    assert got.keys() == expected.keys()

    for key, exp in expected.items():
        g = got[key]
        assert _iso_window(g["window_end"]) == exp["window_end"], key
        assert g["trade_count"] == int(exp["trade_count"]), key
        for f in ("open", "high", "low", "close", "volume", "taker_buy_ratio"):
            assert math.isclose(g[f], float(exp[f]), rel_tol=0, abs_tol=1e-6), (key, f, g[f], exp[f])


def test_one_row_per_window_symbol():
    rows = silver.aggregate_ohlcv(_bronze_rows())
    keys = [(r["window_start"], r["symbol"]) for r in rows]
    assert len(keys) == len(set(keys)), "aggregate produced duplicate (window_start, symbol) rows"


def test_dedup_makes_aggregation_idempotent():
    """Duplicate input (replayed trades) must not change the aggregate — mirrors the
    stream's `dropDuplicatesWithinWatermark` + idempotent MERGE (issue #3)."""
    base = _bronze_rows()
    duplicated = base + base  # every (symbol, trade_id) appears twice
    assert silver.aggregate_ohlcv(duplicated) == silver.aggregate_ohlcv(base)


def test_late_event_lands_in_its_event_time_window():
    """A late-but-in-watermark trade is bucketed by event time, not arrival — the
    'late events within watermark are absorbed' acceptance criterion."""
    base_min = datetime(2026, 6, 27, 10, 0)
    late_min = datetime(2026, 6, 27, 10, 5)
    rows = [
        {"event_ts": base_min.replace(second=10), "symbol": "BTCUSDT", "price": 100.0,
         "qty": 1.0, "side": "buy", "trade_id": "b-1"},
        # Arrives after the 10:05 trade below but its event time is back in 10:00.
        {"event_ts": base_min.replace(second=30), "symbol": "BTCUSDT", "price": 110.0,
         "qty": 1.0, "side": "sell", "trade_id": "b-2"},
        {"event_ts": late_min.replace(second=5), "symbol": "BTCUSDT", "price": 200.0,
         "qty": 1.0, "side": "buy", "trade_id": "b-3"},
    ]
    by_key = {(_iso_window(r["window_start"]), r["symbol"]): r for r in silver.aggregate_ohlcv(rows)}
    first = by_key[("2026-06-27T10:00:00Z", "BTCUSDT")]
    assert first["trade_count"] == 2          # both 10:00 trades absorbed into one window
    assert first["open"] == 100.0 and first["close"] == 110.0


# --------------------------------------------------------------------------- #
# 2. Streaming-decision guard — the headline decisions stay in the notebook
# --------------------------------------------------------------------------- #
def test_constants_match_contract():
    assert silver.WATERMARK_DELAY == "2 minutes"   # CONTRACTS.md tolerance
    assert silver.WINDOW_DURATION == "1 minute"    # 1-min tumbling windows
    assert silver.DEDUP_KEYS == ("symbol", "trade_id")


def test_notebook_keeps_streaming_decisions():
    src = NOTEBOOK.read_text(encoding="utf-8")
    required = [
        "readStream",                       # bronze read as a stream
        "to_silver",                        # shared watermark+window+dedup transform
        "foreachBatch",                     # idempotent sink
        "MERGE INTO",                       # upsert (not append)
        "availableNow=True",                # never an always-on stream
        'outputMode("update")',             # re-emit changed windows for the MERGE
    ]
    for token in required:
        assert token in src, f"02_silver.py lost its streaming decision: {token!r}"


def test_notebook_merges_on_contract_key():
    src = NOTEBOOK.read_text(encoding="utf-8")
    assert 'MERGE_KEYS = ["window_start", "symbol"]' in src, "MERGE must key on the contract (window_start, symbol)"
