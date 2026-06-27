"""Shared gold-signal logic (WP3).

The one place the **business signal semantics** live. ``notebooks/03_gold.py`` runs
them on Databricks as a Spark transform (:func:`to_gold`, window-function over
``silver.trades_1min``); the pure-Python twin below (:func:`compute_market_pulse`)
lets ``pytest`` assert those exact semantics reproduce the committed gold fixture
from the silver fixture — without needing a local Spark (CI stays pure-Python).

Keeping both forms here — one set of constants, two renderings — means each signal
has a single definition: change a threshold and the notebook and the tests move
together. This mirrors :mod:`src.bronze` and :mod:`src.silver`'s pure/Spark twins.

Signals (per (window_start, symbol) row of silver), see ``CONTRACTS.md``:

* **candle_direction** — ``up`` / ``down`` / ``flat`` from ``close`` vs ``open``.
* **momentum_signal** — CASE on price ``candle_direction`` *and* order-flow
  ``taker_buy_ratio``: a rising candle confirmed by buy-dominant flow is the
  strongest bullish read, the mirror for bearish; a flat candle is ``neutral``.
* **volatility** — ``(high - low) / open``, the intra-minute range.
* **rolling_vol_30m** — trailing 30-window (≈30-min) average ``volume`` per symbol,
  *including* the current window so it is always defined (no null edge case).
* **volume_spike** — ``volume > 2 × rolling_vol_30m``; the injected spike minute in
  the fixture (plan R4) gives this a visible positive case.

The 30-min average is computed over the trailing **30 rows** ordered by
``window_start`` per symbol. Silver emits exactly one row per (window_start, symbol)
per minute, so 30 rows ≈ 30 minutes; a row-count window is deterministic and trivial
to mirror in pure Python (a time-range window would behave the same on the
contiguous fixture but is fiddlier to reproduce byte-for-byte).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

# --- signal knobs (single definition, shared by notebook + tests) ----------- #
# Order-flow thresholds on taker_buy_ratio that gate the *strong* momentum reads.
BULL_RATIO = 0.6   # ratio ≥ this + rising candle ⇒ strong_bullish
BEAR_RATIO = 0.4   # ratio ≤ this + falling candle ⇒ strong_bearish
# A window is a volume spike when its volume exceeds this multiple of the rolling avg.
VOLUME_SPIKE_FACTOR = 2.0
# Trailing window length for the rolling average, in rows (= minutes on silver).
ROLLING_WINDOW_ROWS = 30

# Valid momentum_signal enum — the gold assertion + tests check membership.
MOMENTUM_SIGNALS: tuple[str, ...] = (
    "strong_bullish",
    "bullish",
    "neutral",
    "bearish",
    "strong_bearish",
)
CANDLE_DIRECTIONS: tuple[str, ...] = ("up", "down", "flat")

# Rounding precision — kept identical to the fixture generator's so the Spark stream
# and the pure twin both reproduce the committed gold fixture exactly.
VOLATILITY_DP = 6
ROLLING_DP = 6


# --------------------------------------------------------------------------- #
# Pure scalar signal definitions — the single source both renderings encode
# --------------------------------------------------------------------------- #
def candle_direction(open_: float, close: float) -> str:
    """``up`` if the candle rose, ``down`` if it fell, ``flat`` if unchanged."""
    if close > open_:
        return "up"
    if close < open_:
        return "down"
    return "flat"


def momentum_signal(direction: str, taker_buy_ratio: float) -> str:
    """Combine price ``direction`` with order-flow ``taker_buy_ratio`` into a signal.

    A rising candle is *strong_bullish* only when buyers also dominate the flow
    (ratio ≥ :data:`BULL_RATIO`), otherwise a plain *bullish*; the mirror applies to
    falling candles and :data:`BEAR_RATIO`. A flat candle is *neutral* regardless of
    flow. Result is always in :data:`MOMENTUM_SIGNALS`.
    """
    if direction == "up":
        return "strong_bullish" if taker_buy_ratio >= BULL_RATIO else "bullish"
    if direction == "down":
        return "strong_bearish" if taker_buy_ratio <= BEAR_RATIO else "bearish"
    return "neutral"


def volatility(open_: float, high: float, low: float) -> float:
    """Intra-window range as a fraction of the open: ``(high - low) / open``."""
    return round((high - low) / open_, VOLATILITY_DP) if open_ else 0.0


# --------------------------------------------------------------------------- #
# Pure-Python twin — the correctness oracle ``pytest`` runs in CI
# --------------------------------------------------------------------------- #
def compute_market_pulse(rows: Iterable[Mapping[str, Any]]) -> list[dict]:
    """Derive gold ``market_pulse`` signals from silver ``trades_1min`` rows.

    Pure-Python twin of :func:`to_gold`; the two must agree. Each input row is a
    mapping with at least ``window_start`` (sortable — :class:`datetime.datetime` or
    ISO string), ``symbol``, ``open``, ``high``, ``low``, ``close``, ``volume`` and
    ``taker_buy_ratio``.

    Returns one dict per (``window_start``, ``symbol``), sorted by that key, with the
    contract's gold columns (no ``generated_at`` — that wall-clock stamp is added
    only in the Spark sink and is intentionally non-deterministic). The rolling
    average is the trailing :data:`ROLLING_WINDOW_ROWS` windows *per symbol*,
    including the current one, so re-running over the same input is idempotent.
    """
    # Group by symbol so each rolling window is per-symbol, then order by window_start.
    by_symbol: dict[Any, list[Mapping[str, Any]]] = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    out: list[dict] = []
    for symbol, srows in by_symbol.items():
        ordered = sorted(srows, key=lambda r: r["window_start"])
        for i, r in enumerate(ordered):
            # Trailing 30-row average including the current row (i-29 … i).
            lo = max(0, i - (ROLLING_WINDOW_ROWS - 1))
            trailing = ordered[lo : i + 1]
            rolling = sum(t["volume"] for t in trailing) / len(trailing)

            direction = candle_direction(r["open"], r["close"])
            out.append(
                {
                    "window_start": r["window_start"],
                    "symbol": symbol,
                    "candle_direction": direction,
                    "momentum_signal": momentum_signal(direction, r["taker_buy_ratio"]),
                    "volume_spike": r["volume"] > VOLUME_SPIKE_FACTOR * rolling,
                    "volatility": volatility(r["open"], r["high"], r["low"]),
                    "rolling_vol_30m": round(rolling, ROLLING_DP),
                }
            )

    out.sort(key=lambda d: (d["window_start"], d["symbol"]))
    return out


# --------------------------------------------------------------------------- #
# Spark form — what the notebook runs on Databricks serverless
# --------------------------------------------------------------------------- #
def to_gold(silver_df):  # -> pyspark.sql.DataFrame
    """Transform a (batch) ``silver.trades_1min`` DataFrame into the gold shape.

    The rolling average is a **window function** (``avg`` over a per-symbol,
    ``window_start``-ordered trailing 30-row frame) — the headline Spark-SQL move of
    WP3. Everything else is per-row CASE/arithmetic. ``generated_at`` is stamped here
    (non-deterministic, so the pure twin omits it). PySpark is imported lazily so this
    module imports under pure-Python CI without Spark.

    Note: gold reads silver as a **batch** snapshot, not a stream — a rolling window
    function needs to see neighbouring rows, which a streaming aggregation's bounded
    state cannot offer. The notebook drives the incremental/idempotent story with a
    bounded recompute + MERGE (see ``03_gold.py``), not streaming state.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Trailing 30 rows per symbol, ordered by window_start, including the current row
    # — the row-count twin of compute_market_pulse's slice.
    roll_window = (
        Window.partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-(ROLLING_WINDOW_ROWS - 1), 0)
    )
    rolling_vol = F.avg("volume").over(roll_window)

    direction = (
        F.when(F.col("close") > F.col("open"), F.lit("up"))
        .when(F.col("close") < F.col("open"), F.lit("down"))
        .otherwise(F.lit("flat"))
    )

    momentum = (
        F.when(
            direction == "up",
            F.when(F.col("taker_buy_ratio") >= BULL_RATIO, F.lit("strong_bullish")).otherwise(F.lit("bullish")),
        )
        .when(
            direction == "down",
            F.when(F.col("taker_buy_ratio") <= BEAR_RATIO, F.lit("strong_bearish")).otherwise(F.lit("bearish")),
        )
        .otherwise(F.lit("neutral"))
    )

    return (
        silver_df.select(
            F.col("window_start"),
            F.col("symbol"),
            direction.alias("candle_direction"),
            momentum.alias("momentum_signal"),
            (F.col("volume") > F.lit(VOLUME_SPIKE_FACTOR) * rolling_vol).alias("volume_spike"),
            F.round((F.col("high") - F.col("low")) / F.col("open"), VOLATILITY_DP).alias("volatility"),
            F.round(rolling_vol, ROLLING_DP).alias("rolling_vol_30m"),
            F.current_timestamp().alias("generated_at"),
        )
    )
