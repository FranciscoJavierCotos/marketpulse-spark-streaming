"""Shared silver-aggregation logic (WP2).

The one place the **1-minute OHLCV windowing semantics** live. `notebooks/02_silver.py`
runs them on Databricks as a watermarked Spark Structured Streaming aggregation
(:func:`to_silver`); the pure-Python twin below (:func:`aggregate_ohlcv`) lets
``pytest`` assert those exact semantics reproduce the committed silver fixture from
the bronze fixture — without needing a local Spark (CI stays pure-Python).

Keeping both forms here — one set of constants, two renderings — means the
aggregation has a single definition: change a rule and the notebook and the tests
move together. This mirrors :mod:`src.bronze`'s pure/Spark twin pattern.

Streaming decisions encoded here (see the notebook for the full prose):

* **Watermark** ``event_ts`` / ``2 minutes`` (:data:`WATERMARK_DELAY`) — bounds the
  state store so finalised windows are evicted, and tolerates up to 2 min of
  late / out-of-order arrival. Matches ``CONTRACTS.md``.
* **Tumbling 1-minute window** (:data:`WINDOW_DURATION`) keyed by ``symbol``.
* **Watermark-bounded dedup** on (:data:`DEDUP_KEYS`) so a replayed trade never
  double-counts — the row-level half of WP2's idempotency story (the MERGE in the
  notebook is the window-level half).
* ``open`` / ``close`` are first / last **by event time** (tie-broken on
  ``trade_id`` for determinism), not by arrival order.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any

# --- streaming knobs (single definition, shared by notebook + tests) --------- #
WATERMARK_COLUMN = "event_ts"
WATERMARK_DELAY = "2 minutes"  # CONTRACTS.md tolerance; bounds state + late data
WINDOW_DURATION = "1 minute"   # 1-min tumbling OHLCV windows
GROUP_KEY = "symbol"
# A trade is uniquely identified by (symbol, trade_id); dedup on it so re-ingesting
# the same trade within the watermark never inflates volume / counts.
DEDUP_KEYS: tuple[str, ...] = ("symbol", "trade_id")

# Rounding precision — identical to the fixture generator's, so the Spark stream and
# the pure twin both reproduce the committed silver fixture exactly.
PRICE_DP = 2
QTY_DP = 6
RATIO_DP = 6


# --------------------------------------------------------------------------- #
# Pure-Python twin — the correctness oracle ``pytest`` runs in CI
# --------------------------------------------------------------------------- #
def aggregate_ohlcv(rows: Iterable[Mapping[str, Any]]) -> list[dict]:
    """Aggregate clean bronze trades into 1-min tumbling OHLCV per (window, symbol).

    Pure-Python twin of :func:`to_silver`; the two must agree. Each input row is a
    mapping with ``event_ts`` (:class:`datetime.datetime`), ``symbol`` (str),
    ``price`` (float), ``qty`` (float), ``side`` (str) and ``trade_id`` (str).

    Returns one dict per (``window_start``, ``symbol``), sorted by that key, with
    ``window_start`` / ``window_end`` as :class:`datetime.datetime` and the contract's
    OHLCV columns. Duplicate (``symbol``, ``trade_id``) rows are dropped (first wins),
    mirroring the stream's watermark-bounded dedup, so the result is idempotent under
    replayed input.
    """
    deduped = _dedup(rows)

    buckets: dict[tuple[datetime, str], list[Mapping[str, Any]]] = {}
    for t in deduped:
        # window_start = event time truncated to the minute (tumbling 1-min window).
        window_start = t["event_ts"].replace(second=0, microsecond=0)
        buckets.setdefault((window_start, t["symbol"]), []).append(t)

    out: list[dict] = []
    for (window_start, symbol), trades in sorted(buckets.items(), key=lambda kv: kv[0]):
        # open = first by event time, close = last; tie-break on trade_id so the
        # result is deterministic regardless of arrival order (matches to_silver's
        # min/max-of-struct trick).
        ordered = sorted(trades, key=lambda t: (t["event_ts"], t["trade_id"]))
        prices = [t["price"] for t in ordered]
        total_qty = sum(t["qty"] for t in ordered)
        buy_qty = sum(t["qty"] for t in ordered if t["side"] == "buy")
        out.append(
            {
                "window_start": window_start,
                "window_end": window_start + timedelta(minutes=1),
                "symbol": symbol,
                "open": round(ordered[0]["price"], PRICE_DP),
                "high": round(max(prices), PRICE_DP),
                "low": round(min(prices), PRICE_DP),
                "close": round(ordered[-1]["price"], PRICE_DP),
                "volume": round(total_qty, QTY_DP),
                "trade_count": len(ordered),
                "taker_buy_ratio": round(buy_qty / total_qty, RATIO_DP) if total_qty else 0.0,
            }
        )
    return out


def _dedup(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Drop duplicate rows by :data:`DEDUP_KEYS`, keeping the first occurrence."""
    seen: set[tuple] = set()
    kept: list[Mapping[str, Any]] = []
    for r in rows:
        key = tuple(r[k] for k in DEDUP_KEYS)
        if key not in seen:
            seen.add(key)
            kept.append(r)
    return kept


# --------------------------------------------------------------------------- #
# Spark form — what the notebook runs on Databricks serverless
# --------------------------------------------------------------------------- #
def silver_aggregations():  # -> list[pyspark.sql.Column]
    """Build the OHLCV aggregate ``Column`` expressions (Spark twin of the maths above).

    ``open`` / ``close`` must be the first / last price **by event time**, but a
    streaming ``groupBy`` aggregation has no inherent ordering — so we encode the
    ordering into a struct and take ``min`` / ``max`` of it: the struct sorts by
    ``event_ts`` then ``trade_id`` (the deterministic tie-break), and we read back its
    ``price`` field. PySpark is imported lazily so this module imports under
    pure-Python CI without Spark.
    """
    from pyspark.sql import functions as F

    open_price = F.min(F.struct("event_ts", "trade_id", "price")).getField("price")
    close_price = F.max(F.struct("event_ts", "trade_id", "price")).getField("price")
    return [
        F.round(open_price, PRICE_DP).alias("open"),
        F.round(F.max("price"), PRICE_DP).alias("high"),
        F.round(F.min("price"), PRICE_DP).alias("low"),
        F.round(close_price, PRICE_DP).alias("close"),
        F.round(F.sum("qty"), QTY_DP).alias("volume"),
        F.count(F.lit(1)).cast("long").alias("trade_count"),
        F.round(
            F.sum(F.when(F.col("side") == "buy", F.col("qty")).otherwise(F.lit(0.0)))
            / F.sum("qty"),
            RATIO_DP,
        ).alias("taker_buy_ratio"),
    ]


def to_silver(bronze_stream):  # -> pyspark.sql.DataFrame
    """Transform a streaming ``bronze.trades`` DataFrame into the silver OHLCV shape.

    The watermarked, windowed, deduplicated aggregation — the stateful heart of WP2.
    Returns a streaming DataFrame matching the ``silver.trades_1min`` contract; the
    caller (the notebook) sinks it idempotently via ``foreachBatch`` + ``MERGE``.
    """
    from pyspark.sql import functions as F

    return (
        bronze_stream
        # Watermark first: defines how late an event may arrive (2 min) and lets
        # Spark evict finalised window state instead of growing it unbounded.
        .withWatermark(WATERMARK_COLUMN, WATERMARK_DELAY)
        # Watermark-bounded dedup: a replayed (symbol, trade_id) within the watermark
        # is dropped, so volume/counts never double-count on re-ingest (idempotency).
        .dropDuplicatesWithinWatermark(list(DEDUP_KEYS))
        # 1-min tumbling windows per symbol — the stateful aggregation.
        .groupBy(F.window(WATERMARK_COLUMN, WINDOW_DURATION), GROUP_KEY)
        .agg(*silver_aggregations())
        # Flatten the window struct to the contract's window_start / window_end and
        # fix column order to match silver.trades_1min.
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "taker_buy_ratio",
        )
    )
