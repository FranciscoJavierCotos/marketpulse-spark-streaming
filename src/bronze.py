"""Shared bronze-ingestion logic (WP1).

The one place the **quarantine routing rule** lives: a raw trade is sent to
``bronze.trades_quarantine`` (instead of ``bronze.trades``) when any *key* column
is null. ``notebooks/01_bronze.py`` runs this rule on Databricks via a Spark
expression; the pure-Python twin below lets ``pytest`` assert the rule reproduces
the committed fixture split without needing a local Spark (CI stays pure-Python).

Keeping both forms here — one constant, two renderings — means the rule has a
single definition: change ``KEY_COLUMNS`` and both the notebook and the tests move
together.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# Key columns whose null-ness routes a raw row to quarantine instead of bronze
# (matches CONTRACTS.md: "Null-key rows (event_ts/symbol/trade_id null) route to
# bronze.trades_quarantine"). Order fixes the precedence of the recorded
# _quarantine_reason when more than one key is null — and matches the order the
# committed fixtures use.
KEY_COLUMNS: tuple[str, ...] = ("event_ts", "symbol", "trade_id")


def quarantine_reason(record: Mapping[str, Any]) -> str | None:
    """Return the quarantine reason for a raw trade, or ``None`` if it's clean.

    A row is dirty when the first key column (in ``KEY_COLUMNS`` order) that is
    null/empty determines the reason string, e.g. ``"null event_ts"``. This is the
    pure-Python twin of :func:`quarantine_reason_column`; the two must agree.
    """
    for col in KEY_COLUMNS:
        value = record.get(col)
        if value is None or value == "":
            return f"null {col}"
    return None


def quarantine_reason_column():  # -> pyspark.sql.Column
    """Build the Spark ``Column`` form of :func:`quarantine_reason`.

    First null key in ``KEY_COLUMNS`` order wins (chained ``when`` gives
    first-match precedence); a clean row yields ``NULL``. PySpark is imported
    lazily so importing this module under pure-Python CI never needs Spark.
    """
    from pyspark.sql import functions as F

    head, *tail = KEY_COLUMNS
    expr = F.when(F.col(head).isNull(), F.lit(f"null {head}"))
    for col in tail:
        expr = expr.when(F.col(col).isNull(), F.lit(f"null {col}"))
    return expr.otherwise(F.lit(None).cast("string"))
