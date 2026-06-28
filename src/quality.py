"""Reusable data-quality expectation helpers (WP5).

The one place the **expectation semantics** live. Any layer (WP1 bronze, WP2
silver, WP3 gold) can guard a batch with a *one-line* call inside its
``foreachBatch`` and have every violation recorded to ``ops.dq_failures`` — bad
rows are **routed, never silently dropped** (CLAUDE.md code conventions).

As with :mod:`src.bronze` / :mod:`src.silver` / :mod:`src.gold`, each expectation
has a **single definition rendered twice**: a pure-Python predicate (the CI oracle
``pytest`` runs without Spark) and a lazily-built Spark ``Column`` the notebooks
evaluate on Databricks serverless. The two must agree, so a threshold change moves
the notebook and the tests together.

An :class:`Expectation` answers one yes/no question of a row ("is ``price``
positive?", "is ``side`` a valid enum?") and carries a :data:`severity`:

* ``warn``  — record the failure, keep the row (visibility only).
* ``drop``  — record the failure *and* filter the offending rows out of the batch
  (the quarantine-style route for silver/gold inputs).
* ``fail``  — record the failure; the caller may then raise to abort the run
  (a contract-breaking violation that should stop the pipeline).

The recorded row matches the frozen ``ops.dq_failures`` contract in
``CONTRACTS.md`` (``check_ts``/``layer``/``table_name``/``expectation``/
``severity``/``failed_count``/``sample``/``run_id``).

Great Expectations (``requirements-dq.txt``) remains the heavier framework option
for a full suite; these helpers are the lightweight, dependency-free core the
streaming ``foreachBatch`` paths use directly.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

# --- severity levels (single definition, shared by notebooks + tests) ------- #
SEVERITY_WARN = "warn"   # record only — the row stays in the batch
SEVERITY_DROP = "drop"   # record + filter the offending rows out of the batch
SEVERITY_FAIL = "fail"   # record — caller may raise to abort the run
SEVERITIES: tuple[str, ...] = (SEVERITY_WARN, SEVERITY_DROP, SEVERITY_FAIL)

# The frozen ops.dq_failures columns (CONTRACTS.md). check_ts / run_id are stamped
# at write time; the rest come from the expectation result. Kept here as the single
# Python mirror of the contract so the writer and the contract test share one list.
DQ_FAILURES_COLUMNS: tuple[str, ...] = (
    "check_ts",
    "layer",
    "table_name",
    "expectation",
    "severity",
    "failed_count",
    "sample",
    "run_id",
)

# How many offending values to capture into the JSON ``sample`` column by default.
DEFAULT_SAMPLE_SIZE = 5


# --------------------------------------------------------------------------- #
# The expectation type — one definition, two renderings
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Expectation:
    """A single row-level data-quality rule.

    ``predicate(record) -> bool`` is the pure-Python twin (``True`` = the row
    *passes*); ``condition()`` returns the equivalent Spark ``Column`` (``True`` =
    passes), built lazily so importing this module never needs Spark. ``columns``
    records which fields the rule reads — used to build a compact JSON ``sample`` of
    offending values. The two renderings must agree on every row.
    """

    name: str
    severity: str
    columns: tuple[str, ...]
    predicate: Callable[[Mapping[str, Any]], bool]
    condition: Callable[[], Any]  # -> pyspark.sql.Column

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"unknown severity {self.severity!r}; expected one of {SEVERITIES}")


@dataclass(frozen=True)
class ExpectationResult:
    """Outcome of evaluating one :class:`Expectation` over a batch of rows.

    Maps onto the ``ops.dq_failures`` contract minus the runtime ``check_ts`` /
    ``run_id`` stamps (those are added by the writer). ``failed_count == 0`` means
    the expectation held — only failing results are persisted.
    """

    expectation: str
    severity: str
    failed_count: int
    sample: str  # JSON array of offending values


# --------------------------------------------------------------------------- #
# Expectation constructors — the one-line helpers WP1–WP3 call
# --------------------------------------------------------------------------- #
def _is_missing(value: Any) -> bool:
    """A value is missing if it is null or an empty string (CSV/landing nulls)."""
    return value is None or value == ""


def not_null(*columns: str, severity: str = SEVERITY_FAIL) -> Expectation:
    """Expect every named key column to be non-null (and non-empty).

    Mirrors the bronze quarantine intent for *keys*: a null key is a hard
    contract violation, so the default severity is ``fail``.
    """
    cols = tuple(columns)
    name = f"not_null({', '.join(cols)})"

    def predicate(record: Mapping[str, Any]) -> bool:
        return all(not _is_missing(record.get(c)) for c in cols)

    def condition():  # -> pyspark.sql.Column
        from pyspark.sql import functions as F

        cond = F.lit(True)
        for c in cols:
            # Treat null and empty-string alike, matching the pure twin.
            cond = cond & F.col(c).isNotNull() & (F.col(c).cast("string") != F.lit(""))
        return cond

    return Expectation(name, severity, cols, predicate, condition)


def positive(column: str, *, severity: str = SEVERITY_DROP) -> Expectation:
    """Expect a numeric column to be strictly ``> 0`` (e.g. ``price`` / ``qty``).

    Non-positive prices/quantities are nonsensical trades, so the default route is
    ``drop`` — filter them out of the batch and record the loss.
    """
    name = f"positive({column})"

    def predicate(record: Mapping[str, Any]) -> bool:
        value = record.get(column)
        return value is not None and value != "" and float(value) > 0

    def condition():  # -> pyspark.sql.Column
        from pyspark.sql import functions as F

        return F.col(column).isNotNull() & (F.col(column) > F.lit(0))

    return Expectation(name, severity, (column,), predicate, condition)


def in_range(
    column: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    severity: str = SEVERITY_WARN,
) -> Expectation:
    """Expect a numeric column to fall within ``[minimum, maximum]`` (inclusive).

    Either bound may be ``None`` (one-sided). Null values fail the check (a value
    that should be in range cannot be absent).
    """
    bounds = ",".join("" if b is None else repr(b) for b in (minimum, maximum))
    name = f"in_range({column},[{bounds}])"

    def predicate(record: Mapping[str, Any]) -> bool:
        value = record.get(column)
        if value is None or value == "":
            return False
        v = float(value)
        if minimum is not None and v < minimum:
            return False
        if maximum is not None and v > maximum:
            return False
        return True

    def condition():  # -> pyspark.sql.Column
        from pyspark.sql import functions as F

        cond = F.col(column).isNotNull()
        if minimum is not None:
            cond = cond & (F.col(column) >= F.lit(minimum))
        if maximum is not None:
            cond = cond & (F.col(column) <= F.lit(maximum))
        return cond

    return Expectation(name, severity, (column,), predicate, condition)


def is_in(column: str, allowed: Sequence[Any], *, severity: str = SEVERITY_DROP) -> Expectation:
    """Expect a column's value to be one of ``allowed`` (an enum check).

    Used for ``side`` (``buy``/``sell``) or ``momentum_signal``. Null fails the
    check. Default ``drop`` keeps invalid-enum rows out of downstream layers.
    """
    allowed_tuple = tuple(allowed)
    allowed_set = set(allowed_tuple)
    name = f"is_in({column},{{{', '.join(map(str, allowed_tuple))}}})"

    def predicate(record: Mapping[str, Any]) -> bool:
        return record.get(column) in allowed_set

    def condition():  # -> pyspark.sql.Column
        from pyspark.sql import functions as F

        return F.col(column).isin(list(allowed_tuple))

    return Expectation(name, severity, (column,), predicate, condition)


# --------------------------------------------------------------------------- #
# Pure-Python twin — the correctness oracle ``pytest`` runs in CI
# --------------------------------------------------------------------------- #
def evaluate(
    records: Iterable[Mapping[str, Any]],
    expectations: Sequence[Expectation],
    *,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> list[ExpectationResult]:
    """Evaluate every expectation over ``records``; return one result per rule.

    Pure-Python twin of :func:`apply_expectations`'s counting/sampling. Each result
    carries the number of failing rows and a JSON ``sample`` of the first
    ``sample_size`` offending column-projections (the rule's :attr:`Expectation.columns`).
    ``failed_count == 0`` results are returned too so callers can assert a clean
    batch; the Spark writer persists only the failing ones.
    """
    rows = list(records)
    results: list[ExpectationResult] = []
    for exp in expectations:
        failures = [r for r in rows if not exp.predicate(r)]
        sample = [{c: r.get(c) for c in exp.columns} for r in failures[:sample_size]]
        results.append(
            ExpectationResult(
                expectation=exp.name,
                severity=exp.severity,
                failed_count=len(failures),
                # sort_keys so the JSON sample is deterministic (CI-comparable).
                sample=json.dumps(sample, default=str, sort_keys=True),
            )
        )
    return results


def split_kept(
    records: Iterable[Mapping[str, Any]],
    expectations: Sequence[Expectation],
) -> list[Mapping[str, Any]]:
    """Return the rows that survive all ``drop``-severity expectations.

    Pure-Python twin of the row-filtering :func:`apply_expectations` does for
    ``drop`` rules. ``warn``/``fail`` rules never remove rows here — ``warn`` is
    visibility-only and ``fail`` is the caller's decision to abort.
    """
    drop_rules = [e for e in expectations if e.severity == SEVERITY_DROP]
    return [r for r in records if all(e.predicate(r) for e in drop_rules)]


# --------------------------------------------------------------------------- #
# Spark form — what the notebooks run on Databricks serverless
# --------------------------------------------------------------------------- #
def dq_failures_schema():  # -> pyspark.sql.types.StructType
    """The ``ops.dq_failures`` Spark schema (CONTRACTS.md). Lazy Spark import."""
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("check_ts", TimestampType(), False),
            StructField("layer", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("expectation", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("failed_count", LongType(), False),
            StructField("sample", StringType(), True),
            StructField("run_id", StringType(), True),
        ]
    )


def create_dq_failures_table(spark, table: str) -> None:
    """Create the append-only ``ops.dq_failures`` Delta table if absent (idempotent)."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            check_ts     TIMESTAMP,
            layer        STRING,
            table_name   STRING,
            expectation  STRING,
            severity     STRING,
            failed_count BIGINT,
            sample       STRING,
            run_id       STRING
        ) USING DELTA
        """
    )


def apply_expectations(
    df,  # pyspark.sql.DataFrame
    expectations: Sequence[Expectation],
    *,
    layer: str,
    table_name: str,
    dq_table: str,
    run_id: str | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    raise_on_fail: bool = False,
):  # -> pyspark.sql.DataFrame
    """Evaluate ``expectations`` over a batch ``df``; record failures, return survivors.

    The **one-line call** WP1–WP3 drop into a ``foreachBatch``. For each rule it
    counts the failing rows in Spark and, when there are any, appends a row to
    ``dq_failures`` (``check_ts`` = now, plus a JSON ``sample`` of offending values)
    — so a bad row is *recorded*, never silently dropped. ``drop`` rules also filter
    their offenders out of the returned DataFrame; ``warn`` rules keep all rows; a
    ``fail`` rule with offenders optionally raises (``raise_on_fail``) to abort the
    run. PySpark is imported lazily so this module stays Spark-free under CI.
    """
    from pyspark.sql import functions as F

    spark = df.sparkSession
    create_dq_failures_table(spark, dq_table)

    # No cache(): serverless compute forbids the Spark block cache
    # ([NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE). Each expectation re-scans the
    # batch (count + maybe a sample collect); the plan recomputes per rule, which is
    # correct and acceptable for the small Trigger.AvailableNow batches this runs on.

    failure_records: list[tuple] = []
    kept = df
    failed_hard = False
    for exp in expectations:
        passes = exp.condition()
        failing = df.filter(~passes | passes.isNull())  # null condition == not-passing
        failed_count = failing.count()
        if failed_count == 0:
            continue

        # Compact JSON sample of the offending values (only the rule's columns).
        sample_rows = [
            {c: row[c] for c in exp.columns}
            for row in failing.select(*exp.columns).limit(sample_size).collect()
        ]
        failure_records.append(
            (
                layer,
                table_name,
                exp.name,
                exp.severity,
                int(failed_count),
                json.dumps(sample_rows, default=str, sort_keys=True),
                run_id,
            )
        )
        if exp.severity == SEVERITY_DROP:
            kept = kept.filter(passes)
        elif exp.severity == SEVERITY_FAIL:
            failed_hard = True

    if failure_records:
        failures_df = (
            spark.createDataFrame(
                failure_records,
                schema="layer string, table_name string, expectation string, "
                "severity string, failed_count bigint, sample string, run_id string",
            )
            .withColumn("check_ts", F.current_timestamp())
            .select(*DQ_FAILURES_COLUMNS)  # contract column order
        )
        failures_df.write.format("delta").mode("append").saveAsTable(dq_table)

    if failed_hard and raise_on_fail:
        hard = [r for r in failure_records if r[3] == SEVERITY_FAIL]
        raise DataQualityError(
            f"{len(hard)} fail-severity expectation(s) violated on {table_name}: "
            + ", ".join(r[2] for r in hard)
        )

    return kept


class DataQualityError(RuntimeError):
    """Raised by :func:`apply_expectations` when a ``fail``-severity rule is violated
    and ``raise_on_fail=True`` — a contract-breaking batch that must abort the run."""
