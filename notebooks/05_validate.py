# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Validate (data-test gate)
# MAGIC The **automated test gate at the *end* of the pipeline**, where the output
# MAGIC actually exists. After `bronze → silver → gold` has run, this task asserts the
# MAGIC *data* a run produced is sane and **raises to fail the whole Job run** if not —
# MAGIC so a bad run is loud, never silent.
# MAGIC
# MAGIC This is the data-level complement to CI: `pytest` (`.github/workflows/ci.yml`)
# MAGIC tests the *code* on every PR; this notebook tests the *data* on every run. The
# MAGIC two are orthogonal — green code can still produce stale/empty/inconsistent
# MAGIC tables, which only a post-run data check can catch.
# MAGIC
# MAGIC **Spark runs on Databricks serverless only** — not meant to run locally. It is
# MAGIC the `validate` task of `pipelines/marketpulse_job.json`, depending on
# MAGIC `gold_signals`; the file-arrival trigger fires the Job when the producer lands
# MAGIC new data, so the gate runs itself with no human in the loop.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets / parameters
# MAGIC - `catalog` — Unity Catalog catalog (default `mktpulse`); the Job forwards it.
# MAGIC - `dev_suffix` — isolate a parallel namespace via `Config(dev_suffix=…)`.
# MAGIC - `max_lag_minutes` — tolerated lag of gold behind silver, and the recency
# MAGIC   window for the `fail`-severity DQ check (default `60`). Wide enough that a
# MAGIC   normal incremental run never trips it, tight enough that a stalled pipeline does.

# COMMAND ----------
dbutils.widgets.text("catalog", "mktpulse")
dbutils.widgets.text("dev_suffix", "")
dbutils.widgets.text("max_lag_minutes", "60")

CATALOG = dbutils.widgets.get("catalog") or "mktpulse"
DEV_SUFFIX = dbutils.widgets.get("dev_suffix")
MAX_LAG_MINUTES = int(dbutils.widgets.get("max_lag_minutes"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap — put the repo root on `sys.path` and load `Config`
# MAGIC Same resolution as the other notebooks: in a Databricks Git folder this notebook
# MAGIC lives at `…/notebooks/05_validate`, so the repo root is its parent's parent.

# COMMAND ----------
import os
import sys

_nb_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_repo_root_candidates = [
    os.path.abspath(os.path.join("/Workspace" + _nb_dir, "..")),  # Git folder layout
    os.path.abspath(os.path.join(os.getcwd(), "..")),             # fallback
    os.getcwd(),
]
for _root in _repo_root_candidates:
    if os.path.exists(os.path.join(_root, "src", "config.py")) and _root not in sys.path:
        sys.path.insert(0, _root)
        REPO_ROOT = _root
        break
else:
    REPO_ROOT = os.getcwd()
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

from pyspark.sql import functions as F  # noqa: E402

from src.config import Config  # noqa: E402

cfg = Config(catalog=CATALOG, dev_suffix=DEV_SUFFIX)
print(f"Repo root: {REPO_ROOT}")
print(f"Silver : {cfg.tbl_silver_trades_1min}")
print(f"Gold   : {cfg.tbl_gold_market_pulse}")
print(f"DQ     : {cfg.tbl_dq_failures}")
print(f"max_lag_minutes = {MAX_LAG_MINUTES}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## The checks
# MAGIC Each check appends a `(name, ok, detail)` row. We assert at the end so the log
# MAGIC shows *every* failure, not just the first. Any failure → `AssertionError` →
# MAGIC the task (and the Job run) goes red.
# MAGIC
# MAGIC Checks are written to hold under **both** producer modes — faithful Mode A
# MAGIC replay (historical `event_ts`) and live Mode B / restamp (wall-clock `event_ts`)
# MAGIC — so freshness is expressed as *gold-vs-silver lag*, never wall-clock vs
# MAGIC `event_ts` (which a faithful replay of historical seed data would always fail).
# MAGIC The DQ recency window uses `check_ts`, which is processing-time (wall clock)
# MAGIC regardless of replay mode, so it stays correct either way.

# COMMAND ----------
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str) -> None:
    results.append((name, bool(ok), detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} — {detail}")


silver = spark.table(cfg.tbl_silver_trades_1min)
gold = spark.table(cfg.tbl_gold_market_pulse)

silver_count = silver.count()
gold_count = gold.count()

# 1. Non-empty — a run that produced nothing downstream is a failure to surface.
check("silver_non_empty", silver_count > 0, f"{silver_count} silver windows")
check("gold_non_empty", gold_count > 0, f"{gold_count} gold rows")

# 2. Gold key uniqueness — defends the contract's (window_start, symbol) grain
#    independently of the gold notebook's own MERGE assertion.
distinct_keys = gold.select("window_start", "symbol").distinct().count()
check(
    "gold_keys_unique",
    gold_count == distinct_keys,
    f"{gold_count} rows / {distinct_keys} distinct (window_start, symbol)",
)

# 3. Freshness as gold-vs-silver lag — gold must keep up with silver. We compare the
#    high-watermark windows; gold may trail silver by at most the watermark/lookback
#    horizon (max_lag_minutes), never more (which would mean gold stalled).
silver_hw = silver.agg(F.max("window_start").alias("hw")).collect()[0]["hw"]
gold_hw = gold.agg(F.max("window_start").alias("hw")).collect()[0]["hw"]
if silver_hw is None or gold_hw is None:
    check("gold_keeps_up_with_silver", False, f"missing high-watermark (silver={silver_hw}, gold={gold_hw})")
else:
    lag_minutes = (silver_hw - gold_hw).total_seconds() / 60.0
    check(
        "gold_keeps_up_with_silver",
        lag_minutes <= MAX_LAG_MINUTES,
        f"gold trails silver by {lag_minutes:.1f} min (max {MAX_LAG_MINUTES}); "
        f"silver_hw={silver_hw}, gold_hw={gold_hw}",
    )

# 4. No fail-severity DQ failures recorded by this run. `check_ts` is processing-time,
#    so a `fail` row written in the last max_lag_minutes belongs to the run we just
#    orchestrated. Bad rows are *routed* to ops.dq_failures (never dropped) — a `fail`
#    there means an expectation severe enough to abort tripped, so the gate must fail.
fail_rows = (
    spark.table(cfg.tbl_dq_failures)
    .filter(F.col("severity") == "fail")
    .filter(F.col("check_ts") >= F.current_timestamp() - F.expr(f"INTERVAL {MAX_LAG_MINUTES} MINUTES"))
)
fail_count = fail_rows.count()
check(
    "no_fail_severity_dq",
    fail_count == 0,
    f"{fail_count} fail-severity dq_failures in the last {MAX_LAG_MINUTES} min",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gate — raise if anything failed
# MAGIC One `AssertionError` listing every failed check, so the Job run goes red with a
# MAGIC readable reason. A green run here means: data present, grain intact, gold caught
# MAGIC up to silver, and no abort-level DQ failure — i.e. the pipeline is trustworthy.

# COMMAND ----------
failed = [(name, detail) for name, ok, detail in results if not ok]
if failed:
    lines = "\n".join(f"  - {name}: {detail}" for name, detail in failed)
    raise AssertionError(f"{len(failed)} validation check(s) failed:\n{lines}")

print(f"All {len(results)} validation checks PASSED — pipeline output is trustworthy.")
