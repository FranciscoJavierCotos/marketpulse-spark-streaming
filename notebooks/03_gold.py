# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold business marts (WP3)
# MAGIC Turns the `silver.trades_1min` OHLCV windows into **business-grade signals** in
# MAGIC `gold.market_pulse`: `candle_direction`, `momentum_signal` (price direction ×
# MAGIC order-flow), `volume_spike` (>2× a 30-min rolling average via a **window
# MAGIC function**), and `volatility`. Upserted idempotently via an incremental
# MAGIC **`MERGE`** on `(window_start, symbol)` with a small lookback for late arrivals.
# MAGIC
# MAGIC The signal maths lives in [`src/gold.py`](../src/gold.py) (`to_gold`) — one
# MAGIC definition shared with the pure-Python `pytest` oracle so it's verified in CI
# MAGIC without Spark (mirrors WP1/WP2's twin pattern).
# MAGIC
# MAGIC **Spark runs on Databricks serverless only** — not meant to run locally. Run
# MAGIC `00_setup` first, then `01_bronze` and `02_silver`.
# MAGIC
# MAGIC **Acceptance (issue #4):** signals computed in the DataFrame API; 3 assertions —
# MAGIC unique `(window_start, symbol)`, no negative volume context, valid
# MAGIC `momentum_signal` enum; re-running does not duplicate rows (idempotent MERGE).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets / parameters
# MAGIC - `dev_suffix` — isolate a parallel work package's namespace (`gold_dev_wp3`, …)
# MAGIC   via `Config(dev_suffix=…)`; empty in production.
# MAGIC - `catalog` — Unity Catalog catalog (default `mktpulse`); the WP6 Job passes it
# MAGIC   so the pipeline is parameterised by catalog/schema, not hard-coded.
# MAGIC - `reset` — `"true"` truncates `gold.market_pulse` for a clean full recompute.
# MAGIC   Never implicit — re-runs are otherwise incremental and idempotent.

# COMMAND ----------
dbutils.widgets.text("dev_suffix", "")
dbutils.widgets.text("catalog", "mktpulse")
dbutils.widgets.dropdown("reset", "false", ["false", "true"])

DEV_SUFFIX = dbutils.widgets.get("dev_suffix")
CATALOG = dbutils.widgets.get("catalog") or "mktpulse"
RESET = dbutils.widgets.get("reset") == "true"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap — put the repo root on `sys.path` and load shared modules
# MAGIC Same resolution as the other notebooks: in a Databricks Git folder this notebook
# MAGIC lives at `…/notebooks/03_gold`, so the repo root is its parent's parent.

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

from src.config import Config  # noqa: E402
from src.gold import (  # noqa: E402  — shared signal maths + the enum the assertion checks
    MOMENTUM_SIGNALS,
    ROLLING_WINDOW_ROWS,
    to_gold,
)

cfg = Config(catalog=CATALOG, dev_suffix=DEV_SUFFIX)
print(f"Repo root: {REPO_ROOT}")
print(f"Source : {cfg.tbl_silver_trades_1min}")
print(f"Target : {cfg.tbl_gold_market_pulse}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. (optional) Reset — explicit clean recompute
# MAGIC Gold has no streaming checkpoint (it reads silver as a **batch** — see §2), so a
# MAGIC reset is just a `TRUNCATE`. The MERGE makes a reset unnecessary for correctness;
# MAGIC it only forces a full rebuild instead of the incremental lookback below.

# COMMAND ----------
if RESET:
    print("reset=true → truncating the gold table for a full recompute.")
    spark.sql(f"TRUNCATE TABLE {cfg.tbl_gold_market_pulse}")
else:
    print("reset=false → incremental run (recompute only the recent lookback horizon).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Why gold reads silver as a *batch*, not a stream
# MAGIC `rolling_vol_30m` is a **window function** (`avg` over the trailing 30 windows per
# MAGIC symbol). A window function must see a row's *neighbours* — which a streaming
# MAGIC aggregation's bounded, forward-only state cannot provide. So gold reads silver as
# MAGIC a batch snapshot and gets its incrementality + idempotency from a **bounded
# MAGIC recompute + MERGE** (below), not from streaming state. This is the right tool:
# MAGIC silver already did the heavy stateful streaming; gold is a deterministic mart on
# MAGIC top of it, cheap to recompute over a small recent horizon.

# COMMAND ----------
from pyspark.sql import functions as F  # noqa: E402

# Recompute horizon for late arrivals: silver can MERGE-update windows up to its 2-min
# watermark behind, so we recompute a generous recent slice rather than just the newest
# minute. Small enough to stay cheap under Free Edition.
LOOKBACK_MINUTES = 30

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Incremental read with a rolling-window *context* prefix
# MAGIC We only want to **rewrite** gold rows newer than `recompute_from = max(gold) −
# MAGIC LOOKBACK`. But each of those rows' `rolling_vol_30m` depends on its preceding 30
# MAGIC windows — so we must *read* silver back a further `ROLLING_WINDOW_ROWS` minutes
# MAGIC (the **context** prefix) for the window function to be correct, then drop those
# MAGIC context-only rows before the MERGE. On the first run (`gold` empty) we recompute
# MAGIC everything.

# COMMAND ----------
gold_exists_rows = spark.table(cfg.tbl_gold_market_pulse).agg(F.max("window_start").alias("hw")).collect()
high_watermark = gold_exists_rows[0]["hw"] if gold_exists_rows else None

silver = spark.table(cfg.tbl_silver_trades_1min)

if high_watermark is None:
    # First run (or post-reset): recompute the whole table.
    recompute_from = None
    silver_slice = silver
    print("No existing gold rows → full recompute.")
else:
    recompute_from = F.lit(high_watermark) - F.expr(f"INTERVAL {LOOKBACK_MINUTES} MINUTES")
    # Read an extra ROLLING_WINDOW_ROWS minutes of context so the window function is
    # correct for the oldest recomputed row.
    context_from = F.lit(high_watermark) - F.expr(
        f"INTERVAL {LOOKBACK_MINUTES + ROLLING_WINDOW_ROWS} MINUTES"
    )
    silver_slice = silver.filter(F.col("window_start") >= context_from)
    print(f"Incremental recompute from max(gold) − {LOOKBACK_MINUTES} min.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Compute the signals (`src.gold.to_gold`)
# MAGIC Delegated to the shared transform so the exact semantics are unit-tested in CI:
# MAGIC - **`candle_direction`** — `up`/`down`/`flat` from `close` vs `open`.
# MAGIC - **`momentum_signal`** — CASE on direction × `taker_buy_ratio`: a rising candle
# MAGIC   *confirmed* by buy-dominant flow (≥0.6) is `strong_bullish`, else `bullish`;
# MAGIC   the mirror for falling candles (≤0.4 → `strong_bearish`); flat → `neutral`.
# MAGIC - **`volume_spike`** — `volume > 2 × rolling_vol_30m` (the window function).
# MAGIC - **`volatility`** — `(high − low) / open`.

# COMMAND ----------
gold_all = to_gold(silver_slice)

# Drop the context-only prefix: keep just the rows we intend to MERGE (newer than the
# recompute horizon). Their rolling_vol_30m already saw the context rows, so it's correct.
gold_updates = gold_all if recompute_from is None else gold_all.filter(F.col("window_start") >= recompute_from)
gold_updates.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Idempotent sink — `MERGE` on `(window_start, symbol)`
# MAGIC A recompute re-emits rows for windows that already exist in gold. A plain append
# MAGIC would duplicate them, so we **MERGE-upsert** on the contract key
# MAGIC `(window_start, symbol)`: matched → overwrite with the freshly computed signals,
# MAGIC unmatched → insert. Re-running the notebook is therefore a no-op on unchanged
# MAGIC windows → exactly-once *effect* into the table (idempotency). SQL `MERGE` via a
# MAGIC temp view is Spark Connect-friendly on serverless.

# COMMAND ----------
MERGE_KEYS = ["window_start", "symbol"]

gold_updates.createOrReplaceTempView("gold_updates")
on = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEYS)
spark.sql(f"""
    MERGE INTO {cfg.tbl_gold_market_pulse} AS t
    USING gold_updates AS s
    ON {on}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("Gold MERGE complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Verification — the 3 acceptance assertions (issue #4)
# MAGIC 1. `(window_start, symbol)` is unique (no duplicate rows → MERGE works).
# MAGIC 2. No negative volume context (`rolling_vol_30m` ≥ 0).
# MAGIC 3. Every `momentum_signal` is in the valid enum.

# COMMAND ----------
gold = spark.table(cfg.tbl_gold_market_pulse)
total = gold.count()
distinct_keys = gold.select("window_start", "symbol").distinct().count()
assert total == distinct_keys, f"duplicate (window_start, symbol) rows: {total} != {distinct_keys}"

neg = gold.filter(F.col("rolling_vol_30m") < 0).count()
assert neg == 0, f"{neg} gold rows have a negative rolling volume"

bad_signal = gold.filter(~F.col("momentum_signal").isin(list(MOMENTUM_SIGNALS))).count()
assert bad_signal == 0, f"{bad_signal} gold rows have an invalid momentum_signal"

print(f"WP3 gold verification PASSED — {total} unique (window_start, symbol) rows.")
display(
    gold.orderBy(F.col("window_start").desc(), "symbol").limit(20)
)
