# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver stateful streaming (WP2) — headline Spark-depth notebook
# MAGIC Reads `bronze.trades` as a **stream**, applies a watermarked, deduplicated,
# MAGIC 1-min windowed OHLCV aggregation, and upserts idempotently into
# MAGIC `silver.trades_1min` via `foreachBatch` + `MERGE` on `(window_start, symbol)`,
# MAGIC driven by `Trigger.AvailableNow`.
# MAGIC
# MAGIC **Every streaming decision is commented** — this is the headline notebook. The
# MAGIC transform itself lives in [`src/silver.py`](../src/silver.py) (`to_silver`), one
# MAGIC definition shared with the pure-Python `pytest` oracle so the maths is verified
# MAGIC in CI without Spark.
# MAGIC
# MAGIC **Spark runs on Databricks serverless only** — not meant to run locally.
# MAGIC
# MAGIC **Acceptance (issue #3):** late events within the watermark are absorbed;
# MAGIC duplicate input produces no duplicate output rows (MERGE + dedup); the stream
# MAGIC reproduces the silver fixture's OHLCV.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets / parameters
# MAGIC - `dev_suffix` — isolate a parallel work package's namespace (`silver_dev_wp2`,
# MAGIC   …) via `Config(dev_suffix=…)`; empty in production.
# MAGIC - `reset_checkpoint` — `"true"` clears this stream's checkpoint **and** truncates
# MAGIC   `silver.trades_1min` for a clean reprocess from the start of bronze. Never
# MAGIC   implicit — re-runs are otherwise incremental and idempotent.

# COMMAND ----------
dbutils.widgets.text("dev_suffix", "")
dbutils.widgets.dropdown("reset_checkpoint", "false", ["false", "true"])

DEV_SUFFIX = dbutils.widgets.get("dev_suffix")
RESET_CHECKPOINT = dbutils.widgets.get("reset_checkpoint") == "true"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap — put the repo root on `sys.path` and load shared modules
# MAGIC Same resolution as `00_setup`: in a Databricks Git folder the notebook lives at
# MAGIC `…/notebooks/02_silver`, so the repo root is its parent's parent.

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
from src.silver import to_silver  # noqa: E402  — the shared watermark+window+dedup transform

cfg = Config(dev_suffix=DEV_SUFFIX)
CHECKPOINT = cfg.checkpoint("silver_trades_1min")
print(f"Repo root: {REPO_ROOT}")
print(f"Source : {cfg.tbl_bronze_trades}")
print(f"Target : {cfg.tbl_silver_trades_1min}")
print(f"Checkpoint: {CHECKPOINT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. (optional) Reset — explicit clean reprocess
# MAGIC Streaming state lives in the checkpoint; deleting it makes the next run reprocess
# MAGIC all of bronze. We also truncate the target so the rebuilt windows don't merge on
# MAGIC top of stale rows. The MERGE makes this safe either way — reset just forces a
# MAGIC full recompute rather than an incremental one.

# COMMAND ----------
if RESET_CHECKPOINT:
    print("reset_checkpoint=true → clearing checkpoint and truncating the silver table.")
    try:
        dbutils.fs.rm(CHECKPOINT, recurse=True)
    except Exception:  # noqa: BLE001 — first run: nothing to remove
        pass
    spark.sql(f"TRUNCATE TABLE {cfg.tbl_silver_trades_1min}")
else:
    print("reset_checkpoint=false → incremental run (checkpoint resumes where it left off).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read bronze as a stream
# MAGIC `readStream.table(...)` turns the bronze Delta table into a streaming source:
# MAGIC each run picks up only the rows committed since the last checkpoint (incremental,
# MAGIC exactly-once at the source). We stream from bronze rather than re-reading raw
# MAGIC files so the medallion layers compose and quarantine has already been applied.

# COMMAND ----------
bronze_stream = spark.readStream.table(cfg.tbl_bronze_trades)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. The stateful transform (watermark → dedup → 1-min windowed OHLCV)
# MAGIC Delegated to `src.silver.to_silver` so the exact semantics are unit-tested in CI.
# MAGIC In short, and **why** (the streaming decisions):
# MAGIC - **`withWatermark("event_ts", "2 minutes")`** — declares how late an event may
# MAGIC   arrive. Spark can then finalise and **evict** window state older than the
# MAGIC   watermark instead of holding all windows forever (bounded state). Events later
# MAGIC   than 2 min are dropped — that is the watermark working as designed, the price of
# MAGIC   bounded state. Matches `CONTRACTS.md`.
# MAGIC - **`dropDuplicatesWithinWatermark(["symbol","trade_id"])`** — a replayed trade
# MAGIC   within the watermark is dropped, so volume/counts never double-count. This is the
# MAGIC   row-level half of idempotency; the MERGE below is the window-level half.
# MAGIC - **`groupBy(window("event_ts","1 minute"), "symbol")`** — the stateful 1-min
# MAGIC   tumbling aggregation producing OHLCV + volume + `taker_buy_ratio`. `open`/`close`
# MAGIC   are first/last **by event time** (struct min/max trick — streaming aggs have no
# MAGIC   row order), tie-broken on `trade_id` for determinism.

# COMMAND ----------
silver_stream = to_silver(bronze_stream)
silver_stream.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Idempotent sink — `foreachBatch` + `MERGE` on `(window_start, symbol)`
# MAGIC A windowed aggregation in **update** mode re-emits a window every time a new (late,
# MAGIC in-watermark) event lands in it. A plain append would create duplicate rows for the
# MAGIC same window. So each micro-batch is **MERGE-upserted** on the contract key
# MAGIC `(window_start, symbol)`: matched → overwrite with the recomputed aggregate,
# MAGIC unmatched → insert. Re-running a batch is therefore a no-op on already-merged
# MAGIC windows → exactly-once *effect* into the table (idempotency).
# MAGIC
# MAGIC We use SQL `MERGE` via a temp view (Spark Connect-friendly on serverless). The
# MAGIC batch DataFrame's own `sparkSession` is used so the view is visible to the MERGE.

# COMMAND ----------
MERGE_KEYS = ["window_start", "symbol"]


def upsert_to_silver(batch_df, batch_id: int) -> None:
    """MERGE one micro-batch into silver.trades_1min on (window_start, symbol).

    Idempotent: the same batch_id re-applied overwrites the same windows rather than
    appending. A streaming agg yields at most one row per key per batch, so no
    intra-batch key collision is possible.
    """
    session = batch_df.sparkSession
    batch_df.createOrReplaceTempView("silver_updates")
    on = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEYS)
    session.sql(f"""
        MERGE INTO {cfg.tbl_silver_trades_1min} AS t
        USING silver_updates AS s
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Run the stream — `Trigger.AvailableNow`
# MAGIC `availableNow=True` processes everything currently in bronze in a series of
# MAGIC micro-batches (preserving watermark/state/exactly-once semantics) and then
# MAGIC **stops** — no always-on stream, so we stay inside Free Edition's quota and 7-day
# MAGIC cap. Run it on a schedule (WP6) for near-real-time refreshes. `outputMode("update")`
# MAGIC emits changed windows each batch, which the MERGE folds in idempotently.

# COMMAND ----------
query = (
    silver_stream.writeStream
    .queryName("silver_trades_1min")
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("update")
    .foreachBatch(upsert_to_silver)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()  # AvailableNow is finite — block until it drains, then stop.
print("Silver stream finished.")
print("Last progress:", query.lastProgress)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Verification — acceptance evidence
# MAGIC Assert the contract key is unique (no duplicate output rows → MERGE/dedup work),
# MAGIC volumes are non-negative, and `taker_buy_ratio ∈ [0, 1]`, then show a sample.

# COMMAND ----------
silver = spark.table(cfg.tbl_silver_trades_1min)
total = silver.count()
distinct_keys = silver.select("window_start", "symbol").distinct().count()
assert total == distinct_keys, f"duplicate (window_start, symbol) rows: {total} != {distinct_keys}"

from pyspark.sql import functions as F  # noqa: E402

bad = silver.filter(
    (F.col("volume") < 0)
    | (F.col("taker_buy_ratio") < 0)
    | (F.col("taker_buy_ratio") > 1)
    | (F.col("high") < F.col("low"))
).count()
assert bad == 0, f"{bad} silver rows violate volume/ratio/high-low invariants"

print(f"WP2 silver verification PASSED — {total} unique (window_start, symbol) rows.")
display(
    silver.orderBy("window_start", "symbol").limit(20)
)
