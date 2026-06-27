# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze streaming ingestion (WP1)
# MAGIC Auto Loader (`cloudFiles`) over the raw Volume → `bronze.trades` (Delta,
# MAGIC checkpointed, idempotent). Adds `ingest_ts` + `_source_file`; null-key rows
# MAGIC route to `bronze.trades_quarantine`. `Trigger.AvailableNow`.
# MAGIC
# MAGIC **Spark runs on Databricks serverless only** — this notebook is not meant to
# MAGIC run locally. Run `00_setup` first (it creates the catalog/schemas, the raw
# MAGIC landing Volume and the contract tables, and seeds the raw NDJSON).
# MAGIC
# MAGIC **Acceptance** (issue #2): re-running ingests only *new* files (idempotent via
# MAGIC the checkpoint), quarantine is populated for bad rows, and row counts are
# MAGIC logged.
# MAGIC
# MAGIC Everything is parameterised through `src/config.py` — nothing hard-coded.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets / parameters
# MAGIC - `dev_suffix` — isolate a parallel work package's namespace (`bronze_dev_wp1`,
# MAGIC   …) via `Config(dev_suffix=…)`, including its own checkpoint so parallel runs
# MAGIC   never share state.

# COMMAND ----------
dbutils.widgets.text("dev_suffix", "")
DEV_SUFFIX = dbutils.widgets.get("dev_suffix")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap — put the repo root on `sys.path` and load `Config`
# MAGIC Same resolution as `00_setup`: in a Databricks Git folder the notebook lives at
# MAGIC `…/notebooks/01_bronze`, so the repo root is its parent's parent. We add it to
# MAGIC `sys.path` so `from src.config import Config` (and `src.bronze`) resolve.

# COMMAND ----------
import os
import sys

# Resolve the repo root robustly whether running from a Git folder or Workspace.
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
from src.bronze import quarantine_reason_column  # noqa: E402

cfg = Config(dev_suffix=DEV_SUFFIX)
print(f"Repo root: {REPO_ROOT}")
print(f"Landing volume (read):  {cfg.volume_path}")
print(f"Bronze table (write):   {cfg.tbl_bronze_trades}")
print(f"Quarantine (write):     {cfg.tbl_bronze_quarantine}")
print(f"Checkpoint:             {cfg.checkpoint('bronze')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Raw schema — enforce, don't infer
# MAGIC We pass Auto Loader an **explicit schema** rather than letting it infer: the
# MAGIC raw contract is frozen (see `fixtures/README.md`), so inference would only add
# MAGIC nondeterminism and a schema-inference job. `event_ts` lands as a STRING here —
# MAGIC **WP1 owns the cast** to TIMESTAMP (README) so a malformed timestamp surfaces
# MAGIC as a clear cast rather than a silent inference quirk.

# COMMAND ----------
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, StringType, StructField, StructType  # noqa: E402

raw_schema = StructType([
    StructField("event_ts", StringType()),   # ISO-8601 UTC string; cast below
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("qty", DoubleType()),
    StructField("side", StringType()),
    StructField("trade_id", StringType()),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Auto Loader read stream
# MAGIC `cloudFiles` incrementally discovers new files in the landing Volume. The
# MAGIC `schemaLocation` (under the checkpoint) tracks any schema evolution / rescued
# MAGIC data; combined with the explicit schema it stays stable. We add the two
# MAGIC ingestion-time columns the contract requires:
# MAGIC - `ingest_ts` — wall-clock arrival time.
# MAGIC - `_source_file` — the file Auto Loader read the row from (`_metadata.file_path`).
# MAGIC
# MAGIC `_quarantine_reason` is computed once here (shared rule from `src/bronze.py`) and
# MAGIC used to split the batch in `foreachBatch` below.

# COMMAND ----------
raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", cfg.checkpoint("bronze") + "/_schema")
    .schema(raw_schema)
    .load(cfg.volume_path)
    .withColumn("event_ts", F.to_timestamp("event_ts"))  # WP1 owns the cast
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_quarantine_reason", quarantine_reason_column())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. `foreachBatch` — one stream, two sinks (split + idempotent append)
# MAGIC A structured stream writes to a single sink, but we need **two**: clean rows →
# MAGIC `bronze.trades`, null-key rows → `bronze.trades_quarantine` (bad rows are
# MAGIC *routed*, never silently dropped — CLAUDE.md). `foreachBatch` gives us the
# MAGIC micro-batch as a regular DataFrame so we can split and write both.
# MAGIC
# MAGIC **Idempotency.** Two layers:
# MAGIC 1. The **checkpoint** records which files were ingested, so re-running this
# MAGIC    notebook processes only *new* files (the acceptance criterion).
# MAGIC 2. Inside the batch, each append is tagged with `txnAppId` + `txnVersion`
# MAGIC    (`= batch_id`). Delta dedupes by this transaction id, so if a batch is
# MAGIC    retried after a partial failure the same rows are not appended twice.
# MAGIC
# MAGIC We `persist()` the batch because it's scanned twice (clean + quarantine).

# COMMAND ----------
_BRONZE_COLS = ["event_ts", "symbol", "price", "qty", "side", "trade_id",
                "ingest_ts", "_source_file"]
_QUAR_COLS = _BRONZE_COLS + ["_quarantine_reason"]
_TXN_APP_ID = f"bronze_ingest{cfg.dev_suffix}"


def upsert_batch(batch_df, batch_id: int) -> None:
    """Split one micro-batch into clean vs quarantine and append both idempotently."""
    batch_df.persist()
    try:
        clean = batch_df.filter(F.col("_quarantine_reason").isNull()).select(*_BRONZE_COLS)
        bad = batch_df.filter(F.col("_quarantine_reason").isNotNull()).select(*_QUAR_COLS)

        # txnAppId/txnVersion make the append idempotent across batch retries.
        (clean.write.format("delta").mode("append")
            .option("txnAppId", _TXN_APP_ID)
            .option("txnVersion", batch_id)
            .saveAsTable(cfg.tbl_bronze_trades))
        (bad.write.format("delta").mode("append")
            .option("txnAppId", _TXN_APP_ID + "_quarantine")
            .option("txnVersion", batch_id)
            .saveAsTable(cfg.tbl_bronze_quarantine))

        print(f"batch {batch_id}: {clean.count()} clean → bronze, "
              f"{bad.count()} quarantined")
    finally:
        batch_df.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Write stream — `Trigger.AvailableNow`
# MAGIC `Trigger.AvailableNow` processes every file available now in (possibly several)
# MAGIC micro-batches, then **stops** — keeping streaming semantics (checkpoint,
# MAGIC exactly-once offsets) without an always-on stream, which Free Edition forbids
# MAGIC (7-day max runtime + daily quota). Re-run on a schedule to pick up new files.

# COMMAND ----------
query = (
    raw_stream.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", cfg.checkpoint("bronze"))
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()  # AvailableNow → returns once the backlog is drained

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Verification — acceptance evidence
# MAGIC Log the resulting row counts and assert the quarantine path caught the
# MAGIC deliberately-dirty seed rows (6 in the committed fixture).

# COMMAND ----------
n_bronze = spark.table(cfg.tbl_bronze_trades).count()
n_quar = spark.table(cfg.tbl_bronze_quarantine).count()
print(f"bronze.trades rows:            {n_bronze}")
print(f"bronze.trades_quarantine rows: {n_quar}")

display(
    spark.table(cfg.tbl_bronze_quarantine)
    .groupBy("_quarantine_reason").count().orderBy("_quarantine_reason")
)
