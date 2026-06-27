# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (WP0)
# MAGIC Foundation for the whole pipeline. Idempotently creates catalog `mktpulse`,
# MAGIC schemas (`bronze`/`silver`/`gold`/`ops`), the `raw` landing Volume and the
# MAGIC checkpoints Volume, the frozen contract tables (DDL), then seeds the
# MAGIC historical raw NDJSON and the bronze/silver fixtures.
# MAGIC
# MAGIC **Spark runs on Databricks serverless only** — this notebook is not meant to
# MAGIC run locally. **Acceptance:** runs clean on a fresh Free Edition workspace;
# MAGIC fixtures load; contracts (see `CONTRACTS.md`) documented.
# MAGIC
# MAGIC Everything is parameterised through `src/config.py` — nothing hard-coded.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets / parameters
# MAGIC - `reset` — `"true"` drops the schemas `CASCADE` first (explicit clean slate;
# MAGIC   never silent). Default `"false"`.
# MAGIC - `seed_raw` — copy `fixtures/raw/*.json` into the raw landing Volume.
# MAGIC - `seed_fixtures` — load the bronze/silver CSV fixtures into Delta (set
# MAGIC   `"false"` for a pristine end-to-end run where the real streams are the only
# MAGIC   writers).
# MAGIC - `dev_suffix` — isolate a parallel work package's namespace
# MAGIC   (`bronze_dev_wp3`, …) via `Config(dev_suffix=…)`.

# COMMAND ----------
dbutils.widgets.dropdown("reset", "false", ["false", "true"])
dbutils.widgets.dropdown("seed_raw", "true", ["false", "true"])
dbutils.widgets.dropdown("seed_fixtures", "true", ["false", "true"])
dbutils.widgets.text("dev_suffix", "")

RESET = dbutils.widgets.get("reset") == "true"
SEED_RAW = dbutils.widgets.get("seed_raw") == "true"
SEED_FIXTURES = dbutils.widgets.get("seed_fixtures") == "true"
DEV_SUFFIX = dbutils.widgets.get("dev_suffix")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap — put the repo root on `sys.path` and load `Config`
# MAGIC In a Databricks Git folder the notebook lives at
# MAGIC `…/notebooks/00_setup`, so the repo root is its parent's parent. We add it to
# MAGIC `sys.path` so `from src.config import Config` resolves (plan risk R2).

# COMMAND ----------
import os
import sys

# Resolve the repo root robustly whether running from a Git folder or Workspace.
_nb_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
# Workspace paths are absolute under /Workspace when accessed from the driver FS.
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
    # Last resort: assume CWD is the repo root.
    REPO_ROOT = os.getcwd()
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

from src.config import Config  # noqa: E402

cfg = Config(dev_suffix=DEV_SUFFIX)
print(f"Repo root: {REPO_ROOT}")
print(f"Catalog={cfg.catalog}  bronze={cfg.schema_bronze}  silver={cfg.schema_silver} "
      f"gold={cfg.schema_gold}  ops={cfg.schema_ops}")
print(f"Landing volume: {cfg.volume_path}")
print(f"Checkpoint root: {cfg.checkpoint_root}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. (optional) Reset — explicit clean slate
# MAGIC `reset=true` drops the four schemas `CASCADE`. We never drop implicitly: the
# MAGIC normal path below uses `CREATE … IF NOT EXISTS` and so never destroys data.

# COMMAND ----------
if RESET:
    print("RESET=true → dropping schemas CASCADE (all tables/volumes/data lost).")
    for schema in (cfg.schema_bronze, cfg.schema_silver, cfg.schema_gold, cfg.schema_ops):
        spark.sql(f"DROP SCHEMA IF EXISTS {cfg.catalog}.{schema} CASCADE")
        print(f"  dropped {cfg.catalog}.{schema}")
else:
    print("RESET=false → keeping any existing schemas/tables (idempotent create below).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Catalog
# MAGIC `CREATE CATALOG IF NOT EXISTS` may be blocked on Free Edition (managed-location
# MAGIC / metastore constraints — plan risk R1). If it fails, fall back to an existing
# MAGIC catalog by re-running with `Config(catalog="workspace")` and only creating
# MAGIC schemas. We surface the error rather than masking it.

# COMMAND ----------
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    print(f"Catalog ready: {cfg.catalog}")
except Exception as e:  # noqa: BLE001 — we want to advise on the fallback
    print(f"WARNING: could not create catalog {cfg.catalog!r}: {e}")
    print("If Free Edition forbids catalog creation, re-run this notebook after "
          "setting Config(catalog='workspace') (or another existing catalog). "
          "config.py parameterises the catalog, so no contract change is needed.")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Schemas

# COMMAND ----------
for schema in (cfg.schema_bronze, cfg.schema_silver, cfg.schema_gold, cfg.schema_ops):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")
    print(f"Schema ready: {cfg.catalog}.{schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Volumes
# MAGIC - `raw` (in the bronze schema) is the landing folder Auto Loader watches.
# MAGIC - a separate `checkpoints` volume in `ops` holds streaming checkpoints; its
# MAGIC   name carries `dev_suffix` to match `config.checkpoint_root` so parallel work
# MAGIC   packages never share state.

# COMMAND ----------
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema_bronze}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema_ops}.checkpoints{cfg.dev_suffix}")
print(f"Volume ready (landing): {cfg.volume_path}")
print(f"Volume ready (checkpoints): {cfg.checkpoint_root}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. DDL — frozen contract tables (`CREATE TABLE IF NOT EXISTS`)
# MAGIC Exact columns from `CONTRACTS.md`. Idempotent: existing tables are untouched.

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.tbl_bronze_trades} (
    event_ts      TIMESTAMP,
    symbol        STRING,
    price         DOUBLE,
    qty           DOUBLE,
    side          STRING,
    trade_id      STRING,
    ingest_ts     TIMESTAMP,
    _source_file  STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.tbl_bronze_quarantine} (
    event_ts            TIMESTAMP,
    symbol              STRING,
    price               DOUBLE,
    qty                 DOUBLE,
    side                STRING,
    trade_id            STRING,
    ingest_ts           TIMESTAMP,
    _source_file        STRING,
    _quarantine_reason  STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.tbl_silver_trades_1min} (
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    symbol           STRING,
    open             DOUBLE,
    high             DOUBLE,
    low              DOUBLE,
    close            DOUBLE,
    volume           DOUBLE,
    trade_count      BIGINT,
    taker_buy_ratio  DOUBLE
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.tbl_gold_market_pulse} (
    window_start      TIMESTAMP,
    symbol            STRING,
    candle_direction  STRING,
    momentum_signal   STRING,
    volume_spike      BOOLEAN,
    volatility        DOUBLE,
    rolling_vol_30m   DOUBLE,
    generated_at      TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.tbl_dq_failures} (
    check_ts      TIMESTAMP,
    layer         STRING,
    table_name    STRING,
    expectation   STRING,
    severity      STRING,
    failed_count  BIGINT,
    sample        STRING,
    run_id        STRING
) USING DELTA
""")
print("Contract tables ready (bronze.trades, trades_quarantine, silver.trades_1min, "
      "gold.market_pulse, ops.dq_failures).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. (optional) Seed the raw landing Volume
# MAGIC Copy the committed `fixtures/raw/*.json` into the landing Volume so Auto Loader
# MAGIC (WP1) and the Mode A replay (WP4) have a historical seed. We clear the landing
# MAGIC dir first so re-runs don't double-count files (idempotency).

# COMMAND ----------
if SEED_RAW:
    src_raw = f"file:{REPO_ROOT}/fixtures/raw"
    # Clear then recreate the landing dir for idempotency.
    try:
        dbutils.fs.rm(cfg.volume_path, recurse=True)
    except Exception:  # noqa: BLE001 — first run: nothing to remove
        pass
    dbutils.fs.mkdirs(cfg.volume_path)
    dbutils.fs.cp(src_raw, cfg.volume_path, recurse=True)
    n = len([f for f in dbutils.fs.ls(cfg.volume_path) if f.name.endswith(".json")])
    print(f"Seeded {n} raw NDJSON files into {cfg.volume_path}")
else:
    print("seed_raw=false → leaving the landing Volume untouched.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. (optional) Load bronze/silver fixtures into Delta
# MAGIC CSV is typeless, so we read with an **explicit StructType** matching the
# MAGIC contracts, then overwrite the tables (idempotent — re-running never
# MAGIC duplicates rows).

# COMMAND ----------
if SEED_FIXTURES:
    from pyspark.sql.types import (
        BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType,
    )

    fixtures_dir = f"file:{REPO_ROOT}/fixtures"

    bronze_schema = StructType([
        StructField("event_ts", TimestampType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("qty", DoubleType()),
        StructField("side", StringType()),
        StructField("trade_id", StringType()),
        StructField("ingest_ts", TimestampType()),
        StructField("_source_file", StringType()),
    ])
    quarantine_schema = StructType(bronze_schema.fields + [StructField("_quarantine_reason", StringType())])
    silver_schema = StructType([
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("symbol", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("trade_count", LongType()),
        StructField("taker_buy_ratio", DoubleType()),
    ])

    def _load_csv(rel_path, schema, table):
        df = (
            spark.read.option("header", "true")
            .schema(schema)
            .csv(f"{fixtures_dir}/{rel_path}")
        )
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        print(f"Loaded {df.count()} rows → {table}")

    _load_csv("bronze/trades.csv", bronze_schema, cfg.tbl_bronze_trades)
    _load_csv("bronze/trades_quarantine.csv", quarantine_schema, cfg.tbl_bronze_quarantine)
    _load_csv("silver/trades_1min.csv", silver_schema, cfg.tbl_silver_trades_1min)
else:
    print("seed_fixtures=false → leaving Delta tables empty (pristine end-to-end run).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Verification — acceptance evidence
# MAGIC Assert every schema/volume/table exists and (when seeded) fixture row counts
# MAGIC match the generator's expected counts, then display a summary.

# COMMAND ----------
# Expected counts mirror fixtures/generate_fixtures.py (3 symbols × 60 min).
EXPECTED = {"bronze": 2489, "quarantine": 6, "silver": 180, "raw_files": 60}

schemas = {r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}").collect()}
for s in (cfg.schema_bronze, cfg.schema_silver, cfg.schema_gold, cfg.schema_ops):
    assert s in schemas, f"missing schema {s}"

for tbl in (cfg.tbl_bronze_trades, cfg.tbl_bronze_quarantine, cfg.tbl_silver_trades_1min,
            cfg.tbl_gold_market_pulse, cfg.tbl_dq_failures):
    spark.sql(f"DESCRIBE TABLE {tbl}")  # raises if the table is missing

if SEED_RAW:
    raw_files = len([f for f in dbutils.fs.ls(cfg.volume_path) if f.name.endswith(".json")])
    assert raw_files == EXPECTED["raw_files"], f"raw files {raw_files} != {EXPECTED['raw_files']}"

if SEED_FIXTURES:
    n_bronze = spark.table(cfg.tbl_bronze_trades).count()
    n_quar = spark.table(cfg.tbl_bronze_quarantine).count()
    n_silver = spark.table(cfg.tbl_silver_trades_1min).count()
    assert n_bronze == EXPECTED["bronze"], f"bronze {n_bronze} != {EXPECTED['bronze']}"
    assert n_quar == EXPECTED["quarantine"], f"quarantine {n_quar} != {EXPECTED['quarantine']}"
    assert n_silver == EXPECTED["silver"], f"silver {n_silver} != {EXPECTED['silver']}"

summary = spark.createDataFrame(
    [
        ("catalog", cfg.catalog),
        ("schemas", ", ".join((cfg.schema_bronze, cfg.schema_silver, cfg.schema_gold, cfg.schema_ops))),
        ("landing_volume", cfg.volume_path),
        ("checkpoint_root", cfg.checkpoint_root),
        ("reset", str(RESET)),
        ("seed_raw", str(SEED_RAW)),
        ("seed_fixtures", str(SEED_FIXTURES)),
    ],
    ["key", "value"],
)
print("WP0 setup verification PASSED.")
display(summary)
