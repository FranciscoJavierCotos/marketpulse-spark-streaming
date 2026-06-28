# MarketPulse — Spark Structured Streaming Lakehouse

> **One-liner:** *It's the same Bitcoin pipeline, rebuilt the proper way — Spark
> Structured Streaming with watermarking and stateful windowed aggregations on
> Databricks, instead of Python threads with pandas.*

A near-real-time crypto market pipeline built **entirely on Databricks Free
Edition (serverless)** using **Spark Structured Streaming**: Auto Loader
ingestion, watermarked stateful windowed aggregations, idempotent `MERGE`
upserts, and business-grade gold signals — all within Free Edition's serverless
constraints via `Trigger.AvailableNow`.

> ⚙️ Status: **scaffolding**. Implementation is tracked as GitHub issues WP0–WP7
> (see [Work packages](#work-packages)). This README is updated as packages land.

---

## Architecture

```
SOURCE (off-Databricks, optional)
  Mode A: Replay generator (notebook on Databricks) ─┐
  Mode B: Local Binance WS producer ── SDK/CLI push ─┤
                                                      ▼
        Unity Catalog Volume — landing  /Volumes/mktpulse/bronze/raw/  (≈ S3 raw)
                                                      │  Auto Loader (cloudFiles) + checkpoint
  ┌──────────────────────────────────────────────────▼──────────────────────────────────────┐
  │  SPARK STRUCTURED STREAMING — Databricks serverless                                        │
  │  BRONZE  readStream → +ingest_ts,_source_file → Delta (checkpointed, idempotent)           │
  │            │  null-key rows → bronze.trades_quarantine                                      │
  │            ▼                                                                                │
  │  SILVER  withWatermark(event_ts,"2 min") → groupBy(window(...)) 1-min OHLCV/volume          │
  │            dropDuplicates (watermark-bounded) → MERGE upsert via foreachBatch (idempotent)  │
  │            ▼                                                                                 │
  │  GOLD    market_pulse: momentum_signal · volume_spike(>2× rolling) · volatility             │
  └──────────────────────────────────┬──────────────────────────────────────────────────────────┘
                                      ▼
    Multi-task Job: …→gold→validate gate ·   Unity Catalog governance
    expectations (DQ gates) · retries     ·   lineage · schema enforcement
    Trigger.AvailableNow + file-arrival   ·   checkpoints · quarantine tables
```

Catalog `mktpulse`; schemas `bronze`, `silver`, `gold`, `ops`. Table contracts
are frozen in [`CONTRACTS.md`](./CONTRACTS.md).

## Why Spark runs on Databricks (never local)

All Spark code runs on **Databricks serverless** — Free Edition's whole value is
managed serverless Spark for free. The only thing that may run locally is the
optional **Mode B** producer.

### Mode A vs Mode B (same landing folder, so Spark code never changes)

- **Mode A — Replay (backbone, build first):** a Databricks notebook drips a
  seeded historical dataset into the landing Volume on a timer. 100% on
  Databricks, reproducible by anyone who clones the repo.
- **Mode B — Live (optional flex):** a local Binance WebSocket producer writes
  small files and pushes them to the UC Volume via the Databricks SDK/CLI
  (outbound call goes *to* Databricks, which Free Edition allows). Auto Loader
  picks them up.

## Free Edition constraints (designed around)

- **Serverless only** — no custom clusters/GPUs.
- **No always-on streams** — 7-day max runtime + daily quota → use
  **`Trigger.AvailableNow`** on a schedule (keeps streaming semantics —
  watermarks, state, exactly-once — without burning quota). Never a continuous
  `Trigger.ProcessingTime` stream.
- **Restricted outbound internet** — seed data into a Volume; don't pull live
  APIs from a notebook.
- **Spark Connect only / no Scala / no RDDs** — PySpark DataFrame API + Spark SQL.

## Repository layout

```
notebooks/      00_setup · 01_bronze · 02_silver · 03_gold · 04_replay_producer (Mode A) · 05_validate (data-test gate)
producers/      producer.py (Mode B local Binance WS producer)
src/            config.py (parameterisation) · bronze.py (quarantine rule) · silver.py (OHLCV transform + oracle) · gold.py (signals + oracle) · producer.py (shared landing-file shape, Mode A+B) · quality.py (DQ helpers)
fixtures/       generate_fixtures.py + committed raw/bronze/silver/gold seed (see fixtures/README.md)
tests/          pytest suites (config · contracts · fixtures · bronze · silver · gold · producer)
pipelines/      marketpulse_job.json (multi-task Job: bronze→silver→gold→validate, file-arrival triggered) + README
.github/        workflows/ci.yml (pytest on every PR)
CONTRACTS.md    frozen table contracts (read-only after WP0)
```

### Seed data & fixtures

The pipeline is fed by a **committed, deterministic seed** produced by
[`fixtures/generate_fixtures.py`](./fixtures/generate_fixtures.py) (stdlib only,
fixed seed → byte-identical on every run). It emits the historical raw NDJSON and
derives the bronze/silver fixtures from it, so each layer's fixture is internally
consistent. Regenerate with:

```bash
python fixtures/generate_fixtures.py
```

The raw landing format is **JSON Lines (NDJSON)** — one trade per line, clean field
names (Auto Loader reads it with `cloudFiles.format = "json"`):

```json
{"event_ts": "2026-06-27T10:00:03.412Z", "symbol": "BTCUSDT", "price": 61234.5, "qty": 0.0123, "side": "buy", "trade_id": "BTCUSDT-1000001"}
```

`event_ts` is ISO-8601 UTC (epoch-millis is the realistic alternative; WP1 owns the
cast). See [`fixtures/README.md`](./fixtures/README.md) for the full schema, the
derivation rules, and the deliberate dirty / late / volume-spike rows that exercise
quarantine, the watermark, and gold's spike signal.

## Local development

The Spark code targets Databricks serverless and is not meant to run locally.
Locally you can run the unit tests and the Mode B producer:

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows
pip install -r requirements.txt                    # minimal: pytest
pytest                                              # unit/contract/fixture tests
```

`requirements.txt` stays minimal so CI is fast (the tests are pure-Python against
the fixture generator output — no local Spark). The heavier data-quality framework
option for WP5, **Great Expectations**, lives in a separate `requirements-dq.txt`.

### Data quality & expectations (WP5)

[`src/quality.py`](./src/quality.py) is the dependency-free core the streaming
`foreachBatch` paths use directly (Great Expectations stays optional, for a fuller
suite). Each expectation is defined once and rendered twice — a pure-Python
`predicate` (the CI oracle, no Spark) and an equivalent Spark `condition` column —
the same twin pattern as the `bronze`/`silver`/`gold` modules. The constructors:

- `not_null(*cols)` — keys must be present (default severity `fail`).
- `positive(col)` — strictly `> 0`, e.g. `price`/`qty` (default `drop`).
- `in_range(col, minimum=…, maximum=…)` — inclusive bounds (default `warn`).
- `is_in(col, allowed)` — enum membership, e.g. `side` (default `drop`).

Each rule carries a **severity**: `warn` records the failure and keeps the row,
`drop` records it and filters the offenders out of the batch, `fail` records it and
may raise to abort the run. WP1–WP3 guard a batch with one line inside their
`foreachBatch`:

```python
from src.quality import apply_expectations, not_null, positive, is_in

clean = apply_expectations(
    batch_df,
    [not_null("event_ts", "symbol", "trade_id"), positive("price"), is_in("side", ["buy", "sell"])],
    layer="bronze", table_name=cfg.tbl_bronze_trades,
    dq_table=cfg.tbl_dq_failures, run_id=run_id,
)
```

Every violated rule appends a row to `ops.dq_failures` (`check_ts`, `layer`,
`table_name`, `expectation`, `severity`, `failed_count`, a JSON `sample` of offending
values, `run_id`) — bad rows are **routed, never silently dropped**. The pure-Python
twins (`evaluate` / `split_kept`) are unit-tested in CI without Spark.

### Producers (WP4)

Both producers write the **same raw NDJSON landing shape** (defined once in
[`src/producer.py`](./src/producer.py)), so the Spark layers never change which
one fed them.

- **Mode A — replay (`notebooks/04_replay_producer.py`, on Databricks):** drips
  the committed `fixtures/raw/*.json` seed into the landing Volume a few files per
  tick (`batch_files` / `interval_seconds` / `max_ticks` widgets), then stops — a
  bounded drip, never an always-on stream. Run `00_setup` with `seed_raw=false`
  first so the replay is the only writer, then re-run `01_bronze`
  (`Trigger.AvailableNow`) to ingest each batch. Optional `restamp=true` re-bases
  the seed's `event_ts` onto wall-clock now for a live-looking demo.

- **Mode B — live (`producers/producer.py`, local):** subscribes to the Binance
  trade WebSocket, normalises each message into the landing shape, batches trades
  into small files, and pushes them to the UC Volume via the Databricks SDK
  (`event_ts`/`side`/`trade_id` derived from Binance's `T`/`m`/`t`). Needs the
  separate `requirements-producer.txt` (websocket-client + databricks-sdk):

  ```bash
  pip install -r requirements-producer.txt
  export DATABRICKS_HOST=...  DATABRICKS_TOKEN=...     # or a CLI profile
  python producers/producer.py --symbols BTCUSDT ETHUSDT --max-trades 200
  python producers/producer.py --dry-run --max-trades 20   # print, never upload
  ```

CI runs the same `pytest` suite on every PR via
[`.github/workflows/ci.yml`](./.github/workflows/ci.yml). A PR is **never merged
before that check is green** (the workflow watches `gh pr checks --watch` first).

### Orchestration (WP6)

[`pipelines/marketpulse_job.json`](./pipelines/marketpulse_job.json) is a
multi-task **Databricks Job** that wires the medallion into one event-driven run:
`bronze_ingest → silver_aggregate → gold_signals → validate` (a linear
`depends_on` DAG). It is fired by a **file-arrival trigger** on the landing Volume
— when the producer lands NDJSON, the Job runs itself **source→target with minimum
human interaction** (no clock). Because each notebook uses `Trigger.AvailableNow`,
a single run drains each layer's backlog **from its checkpoint** and stops —
near-real-time without an always-on stream. It runs on **serverless** (no cluster
pinned), with `max_retries: 2` per task, `max_concurrent_runs: 1` (runs never
share a checkpoint), the trigger shipped **`PAUSED`** so importing it never
silently burns quota. Job params `catalog` / `dev_suffix` flow into every notebook
widget via `{{job.parameters.*}}`, so the pipeline is parameterised by
catalog/schema end-to-end.

The tail **`validate` task** ([`notebooks/05_validate.py`](./notebooks/05_validate.py))
is an automated **data-test gate**: after gold is written it asserts the output is
non-empty, the `(window_start, symbol)` grain is intact, gold keeps up with silver,
and no `fail`-severity rows hit `ops.dq_failures` — and **raises to fail the run**
if not. This is the data-level counterpart to CI: `pytest` gates the *code* on
every PR; `validate` gates the *data* on every run.

**Live ingestion** uses **Mode B** (`producers/producer.py`, Binance WebSocket),
which runs **off Databricks** (Free Edition forbids outbound calls from notebooks)
on your machine or a small always-on host and pushes to the Volume — tripping the
file-arrival trigger. Mode A replay trips the same trigger for a reproducible,
fully-on-Databricks demo.

A multi-task Job (not a Lakeflow *Declarative* Pipeline) is the right tool here:
the notebooks are imperative streaming (`foreachBatch` + `MERGE` +
`Trigger.AvailableNow`) — the project's headline depth — and a Job orchestrates
them as-is rather than forcing a `@dlt.table` rewrite. Deploy with
`databricks jobs create --json @pipelines/marketpulse_job.json`; see
[`pipelines/README.md`](./pipelines/README.md) for the full deploy/run steps and
parameter table. The committed JSON is CI-validated by `tests/test_pipeline.py`.

### Branch protection (active)

Merges are gated **server-side** by the `main` ruleset **"Pytest has to pass"**: it
requires the `pytest (3.12)` check, has **no bypass actors** (so the gate applies to
the repo owner too — no silent self-override), and blocks branch deletion and
non-fast-forward pushes. Combined with the always-on `gh pr checks --watch` step in
the workflow, no PR — including `gh pr merge --auto` — can land on red.

Notebooks import [`src/config.py`](./src/config.py) for catalog/schema/volume/
checkpoint values. Override per work package with `Config(dev_suffix="_dev_wpN")`
to isolate concurrent runs.

## Work packages

Implementation is broken into 8 GitHub issues. WP0 blocks everything; WP1–WP5 can
be built in parallel against the frozen contracts + seeded fixtures; WP6–WP7
converge.

| # | Package | Parallel? |
|---|---------|-----------|
| WP0 | Foundation, contracts & fixtures | solo (blocks all) |
| WP1 | Bronze streaming ingestion | ✅ |
| WP2 | Silver stateful streaming (headline) | ✅ |
| WP3 | Gold business marts | ✅ |
| WP4 | Producers (Mode A + Mode B) | ✅ |
| WP5 | Data quality & expectations | ✅ |
| WP6 | Orchestration | solo (needs WP1–WP3) |
| WP7 | Polish & README | solo (needs all) |

## How I'd productionise on AWS

Auto Loader ← S3 raw bucket · MSK/Kinesis for live ingest · Glue/EMR for managed
Spark · the same medallion (bronze/silver/gold) on Delta/Iceberg · the same
watermark + `MERGE` idempotency story. *(Expanded in WP7.)*

## Issues encountered

_None yet — notable diagnosed bugs get an entry here._
