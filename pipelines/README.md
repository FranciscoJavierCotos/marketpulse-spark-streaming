# Orchestration (WP6)

[`marketpulse_job.json`](./marketpulse_job.json) is the Databricks **multi-task
Job** that wires the medallion together: it runs the streaming notebooks in
order — `bronze_ingest → silver_aggregate → gold_signals → validate` — **fired by
a file-arrival trigger** when the producer lands data, with retries, on serverless
compute, inside Free Edition's quota.

## Why a Job, not a Lakeflow Declarative Pipeline

The notebooks are imperative Structured Streaming: `foreachBatch` + `MERGE` for
idempotent upserts and `Trigger.AvailableNow` for bounded, scheduled runs — that
streaming depth is the point of the project (especially the headline `02_silver`).
A Lakeflow **Declarative** Pipeline (DLT) would require rewriting those notebooks
as `@dlt.table` definitions, discarding the explicit watermark/MERGE/checkpoint
control. A **multi-task Job orchestrates the notebooks exactly as they are**, so
it's the right tool here. (The DLT route stays a documented alternative for a
fully-declarative rebuild.)

## How one run advances all three layers

Each notebook uses `Trigger.AvailableNow`: a run drains whatever backlog exists
**from that layer's checkpoint**, then stops — no always-on stream (Free Edition
forbids that). The Job's linear `depends_on` DAG means a single run walks new data
bronze → silver → gold, then **validates the output**, and exits:

```
bronze_ingest ──▶ silver_aggregate ──▶ gold_signals ──▶ validate
(Auto Loader)     (watermark+MERGE)     (signals+MERGE)    (data-test gate)
```

- **Idempotent / from-checkpoint:** re-running never duplicates rows (checkpoint
  offsets + `MERGE`), so a missed tick simply catches up on the next run.
- **Retries:** every task has `max_retries: 2` with a 60 s floor and
  `retry_on_timeout` — transient serverless hiccups self-heal without a duplicate.
- **Serverless:** no `new_cluster` / `existing_cluster_id` on any task → tasks run
  on serverless compute (the only Free Edition option).
- **`max_concurrent_runs: 1`:** overlapping runs would share a streaming
  checkpoint, so runs are serialised; `queue.enabled` holds a trigger that overlaps
  a slow run instead of dropping it.
- **Trigger — event-driven (file arrival):** instead of a clock, the Job fires on a
  **file-arrival trigger** watching the landing Volume
  (`/Volumes/mktpulse/bronze/raw/`). When the producer lands NDJSON, the Job runs —
  source→target with minimum human interaction. `min_time_between_triggers_seconds`
  (60) and `wait_after_last_change_seconds` (30) debounce a burst of files into one
  run. Shipped **`PAUSED`** so importing the Job never silently starts burning quota
  — unpause when you want it live (or just **Run now**). The trigger `url` is the
  default-catalog landing path because a job-level trigger can't template per-run
  parameters; a `dev_suffix` run is driven with **Run now** instead.
- **`validate` task:** the tail of the DAG runs `notebooks/05_validate` — a
  data-level test gate that asserts gold is non-empty, the `(window_start, symbol)`
  grain is intact, gold keeps up with silver (lag ≤ `max_lag_minutes`), and no
  `fail`-severity rows hit `ops.dq_failures` during the run. It **raises to fail the
  run** on any violation, so a bad batch is loud, not silent. This complements CI:
  pytest gates the *code* on every PR; `validate` gates the *data* on every run.

## Parameters

Job-level parameters flow into each notebook widget via `{{job.parameters.*}}`,
so the whole pipeline is parameterised by catalog/schema — nothing hard-coded:

| Parameter | Default | Effect |
|---|---|---|
| `catalog` | `mktpulse` | Unity Catalog catalog (`Config(catalog=…)`). |
| `dev_suffix` | `""` (empty) | Suffix on every schema + checkpoint (`Config(dev_suffix=…)`) to isolate a parallel/dev run, e.g. `_dev_wp6`. |

## Deploy

Spark runs on Databricks only — there's nothing to run locally here. Import the
Job with the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/):

```bash
# Point the CLI at your workspace (host + token / profile), then:
databricks jobs create --json @pipelines/marketpulse_job.json

# Run once on demand (replace <job-id> with the id the create call returns):
databricks jobs run-now <job-id>

# Override catalog / dev_suffix for a dev run:
databricks jobs run-now <job-id> \
  --json '{"job_parameters": {"dev_suffix": "_dev_wp6"}}'
```

Prerequisite: run `notebooks/00_setup` once first (it creates the catalog,
schemas, the landing Volume, and the contract tables, and seeds the fixtures).
The Job assumes those exist. To re-deploy after editing the JSON, use
`databricks jobs reset --job-id <job-id> --json @pipelines/marketpulse_job.json`.

The committed JSON is validated in CI by
[`tests/test_pipeline.py`](../tests/test_pipeline.py) (DAG order, retries,
serverless, schedule, params, and that each task points at a real notebook), so
it can't silently drift from the notebooks it orchestrates.
