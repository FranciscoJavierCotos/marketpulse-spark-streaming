# Orchestration (WP6)

[`marketpulse_job.json`](./marketpulse_job.json) is the Databricks **multi-task
Job** that wires the medallion together: it runs the three streaming notebooks in
order — `bronze_ingest → silver_aggregate → gold_signals` — on a schedule, with
retries, on serverless compute, inside Free Edition's quota.

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
forbids that). The Job's linear `depends_on` DAG means a single scheduled run
walks new data bronze → silver → gold and exits:

```
bronze_ingest ──▶ silver_aggregate ──▶ gold_signals
(Auto Loader)     (watermark+MERGE)     (signals+MERGE)
```

- **Idempotent / from-checkpoint:** re-running never duplicates rows (checkpoint
  offsets + `MERGE`), so a missed tick simply catches up on the next run.
- **Retries:** every task has `max_retries: 2` with a 60 s floor and
  `retry_on_timeout` — transient serverless hiccups self-heal without a duplicate.
- **Serverless:** no `new_cluster` / `existing_cluster_id` on any task → tasks run
  on serverless compute (the only Free Edition option).
- **`max_concurrent_runs: 1`:** overlapping runs would share a streaming
  checkpoint, so runs are serialised; `queue.enabled` holds a tick that overlaps a
  slow one instead of dropping it.
- **Schedule:** every 15 min (`0 0/15 * * * ?`, UTC). Shipped **`PAUSED`** so
  importing the Job never silently starts burning quota — unpause when you want it
  live (or just **Run now**).

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
