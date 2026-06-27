# CLAUDE.md — MarketPulse Spark Streaming

Agent-facing source of truth for this repo. Paired with the human-facing
[`README.md`](../README.md); keep both in sync (see table below).

## Project overview

Near-real-time crypto market pipeline on **Databricks Free Edition (serverless)**
using **Spark Structured Streaming**. Proves *real streaming Spark depth*:
Auto Loader ingestion, watermarked stateful 1-min windowed aggregations,
idempotent `foreachBatch` + `MERGE` upserts, and business-grade gold signals —
all within Free Edition's serverless limits via `Trigger.AvailableNow`.

## Hard constraints (never violate)

- **Spark runs on Databricks serverless only — never local.** Do not install or
  assume local Spark. Only the Mode B producer runs locally.
- **PySpark DataFrame API + Spark SQL only.** No Scala, no RDDs, Spark Connect
  only.
- **No always-on streams.** Use `Trigger.AvailableNow` on a schedule — never a
  continuous `Trigger.ProcessingTime` stream (7-day max runtime + daily quota).
- **Restricted outbound internet** — seed data into a UC Volume; don't pull live
  APIs from a notebook.
- **`CONTRACTS.md` is read-only after WP0.** Changing a table contract is a
  coordinated breaking change.

## Architecture / key files

- `notebooks/00_setup.py` — catalog/schemas/Volume + fixture seeding (WP0).
- `notebooks/01_bronze.py` — Auto Loader → `bronze.trades` (+ quarantine) (WP1).
- `notebooks/02_silver.py` — watermark + windowed agg + MERGE (WP2, headline).
- `notebooks/03_gold.py` — `gold.market_pulse` signals (WP3).
- `notebooks/04_replay_producer.py` — Mode A: bounded timed drip of the seed into
  the landing Volume (WP4).
- `producers/producer.py` — Mode B local Binance WS producer (WP4): WS → normalize
  → batched small files → push to Volume via the Databricks SDK; CLI with
  `--dry-run`.
- `src/config.py` — single source of truth for catalog/schema/volume/checkpoint;
  every notebook imports it. Isolate parallel runs with `Config(dev_suffix=...)`.
- `src/bronze.py` — shared bronze quarantine-routing rule (WP1): one definition,
  rendered both as a pure-Python `quarantine_reason` (unit-tested in CI without
  Spark) and a `quarantine_reason_column` Spark expression `01_bronze.py` uses.
- `src/silver.py` — shared 1-min OHLCV windowing semantics (WP2): one definition,
  rendered both as a pure-Python `aggregate_ohlcv` (CI oracle, no Spark) and the
  `to_silver` watermark+window+dedup Spark transform `02_silver.py` runs.
- `src/gold.py` — shared business-signal semantics (WP3): one definition, rendered
  both as a pure-Python `compute_market_pulse` (CI oracle, no Spark) and the `to_gold`
  Spark transform `03_gold.py` runs (candle direction, momentum, `volume_spike` via a
  rolling-avg window function, volatility).
- `src/producer.py` — shared landing-file shape for both producers (WP4): one
  definition of the raw NDJSON record, the Binance→clean `normalize_binance_trade`
  (Mode B core), NDJSON (de)serialization, and replay pacing helpers (chunk /
  restamp). Pure-Python, unit-tested in CI; the notebook and the local script are
  thin I/O shells over it.
- `src/quality.py` — reusable DQ expectation helpers (WP5).
- `fixtures/generate_fixtures.py` — deterministic stdlib generator (WP0); emits the
  raw **NDJSON** seed and derives the committed bronze/silver/gold fixtures. Regenerate
  with `python fixtures/generate_fixtures.py` (byte-identical, fixed seed).
- `CONTRACTS.md` — frozen `bronze`/`silver`/`gold`/`ops.dq_failures` schemas.
- `pipelines/` — Lakeflow Declarative Pipeline / Job JSON (WP6).
- `requirements.txt` — minimal local-dev deps (pytest); `requirements-dq.txt` —
  Great Expectations for WP5; `requirements-producer.txt` — Mode B's
  websocket-client + databricks-sdk (all kept separate so CI stays lean).
- `.github/workflows/ci.yml` — runs the pure-Python pytest suite on PRs/pushes;
  the required gate for CI-gated auto-merge.

**Raw landing = JSON Lines (NDJSON), clean field names** (`event_ts`/`symbol`/
`price`/`qty`/`side`/`trade_id`, not Binance-native `a`/`p`/`q`). WP1 reads it with
`cloudFiles.format = "json"`; Mode B (WP4) normalises Binance → this shape.

## Tech stack

PySpark (Structured Streaming, DataFrame API) · Spark SQL · Delta Lake · Unity
Catalog · Auto Loader (`cloudFiles`) · Lakeflow Declarative Pipelines · Databricks
SDK/CLI + websocket-client (Mode B) · pytest · Great Expectations (data quality,
WP5) · GitHub Actions.

## Code conventions

- Parameterise everything via `src/config.py`; never hard-code catalog/schema/paths.
- Comment **every streaming decision** (why a watermark, why `foreachBatch`+`MERGE`
  for idempotency, why `Trigger.AvailableNow`) — especially in `02_silver.py`.
- Idempotency is non-negotiable: re-running a layer must not duplicate rows.
- Bad rows are routed (quarantine / `ops.dq_failures`), never silently dropped.

## Build & run

Spark code runs on Databricks (import notebooks, run `00_setup` first). Locally:
`pytest` for unit/contract tests; `producers/producer.py` for Mode B. See
README → Local development.

---

## Workflow rules (always follow)

- **Bug fix or feature:** use the `github-flow` skill FIRST to open a GitHub issue before writing code. Never push to `main` — branch and open a PR.
- **After a fix/feature:** use the `testing` skill to add tests (a regression test for bugs) before opening the PR.
- **If no CI runs the tests:** use the `ci-cd-pipelines` skill.
- **Document every change** in the Obsidian vault (see Engineering journal below): create or update the change's page and keep the database index in sync — in the same change, before the PR.
- **Ship it (CI-gated, low human review) — never merge before CI is green:** the security + test gates *are* the review (GitHub forbids approving your own PR), so a green CI run is a hard prerequisite for merge. Once the PR is open:
  1. **Block on CI:** run `gh pr checks <#> --watch` and wait for every check to pass. This is the gate, and it works regardless of repo settings — do not skip it.
  2. **If a check is red:** fix it and push; re-watch. Never merge red, never `--admin`/override, never bypass branch protection.
  3. **Merge only once green:** `gh pr merge <#> --squash --delete-branch`.
  - **Server-side gate is active:** the `main` ruleset *"Pytest has to pass"* requires the `pytest (3.12)` check with **no bypass actors** (applies to admins too), so a merge — including `gh pr merge --auto` — cannot land on red. Step 1's `--watch` is still the rule (works even if the ruleset changes); `--auto` may be added on top, never as a substitute.
  - After it merges, flip the Obsidian page + `_Database.md` row to `done` and fill in the PR link.

## Keep docs in sync (same change, never a follow-up)

Two living docs, kept consistent with each other:

- `.claude/CLAUDE.md` (this file) — agent-facing source of truth.
- `README.md` — the single human-facing README. Do not create per-service READMEs.

Update both in the same change when a change touches any of the triggers below. New env vars also go in the relevant `.env.example`; notable diagnosed bugs get an entry in README's _Issues Encountered_.

|Trigger|Update|
|---|---|
|Public interface added/removed/renamed, or request/response (or function signature) shape changes|README API/Usage Reference + this file's Architecture / Key Files|
|DB / persistence / external session or state store introduced or changed|Schema + config + migrations in both; revisit Out of Scope / Do-Don't assumptions|
|Core domain data model / record shape changes|README data-model section, Code Conventions here, and the shared types file|
|A structured-output / message / event contract between components changes|README protocol section + Do-Don't note here|
|Tool/integration added/removed/repurposed, or agent type / model / iteration cap changes|Tool count + descriptions in both|
|Major dependency / runtime / LLM or service provider changes|Tech Stack in both|
|Commands / ports / env vars / deploy config change|Build & Run + Environment here, README Local Development / Deployment|
|New service or top-level directory|Architecture tree in both|

Routine bug fixes, internal refactors, and copy tweaks need no doc update. Litmus test: "would a new contributor reading the docs now be misled?"

## Engineering journal (Obsidian vault)

A running log of what was built, separate from the living docs above. Lives in the Obsidian vault at `C:\Users\franc\Documents\Obsidian\Vault\Project Journals\MarketPulse Spark Streaming` (outside the repo — not committed). Every bug fix, feature, refactor, chore, or docs change gets its own page; a master index page tracks pending issues.

**Structure**

- `_Database.md` — master index. Two tables (Pending / in progress and Done) plus optional Dataview blocks. This is the page to open to see outstanding work.
- `Changes/` — one page per change, named `YYYY-MM-DD-<type>-<slug>.md` (e.g. `2026-06-27-feature-bronze-autoloader.md`). `type` ∈ `bug | feature | refactor | chore | docs`.
- `Templates/Change.md` — the page template (frontmatter: `title`, `type`, `status`, `issue`, `pr`, `branch`, `created`, `updated`, `tags`). `status` ∈ `pending | in-progress | done`.

**When (same change, before the PR — like the docs rule above)**

1. Starting a fix/feature/refactor: copy `Templates/Change.md` into `Changes/` with a dated slug, fill `status: in-progress`, link the GitHub issue, and add a Pending / in progress row to `_Database.md`.
2. Finishing (PR opened/merged): fill in the PR link, flip `status: done`, bump `updated`, and move the row from the Pending table to the Done table in `_Database.md`.

Keep each change page's frontmatter `status` and the `_Database.md` tables consistent — the frontmatter is the source of truth; the tables (and Dataview blocks) are the human view. Use absolute dates (today is resolvable from context). This journal records narrative (problem, decisions, outcome); it does not replace updating `README.md` / this file per the table above.
