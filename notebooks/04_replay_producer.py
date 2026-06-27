# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Mode A replay producer (WP4)
# MAGIC Drips the seeded historical raw NDJSON (`fixtures/raw/*.json`) into the
# MAGIC landing Volume **on a timer**, a few files per tick, to simulate a live
# MAGIC trade stream. Auto Loader (`01_bronze`, `Trigger.AvailableNow` on a schedule)
# MAGIC then consumes the growing backlog incrementally — so you can watch bronze →
# MAGIC silver → gold fill up over successive runs instead of all 60 files at once.
# MAGIC
# MAGIC **Same file schema as Mode B** (`producers/producer.py`): clean-field-name
# MAGIC NDJSON, written through the shared `src.producer` helpers, so the Spark code
# MAGIC never cares which producer fed it.
# MAGIC
# MAGIC **Spark runs on Databricks serverless only** — this notebook is not meant to
# MAGIC run locally. Run `00_setup` first; run it with **`seed_raw=false`** if you
# MAGIC want this replay to be the *only* writer into the landing Volume (otherwise
# MAGIC `00_setup` already bulk-seeds all 60 files and there is nothing left to drip).
# MAGIC
# MAGIC **Acceptance** (issue #5): produces a steady file stream Auto Loader consumes.
# MAGIC
# MAGIC Everything is parameterised through `src/config.py` — nothing hard-coded.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets / parameters
# MAGIC - `batch_files` — files dripped per tick (default `5`).
# MAGIC - `interval_seconds` — seconds to sleep between ticks (default `10`).
# MAGIC - `max_ticks` — stop after N ticks; `0` = drip the whole seed then stop.
# MAGIC - `restamp` — `"true"` re-bases each trade's `event_ts` onto wall-clock now so
# MAGIC   the dashboard looks live. Default `"false"` (faithful replay keeps the seed's
# MAGIC   timestamps so silver/gold reproduce the committed fixtures end-to-end).
# MAGIC - `dev_suffix` — isolate a parallel work package's namespace via
# MAGIC   `Config(dev_suffix=…)` (its own landing Volume).
# MAGIC
# MAGIC > **Why a bounded drip, not an always-on stream.** Free Edition forbids
# MAGIC > continuous streams (7-day cap + quota). This loop dribbles a *finite* seed
# MAGIC > and then stops — the producer side of the same "scheduled bursts, never
# MAGIC > always-on" discipline `Trigger.AvailableNow` gives the consumer side.

# COMMAND ----------
dbutils.widgets.text("batch_files", "5")
dbutils.widgets.text("interval_seconds", "10")
dbutils.widgets.text("max_ticks", "0")
dbutils.widgets.dropdown("restamp", "false", ["false", "true"])
dbutils.widgets.text("dev_suffix", "")

BATCH_FILES = int(dbutils.widgets.get("batch_files"))
INTERVAL_SECONDS = float(dbutils.widgets.get("interval_seconds"))
MAX_TICKS = int(dbutils.widgets.get("max_ticks"))
RESTAMP = dbutils.widgets.get("restamp") == "true"
DEV_SUFFIX = dbutils.widgets.get("dev_suffix")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap — put the repo root on `sys.path` and load `Config` + producer helpers
# MAGIC Same resolution as the other notebooks: in a Databricks Git folder the notebook
# MAGIC lives at `…/notebooks/04_replay_producer`, so the repo root is its parent's
# MAGIC parent. We add it to `sys.path` so `from src.config import Config` (and
# MAGIC `src.producer`) resolve.

# COMMAND ----------
import os
import sys
import time

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

from datetime import datetime, timezone  # noqa: E402

from src.config import Config  # noqa: E402
from src import producer  # noqa: E402

cfg = Config(dev_suffix=DEV_SUFFIX)
print(f"Repo root: {REPO_ROOT}")
print(f"Landing volume (write): {cfg.volume_path}")
print(f"batch_files={BATCH_FILES}  interval_seconds={INTERVAL_SECONDS}  "
      f"max_ticks={MAX_TICKS or 'all'}  restamp={RESTAMP}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Source seed + landing folder
# MAGIC The drip source is the committed `fixtures/raw/*.json` seed (one file per
# MAGIC landing minute). We ensure the landing Volume exists but — unlike `00_setup`'s
# MAGIC bulk seed — we **do not** clear it: this notebook only ever *adds* files, the
# MAGIC way a real producer would.

# COMMAND ----------
SRC_RAW = os.path.join(REPO_ROOT, "fixtures", "raw")
source_files = sorted(f for f in os.listdir(SRC_RAW) if f.endswith(".json"))
dbutils.fs.mkdirs(cfg.volume_path)
print(f"{len(source_files)} seed files to drip from {SRC_RAW}")

# Files already present in the landing Volume (so a re-run resumes, never
# re-drips what is already there — idempotent like the rest of the pipeline).
existing = {f.name for f in dbutils.fs.ls(cfg.volume_path)} if source_files else set()
pending = [f for f in source_files if f not in existing]
print(f"{len(existing)} already landed · {len(pending)} pending")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Drip loop — `batch_files` per tick, sleep `interval_seconds`, then stop
# MAGIC Each pending file is read, optionally re-stamped to wall-clock now, and written
# MAGIC into the landing Volume through `src.producer.to_ndjson` (the same serializer
# MAGIC Mode B uses). `restamp=false` simply copies the seed bytes' records verbatim.
# MAGIC We `dbutils.fs.put` small NDJSON files, which is exactly what Auto Loader
# MAGIC incrementally discovers.

# COMMAND ----------
def _read_seed(name: str) -> list[dict]:
    """Read one seed NDJSON file into records (driver-local file read)."""
    with open(os.path.join(SRC_RAW, name), encoding="utf-8") as fh:
        return producer.parse_ndjson(fh.read())


def _drip_file(name: str, *, shift) -> int:
    """Write one seed file into the landing Volume; return the row count."""
    records = _read_seed(name)
    if shift is not None:
        records = producer.restamp_records(records, shift=shift)
    # Keep the seed's filename so a resumed run recognises what already landed.
    dbutils.fs.put(f"{cfg.volume_path}/{name}", producer.to_ndjson(records), overwrite=True)
    return len(records)


# When re-stamping, shift every event by (now − seed start) so the historical seed
# is mapped onto the current wall clock while preserving inter-trade spacing.
shift = None
if RESTAMP and pending:
    seed_start = producer._parse_iso_ms(_read_seed(pending[0])[0]["event_ts"])
    shift = datetime.now(timezone.utc) - seed_start
    print(f"restamp shift = {shift}")

batches = producer.chunk(pending, BATCH_FILES)
total_files = total_rows = 0
for tick, batch in enumerate(batches, start=1):
    if MAX_TICKS and tick > MAX_TICKS:
        print(f"Reached max_ticks={MAX_TICKS}; stopping with files still pending.")
        break
    rows = sum(_drip_file(name, shift=shift) for name in batch)
    total_files += len(batch)
    total_rows += rows
    print(f"tick {tick}: dripped {len(batch)} files ({rows} trades) → {cfg.volume_path}")
    # Sleep *between* ticks only (not after the last) so the run ends promptly.
    if INTERVAL_SECONDS > 0 and not (MAX_TICKS and tick >= MAX_TICKS) and tick < len(batches):
        time.sleep(INTERVAL_SECONDS)

print(f"Done: dripped {total_files} files / {total_rows} trades this run.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Verification — files are landing for Auto Loader
# MAGIC Count the NDJSON files now in the landing Volume. Re-run `01_bronze`
# MAGIC (`Trigger.AvailableNow`) and they get ingested; re-run *this* notebook to drip
# MAGIC the next batch until the seed is exhausted.

# COMMAND ----------
landed = [f for f in dbutils.fs.ls(cfg.volume_path) if f.name.endswith(".json")]
print(f"Landing Volume now holds {len(landed)} NDJSON files at {cfg.volume_path}")
