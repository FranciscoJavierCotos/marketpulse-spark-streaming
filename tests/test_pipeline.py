"""Pipeline/orchestration tests (WP6).

Pure-Python guards on the committed Databricks Job definition
(`pipelines/marketpulse_job.json`) so it can't silently drift from the notebooks
it orchestrates. No Spark / no Databricks — just structural invariants the
acceptance criteria of issue #7 depend on:

* a linear bronze -> silver -> gold -> validate DAG (advances all layers, then
  asserts the output is sane);
* retries configured on every task;
* serverless compute (no cluster pinned), inside Free Edition's quota;
* a file-arrival trigger (event-driven on data landing), and catalog/dev_suffix
  parameterisation wired through to widgets;
* every task points at a notebook that actually exists in the repo.
"""

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
JOB_PATH = ROOT / "pipelines" / "marketpulse_job.json"

# Expected linear DAG: each task and the task it depends on.
EXPECTED_FLOW = [
    ("bronze_ingest", None),
    ("silver_aggregate", "bronze_ingest"),
    ("gold_signals", "silver_aggregate"),
    ("validate", "gold_signals"),
]

# The landing Volume the file-arrival trigger watches (≈ the S3 raw bucket): new
# files there fire the Job. Default-catalog path, since a job-level trigger url
# can't template per-run parameters.
LANDING_VOLUME = "/Volumes/mktpulse/bronze/raw"


@pytest.fixture(scope="module")
def job() -> dict:
    return json.loads(JOB_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def tasks_by_key(job) -> dict:
    return {t["task_key"]: t for t in job["tasks"]}


def test_job_json_parses_and_is_named(job):
    assert job["name"]
    assert isinstance(job["tasks"], list) and job["tasks"]


def test_linear_bronze_silver_gold_dag(tasks_by_key):
    """bronze -> silver -> gold via depends_on (one run advances all three layers)."""
    assert set(tasks_by_key) == {k for k, _ in EXPECTED_FLOW}
    for task_key, parent in EXPECTED_FLOW:
        deps = [d["task_key"] for d in tasks_by_key[task_key].get("depends_on", [])]
        if parent is None:
            assert deps == [], f"{task_key} should be the root task"
        else:
            assert deps == [parent], f"{task_key} must depend on {parent}, got {deps}"


def test_every_task_has_retries(tasks_by_key):
    """Acceptance: retries configured — transient serverless hiccups self-heal."""
    for task_key, task in tasks_by_key.items():
        assert task.get("max_retries", 0) >= 1, f"{task_key} has no retries"


def test_tasks_are_serverless(tasks_by_key):
    """Free Edition is serverless-only: no task may pin a cluster."""
    for task_key, task in tasks_by_key.items():
        assert "new_cluster" not in task, f"{task_key} pins a cluster (not serverless)"
        assert "existing_cluster_id" not in task, f"{task_key} pins a cluster"
        assert "job_cluster_key" not in task, f"{task_key} pins a cluster"


def test_single_concurrent_run(job):
    """Overlapping runs would share a streaming checkpoint — serialise them."""
    assert job.get("max_concurrent_runs") == 1


def test_file_arrival_trigger_on_landing_volume(job):
    """Event-driven: the producer landing files fires the Job (not a clock).

    A file-arrival trigger on the landing Volume is what makes ingestion run
    itself source->target with minimum human interaction.
    """
    # Event-driven only: no cron schedule competing with the trigger.
    assert "schedule" not in job, "file-arrival trigger replaces the cron schedule"

    trigger = job["trigger"]
    # Shipped paused so importing never silently burns quota.
    assert trigger.get("pause_status") == "PAUSED"

    fa = trigger["file_arrival"]
    # Watches the landing Volume Auto Loader reads (normalise a trailing slash).
    assert fa["url"].rstrip("/") == LANDING_VOLUME
    # Debounce knobs present so a burst of producer files coalesces into one run.
    assert fa["min_time_between_triggers_seconds"] >= 1
    assert fa["wait_after_last_change_seconds"] >= 1


def test_job_level_catalog_and_dev_suffix_params(job):
    params = {p["name"]: p for p in job.get("parameters", [])}
    assert params["catalog"]["default"] == "mktpulse"
    assert params["dev_suffix"]["default"] == ""


def test_params_flow_into_every_notebook_widget(tasks_by_key):
    """Each task forwards the job params to its notebook (parameterised, not hard-coded)."""
    for task_key, task in tasks_by_key.items():
        base = task["notebook_task"]["base_parameters"]
        assert base["catalog"] == "{{job.parameters.catalog}}", task_key
        assert base["dev_suffix"] == "{{job.parameters.dev_suffix}}", task_key


def test_tasks_point_at_existing_notebooks(tasks_by_key):
    """Guards against a renamed/moved notebook breaking orchestration silently."""
    for task_key, task in tasks_by_key.items():
        nb = task["notebook_task"]
        assert nb["source"] == "GIT", task_key
        path = ROOT / (nb["notebook_path"] + ".py")
        assert path.exists(), f"{task_key} -> missing notebook {path}"


def test_uses_git_source_on_main(job):
    src = job["git_source"]
    assert src["git_url"].endswith(".git")
    assert src["git_branch"] == "main"
