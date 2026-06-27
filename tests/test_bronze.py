"""Bronze quarantine-routing tests (WP1).

`notebooks/01_bronze.py` runs on Databricks (Spark), which CI does not. The
routing *rule* — which raw rows go to `bronze.trades_quarantine` vs `bronze.trades`
— lives in `src/bronze.py` as a pure-Python function so it can be verified here
without Spark.

Two guarantees:
1. **Unit behaviour** of `quarantine_reason` (precedence, empty == null, clean).
2. **Oracle:** replaying the committed *raw* NDJSON through the rule reproduces the
   committed bronze/quarantine fixture split exactly — the same split the Spark
   `foreachBatch` must produce.
"""

import csv
import json
from pathlib import Path

import pytest

from src.bronze import KEY_COLUMNS, quarantine_reason

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "fixtures"


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_raw() -> list[dict]:
    rows: list[dict] = []
    for path in sorted((FIX / "raw").glob("*.json")):
        with path.open(encoding="utf-8") as fh:
            rows.extend(json.loads(ln) for ln in fh if ln.strip())
    return rows


# --------------------------------------------------------------------------- #
# Unit behaviour
# --------------------------------------------------------------------------- #
def test_clean_row_has_no_reason():
    row = {"event_ts": "2026-06-27T10:00:00.000Z", "symbol": "BTCUSDT",
           "price": 61000.0, "qty": 0.01, "side": "buy", "trade_id": "BTCUSDT-1"}
    assert quarantine_reason(row) is None


@pytest.mark.parametrize("key", KEY_COLUMNS)
def test_null_key_is_quarantined(key):
    row = {"event_ts": "t", "symbol": "BTCUSDT", "trade_id": "id"}
    row[key] = None
    assert quarantine_reason(row) == f"null {key}"


@pytest.mark.parametrize("key", KEY_COLUMNS)
def test_empty_string_counts_as_null(key):
    # JSON nulls render as empty strings once they round-trip through CSV; the rule
    # must treat "" the same as None so both fixture forms route identically.
    row = {"event_ts": "t", "symbol": "BTCUSDT", "trade_id": "id"}
    row[key] = ""
    assert quarantine_reason(row) == f"null {key}"


def test_first_null_key_wins_precedence():
    # event_ts precedes symbol in KEY_COLUMNS, so it determines the reason.
    row = {"event_ts": None, "symbol": None, "trade_id": "id"}
    assert quarantine_reason(row) == "null event_ts"
    assert KEY_COLUMNS[0] == "event_ts"


# --------------------------------------------------------------------------- #
# Oracle: rule applied to raw reproduces the committed fixture split
# --------------------------------------------------------------------------- #
def test_routing_reproduces_fixture_split():
    raw = _read_raw()
    clean_ids = {r["trade_id"] for r in raw if quarantine_reason(r) is None}
    dirty = [r for r in raw if quarantine_reason(r) is not None]

    bronze_ids = {r["trade_id"] for r in _read_csv(FIX / "bronze" / "trades.csv")}
    quar = _read_csv(FIX / "bronze" / "trades_quarantine.csv")

    # Clean rows are exactly the bronze rows; no dirty row leaks into bronze.
    assert clean_ids == bronze_ids
    assert len(dirty) == len(quar)


def test_routing_reasons_match_fixture():
    """Each quarantine fixture row carries the reason the rule would assign."""
    quar = _read_csv(FIX / "bronze" / "trades_quarantine.csv")
    for r in quar:
        # CSV renders nulls as "", which quarantine_reason treats as null.
        assert quarantine_reason(r) == r["_quarantine_reason"]
