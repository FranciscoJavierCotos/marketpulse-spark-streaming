"""Data-quality expectation tests (WP5).

All pure-Python so CI needs no Spark — they exercise the expectation *predicates*
(the twins of the Spark ``condition`` columns) and the :func:`evaluate` /
:func:`split_kept` batch helpers:

1. **Expectation semantics** — ``not_null`` / ``positive`` / ``in_range`` /
   ``is_in`` classify rows the way their docstrings promise, including the
   null/empty edge cases the landing data actually contains.
2. **Batch evaluation** — :func:`evaluate` counts failures and emits a JSON sample
   shaped like the ``ops.dq_failures`` contract; :func:`split_kept` removes only
   ``drop``-severity offenders.
3. **Contract alignment** — :data:`DQ_FAILURES_COLUMNS` matches the frozen
   ``ops.dq_failures`` table in ``CONTRACTS.md`` (fixtures/contract can't drift).
4. **Fixture integration** — the bronze key-not-null rule reproduces the committed
   bronze/quarantine split, proving these helpers agree with WP1's routing.
"""

import csv
import json
import re
from pathlib import Path

import pytest

from src import quality as q

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "CONTRACTS.md"
FIX = ROOT / "fixtures"


# --------------------------------------------------------------------------- #
# 1. Expectation semantics — predicates classify rows correctly
# --------------------------------------------------------------------------- #
def test_not_null_rejects_null_and_empty_keys():
    exp = q.not_null("event_ts", "symbol", "trade_id")
    assert exp.severity == q.SEVERITY_FAIL  # null key is a hard contract violation
    assert exp.predicate({"event_ts": "t", "symbol": "BTCUSDT", "trade_id": "x"}) is True
    assert exp.predicate({"event_ts": None, "symbol": "BTCUSDT", "trade_id": "x"}) is False
    assert exp.predicate({"event_ts": "t", "symbol": "", "trade_id": "x"}) is False
    assert exp.predicate({"event_ts": "t", "symbol": "BTCUSDT", "trade_id": None}) is False


def test_positive_requires_strictly_greater_than_zero():
    exp = q.positive("price")
    assert exp.severity == q.SEVERITY_DROP
    assert exp.predicate({"price": 1.5}) is True
    assert exp.predicate({"price": 0}) is False
    assert exp.predicate({"price": -3.0}) is False
    assert exp.predicate({"price": None}) is False
    assert exp.predicate({"price": ""}) is False


def test_in_range_is_inclusive_and_supports_one_sided_bounds():
    both = q.in_range("price", minimum=10, maximum=20)
    assert both.predicate({"price": 10}) is True   # inclusive low
    assert both.predicate({"price": 20}) is True   # inclusive high
    assert both.predicate({"price": 9.99}) is False
    assert both.predicate({"price": 20.01}) is False
    assert both.predicate({"price": None}) is False

    lower_only = q.in_range("qty", minimum=0)
    assert lower_only.predicate({"qty": 0}) is True
    assert lower_only.predicate({"qty": 5}) is True
    assert lower_only.predicate({"qty": -1}) is False


def test_is_in_enforces_enum_membership():
    exp = q.is_in("side", ["buy", "sell"])
    assert exp.severity == q.SEVERITY_DROP
    assert exp.predicate({"side": "buy"}) is True
    assert exp.predicate({"side": "sell"}) is True
    assert exp.predicate({"side": "hold"}) is False
    assert exp.predicate({"side": None}) is False


def test_unknown_severity_is_rejected():
    with pytest.raises(ValueError):
        q.not_null("event_ts", severity="explode")


# --------------------------------------------------------------------------- #
# 2. Batch evaluation — evaluate() + split_kept()
# --------------------------------------------------------------------------- #
def _sample_batch() -> list[dict]:
    return [
        {"symbol": "BTCUSDT", "price": 100.0, "qty": 1.0, "side": "buy"},
        {"symbol": "BTCUSDT", "price": -5.0, "qty": 1.0, "side": "buy"},   # bad price
        {"symbol": "BTCUSDT", "price": 100.0, "qty": 1.0, "side": "hold"},  # bad enum
        {"symbol": None, "price": 100.0, "qty": 1.0, "side": "sell"},       # null key
    ]


def test_evaluate_counts_failures_and_emits_json_sample():
    rows = _sample_batch()
    expectations = [
        q.not_null("symbol"),
        q.positive("price"),
        q.is_in("side", ["buy", "sell"]),
    ]
    results = {r.expectation: r for r in q.evaluate(rows, expectations)}

    assert results["not_null(symbol)"].failed_count == 1
    assert results["positive(price)"].failed_count == 1
    assert results["is_in(side,{buy, sell})"].failed_count == 1

    # The sample column is valid JSON projecting only the rule's columns.
    sample = json.loads(results["positive(price)"].sample)
    assert sample == [{"price": -5.0}]


def test_evaluate_returns_clean_result_when_all_pass():
    rows = [{"symbol": "BTCUSDT", "price": 1.0}]
    (result,) = q.evaluate(rows, [q.positive("price")])
    assert result.failed_count == 0
    assert json.loads(result.sample) == []


def test_evaluate_sample_is_capped_and_deterministic():
    rows = [{"price": -float(i)} for i in range(1, 11)]
    (result,) = q.evaluate(rows, [q.positive("price")], sample_size=3)
    assert result.failed_count == 10
    assert len(json.loads(result.sample)) == 3
    # sort_keys makes the serialized sample stable across runs.
    again = q.evaluate(rows, [q.positive("price")], sample_size=3)[0].sample
    assert again == result.sample


def test_split_kept_drops_only_drop_severity_offenders():
    rows = _sample_batch()
    expectations = [
        q.not_null("symbol", severity=q.SEVERITY_FAIL),   # fail → does NOT remove rows
        q.positive("price"),                              # drop → removes the bad-price row
        q.is_in("side", ["buy", "sell"]),                 # drop → removes the bad-enum row
    ]
    kept = q.split_kept(rows, expectations)
    # The two drop-rule offenders are gone; the fail-rule (null symbol) row stays.
    assert len(kept) == 2
    assert {r["side"] for r in kept} == {"buy", "sell"}
    assert any(r["symbol"] is None for r in kept)  # fail severity never filters


# --------------------------------------------------------------------------- #
# 3. Contract alignment — DQ_FAILURES_COLUMNS matches CONTRACTS.md
# --------------------------------------------------------------------------- #
def _contract_columns(section_token: str) -> list[str]:
    lines = CONTRACTS.read_text(encoding="utf-8").splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith("## ") and section_token in ln)
    cols: list[str] = []
    for ln in lines[start + 1:]:
        if ln.startswith("## "):
            break
        m = re.match(r"\|\s*`([^`]+)`\s*\|\s*([A-Z]+)\s*\|", ln)
        if m:
            cols.append(m.group(1))
    return cols


def test_dq_failures_columns_match_contract():
    assert list(q.DQ_FAILURES_COLUMNS) == _contract_columns("`ops.dq_failures`")


def test_severity_values_match_contract_enum():
    """CONTRACTS.md documents severity as warn / drop / fail — the module mirrors it."""
    assert set(q.SEVERITIES) == {"warn", "drop", "fail"}


# --------------------------------------------------------------------------- #
# 4. Fixture integration — the key not_null rule reproduces WP1's split
# --------------------------------------------------------------------------- #
def _read_csv(rel: str) -> list[dict]:
    with (FIX / rel).open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_not_null_keys_reproduces_committed_quarantine_split():
    """Running the bronze key not_null expectation over (clean + quarantined) bronze
    rows must flag exactly the committed quarantine rows — the helpers agree with the
    WP1 routing rule (src/bronze.py), evaluated independently here."""
    clean = _read_csv("bronze/trades.csv")
    quarantined = _read_csv("bronze/trades_quarantine.csv")
    key_rule = q.not_null("event_ts", "symbol", "trade_id")

    # Every committed clean row passes; every committed quarantine row fails.
    assert all(key_rule.predicate(r) for r in clean)
    assert all(not key_rule.predicate(r) for r in quarantined)

    # And evaluate() over the union counts exactly the quarantined rows as failures.
    (result,) = q.evaluate(clean + quarantined, [key_rule])
    assert result.failed_count == len(quarantined)
