"""Gold signal tests (WP3).

Two layers, both pure-Python so CI needs no Spark:

1. **Correctness oracle** — `src.gold.compute_market_pulse` (the pure twin of the
   Spark `to_gold` transform) must reproduce the committed `gold/market_pulse.csv`
   from `silver/trades_1min.csv`, must be idempotent under a repeated run, and must
   only ever emit valid `candle_direction` / `momentum_signal` enums (issue #4's
   "valid signal enum" acceptance assertion). Targeted cases pin down the momentum
   thresholds and the volume-spike rule.
2. **Notebook decision guard** — static checks that `notebooks/03_gold.py` keeps its
   headline decisions (window-function rolling average, incremental lookback, MERGE
   on the contract key, the 3 acceptance assertions), so they can't silently regress.
"""

import csv
from pathlib import Path

from src import gold

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "fixtures"
NOTEBOOK = ROOT / "notebooks" / "03_gold.py"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _silver_rows() -> list[dict]:
    with (FIX / "silver" / "trades_1min.csv").open(encoding="utf-8", newline="") as fh:
        return [
            {
                "window_start": r["window_start"],  # ISO strings sort correctly
                "symbol": r["symbol"],
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
                "taker_buy_ratio": float(r["taker_buy_ratio"]),
            }
            for r in csv.DictReader(fh)
        ]


def _gold_fixture() -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    with (FIX / "gold" / "market_pulse.csv").open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            out[(r["window_start"], r["symbol"])] = r
    return out


# --------------------------------------------------------------------------- #
# 1. Correctness oracle — the twin reproduces the committed gold fixture
# --------------------------------------------------------------------------- #
def test_compute_market_pulse_matches_gold_fixture():
    got = {(r["window_start"], r["symbol"]): r for r in gold.compute_market_pulse(_silver_rows())}
    expected = _gold_fixture()
    assert got.keys() == expected.keys()

    for key, exp in expected.items():
        g = got[key]
        assert g["candle_direction"] == exp["candle_direction"], key
        assert g["momentum_signal"] == exp["momentum_signal"], key
        # Fixture renders the boolean lowercase ("true"/"false").
        assert str(g["volume_spike"]).lower() == exp["volume_spike"], key
        assert abs(g["volatility"] - float(exp["volatility"])) < 1e-6, (key, g["volatility"], exp["volatility"])
        assert abs(g["rolling_vol_30m"] - float(exp["rolling_vol_30m"])) < 1e-6, key


def test_one_row_per_window_symbol():
    rows = gold.compute_market_pulse(_silver_rows())
    keys = [(r["window_start"], r["symbol"]) for r in rows]
    assert len(keys) == len(set(keys)), "compute_market_pulse produced duplicate (window_start, symbol) rows"


def test_recompute_is_idempotent():
    """Re-deriving over the same silver input yields identical gold — the property the
    notebook's MERGE relies on for exactly-once effect (issue #4)."""
    rows = _silver_rows()
    assert gold.compute_market_pulse(rows) == gold.compute_market_pulse(rows)


def test_only_valid_enums_emitted():
    """Every row's signals are in the contract enums — the 'valid signal enum'
    acceptance assertion, checked against the whole fixture-derived output."""
    for r in gold.compute_market_pulse(_silver_rows()):
        assert r["candle_direction"] in gold.CANDLE_DIRECTIONS, r
        assert r["momentum_signal"] in gold.MOMENTUM_SIGNALS, r


def test_volume_spike_fires_on_the_injected_spike_minute():
    """The fixture injects a >2× volume spike at 10:45 (plan R4); gold must flag it,
    and must not flag the calm opening minute."""
    by_key = {(r["window_start"], r["symbol"]): r for r in gold.compute_market_pulse(_silver_rows())}
    assert by_key[("2026-06-27T10:45:00Z", "BTCUSDT")]["volume_spike"] is True
    assert by_key[("2026-06-27T10:00:00Z", "BTCUSDT")]["volume_spike"] is False


# --------------------------------------------------------------------------- #
# Targeted unit cases — pin the signal definitions
# --------------------------------------------------------------------------- #
def test_candle_direction():
    assert gold.candle_direction(100.0, 101.0) == "up"
    assert gold.candle_direction(101.0, 100.0) == "down"
    assert gold.candle_direction(100.0, 100.0) == "flat"


def test_momentum_signal_thresholds():
    # Rising candle is "strong" only when buy-flow dominates (ratio >= BULL_RATIO).
    assert gold.momentum_signal("up", 0.6) == "strong_bullish"
    assert gold.momentum_signal("up", 0.59) == "bullish"
    # Falling candle is "strong" only when sell-flow dominates (ratio <= BEAR_RATIO).
    assert gold.momentum_signal("down", 0.4) == "strong_bearish"
    assert gold.momentum_signal("down", 0.41) == "bearish"
    # Flat candle is neutral regardless of flow.
    assert gold.momentum_signal("flat", 0.9) == "neutral"
    assert gold.momentum_signal("flat", 0.1) == "neutral"


def test_volatility_is_range_over_open():
    assert gold.volatility(100.0, 110.0, 90.0) == 0.2
    assert gold.volatility(0.0, 1.0, 0.0) == 0.0  # guard against divide-by-zero


def test_rolling_average_includes_current_row_so_first_row_never_spikes():
    """With the current row in its own trailing average, a lone first window can't be a
    spike (volume > 2× itself is impossible for volume > 0) — no null edge case."""
    rows = [
        {"window_start": "2026-06-27T10:00:00Z", "symbol": "X", "open": 1.0, "high": 1.0,
         "low": 1.0, "close": 1.0, "volume": 5.0, "taker_buy_ratio": 0.5},
    ]
    out = gold.compute_market_pulse(rows)
    assert out[0]["rolling_vol_30m"] == 5.0
    assert out[0]["volume_spike"] is False


# --------------------------------------------------------------------------- #
# 2. Notebook decision guard — the headline decisions stay in the notebook
# --------------------------------------------------------------------------- #
def test_notebook_keeps_gold_decisions():
    src = NOTEBOOK.read_text(encoding="utf-8")
    required = [
        "to_gold",            # shared signal transform
        "MERGE INTO",         # idempotent upsert (not append)
        "LOOKBACK_MINUTES",   # incremental lookback for late arrivals
        "TRUNCATE TABLE",     # explicit reset path
    ]
    for token in required:
        assert token in src, f"03_gold.py lost its decision: {token!r}"


def test_notebook_merges_on_contract_key():
    src = NOTEBOOK.read_text(encoding="utf-8")
    assert 'MERGE_KEYS = ["window_start", "symbol"]' in src, "MERGE must key on the contract (window_start, symbol)"


def test_notebook_has_three_acceptance_assertions():
    """Issue #4 asks for 3 assertions: unique key, no negative volume, valid enum."""
    src = NOTEBOOK.read_text(encoding="utf-8")
    assert src.count("assert ") >= 3
    assert "momentum_signal" in src and "isin" in src  # the enum check
