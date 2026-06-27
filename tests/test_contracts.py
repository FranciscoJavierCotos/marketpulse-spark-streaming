"""Contract tests (WP0).

Two guarantees:
1. The committed fixture CSV headers + inferred dtypes match the frozen columns in
   `CONTRACTS.md` (so fixtures never drift from the contract).
2. **Silver-is-consistent-with-bronze oracle:** independently recompute the 1-min
   OHLCV from `bronze/trades.csv` and assert it equals `silver/trades_1min.csv`.
   This is also WP2's correctness oracle — the watermarked stream must reproduce it.
"""

import csv
import math
import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "CONTRACTS.md"
FIX = ROOT / "fixtures"

PRICE_DP, QTY_DP, RATIO_DP = 2, 6, 6


# --------------------------------------------------------------------------- #
# Parse the frozen contract columns out of CONTRACTS.md
# --------------------------------------------------------------------------- #
def _contract_columns(section_token: str) -> list[tuple[str, str]]:
    """Return [(column, TYPE), …] for the markdown table under a `## ...token...` heading."""
    text = CONTRACTS.read_text(encoding="utf-8")
    # Grab from the section heading to the next blank-line-separated heading.
    lines = text.splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith("## ") and section_token in ln)
    cols: list[tuple[str, str]] = []
    for ln in lines[start + 1:]:
        if ln.startswith("## "):
            break
        m = re.match(r"\|\s*`([^`]+)`\s*\|\s*([A-Z]+)\s*\|", ln)
        if m:
            cols.append((m.group(1), m.group(2)))
    return cols


def _csv_header(path: Path) -> list[str]:
    with path.open(encoding="utf-8", newline="") as fh:
        return next(csv.reader(fh))


def _coercible(value: str, sql_type: str) -> bool:
    if value == "":
        return True  # nulls render empty in CSV (quarantine rows)
    try:
        if sql_type == "TIMESTAMP":
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ") if "." in value else \
                datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        elif sql_type == "DOUBLE":
            float(value)
        elif sql_type == "BIGINT":
            int(value)
        # STRING accepts anything
        return True
    except ValueError:
        return False


def test_bronze_header_matches_contract():
    contract = _contract_columns("`bronze.trades`")
    names = [c for c, _ in contract]
    assert _csv_header(FIX / "bronze" / "trades.csv") == names


def test_quarantine_header_is_bronze_plus_reason():
    contract = [c for c, _ in _contract_columns("`bronze.trades`")]
    assert _csv_header(FIX / "bronze" / "trades_quarantine.csv") == contract + ["_quarantine_reason"]


def test_silver_header_matches_contract():
    contract = [c for c, _ in _contract_columns("`silver.trades_1min`")]
    assert _csv_header(FIX / "silver" / "trades_1min.csv") == contract


@pytest.mark.parametrize("rel,section", [
    ("bronze/trades.csv", "`bronze.trades`"),
    ("silver/trades_1min.csv", "`silver.trades_1min`"),
])
def test_dtypes_inferred_from_csv_match_contract(rel, section):
    contract = dict(_contract_columns(section))
    with (FIX / rel).open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            for col, val in row.items():
                assert _coercible(val, contract[col]), f"{rel}:{col}={val!r} not a {contract[col]}"


# --------------------------------------------------------------------------- #
# Independent OHLCV oracle: recompute silver from bronze
# --------------------------------------------------------------------------- #
def _parse_ts(value: str) -> datetime:
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if "." in value else "%Y-%m-%dT%H:%M:%SZ"
    return datetime.strptime(value, fmt)


def _recompute_silver() -> dict[tuple[str, str], dict]:
    rows: list[dict] = []
    with (FIX / "bronze" / "trades.csv").open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            rows.append({
                "event_dt": _parse_ts(r["event_ts"]),
                "symbol": r["symbol"],
                "price": float(r["price"]),
                "qty": float(r["qty"]),
                "side": r["side"],
                "trade_id": r["trade_id"],
            })

    buckets: dict[tuple[datetime, str], list[dict]] = {}
    for t in rows:
        ws = t["event_dt"].replace(second=0, microsecond=0)
        buckets.setdefault((ws, t["symbol"]), []).append(t)

    out: dict[tuple[str, str], dict] = {}
    for (ws, symbol), trades in buckets.items():
        ordered = sorted(trades, key=lambda t: (t["event_dt"], t["trade_id"]))
        prices = [t["price"] for t in ordered]
        total = sum(t["qty"] for t in ordered)
        buy = sum(t["qty"] for t in ordered if t["side"] == "buy")
        ws_s = ws.strftime("%Y-%m-%dT%H:%M:%SZ")
        out[(ws_s, symbol)] = {
            "window_end": (ws + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "open": ordered[0]["price"],
            "high": round(max(prices), PRICE_DP),
            "low": round(min(prices), PRICE_DP),
            "close": ordered[-1]["price"],
            "volume": round(total, QTY_DP),
            "trade_count": len(ordered),
            "taker_buy_ratio": round(buy / total, RATIO_DP) if total else 0.0,
        }
    return out


def _read_silver_csv() -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    with (FIX / "silver" / "trades_1min.csv").open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            out[(r["window_start"], r["symbol"])] = {
                "window_end": r["window_end"],
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
                "trade_count": int(r["trade_count"]),
                "taker_buy_ratio": float(r["taker_buy_ratio"]),
            }
    return out


def test_silver_is_consistent_with_bronze():
    oracle = _recompute_silver()
    committed = _read_silver_csv()
    assert oracle.keys() == committed.keys()
    for key, exp in oracle.items():
        got = committed[key]
        assert got["window_end"] == exp["window_end"], key
        assert got["trade_count"] == exp["trade_count"], key
        for f in ("open", "high", "low", "close", "volume", "taker_buy_ratio"):
            assert math.isclose(got[f], exp[f], rel_tol=0, abs_tol=1e-6), (key, f, got[f], exp[f])
