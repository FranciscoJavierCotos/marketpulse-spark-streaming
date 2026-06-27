"""Fixture generator tests (WP0) — determinism, dirty/late rows, scale."""

import csv
import json
from datetime import datetime
from pathlib import Path

import generate_fixtures as gen

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "fixtures"


def _read_csv(path):
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_generator_is_deterministic(tmp_path, monkeypatch):
    """Regenerate into a temp dir and assert byte-identical to the committed files."""
    monkeypatch.setattr(gen, "OUT", tmp_path)
    gen.main()

    def _data_files(base):
        skip = {"README.md", "generate_fixtures.py", ".gitkeep"}
        return {p.relative_to(base) for p in base.rglob("*")
                if p.is_file() and p.name not in skip and "__pycache__" not in p.parts}

    committed = _data_files(FIX)
    regenerated = _data_files(tmp_path)
    assert regenerated == committed, "regeneration produced a different set of files"

    for rel in committed:
        assert (tmp_path / rel).read_bytes() == (FIX / rel).read_bytes(), f"{rel} differs"


def test_three_symbols_and_sixty_minutes():
    raw_files = sorted((FIX / "raw").glob("*.json"))
    assert len(raw_files) == gen.MINUTES == 60

    rows = _read_csv(FIX / "bronze" / "trades.csv")
    symbols = {r["symbol"] for r in rows}
    assert symbols == set(gen.SYMBOLS)
    assert len(symbols) == 3

    # Silver has one window per (minute, symbol) across the full hour.
    silver = _read_csv(FIX / "silver" / "trades_1min.csv")
    assert len(silver) == 3 * 60


def test_raw_ndjson_parses_with_clean_field_names():
    sample = sorted((FIX / "raw").glob("*.json"))[0]
    expected_keys = {"event_ts", "symbol", "price", "qty", "side", "trade_id"}
    with sample.open(encoding="utf-8") as fh:
        lines = [json.loads(ln) for ln in fh if ln.strip()]
    assert lines, "raw file is empty"
    for rec in lines:
        assert set(rec.keys()) == expected_keys  # option A clean names, not Binance a/p/q


def test_dirty_rows_are_quarantined_not_in_bronze():
    quar = _read_csv(FIX / "bronze" / "trades_quarantine.csv")
    reasons = sorted(r["_quarantine_reason"] for r in quar)
    assert reasons == ["null event_ts", "null event_ts",
                       "null symbol", "null symbol",
                       "null trade_id", "null trade_id"]

    # Each quarantine row genuinely has its offending key null (empty in CSV).
    for r in quar:
        if r["_quarantine_reason"] == "null event_ts":
            assert r["event_ts"] == ""
        elif r["_quarantine_reason"] == "null symbol":
            assert r["symbol"] == ""
        elif r["_quarantine_reason"] == "null trade_id":
            assert r["trade_id"] == ""

    # No bronze row is missing a key column.
    bronze = _read_csv(FIX / "bronze" / "trades.csv")
    for r in bronze:
        assert r["event_ts"] and r["symbol"] and r["trade_id"]

    # Quarantine trade_ids never leak into the clean bronze table.
    quar_ids = {r["trade_id"] for r in quar if r["trade_id"]}
    bronze_ids = {r["trade_id"] for r in bronze}
    assert quar_ids.isdisjoint(bronze_ids)


def _source_file_minute(source_file: str) -> int:
    # /Volumes/.../trades_2026-06-27T1030.json → minute index 30
    stamp = source_file.rsplit("trades_", 1)[1].split(".json")[0]  # 2026-06-27T1030
    landed = datetime.strptime(stamp, "%Y-%m-%dT%H%M")
    return (landed - gen.START).seconds // 60


def test_late_rows_present():
    """At least one clean row lands in a file later than its event-time minute."""
    bronze = _read_csv(FIX / "bronze" / "trades.csv")
    late = []
    for r in bronze:
        event_minute = datetime.strptime(
            r["event_ts"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(second=0, microsecond=0)
        event_idx = (event_minute - gen.START).seconds // 60
        if _source_file_minute(r["_source_file"]) > event_idx:
            late.append((event_idx, _source_file_minute(r["_source_file"])))
    assert late, "no late/out-of-order rows found"
    # One should be beyond the 2-min watermark (file_minute - event_minute > 2).
    assert any(file_idx - ev_idx > 2 for ev_idx, file_idx in late), "no beyond-watermark late row"


def test_volume_spike_present():
    """The spike minute's volume exceeds 2× the symbol's median minute volume (WP3 R4)."""
    silver = _read_csv(FIX / "silver" / "trades_1min.csv")
    spike_ws = (gen.START.replace(second=0)
                ).strftime("%Y-%m-%dT%H:") + f"{gen.SPIKE_MINUTE:02d}:00Z"
    for sym in gen.SYMBOLS:
        vols = sorted(float(r["volume"]) for r in silver if r["symbol"] == sym)
        median = vols[len(vols) // 2]
        spike = next(float(r["volume"]) for r in silver
                     if r["symbol"] == sym and r["window_start"] == spike_ws)
        assert spike > 2 * median, f"{sym} spike {spike} not > 2× median {median}"
