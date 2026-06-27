"""Producer-core tests (WP4).

`notebooks/04_replay_producer.py` (Mode A) and `producers/producer.py` (Mode B)
both run their I/O off-CI (dbutils / Binance WebSocket / Databricks SDK), but the
logic that matters — turning a trade into the project's **clean raw NDJSON landing
shape** — lives in `src/producer.py` as pure functions, verified here without
Spark, a network, or the SDK.

Guarantees:
1. **Binance normalisation** maps native keys → the landing contract, with the
   subtle taker-side rule (`m` = buyer-is-maker) correct.
2. **Same file schema** — the producer's field set is exactly the committed raw
   NDJSON contract, so Mode B output is indistinguishable from the Mode A seed.
3. **Replay pacing** (chunk / restamp) drops or duplicates nothing.
4. **Mode B batching** flushes on the size boundary and on the final drain.
"""

import importlib.util
import json
from datetime import timedelta
from pathlib import Path

import pytest

from src import producer

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "fixtures"


# A representative Binance `<symbol>@trade` payload (native single-letter keys).
BINANCE_TRADE = {
    "e": "trade", "E": 1751120553000, "s": "BTCUSDT", "t": 1000001,
    "p": "61234.50", "q": "0.012300", "T": 1751120553412, "m": True, "M": True,
}


# --------------------------------------------------------------------------- #
# 1. Binance normalisation
# --------------------------------------------------------------------------- #
def test_normalize_maps_native_keys_to_landing_contract():
    rec = producer.normalize_binance_trade(BINANCE_TRADE)
    assert rec == {
        "event_ts": "2025-06-28T14:22:33.412Z",  # from T=…412 ms, UTC, ms precision
        "symbol": "BTCUSDT",
        "price": 61234.5,
        "qty": 0.0123,
        "side": "sell",                          # m=True ⇒ buyer is maker ⇒ taker sells
        "trade_id": "BTCUSDT-1000001",           # namespaced with the symbol
    }


def test_taker_side_follows_market_maker_flag():
    # m=True: buyer is the maker, so the taker is the seller.
    assert producer.normalize_binance_trade({**BINANCE_TRADE, "m": True})["side"] == "sell"
    # m=False: buyer is the taker.
    assert producer.normalize_binance_trade({**BINANCE_TRADE, "m": False})["side"] == "buy"


def test_price_and_qty_parse_to_float():
    rec = producer.normalize_binance_trade(BINANCE_TRADE)
    assert isinstance(rec["price"], float) and isinstance(rec["qty"], float)


def test_iso_ms_from_epoch_is_exact_at_the_millisecond():
    # Integer divmod (not ms/1000 float division) keeps the millis field exact.
    assert producer.iso_ms_from_epoch_millis(1751120553001) == "2025-06-28T14:22:33.001Z"
    assert producer.iso_ms_from_epoch_millis(1751120553999) == "2025-06-28T14:22:33.999Z"


# --------------------------------------------------------------------------- #
# 2. Same file schema as the committed seed
# --------------------------------------------------------------------------- #
def test_record_fields_match_raw_fixture_contract():
    """The producer's field set is exactly what the committed raw NDJSON carries."""
    sample = next((FIX / "raw").glob("*.json")).read_text(encoding="utf-8").splitlines()[0]
    assert set(json.loads(sample)) == set(producer.RECORD_FIELDS)


def test_normalized_record_has_exactly_the_contract_fields():
    assert tuple(producer.normalize_binance_trade(BINANCE_TRADE)) == producer.RECORD_FIELDS


def test_ndjson_pins_field_order_and_drops_extra_keys():
    # Input with shuffled order + an extra key still serialises in contract order.
    messy = {"trade_id": "BTCUSDT-1", "extra": "drop me", "symbol": "BTCUSDT",
             "qty": 0.5, "side": "buy", "price": 100.0, "event_ts": "2026-06-27T10:00:00.000Z"}
    line = producer.record_to_ndjson_line(messy)
    assert list(json.loads(line)) == list(producer.RECORD_FIELDS)
    assert "extra" not in line


def test_to_ndjson_round_trips():
    rec = producer.normalize_binance_trade(BINANCE_TRADE)
    text = producer.to_ndjson([rec, rec])
    assert text.endswith("\n") and text.count("\n") == 2
    assert producer.parse_ndjson(text) == [
        {k: rec[k] for k in producer.RECORD_FIELDS}] * 2


# --------------------------------------------------------------------------- #
# 3. Replay pacing — chunk / restamp lose nothing
# --------------------------------------------------------------------------- #
def test_chunk_partitions_without_loss_or_overlap():
    items = list(range(13))
    batches = producer.chunk(items, 5)
    assert [len(b) for b in batches] == [5, 5, 3]
    assert [x for b in batches for x in b] == items  # order preserved, nothing dropped


def test_chunk_rejects_non_positive_size():
    with pytest.raises(ValueError):
        producer.chunk([1, 2, 3], 0)


def test_restamp_shifts_event_ts_preserving_format_and_other_fields():
    seed_file = sorted((FIX / "raw").glob("*.json"))[0]
    rows = producer.parse_ndjson(seed_file.read_text(encoding="utf-8"))
    shifted = producer.restamp_records(rows, shift=timedelta(hours=2))
    for before, after in zip(rows, shifted):
        if before["event_ts"] is None:  # dirty seed rows carry null event_ts
            continue
        assert after["event_ts"].endswith("Z") and "T" in after["event_ts"]
        assert producer._parse_iso_ms(after["event_ts"]) - producer._parse_iso_ms(
            before["event_ts"]) == timedelta(hours=2)
        # Non-timestamp fields untouched.
        assert {k: after[k] for k in after if k != "event_ts"} == {
            k: before[k] for k in before if k != "event_ts"}


# --------------------------------------------------------------------------- #
# 4. Filenames sort lexically by (time, seq)
# --------------------------------------------------------------------------- #
def test_landing_filenames_are_unique_and_sortable():
    from datetime import datetime, timezone

    at = datetime(2026, 6, 27, 14, 22, 33, tzinfo=timezone.utc)
    names = [producer.landing_filename(i, at=at) for i in range(1, 4)]
    assert names == sorted(names)
    assert names[0] == "live_20260627T142233_000001.json"
    assert len(set(names)) == 3


# --------------------------------------------------------------------------- #
# 5. Mode B batching — flush on the size boundary and the final drain
# --------------------------------------------------------------------------- #
def _load_mode_b():
    """Import producers/producer.py (not a package) by file path."""
    spec = importlib.util.spec_from_file_location(
        "mode_b_producer", ROOT / "producers" / "producer.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _CollectingSink:
    def __init__(self):
        self.files = []

    def write(self, filename, content):
        self.files.append((filename, content))


def test_batcher_flushes_on_size_and_final_drain():
    mode_b = _load_mode_b()
    sink = _CollectingSink()
    # Large flush window so only the size boundary (not time) triggers mid-run.
    batcher = mode_b._Batcher(sink, batch_size=2, flush_seconds=9999)

    rec = producer.normalize_binance_trade(BINANCE_TRADE)
    batcher.add(rec)
    assert sink.files == []          # buffer not yet full
    batcher.add(rec)
    assert len(sink.files) == 1      # size boundary flushed a 2-trade file
    batcher.add(rec)
    batcher.flush()                  # final drain emits the partial buffer
    assert len(sink.files) == 2

    first_name, first_content = sink.files[0]
    assert first_name.startswith("live_") and first_name.endswith(".json")
    assert producer.parse_ndjson(first_content) == [
        {k: rec[k] for k in producer.RECORD_FIELDS}] * 2


def test_dry_run_sink_makes_no_network_calls():
    """--dry-run path uses a sink that only prints — safe to construct offline."""
    mode_b = _load_mode_b()
    sink = mode_b._DryRunSink()
    sink.write("live_x.json", producer.to_ndjson([]))  # must not raise / connect
