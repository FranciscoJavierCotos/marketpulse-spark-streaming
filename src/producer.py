"""Shared producer logic (WP4) — one definition of the landing-file shape.

Both producers write the **same raw NDJSON file schema** so the Spark code never
changes which producer fed it (CLAUDE.md / README "Mode A vs Mode B"):

* **Mode A — replay** (``notebooks/04_replay_producer.py``): drips the committed
  ``fixtures/raw/*.json`` seed into the landing Volume on a timer.
* **Mode B — live** (``producers/producer.py``): subscribes to the Binance trade
  WebSocket, **normalises** each Binance-native message into this shape, batches
  them into small files, and pushes them to the Volume via the Databricks SDK.

The reusable, side-effect-free core lives here so it is unit-tested in CI without
PySpark, a network, or the Databricks SDK — mirroring the pure twin pattern of
:mod:`src.bronze` and :mod:`src.silver`. The notebook and the local script render
these helpers; the I/O (dbutils / websocket / SDK) stays in the thin outer shells.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

# The raw landing contract: clean field names, fixed order (matches the raw NDJSON
# the fixture generator emits and the schema Auto Loader reads in 01_bronze.py). A
# single tuple keeps every producer — and the tests — agreed on the shape.
RECORD_FIELDS: tuple[str, ...] = ("event_ts", "symbol", "price", "qty", "side", "trade_id")

# Default symbols for the Mode B live producer (lowercase is what Binance stream
# names use; the normalised record carries the upper-case symbol).
DEFAULT_SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


# --------------------------------------------------------------------------- #
# Timestamp formatting — identical millisecond ISO shape to the fixtures
# --------------------------------------------------------------------------- #
def iso_ms(dt: datetime) -> str:
    """ISO-8601 UTC string with millisecond precision and a trailing ``Z``.

    Byte-for-byte the format ``fixtures/generate_fixtures.py`` emits, so a live
    Mode B record is indistinguishable from a replayed one to Auto Loader.
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def iso_ms_from_epoch_millis(ms: int) -> str:
    """Render Binance's epoch-millisecond trade time as :func:`iso_ms`.

    Integer ``divmod`` (not float division) keeps the millisecond field exact —
    ``ms / 1000`` would introduce binary-float rounding in the sub-second part.
    """
    seconds, millis = divmod(int(ms), 1000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


# --------------------------------------------------------------------------- #
# Mode B core — normalise a Binance trade message into the landing shape
# --------------------------------------------------------------------------- #
def normalize_binance_trade(msg: Mapping[str, Any]) -> dict:
    """Map one Binance ``<symbol>@trade`` payload onto the raw landing contract.

    Binance sends native single-letter keys; we translate them to the project's
    clean field names so the Spark layers never see Binance-isms (CLAUDE.md):

    ======  =====================================  ============================
    Binance Meaning                                Landing field
    ======  =====================================  ============================
    ``T``   trade time (epoch ms)                  ``event_ts`` (ISO-8601 ms)
    ``s``   symbol                                 ``symbol``
    ``p``   price (string)                         ``price`` (float)
    ``q``   quantity (string)                      ``qty`` (float)
    ``m``   *is the buyer the market maker?*       ``side`` (taker side)
    ``t``   trade id                               ``trade_id`` (``<symbol>-<t>``)
    ======  =====================================  ============================

    The taker side is the subtle one: Binance's ``m`` flags whether the **buyer**
    was the market maker. If the buyer is the maker, the *taker* is the seller, so
    ``m == True`` ⇒ ``side == "sell"`` (and ``False`` ⇒ ``"buy"``). The trade id is
    namespaced with the symbol to match the fixture style (``BTCUSDT-1000001``) and
    stay globally unique across the multi-symbol stream.
    """
    symbol = str(msg["s"])
    return {
        "event_ts": iso_ms_from_epoch_millis(msg["T"]),
        "symbol": symbol,
        "price": float(msg["p"]),
        "qty": float(msg["q"]),
        "side": "sell" if msg["m"] else "buy",
        "trade_id": f"{symbol}-{msg['t']}",
    }


# --------------------------------------------------------------------------- #
# Serialization — records → NDJSON (one trade object per line)
# --------------------------------------------------------------------------- #
def record_to_ndjson_line(record: Mapping[str, Any]) -> str:
    """Serialise one record to a single JSON line in the fixed field order.

    Projecting onto :data:`RECORD_FIELDS` drops any extra keys and pins column
    order, so every producer's output is identical regardless of input dict order.
    """
    return json.dumps({k: record[k] for k in RECORD_FIELDS})


def to_ndjson(records: Iterable[Mapping[str, Any]]) -> str:
    """Serialise records to an NDJSON document (trailing newline per line).

    Matches the fixture generator's ``json.dumps(rec) + "\\n"`` writer so a file
    produced here is byte-shaped like the committed seed.
    """
    return "".join(record_to_ndjson_line(r) + "\n" for r in records)


def parse_ndjson(text: str) -> list[dict]:
    """Parse an NDJSON document back into a list of records (blank lines ignored)."""
    return [json.loads(line) for line in text.splitlines() if line.strip()]


# --------------------------------------------------------------------------- #
# Mode A core — pacing helpers (pure; the notebook adds the sleep + dbutils I/O)
# --------------------------------------------------------------------------- #
def chunk(items: Sequence[Any], size: int) -> list[list[Any]]:
    """Split ``items`` into consecutive chunks of at most ``size`` (last is shorter).

    The replay notebook drips one chunk per timer tick; keeping the chunking pure
    lets a test assert no file is dropped or duplicated across the batches.
    """
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def restamp_records(records: Iterable[Mapping[str, Any]], *, shift: timedelta) -> list[dict]:
    """Return copies of ``records`` with ``event_ts`` shifted by ``shift``.

    Mode A's optional "live" demo mode re-bases the historical seed's event times
    onto wall-clock now so the dashboard looks live. Off by default — the faithful
    replay keeps the seed's timestamps so silver/gold reproduce the fixtures
    end-to-end. ``event_ts`` is parsed and re-rendered with :func:`iso_ms` so the
    output format is unchanged.
    """
    out: list[dict] = []
    for r in records:
        shifted = dict(r)
        shifted["event_ts"] = iso_ms(_parse_iso_ms(r["event_ts"]) + shift)
        out.append(shifted)
    return out


def _parse_iso_ms(value: str) -> datetime:
    """Parse an :func:`iso_ms` string (``…Z``) back into an aware UTC datetime."""
    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Filenames — unique, lexically sortable so Auto Loader discovery is stable
# --------------------------------------------------------------------------- #
def landing_filename(seq: int, *, prefix: str = "live", at: datetime | None = None) -> str:
    """Build a landing-file name like ``live_20260627T142233_000007.json``.

    The UTC timestamp plus a zero-padded sequence makes names unique and
    lexically sortable, so Auto Loader's file discovery sees a stable order even
    when several files land in the same second.
    """
    at = at or datetime.now(timezone.utc)
    return f"{prefix}_{at.strftime('%Y%m%dT%H%M%S')}_{seq:06d}.json"
