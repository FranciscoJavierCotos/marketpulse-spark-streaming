"""Mode B — local Binance WebSocket producer (WP4, optional flex).

Free Edition can't reach the Binance WebSocket *from a notebook* (restricted
outbound internet), so this runs on **your laptop**: it subscribes to the Binance
trade stream, normalises each message into the project's clean NDJSON landing
shape (via :mod:`src.producer`, shared with the Mode A replay), batches trades
into small files, and **pushes them to the Unity Catalog Volume** with the
Databricks SDK. The outbound call goes *to* Databricks, which Free Edition allows;
Auto Loader (`01_bronze`) then picks the files up exactly as it does Mode A's.

Because both producers emit the identical file schema, nothing in the Spark layers
changes when you switch from replay to live.

Usage
-----
::

    pip install -r requirements-producer.txt
    export DATABRICKS_HOST=...  DATABRICKS_TOKEN=...   # or use a CLI profile
    python producers/producer.py --symbols BTCUSDT ETHUSDT --max-trades 200

    # Inspect the normalised output without any Databricks calls:
    python producers/producer.py --dry-run --max-trades 20

Flags: ``--volume-path`` (target, defaults to ``src.config``'s landing path),
``--batch-size`` / ``--flush-seconds`` (file cadence), ``--max-trades`` (stop
after N — keep runs bounded), ``--dry-run`` (print files, never upload).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

# Make the repo root importable so `src.*` resolves when run as a script from anywhere.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src import producer  # noqa: E402
from src.config import Config  # noqa: E402

BINANCE_WS = "wss://stream.binance.com:9443/stream"


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mode B local Binance WS → UC Volume producer.")
    p.add_argument("--symbols", nargs="+", default=list(producer.DEFAULT_SYMBOLS),
                   help="Symbols to subscribe to (e.g. BTCUSDT ETHUSDT).")
    p.add_argument("--volume-path", default=None,
                   help="Target UC Volume path. Defaults to src.config's landing path.")
    p.add_argument("--dev-suffix", default="",
                   help="Config dev_suffix to isolate a parallel namespace's Volume.")
    p.add_argument("--batch-size", type=int, default=50,
                   help="Trades buffered before a file is flushed.")
    p.add_argument("--flush-seconds", type=float, default=10.0,
                   help="Max seconds before a partial buffer is flushed.")
    p.add_argument("--max-trades", type=int, default=0,
                   help="Stop after N trades (0 = run until interrupted). Keep runs bounded.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the NDJSON files instead of uploading to the Volume.")
    return p.parse_args(argv)


# --------------------------------------------------------------------------- #
# Sinks — where a flushed batch of normalised records goes
# --------------------------------------------------------------------------- #
class _DryRunSink:
    """Prints each batch as NDJSON; makes no Databricks calls (offline demo/test)."""

    def write(self, filename: str, content: str) -> None:
        print(f"--- {filename} ({content.count(chr(10))} trades) ---")
        print(content, end="")


class _VolumeSink:
    """Uploads each batch to the UC Volume via the Databricks SDK Files API."""

    def __init__(self, volume_path: str) -> None:
        from databricks.sdk import WorkspaceClient  # lazy: only needed for real runs

        self._client = WorkspaceClient()
        self._volume_path = volume_path.rstrip("/")

    def write(self, filename: str, content: str) -> None:
        import io

        dest = f"{self._volume_path}/{filename}"
        # Files API takes a binary stream; wrap the NDJSON text in a BytesIO.
        self._client.files.upload(dest, io.BytesIO(content.encode("utf-8")), overwrite=True)
        print(f"uploaded {dest} ({content.count(chr(10))} trades)")


# --------------------------------------------------------------------------- #
# Batching — buffer normalised trades, flush to small NDJSON files
# --------------------------------------------------------------------------- #
class _Batcher:
    """Buffers normalised records and flushes them as size/time-bounded files."""

    def __init__(self, sink, batch_size: int, flush_seconds: float) -> None:
        self._sink = sink
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._buf: list[dict] = []
        self._seq = 0
        self._last_flush = time.monotonic()

    def add(self, record: dict) -> None:
        self._buf.append(record)
        if len(self._buf) >= self._batch_size:
            self.flush()

    def maybe_flush(self) -> None:
        if self._buf and (time.monotonic() - self._last_flush) >= self._flush_seconds:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        self._seq += 1
        filename = producer.landing_filename(self._seq, prefix="live", at=datetime.now(timezone.utc))
        self._sink.write(filename, producer.to_ndjson(self._buf))
        self._buf.clear()
        self._last_flush = time.monotonic()


# --------------------------------------------------------------------------- #
# Run loop
# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> None:
    cfg = Config(dev_suffix=args.dev_suffix)
    volume_path = args.volume_path or cfg.volume_path
    sink = _DryRunSink() if args.dry_run else _VolumeSink(volume_path)
    batcher = _Batcher(sink, args.batch_size, args.flush_seconds)

    # Combined stream: wss://…/stream?streams=btcusdt@trade/ethusdt@trade
    streams = "/".join(f"{s.lower()}@trade" for s in args.symbols)
    url = f"{BINANCE_WS}?streams={streams}"
    print(f"Subscribing to {len(args.symbols)} symbol(s): {', '.join(args.symbols)}")
    print(f"Sink: {'dry-run (no upload)' if args.dry_run else volume_path}")

    from websocket import create_connection  # lazy: only the live path needs it

    ws = create_connection(url)
    count = 0
    try:
        while True:
            # Combined streams wrap the payload as {"stream": ..., "data": {...}}.
            payload = json.loads(ws.recv())
            data = payload.get("data", payload)
            if data.get("e") != "trade":  # ignore non-trade control frames
                batcher.maybe_flush()
                continue
            batcher.add(producer.normalize_binance_trade(data))
            count += 1
            batcher.maybe_flush()
            if args.max_trades and count >= args.max_trades:
                print(f"Reached max_trades={args.max_trades}; stopping.")
                break
    except KeyboardInterrupt:
        print("\nInterrupted; flushing buffered trades.")
    finally:
        batcher.flush()
        ws.close()
        print(f"Produced {count} trades.")


def main(argv: list[str] | None = None) -> None:
    run(parse_args(argv))


if __name__ == "__main__":
    main()
