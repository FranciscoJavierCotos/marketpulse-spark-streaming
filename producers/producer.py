"""Mode B — local Binance WebSocket producer (WP4, optional flex).

Runs on your laptop (Free Edition can't reach the Binance WS from a notebook),
writes small JSON/Parquet files, and pushes them to the UC Volume via the
Databricks SDK/CLI. Emits the SAME file schema as the Mode A replay producer so
the Spark code never changes.
"""

# TODO(WP4): implement Binance WS subscription -> small files -> Volume push.
