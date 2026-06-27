# Table contracts (frozen in WP0)

These schemas are the **glue that makes parallel work possible**. They are frozen
in Work Package 0 *before* any layer is built. Every downstream package develops
against these contracts using a seeded fixture, so layers can be built
simultaneously without the upstream layer existing yet.

> **Once committed in WP0, treat this file as read-only.** Changing a contract is
> a coordinated, breaking change across every package that reads it.

Catalog: `mktpulse`. Schemas: `bronze`, `silver`, `gold`, `ops`.

---

## `bronze.trades`  (raw landing → Delta)

| Column        | Type      | Notes                                  |
|---------------|-----------|----------------------------------------|
| `event_ts`    | TIMESTAMP | Trade event time (from source)         |
| `symbol`      | STRING    | e.g. `BTCUSDT`                         |
| `price`       | DOUBLE    | Trade price                            |
| `qty`         | DOUBLE    | Trade quantity                         |
| `side`        | STRING    | `buy` / `sell` (taker side)            |
| `trade_id`    | STRING    | Source trade id                        |
| `ingest_ts`   | TIMESTAMP | Added at ingestion                     |
| `_source_file`| STRING    | Auto Loader source file path           |

Null-key rows (`event_ts`/`symbol`/`trade_id` null) route to `bronze.trades_quarantine`
(same schema + a `_quarantine_reason STRING` column).

## `silver.trades_1min`  (stateful 1-min windows · key = `window_start` + `symbol`)

| Column           | Type      | Notes                                 |
|------------------|-----------|---------------------------------------|
| `window_start`   | TIMESTAMP | Window start (1-min tumbling)         |
| `window_end`     | TIMESTAMP | Window end                            |
| `symbol`         | STRING    |                                       |
| `open`           | DOUBLE    | First trade price in window           |
| `high`           | DOUBLE    | Max price                             |
| `low`            | DOUBLE    | Min price                             |
| `close`          | DOUBLE    | Last trade price                      |
| `volume`         | DOUBLE    | Sum of `qty`                          |
| `trade_count`    | BIGINT    | Count of trades                       |
| `taker_buy_ratio`| DOUBLE    | buy-volume / total-volume             |

Watermark: `event_ts` with `2 minutes` tolerance. Upsert via `foreachBatch` + `MERGE`
on `(window_start, symbol)`.

## `gold.market_pulse`  (one row per minute per symbol)

| Column           | Type      | Notes                                          |
|------------------|-----------|------------------------------------------------|
| `window_start`   | TIMESTAMP |                                                |
| `symbol`         | STRING    |                                                |
| `candle_direction`| STRING   | `up` / `down` / `flat`                         |
| `momentum_signal`| STRING    | CASE on `taker_buy_ratio` + direction          |
| `volume_spike`   | BOOLEAN   | `volume` > 2× 30-min rolling avg               |
| `volatility`     | DOUBLE    | (high − low) / open                            |
| `rolling_vol_30m`| DOUBLE    | 30-min rolling avg volume                      |
| `generated_at`   | TIMESTAMP |                                                |

Three gold assertions: unique `(window_start, symbol)`, no negative volume, valid
`momentum_signal` enum.

## `ops.dq_failures`  (data-quality expectation failures · append-only)

Generic, forward-compatible sink so WP5's expectation helpers can record failures
without a contract break. Written by `src/quality.py` (and Lakeflow expectations);
never silently drop bad rows.

| Column         | Type      | Notes                                  |
|----------------|-----------|----------------------------------------|
| `check_ts`     | TIMESTAMP | When the expectation ran               |
| `layer`        | STRING    | `bronze` / `silver` / `gold`           |
| `table_name`   | STRING    | Fully-qualified table checked          |
| `expectation`  | STRING    | Rule name                              |
| `severity`     | STRING    | `warn` / `drop` / `fail`               |
| `failed_count` | BIGINT    | Rows failing the rule                  |
| `sample`       | STRING    | JSON sample of offending value(s)      |
| `run_id`       | STRING    | Streaming / job run id                 |

---

The shared `src/config.py` module exposes `catalog`, `schema_*`, `volume_path`,
`checkpoint_root` so every notebook is parameterised against these contracts.
