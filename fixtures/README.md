# Fixtures — seeded MarketPulse dataset (frozen in WP0)

Deterministic seed data for the pipeline, produced by
[`generate_fixtures.py`](./generate_fixtures.py). Everything here is **generated and
committed** so layers can be built in parallel against the frozen
[`CONTRACTS.md`](../CONTRACTS.md), and so local `pytest` needs no PySpark.

## Regenerate

```bash
python fixtures/generate_fixtures.py
```

Stdlib only, fixed seed (`SEED = 20260627`) — output is **byte-identical** on every
run (a test asserts this). If you change the generator, regenerate and commit the
result in the same change.

## Scale

3 symbols (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`) × 60 minutes starting
`2026-06-27T10:00:00Z`, ~8–20 trades/min/symbol (~2.5k trades). Includes:

- **6 deliberately dirty rows** (null `event_ts` / `symbol` / `trade_id`) → land in
  quarantine, not bronze — feed WP1's quarantine path and WP5's DQ.
- **2 late / out-of-order rows** — one inside the 2-min watermark, one beyond it —
  exercise WP2's watermark. Silver places them in their true event-time window.
- **A volume spike** (minute 45, ~12×) per symbol so WP3's `volume_spike` has a
  visible positive case.

## Layout

```
fixtures/
  raw/        trades_2026-06-27T1000.json … (one NDJSON file per landing minute, 60 files)
  bronze/     trades.csv            (clean rows + ingest_ts + _source_file)
              trades_quarantine.csv (the dirty rows + _quarantine_reason)
  silver/     trades_1min.csv       (1-min tumbling OHLCV/volume/trade_count/taker_buy_ratio)
```

## Raw NDJSON record (the ingest contract)

One trade object per line, **clean field names** (not Binance-native `a`/`p`/`q`/…).
WP1 reads these via Auto Loader with `cloudFiles.format = "json"`.

```json
{"event_ts": "2026-06-27T10:00:03.412Z", "symbol": "BTCUSDT", "price": 61234.5, "qty": 0.0123, "side": "buy", "trade_id": "BTCUSDT-1000001"}
```

| Field      | Type   | Notes                                            |
|------------|--------|--------------------------------------------------|
| `event_ts` | string | ISO-8601 UTC (millis). Epoch-millis is the realistic alt; WP1 owns the cast. |
| `symbol`   | string | e.g. `BTCUSDT`                                   |
| `price`    | number | trade price (double)                             |
| `qty`      | number | trade quantity (double)                          |
| `side`     | string | `buy` / `sell` (taker side)                      |
| `trade_id` | string | source trade id                                  |

Dirty rows carry `null` in the offending key — that's intentional.

## Derivation (matches the frozen contracts)

- **bronze** = raw rows with non-null `event_ts`/`symbol`/`trade_id`, plus
  `ingest_ts` and `_source_file` (the raw filename under the landing Volume).
- **quarantine** = the excluded dirty rows plus `_quarantine_reason`.
- **silver** = 1-min tumbling OHLCV per `(window_start, symbol)` over the clean
  bronze rows (`open`/`close` = first/last by `event_ts`, `high`/`low` = max/min
  `price`, `volume` = Σ`qty`, `taker_buy_ratio` = buy-`qty`/total-`qty`). This makes
  the silver fixture WP2's correctness oracle — see `tests/test_contracts.py`.
