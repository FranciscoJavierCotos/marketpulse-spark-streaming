# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver stateful streaming (WP2) — headline Spark-depth notebook
# MAGIC `withWatermark("event_ts","2 minutes")` → `groupBy(window("event_ts","1 minute"), "symbol")`
# MAGIC aggregating OHLCV + volume + `taker_buy_ratio`; watermark-bounded
# MAGIC `dropDuplicates`; idempotent upsert via `foreachBatch` + `MERGE` on
# MAGIC `(window_start, symbol)`. **Comment every streaming decision.**

# COMMAND ----------
# TODO(WP2): implement watermarked windowed aggregation + MERGE upsert.
