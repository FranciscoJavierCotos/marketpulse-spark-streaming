# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold business marts (WP3)
# MAGIC `candle_direction`, `momentum_signal`, `volume_spike` (>2× 30-min rolling avg
# MAGIC via window function), `volatility`; incremental MERGE with a small lookback
# MAGIC for late arrivals → `gold.market_pulse`.

# COMMAND ----------
# TODO(WP3): implement gold signals + incremental MERGE.
