# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze streaming ingestion (WP1)
# MAGIC Auto Loader (`cloudFiles`) over the raw Volume → `bronze.trades` (Delta,
# MAGIC checkpointed, idempotent). Adds `ingest_ts` + `_source_file`; null-key rows
# MAGIC route to `bronze.trades_quarantine`. `Trigger.AvailableNow`.

# COMMAND ----------
# TODO(WP1): implement Auto Loader readStream + writeStream + quarantine.
