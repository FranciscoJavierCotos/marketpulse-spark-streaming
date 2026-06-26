# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Mode A replay producer (WP4)
# MAGIC Drips the seeded historical dataset into the landing Volume on a timer to
# MAGIC simulate a stream. Emits the **same file schema** as the Mode B local
# MAGIC producer (`producers/producer.py`).

# COMMAND ----------
# TODO(WP4): implement timed replay generator writing into volume_path.
