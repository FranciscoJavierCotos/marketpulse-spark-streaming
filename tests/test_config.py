"""Unit tests for the shared `src/config.py` parameterisation (WP0)."""

from src.config import Config


def test_defaults():
    cfg = Config()
    assert cfg.catalog == "mktpulse"
    assert cfg.dev_suffix == ""
    assert cfg.schema_bronze == "bronze"
    assert cfg.schema_silver == "silver"
    assert cfg.schema_gold == "gold"
    assert cfg.schema_ops == "ops"


def test_default_table_names():
    cfg = Config()
    assert cfg.tbl_bronze_trades == "mktpulse.bronze.trades"
    assert cfg.tbl_bronze_quarantine == "mktpulse.bronze.trades_quarantine"
    assert cfg.tbl_silver_trades_1min == "mktpulse.silver.trades_1min"
    assert cfg.tbl_gold_market_pulse == "mktpulse.gold.market_pulse"
    assert cfg.tbl_dq_failures == "mktpulse.ops.dq_failures"


def test_default_paths():
    cfg = Config()
    assert cfg.volume_path == "/Volumes/mktpulse/bronze/raw"
    assert cfg.checkpoint_root == "/Volumes/mktpulse/ops/checkpoints"
    assert cfg.checkpoint("bronze") == "/Volumes/mktpulse/ops/checkpoints/bronze"


def test_dev_suffix_propagates_into_schemas():
    cfg = Config(dev_suffix="_dev_wp2")
    assert cfg.schema_bronze == "bronze_dev_wp2"
    assert cfg.schema_silver == "silver_dev_wp2"
    assert cfg.schema_gold == "gold_dev_wp2"
    assert cfg.schema_ops == "ops_dev_wp2"


def test_dev_suffix_propagates_into_tables_and_paths():
    cfg = Config(dev_suffix="_dev_wp2")
    assert cfg.tbl_bronze_trades == "mktpulse.bronze_dev_wp2.trades"
    assert cfg.tbl_silver_trades_1min == "mktpulse.silver_dev_wp2.trades_1min"
    assert cfg.tbl_dq_failures == "mktpulse.ops_dev_wp2.dq_failures"
    assert cfg.volume_path == "/Volumes/mktpulse/bronze_dev_wp2/raw"
    # The checkpoints volume name itself carries the suffix (00_setup creates it so).
    assert cfg.checkpoint_root == "/Volumes/mktpulse/ops_dev_wp2/checkpoints_dev_wp2"


def test_custom_catalog():
    # Free Edition fallback (plan R1): catalog is parameterised, no contract break.
    cfg = Config(catalog="workspace")
    assert cfg.tbl_bronze_trades == "workspace.bronze.trades"
    assert cfg.volume_path == "/Volumes/workspace/bronze/raw"
