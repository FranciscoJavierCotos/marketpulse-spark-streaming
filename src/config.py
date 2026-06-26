"""Shared, parameterised configuration for every MarketPulse notebook and module.

Single source of truth for catalog/schema names, the Unity Catalog Volume landing
path, and checkpoint roots. Every notebook imports from here so nothing is
hard-coded and parallel work packages can isolate their runtime namespaces via a
``dev_suffix`` (see the parallelization strategy in the README).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Resolved configuration for a MarketPulse run.

    Parameters
    ----------
    catalog:
        Unity Catalog catalog name. Defaults to ``mktpulse``.
    dev_suffix:
        Optional suffix appended to schema names (e.g. ``_dev_wp2``) so a CLI
        instance building one work package never collides with another's Delta
        logs or state. Empty string in production.
    """

    catalog: str = "mktpulse"
    dev_suffix: str = ""

    # ---- schemas -------------------------------------------------------
    @property
    def schema_bronze(self) -> str:
        return f"bronze{self.dev_suffix}"

    @property
    def schema_silver(self) -> str:
        return f"silver{self.dev_suffix}"

    @property
    def schema_gold(self) -> str:
        return f"gold{self.dev_suffix}"

    @property
    def schema_ops(self) -> str:
        """Ops schema: checkpoints metadata, quarantine and DQ-failure tables."""
        return f"ops{self.dev_suffix}"

    # ---- fully-qualified table names ----------------------------------
    @property
    def tbl_bronze_trades(self) -> str:
        return f"{self.catalog}.{self.schema_bronze}.trades"

    @property
    def tbl_bronze_quarantine(self) -> str:
        return f"{self.catalog}.{self.schema_bronze}.trades_quarantine"

    @property
    def tbl_silver_trades_1min(self) -> str:
        return f"{self.catalog}.{self.schema_silver}.trades_1min"

    @property
    def tbl_gold_market_pulse(self) -> str:
        return f"{self.catalog}.{self.schema_gold}.market_pulse"

    @property
    def tbl_dq_failures(self) -> str:
        return f"{self.catalog}.{self.schema_ops}.dq_failures"

    # ---- volume / filesystem paths ------------------------------------
    @property
    def volume_path(self) -> str:
        """Landing folder Auto Loader watches (≈ the S3 raw bucket)."""
        return f"/Volumes/{self.catalog}/{self.schema_bronze}/raw"

    @property
    def checkpoint_root(self) -> str:
        """Root for streaming checkpoints; one subfolder per stream."""
        return f"/Volumes/{self.catalog}/{self.schema_ops}/checkpoints{self.dev_suffix}"

    def checkpoint(self, stream: str) -> str:
        return f"{self.checkpoint_root}/{stream}"


# Default production config; override in notebooks with Config(dev_suffix="_dev_wpN").
CONFIG = Config()
