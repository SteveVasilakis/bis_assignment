"""LakehouseConfig + per-environment values."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# Defaults are anchored to the repository, so runs work from any working directory
# (e.g. PyCharm runs a file from its own folder).
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class LakehouseConfig:
    """
    Settings for one environment.

    Attributes:
        env: environment name, e.g. "local".
        catalog: name of the Iceberg catalog in Spark.
        warehouse: where the Iceberg tables are stored (path or URI).
        landing_dir: folder the source CSV files are read from.
        spark_master: Spark master URL, e.g. "local[*]".
        iceberg_package: Maven coordinates of the Iceberg Spark runtime.
    """

    env: str
    catalog: str = "lakehouse"
    warehouse: str = str(PROJECT_ROOT / "data" / "warehouse")
    # Folder the source systems drop their CSV files into. Each bronze table picks up the
    # files matching its own prefix (customers*.csv, products*.csv, orders*.csv).
    landing_dir: str = str(PROJECT_ROOT / "assignment")
    spark_master: str = "local[*]"
    iceberg_package: str = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"


ENVIRONMENTS: dict[str, LakehouseConfig] = {
    "local": LakehouseConfig(env="local"),
}


def get_config(env: str) -> LakehouseConfig:
    """Return the config of an environment."""
    return ENVIRONMENTS[env]
