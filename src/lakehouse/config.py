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
        shuffle_partitions: number of partitions after a shuffle.
        iceberg_package: Maven coordinates of the Iceberg Spark runtime.
        price_history_anchor: month in which the current product prices become valid
            (products.csv has no dates, see assumption A1 in docs/data_analysis.md).
    """

    env: str
    catalog: str = "lakehouse"
    warehouse: str = str(PROJECT_ROOT / "data" / "warehouse")
    # Folder the source systems drop their CSV files into. Each bronze table picks up the
    # files matching its own prefix (customers*.csv, products*.csv, orders*.csv).
    landing_dir: str = str(PROJECT_ROOT / "assignment")
    spark_master: str = "local[*]"
    shuffle_partitions: int = 8
    iceberg_package: str = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"
    price_history_anchor: str = "2011-12-01"


ENVIRONMENTS: dict[str, LakehouseConfig] = {
    "local": LakehouseConfig(env="local"),
}


def get_config(env: str) -> LakehouseConfig:
    """Return the config of an environment."""
    return ENVIRONMENTS[env]
