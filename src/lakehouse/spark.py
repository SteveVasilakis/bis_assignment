"""create_spark()"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from lakehouse.config import LakehouseConfig


def create_spark(config: LakehouseConfig) -> SparkSession:
    """
    Create (or reuse) a SparkSession with an Iceberg catalog named `config.catalog`.

    It's a Hadoop catalog: the tables are folders under `config.warehouse`, so no metastore
    or catalog service is needed.
    """
    catalog = f"spark.sql.catalog.{config.catalog}"
    warehouse = config.warehouse
    if "://" not in warehouse:
        warehouse = Path(warehouse).resolve().as_uri()

    builder = (
        SparkSession.builder.appName("lakehouse")
        .master(config.spark_master)
        .config("spark.jars.packages", config.iceberg_package)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "hadoop")
        .config(f"{catalog}.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        # Iceberg's vectorized Parquet reader crashes the JVM on macOS arm64.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
    )
    if config.spark_master.startswith("local"):
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
