# AI-assisted: written with the help of an AI coding assistant (Claude Code).
"""Local SparkSession fixture + a fresh, isolated Iceberg catalog per test."""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakehouse.config import LakehouseConfig
from lakehouse.spark import create_spark

# Spark runs in UTC; make Python agree so collected/created timestamps aren't shifted.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    config = LakehouseConfig(
        env="test",
        warehouse=str(tmp_path_factory.mktemp("warehouse")),
        spark_master="local[2]",
    )
    session = create_spark(config)
    yield session
    session.stop()


@pytest.fixture
def config(spark: SparkSession, tmp_path: Path) -> LakehouseConfig:
    """A config pointing at a brand-new catalog and landing folder, so tests never share tables."""
    catalog = f"test_{uuid.uuid4().hex[:8]}"
    warehouse = tmp_path / "warehouse"
    landing = tmp_path / "landing"
    landing.mkdir()
    prefix = f"spark.sql.catalog.{catalog}"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{prefix}.type", "hadoop")
    spark.conf.set(f"{prefix}.warehouse", warehouse.as_uri())
    return LakehouseConfig(
        env="test", catalog=catalog, warehouse=str(warehouse), landing_dir=str(landing)
    )


def write_csv(folder: str | Path, name: str, header: str, rows: list[str]) -> Path:
    """Write a CSV the way the source system does: UTF-8 with a byte-order mark."""
    path = Path(folder) / name
    path.write_text("﻿" + "\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path
