from __future__ import annotations

from abc import ABC
from typing import ClassVar

from pyspark.sql import SparkSession

from lakehouse.config import LakehouseConfig
from lakehouse.core.layers import Layer


class Table(ABC):
    """
    Base for every Iceberg table.
    """

    layer: ClassVar[Layer]

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Register every concrete table (a class with its own table_name) in its layer."""
        super().__init_subclass__(**kwargs)
        if "table_name" in cls.__dict__:
            cls.layer.register(cls)

    def __init__(self, spark: SparkSession, config: LakehouseConfig) -> None:
        """Bind the table to a Spark session and an environment config."""
        self.spark = spark
        self.config = config

