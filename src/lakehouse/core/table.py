from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

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

    @abstractmethod
    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        """Transform the inputs and return the resulting DataFrame."""

    def enforce_schema(self, df: DataFrame) -> DataFrame:
        """Cast to the declared schema: same columns, same order, missing ones as NULL."""
        return df.select(
            *[
                (F.col(f.name) if f.name in df.columns else F.lit(None))
                .cast(f.dataType)
                .alias(f.name)
                for f in self.schema
            ]
        )
