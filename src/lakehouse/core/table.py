from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lakehouse.config import LakehouseConfig
from lakehouse.core.dq import Rule
from lakehouse.core.layers import Layer


class Table(ABC):
    """
    Base for every Iceberg table.
    """

    layer: ClassVar[Layer]
    table_name: ClassVar[str]
    schema: ClassVar[StructType]
    dependencies: ClassVar[tuple[type[Table], ...]] = ()

    rules: ClassVar[tuple[Rule, ...]] = ()

    write_mode: ClassVar[str] = "overwrite"  # "append" | "overwrite" | "merge"
    merge_keys: ClassVar[tuple[str, ...]] = ()
    partitioned_by: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Register every concrete table (a class with its own table_name) in its layer."""
        super().__init_subclass__(**kwargs)
        if "table_name" in cls.__dict__:
            cls.layer.register(cls)

    def __init__(self, spark: SparkSession, config: LakehouseConfig) -> None:
        """Bind the table to a Spark session and an environment config."""
        self.spark = spark
        self.config = config

    @classmethod
    def qualified_name(cls) -> str:
        """Name without the catalog, e.g. "silver.orders"."""
        return f"{cls.layer.database}.{cls.table_name}"

    @property
    def full_name(self) -> str:
        """Name with the catalog, e.g. "lakehouse.silver.orders"."""
        return f"{self.config.catalog}.{self.qualified_name()}"

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

    # ---------------- Iceberg I/O ----------------

    def read(self) -> DataFrame:
        """Read the Iceberg table, creating it empty first if it doesn't exist yet."""
        self.create_if_not_exists()
        return self.spark.table(self.full_name)

    def create_if_not_exists(self) -> None:
        """Create the table from the declared schema, not from whatever df happened to be."""
        if self.spark.catalog.tableExists(self.full_name):
            return
        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {self.config.catalog}.{self.layer.database}"
        )
        (
            self.spark.createDataFrame([], self.schema)
            .writeTo(self.full_name)
            .using("iceberg")
            .create()
        )
        for transform in self.partitioned_by:
            self.spark.sql(f"ALTER TABLE {self.full_name} ADD PARTITION FIELD {transform}")

    def write(self, df: DataFrame) -> None:
        """Write df according to write_mode: append, replace the table, or merge on merge_keys."""
        self.create_if_not_exists()
        if self.write_mode == "append":
            df.writeTo(self.full_name).append()
        elif self.write_mode == "overwrite":
            df.writeTo(self.full_name).overwrite(F.lit(True))
        else:
            self._merge(df)

    def _merge(self, df: DataFrame) -> None:
        view = f"_src_{self.layer.database}_{self.table_name}"
        on = " AND ".join(f"t.{k} = s.{k}" for k in self.merge_keys)
        df.createOrReplaceTempView(view)
        try:
            self.spark.sql(f"""
                MERGE INTO {self.full_name} t
                USING {view} s
                ON {on}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        finally:
            self.spark.catalog.dropTempView(view)
