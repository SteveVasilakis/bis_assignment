from __future__ import annotations

import logging
from graphlib import TopologicalSorter

from pyspark.sql import SparkSession

from lakehouse.config import LakehouseConfig
from lakehouse.core.layers import LAYERS
from lakehouse.core.table import Table

log = logging.getLogger("lakehouse.pipeline")


class Pipeline:
    """Builds every table registered in the layers, in dependency order."""

    def __init__(self, spark: SparkSession, config: LakehouseConfig) -> None:
        self.spark = spark
        self.config = config

    # this method is product of AI,
    # in order to be able to run the tables in order based on dependencies
    def execution_order(self) -> list[type[Table]]:
        """
        All registered tables, each one after the tables it depends on (topological order).
        Usage of AI to for an easier way to sort out the dependencies.
        """
        tables = [t for layer in LAYERS for t in layer.tables.values()]
        return list(TopologicalSorter({t: set(t.dependencies) for t in tables}).static_order())

    def run(self) -> None:
        """
        Build every table in dependency order.

        A failing table only skips the tables that depend on it; raises RuntimeError at the
        end if any table failed or was skipped.
        """
        failed: set[type[Table]] = set()

        for table_cls in self.execution_order():
            name = table_cls.qualified_name()
            # A failed table blocks only its downstream tables, not unrelated ones.
            if any(dep in failed for dep in table_cls.dependencies):
                log.warning(f"Skipping {name}: an upstream table failed")
                failed.add(table_cls)
                continue
            try:
                self.build(table_cls)
            except Exception:
                log.exception(f"Failed {name}")
                failed.add(table_cls)

        if failed:
            names = sorted(t.qualified_name() for t in failed)
            raise RuntimeError(f"Run finished with failed/skipped tables: {names}")
        log.info("Run succeeded")

    def build(self, table_cls: type[Table]) -> None:
        """Build one table: compute, cast to the schema and write it."""
        table = table_cls(self.spark, self.config)
        inputs = {dep: dep(self.spark, self.config).read() for dep in table_cls.dependencies}
        log.info(f"Building {table.full_name}")

        df = table.enforce_schema(table.run(inputs))
        table.write(df)
