from __future__ import annotations

import logging
from graphlib import TopologicalSorter

from pyspark.sql import SparkSession

from lakehouse.config import LakehouseConfig
from lakehouse.core.audit import RunLog, now
from lakehouse.core.layers import LAYERS
from lakehouse.core.table import Table

log = logging.getLogger("lakehouse.pipeline")


class Pipeline:
    """Builds every table registered in the layers, in dependency order."""

    def __init__(self, spark: SparkSession, config: LakehouseConfig) -> None:
        self.spark = spark
        self.config = config
        self.run_log = RunLog(spark, config.catalog)

    # this method is product of AI,
    # in order to be able to run the tables in order based on dependencies
    def execution_order(self) -> list[type[Table]]:
        """
        All registered tables, each one after the tables it depends on (topological order).
        Usage of AI to for an easier way to sort out the dependencies.
        """
        tables = [t for layer in LAYERS for t in layer.tables.values()]
        return list(TopologicalSorter({t: set(t.dependencies) for t in tables}).static_order())

    def run(self, run_id: str | None = None) -> str:
        """
        Build every table in dependency order and log each build to ops.run_log.

        A failing table only skips the tables that depend on it. Returns the run id; raises
        RuntimeError at the end if any table failed or was skipped.
        """
        run_id = run_id or f"{now():%Y%m%d%H%M%S}"
        self.run_log.setup()
        failed: set[type[Table]] = set()

        for table_cls in self.execution_order():
            started = now()
            name, layer = table_cls.qualified_name(), table_cls.layer.database
            # A failed table blocks only its downstream tables, not unrelated ones.
            if any(dep in failed for dep in table_cls.dependencies):
                log.warning(f"Skipping {name}: an upstream table failed")
                failed.add(table_cls)
                self.run_log.log(run_id, name, layer, "SKIPPED", started, message="upstream failed")
                continue
            try:
                total = self.build(table_cls)
            except Exception as exc:
                log.exception(f"Failed {name}")
                failed.add(table_cls)
                self.run_log.log(run_id, name, layer, "FAILED", started, message=str(exc))
            else:
                self.run_log.log(run_id, name, layer, "SUCCESS", started, total, total, 0)

        if failed:
            names = sorted(t.qualified_name() for t in failed)
            raise RuntimeError(f"Run {run_id} finished with failed/skipped tables: {names}")
        log.info(f"Run {run_id} succeeded")
        return run_id

    def build(self, table_cls: type[Table]) -> int:
        """
        Build one table: compute, cast to the schema and write it.

        Returns the number of rows written.
        """
        table = table_cls(self.spark, self.config)
        inputs = {dep: dep(self.spark, self.config).read() for dep in table_cls.dependencies}
        log.info(f"Building {table.full_name}")

        df = table.enforce_schema(table.run(inputs)).cache()  # computed once, counted and written
        try:
            total = df.count()
            table.write(df)
        finally:
            df.unpersist()
        return total
