from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

SCHEMA = (
    "run_id string, table_name string, layer string, status string, rows_total bigint, "
    "rows_published bigint, rows_quarantined bigint, message string, "
    "started_at timestamp, finished_at timestamp"
)


def now() -> datetime:
    """Current time in UTC, used for all run log timestamps."""
    return datetime.now(timezone.utc)


class RunLog:
    """Writes one row per table build to ops.run_log."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        """Log to the ops namespace of the given Iceberg catalog."""
        self.spark = spark
        self.namespace = f"{catalog}.ops"
        self.table = f"{self.namespace}.run_log"

    def setup(self) -> None:
        """Create the ops namespace and the run_log table if they don't exist yet."""
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.namespace}")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({SCHEMA}) USING iceberg")

    def log(
        self,
        run_id: str,
        table_name: str,
        layer: str,
        status: str,
        started_at: datetime,
        rows_total: int | None = None,
        rows_published: int | None = None,
        rows_quarantined: int | None = None,
        message: str | None = None,
    ) -> None:
        """Append one row for a table build: its status, row counts and an optional message."""
        row = (
            run_id,
            table_name,
            layer,
            status,
            rows_total,
            rows_published,
            rows_quarantined,
            message,
            started_at,
            now(),
        )
        self.spark.createDataFrame([row], SCHEMA).writeTo(self.table).append()
