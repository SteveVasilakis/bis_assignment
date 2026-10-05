"""
Data quality: tag each row with the rules it fails, record the results and quarantine the
rows that fail an "error" rule. A row either passes (published) or fails (quarantined).

    dq.results      one row per (run, table, rule): how many rows failed
    dq.quarantine   rows that failed an "error" rule, stored as JSON with the rule names
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("lakehouse.dq")

FAILED = "_dq_failed"  # array<string>: names of every rule the row failed
QUARANTINE = "_dq_quarantine"  # boolean: row failed at least one "error" rule


@dataclass(frozen=True)
class Rule:
    """
    A row-level check. `condition` is a SQL predicate that must be TRUE for a good row
    (NULL counts as a failure).

    severity="error": the row is quarantined and not published.
    severity="warn":  the row is published; the failure is only counted.
    """

    name: str
    condition: str
    severity: str = "error"


def evaluate(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Tag each row with the rules it failed. Rows are never removed here."""
    checks = F.array(
        *[F.when(~F.coalesce(F.expr(r.condition), F.lit(False)), F.lit(r.name)) for r in rules]
    )
    error_rules = [r.name for r in rules if r.severity == "error"]
    return df.withColumn(
        FAILED, F.filter(checks, lambda x: x.isNotNull()).cast("array<string>")
    ).withColumn(QUARANTINE, F.exists(F.col(FAILED), lambda x: x.isin(error_rules)))


class DataQuality:
    """Writes rule results to dq.results and failing rows to dq.quarantine."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        """Use the dq namespace of the given Iceberg catalog."""
        self.spark = spark
        self.results_table = f"{catalog}.dq.results"
        self.quarantine_table = f"{catalog}.dq.quarantine"
        self.namespace = f"{catalog}.dq"

    def setup(self) -> None:
        """Create the dq namespace, dq.results and dq.quarantine if they don't exist yet."""
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.namespace}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.results_table} (
                run_id string, table_name string, rule string, severity string,
                failed_rows bigint, total_rows bigint, checked_at timestamp
            ) USING iceberg
        """)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.quarantine_table} (
                run_id string, table_name string, failed_rules array<string>,
                record string, quarantined_at timestamp
            ) USING iceberg
            PARTITIONED BY (table_name)
        """)

    def audit(self, tagged: DataFrame, table: object, run_id: str) -> tuple[int, int]:
        """Record the rule results and quarantine the rows that fail an "error" rule.

        Returns the number of rows and the number of quarantined rows.

        `tagged` is the output of evaluate(), cached by the caller because it is read
        several times (counts, quarantine, publish).
        """
        rules = table.rules
        stats = tagged.agg(
            F.count("*").alias("total"),
            F.sum(F.col(QUARANTINE).cast("int")).alias("quarantined"),
            *[
                F.sum(F.array_contains(FAILED, r.name).cast("int")).alias(f"rule_{i}")
                for i, r in enumerate(rules)
            ],
        ).first()
        total, quarantined = stats["total"], stats["quarantined"] or 0

        if rules:
            results = [
                (run_id, table.full_name, r.name, r.severity, stats[f"rule_{i}"] or 0, total)
                for i, r in enumerate(rules)
            ]
            (
                self.spark.createDataFrame(
                    results,
                    "run_id string, table_name string, rule string, severity string, "
                    "failed_rows long, total_rows long",
                )
                .withColumn("checked_at", F.current_timestamp())
                .writeTo(self.results_table)
                .append()
            )
        if quarantined:
            data_cols = [c for c in tagged.columns if c not in (FAILED, QUARANTINE)]
            (
                tagged.filter(F.col(QUARANTINE))
                .select(
                    F.lit(run_id).alias("run_id"),
                    F.lit(table.full_name).alias("table_name"),
                    F.col(FAILED).alias("failed_rules"),
                    # Keep NULL fields in the JSON: a missing value is often why the row failed.
                    F.to_json(F.struct(*data_cols), {"ignoreNullFields": "false"}).alias("record"),
                    F.current_timestamp().alias("quarantined_at"),
                )
                .writeTo(self.quarantine_table)
                .append()
            )

        log.info(f"{table.full_name}: {quarantined} of {total} rows quarantined")
        return total, quarantined

    @staticmethod
    def passed(tagged: DataFrame) -> DataFrame:
        """Rows to publish: everything not quarantined, without the DQ columns."""
        return tagged.filter(~F.col(QUARANTINE)).drop(FAILED, QUARANTINE)
