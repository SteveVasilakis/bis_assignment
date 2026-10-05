from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.core.layers import BRONZE, SILVER
from lakehouse.core.table import Table

# Metadata columns every bronze schema carries next to the source columns.
SOURCE_FILE = "_source_file"
ROW_ID = "_row_id"
INGESTED_AT = "_ingested_at"


class BronzeTable(Table):
    """
    Raw data, append-only.
    DQ: no rules. Bronze keeps exactly what arrived, so every row is published and stays
    available for reprocessing.
    """

    layer = BRONZE
    write_mode = "append"

    def read_landing(self, file_prefix: str) -> DataFrame:
        """
        Read the `<file_prefix>*.csv` files from the landing folder that are not in this
        table yet, with every source column as a string.
        """
        source_schema = StructType(
            [StructField(f.name, StringType()) for f in self.schema if not f.name.startswith("_")]
        )
        already_loaded = [
            r[SOURCE_FILE] for r in self.read().select(SOURCE_FILE).distinct().collect()
        ]
        df = (
            self.spark.read.schema(source_schema)
            .option("header", True)
            .option("pathGlobFilter", f"{file_prefix}*.csv")
            .csv(self.config.landing_dir)
            .withColumn(SOURCE_FILE, F.col("_metadata.file_name"))
            .filter(~F.col(SOURCE_FILE).isin(already_loaded))
        )
        return df.withColumn(ROW_ID, F.monotonically_increasing_id()).withColumn(
            INGESTED_AT, F.current_timestamp()
        )


class SilverTable(Table):
    """
    Clean, typed, one row per key. Upserted on merge_keys.
    DQ: row-level rules. Bad rows are quarantined and the rest is published.
    """

    layer = SILVER
    write_mode = "merge"

    def latest(self, df: DataFrame) -> DataFrame:
        """
        Keep the most recent row per merge key (MERGE needs unique source keys).
        "Most recent" = latest ingestion, then the last row in the file.
        Rows with a NULL key pass through untouched so DQ can quarantine each one.
        """
        w = Window.partitionBy(*self.merge_keys).orderBy(F.desc(INGESTED_AT), F.desc(ROW_ID))
        null_key = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in self.merge_keys])
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter((F.col("_rn") == 1) | null_key)
            .drop("_rn")
        )
