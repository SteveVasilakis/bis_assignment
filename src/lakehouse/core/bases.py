from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.core.layers import BRONZE
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
