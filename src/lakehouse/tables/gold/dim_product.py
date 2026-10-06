from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import rules, schemas
from lakehouse.tables.silver.orders import SilverOrders
from lakehouse.tables.silver.products import SilverProducts

BEGINNING_OF_TIME = "1900-01-01"
END_OF_TIME = "9999-12-31"


class GoldDimProduct(GoldTable):
    """
    SCD type 2: one row per price version, valid in [valid_from, valid_to).

    The current price is valid from the month of the latest order, each older price one month
    earlier, the oldest from the beginning of time (assumption A1).
    """

    table_name = "dim_product"
    schema = schemas.DIM_PRODUCT
    rules = rules.DIM_PRODUCT
    dependencies = (SilverProducts, SilverOrders)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        """Add product_key and the validity period of every price version."""
        versions = Window.partitionBy("stock_code")
        in_order = versions.orderBy("version")
        # Month of the latest order: the current prices become valid in this month.
        last_month = inputs[SilverOrders].select(
            F.date_trunc("month", F.max("invoice_ts")).alias("anchor")
        )
        anchor = F.col("anchor")

        position = F.row_number().over(in_order)
        valid_from = F.when(position == 1, F.to_timestamp(F.lit(BEGINNING_OF_TIME))).otherwise(
            F.add_months(anchor, position - F.count("*").over(versions)).cast("timestamp")
        )
        return (
            inputs[SilverProducts]
            .crossJoin(last_month)
            .withColumn("product_key", F.xxhash64("stock_code", "version"))
            .withColumn("valid_from", valid_from)
            .withColumn(
                "valid_to",
                F.coalesce(F.lead("valid_from").over(in_order), F.to_timestamp(F.lit(END_OF_TIME))),
            )
            .withColumn("is_current", F.col("valid_to") == F.to_timestamp(F.lit(END_OF_TIME)))
        )
