from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import rules, schemas
from lakehouse.tables.gold.dim_product import GoldDimProduct
from lakehouse.tables.gold.fact_sales import GoldFactSales


class GoldTopPriceDrops(GoldTable):
    """
    Q4. Top 3 products with the largest unit price drop in the last month.
    """

    table_name = "top_price_drops"
    schema = schemas.TOP_PRICE_DROPS
    rules = rules.TOP_PRICE_DROPS
    dependencies = (GoldFactSales, GoldDimProduct)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        month = (
            inputs[GoldFactSales]
            .select(F.date_trunc("month", F.max("invoice_ts")).alias("month_start"))
            .withColumn("month_end", F.col("month_start") + F.expr("INTERVAL 1 MONTH"))
        )

        products = (
            inputs[GoldDimProduct].filter(F.col("product_type") == "MERCHANDISE").crossJoin(month)
        )

        def price_valid_just_before(dt_month: str) -> Column:
            return (F.col("valid_from") < F.col(dt_month)) & (F.col("valid_to") >= F.col(dt_month))

        before = products.filter(price_valid_just_before("month_start")).select(
            "stock_code", F.col("unit_price").alias("price_before")
        )
        after = products.filter(price_valid_just_before("month_end")).select(
            "stock_code",
            "description",
            F.date_format("month_start", "yyyy-MM").alias("month"),
            F.col("unit_price").alias("price_after"),
        )

        ranking = Window.orderBy(F.desc("price_drop"))
        return (
            before.join(after, "stock_code")
            .withColumn("price_drop", F.col("price_before") - F.col("price_after"))
            .filter(F.col("price_drop") > 0)
            .withColumn("rank", F.dense_rank().over(ranking))
            .filter(F.col("rank") <= 3)
        )
