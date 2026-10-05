from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import rules, schemas
from lakehouse.tables.gold.dim_customer import GoldDimCustomer
from lakehouse.tables.gold.fact_sales import GoldFactSales

UNKNOWN_COUNTRY = "Unknown"


class GoldRevenueByCountry(GoldTable):
    """
    Q2. Revenue distribution by country (revenue as defined in fact_sales.REVENUE_*).
    """

    table_name = "revenue_by_country"
    schema = schemas.REVENUE_BY_COUNTRY
    rules = rules.REVENUE_BY_COUNTRY
    dependencies = (GoldFactSales, GoldDimCustomer)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        revenue = (
            inputs[GoldFactSales]
            .filter("is_revenue")
            .join(F.broadcast(inputs[GoldDimCustomer]), "customer_id", "left")  # small table
            .groupBy(F.coalesce("country", F.lit(UNKNOWN_COUNTRY)).alias("country"))
            .agg(F.sum("line_amount").alias("revenue"))
        )
        everything = Window.partitionBy()
        return revenue.withColumn(
            "revenue_share_pct",
            F.round(100 * F.col("revenue") / F.sum("revenue").over(everything), 2),
        ).withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("revenue"))))
