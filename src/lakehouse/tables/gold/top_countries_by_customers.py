from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import rules, schemas
from lakehouse.tables.gold.dim_customer import GoldDimCustomer


class GoldTopCountriesByCustomers(GoldTable):
    """
    Q1. Top 10 countries with the most customers.
    """

    table_name = "top_countries_by_customers"
    schema = schemas.TOP_COUNTRIES_BY_CUSTOMERS
    rules = rules.TOP_COUNTRIES_BY_CUSTOMERS
    dependencies = (GoldDimCustomer,)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        ranking = Window.orderBy(F.desc("customers"))
        return (
            inputs[GoldDimCustomer]
            .groupBy("country")
            .agg(F.count("*").alias("customers"))
            .withColumn("rank", F.dense_rank().over(ranking))
            .filter(F.col("rank") <= 10)
        )
