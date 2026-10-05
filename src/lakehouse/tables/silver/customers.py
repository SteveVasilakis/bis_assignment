from itertools import chain

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.core.bases import SilverTable
from lakehouse.core.table import Table
from lakehouse.tables.bronze.customers import BronzeCustomers
from lakehouse.tables.silver import schemas

# Used AI for EIRE and RSA
COUNTRY_NAMES = {
    "EIRE": "Ireland",
    "RSA": "South Africa",
    "USA": "United States",
    "Unspecified": "Unknown",
}
MULTIPLE = "Multiple"


class SilverCustomers(SilverTable):
    """One row per customer: its country, or "Multiple" when listed with several."""

    table_name = "customers"
    schema = schemas.CUSTOMERS
    dependencies = (BronzeCustomers,)
    merge_keys = ("customer_id",)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        country = F.trim("country")
        names = F.create_map(*[F.lit(x) for x in chain.from_iterable(COUNTRY_NAMES.items())])
        customers = (
            inputs[BronzeCustomers]
            .withColumn("customer_id", F.trim("customer_id").cast("long"))
            .withColumn("country", F.coalesce(names[country], country))
        )

        newest = F.max("_ingested_at").over(Window.partitionBy("customer_id"))
        current = (
            customers.withColumn("_newest", newest)
            .filter(F.col("_ingested_at") == F.col("_newest"))
            .drop("_newest")
        )

        known = (
            current.filter(F.col("customer_id").isNotNull())
            .groupBy("customer_id")
            .agg(
                F.array_sort(F.collect_set("country")).alias("countries_arr"),
                F.min("_source_file").alias("_source_file"),
                F.min("_row_id").alias("_row_id"),
                F.max("_ingested_at").alias("_ingested_at"),
            )
            .select(
                "customer_id",
                F.when(F.size("countries_arr") > 1, F.lit(MULTIPLE))
                .otherwise(F.element_at("countries_arr", 1))
                .alias("country"),
                F.when(F.size("countries_arr") > 0, F.array_join("countries_arr", ", ")).alias(
                    "countries"
                ),
                "_source_file",
                "_row_id",
                "_ingested_at",
            )
        )
        unknown = current.filter(F.col("customer_id").isNull()).withColumn(
            "countries", F.col("country")
        )
        return known.unionByName(unknown.select(*known.columns))
