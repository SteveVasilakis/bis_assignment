from itertools import chain

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.core.bases import SilverTable
from lakehouse.core.table import Table
from lakehouse.tables.bronze.products import BronzeProducts
from lakehouse.tables.silver import rules, schemas

# Used AI to differentiate product type codes based on description
PRODUCT_TYPE_BY_CODE = {
    "POST": "SHIPPING",
    "DOT": "SHIPPING",
    "C2": "SHIPPING",  # carriage
    "D": "ADJUSTMENT",  # discount
    "M": "ADJUSTMENT",  # manual
    "BANK CHARGES": "NON_REVENUE",
    "AMAZONFEE": "NON_REVENUE",
    "CRUK": "NON_REVENUE",  # charity commission
    "B": "NON_REVENUE",  # bad debt
    "S": "NON_REVENUE",  # samples
}


class SilverProducts(SilverTable):
    """
    One row per (stock_code, version). The file lists several prices per stock code and no
    dates. A stock code's rows are its price history read from bottom (oldest) to top
    (current), see assumption A1 in the docs. Rows with notes or no description stay in the
    history; they are flagged by warn rules.
    """

    table_name = "products"
    schema = schemas.PRODUCTS
    rules = rules.PRODUCTS
    dependencies = (BronzeProducts,)
    merge_keys = ("stock_code", "version")

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        bronze = inputs[BronzeProducts]
        # Each products file is a full snapshot: only the latest one matters.
        latest_load = bronze.agg(F.max("_ingested_at")).first()[0]
        snapshot = bronze.filter(F.col("_ingested_at") == latest_load)

        description = F.trim("description")
        products = (
            snapshot
            # Codes are case-insensitive in the source ('85049a' vs '85049A').
            .withColumn("stock_code", F.upper(F.trim("stock_code")))
            .withColumn("description_raw", F.when(description != "", description))
            .withColumn("unit_price", F.trim("unit_price").cast("decimal(10,2)"))
        )

        top_first = Window.partitionBy("stock_code").orderBy("_row_id")
        whole_code = top_first.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        oldest_first = Window.partitionBy("stock_code").orderBy(F.col("_row_id").desc())

        # Product names are written in capitals; notes like 'check' or 'damaged' are not.
        raw = F.col("description_raw")
        name = F.when(raw.rlike("[A-Z]") & ~raw.rlike("[a-z]"), raw)
        by_code = F.create_map(
            *[F.lit(x) for x in chain.from_iterable(PRODUCT_TYPE_BY_CODE.items())]
        )
        is_voucher = F.col("stock_code").startswith("GIFT_") | F.upper("description").contains(
            "GIFT VOUCHER"
        )
        return (
            products.withColumn("version", F.row_number().over(oldest_first))
            .withColumn(
                "description",
                F.coalesce(
                    F.first(name, ignorenulls=True).over(whole_code),
                    F.first(raw, ignorenulls=True).over(whole_code),
                ),
            )
            .withColumn(
                "product_type",
                F.coalesce(
                    by_code[F.col("stock_code")],
                    F.when(is_voucher, "NON_REVENUE"),
                    F.lit("MERCHANDISE"),
                ),
            )
        )
