from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.core.bases import SilverTable
from lakehouse.core.table import Table
from lakehouse.tables.bronze.orders import BronzeOrders
from lakehouse.tables.silver import schemas


class SilverOrders(SilverTable):
    """One row per order line with its transaction type; exact duplicates collapse."""

    table_name = "orders"
    schema = schemas.ORDERS
    dependencies = (BronzeOrders,)
    merge_keys = ("order_line_id",)
    partitioned_by = ("months(invoice_ts)",)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        invoice_no = F.upper(F.trim("invoice_no"))
        stock_code = F.upper(F.trim("stock_code"))
        quantity = F.trim("quantity")
        invoice_date = F.trim("invoice_date")
        customer_id = F.trim("customer_id")

        orders = inputs[BronzeOrders].select(
            # using all columns to deduplicate in latest
            F.sha2(
                F.concat_ws(
                    "|",
                    *[
                        F.coalesce(c, F.lit(""))
                        for c in (invoice_no, stock_code, quantity, invoice_date, customer_id)
                    ],
                ),
                256,
            ).alias("order_line_id"),
            invoice_no.alias("invoice_no"),
            stock_code.alias("stock_code"),
            quantity.cast("int").alias("quantity"),
            F.to_timestamp(invoice_date, "M/d/yyyy H:mm").alias("invoice_ts"),
            customer_id.cast("long").alias("customer_id"),
            F.when(invoice_no.startswith("C"), "CANCELLATION")
            .when(invoice_no.startswith("A") | (quantity.cast("int") < 0), "ADJUSTMENT")
            .otherwise("SALE")
            .alias("transaction_type"),
            "_source_file",
            "_row_id",
            "_ingested_at",
        )

        return self.latest(orders)
