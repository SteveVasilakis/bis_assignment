from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import schemas
from lakehouse.tables.gold.dim_product import GoldDimProduct
from lakehouse.tables.silver.orders import SilverOrders

REVENUE_TRANSACTION_TYPES = ("SALE", "CANCELLATION")


class GoldFactSales(GoldTable):
    """One row per order line, priced with the product version valid at invoice time."""

    table_name = "fact_sales"
    schema = schemas.FACT_SALES

    dependencies = (SilverOrders, GoldDimProduct)

    partitioned_by = ("months(invoice_ts)",)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        """Join every order line to the product version valid at its invoice time."""
        orders = inputs[SilverOrders].alias("o")
        products = inputs[GoldDimProduct].alias("p")

        price_valid_at_invoice = (
            (F.col("o.stock_code") == F.col("p.stock_code"))
            & (F.col("o.invoice_ts") >= F.col("p.valid_from"))
            & (F.col("o.invoice_ts") < F.col("p.valid_to"))
        )
        return orders.join(F.broadcast(products), price_valid_at_invoice, "left").select(
            "o.order_line_id",
            "o.invoice_no",
            "o.invoice_ts",
            "o.customer_id",
            "p.product_key",
            "o.quantity",
            "p.unit_price",
            (F.col("o.quantity") * F.col("p.unit_price")).alias("line_amount"),
            "o.transaction_type",
            (
                F.col("o.transaction_type").isin(*REVENUE_TRANSACTION_TYPES)
                & (F.col("p.product_type") != "NON_REVENUE")
            ).alias("is_revenue"),
        )
