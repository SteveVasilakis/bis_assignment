from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import schemas
from lakehouse.tables.gold.dim_product import GoldDimProduct
from lakehouse.tables.gold.fact_sales import GoldFactSales


class GoldPriceVsVolume(GoldTable):
    """
    Q3. Relationship between the average unit price of products and their sales volume.
    """

    table_name = "price_vs_volume"
    schema = schemas.PRICE_VS_VOLUME

    dependencies = (GoldFactSales, GoldDimProduct)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        """Average price and units sold per merchandise product."""
        products = inputs[GoldDimProduct]
        avg_price = (
            products.filter(F.col("product_type") == "MERCHANDISE")
            .groupBy("stock_code")
            .agg(
                F.max("description").alias("description"),
                F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            )
        )
        units = (
            inputs[GoldFactSales]
            .filter(F.col("transaction_type").isin("SALE", "CANCELLATION"))
            .join(products.select("product_key", "stock_code"), "product_key")
            .groupBy("stock_code")
            .agg(F.sum("quantity").alias("units_sold"))
        )
        return avg_price.join(units, "stock_code", "left").fillna(0, ["units_sold"])
