from pyspark.sql import DataFrame

from lakehouse.core.bases import BronzeTable
from lakehouse.core.table import Table
from lakehouse.tables.bronze import schemas


class BronzeProducts(BronzeTable):
    """products*.csv files, delivered daily."""

    table_name = "products"
    schema = schemas.PRODUCTS

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        return self.read_landing("products")
