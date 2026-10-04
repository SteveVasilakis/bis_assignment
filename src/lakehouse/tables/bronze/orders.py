from pyspark.sql import DataFrame

from lakehouse.core.bases import BronzeTable
from lakehouse.core.table import Table
from lakehouse.tables.bronze import schemas


class BronzeOrders(BronzeTable):
    """orders*.csv files, delivered hourly."""

    table_name = "orders"
    schema = schemas.ORDERS

    partitioned_by = ("days(_ingested_at)",)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        return self.read_landing("orders")
