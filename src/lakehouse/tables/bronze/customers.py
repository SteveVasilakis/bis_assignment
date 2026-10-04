from pyspark.sql import DataFrame

from lakehouse.core.bases import BronzeTable
from lakehouse.core.table import Table
from lakehouse.tables.bronze import schemas


class BronzeCustomers(BronzeTable):
    """customers*.csv files, delivered daily."""

    table_name = "customers"
    schema = schemas.CUSTOMERS

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        return self.read_landing("customers")
