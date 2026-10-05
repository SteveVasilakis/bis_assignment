"""Gold customer dimension."""

from pyspark.sql import DataFrame

from lakehouse.core.bases import GoldTable
from lakehouse.core.table import Table
from lakehouse.tables.gold import rules, schemas
from lakehouse.tables.silver.customers import SilverCustomers


class GoldDimCustomer(GoldTable):
    """SCD type 1: one row per customer with the latest country."""

    table_name = "dim_customer"
    schema = schemas.DIM_CUSTOMER
    rules = rules.DIM_CUSTOMER
    dependencies = (SilverCustomers,)

    def run(self, inputs: dict[type[Table], DataFrame]) -> DataFrame:
        return inputs[SilverCustomers]
