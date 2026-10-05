"""Gold layer: star schema and analytics. Importing it registers its tables."""

from lakehouse.tables.gold import (  # noqa: F401
    dim_customer,
    dim_product,
    fact_sales,
    top_countries_by_customers,
)
