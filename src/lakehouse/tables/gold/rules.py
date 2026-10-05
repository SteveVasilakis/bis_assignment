"""Data quality rules for the gold tables, one tuple per table."""

from lakehouse.core.dq import Rule

DIM_CUSTOMER = (
    Rule("customer_id_not_null", "customer_id IS NOT NULL"),
    Rule("country_not_null", "country IS NOT NULL"),
)

DIM_PRODUCT = (
    Rule("valid_range", "valid_from < valid_to"),
    Rule("unit_price_not_null", "unit_price IS NOT NULL"),
)

FACT_SALES = (
    Rule("product_found", "product_key IS NOT NULL"),  # a valid price at invoice time
    Rule("line_amount_not_null", "product_key IS NULL OR line_amount IS NOT NULL"),
)

TOP_COUNTRIES_BY_CUSTOMERS = (Rule("customers_positive", "customers > 0"),)

REVENUE_BY_COUNTRY = (
    Rule("country_not_null", "country IS NOT NULL"),
    Rule("revenue_not_null", "revenue IS NOT NULL"),
)

PRICE_VS_VOLUME = (Rule("avg_unit_price_not_null", "avg_unit_price IS NOT NULL"),)

TOP_PRICE_DROPS = (Rule("is_a_drop", "price_drop > 0"),)
