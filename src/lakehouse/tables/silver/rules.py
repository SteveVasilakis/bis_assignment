"""
Data quality rules, one tuple per table. A rule's condition is TRUE for a good row.
"""

from lakehouse.core.dq import Rule

CUSTOMERS = (
    Rule("customer_id_not_null", "customer_id IS NOT NULL"),
    Rule("country_not_null", "country IS NOT NULL"),
    Rule("country_unambiguous", "country IS NULL OR country <> 'Multiple'", "warn"),
    Rule(
        "country_is_specific",
        "country IS NULL OR country NOT IN ('Unknown', 'European Community')",
        "warn",
    ),
)

PRODUCTS = (
    Rule("stock_code_not_null", "stock_code IS NOT NULL"),
    Rule("version_not_null", "version IS NOT NULL"),
    Rule("unit_price_not_null", "unit_price IS NOT NULL"),
    Rule("unit_price_positive", "unit_price IS NULL OR unit_price > 0"),
)

ORDERS = (
    Rule("order_line_id_not_null", "order_line_id IS NOT NULL"),
    Rule("customer_id_not_null", "customer_id IS NOT NULL", "warn"),
    Rule("invoice_no_not_null", "invoice_no IS NOT NULL"),
    Rule("stock_code_not_null", "stock_code IS NOT NULL"),
    Rule("invoice_ts_valid", "invoice_ts IS NOT NULL"),
    Rule("quantity_valid", "quantity <> 0"),
)
