"""
Silver schemas: typed and cleaned, one row per merge key.

After a table's run(), its DataFrame is shaped to exactly these columns:
present columns are cast to the declared type, missing ones are added as NULL.
_source_file + _row_id point back to the exact bronze row (also inside quarantined records).
"""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LINEAGE = [
    StructField("_source_file", StringType()),
    StructField("_row_id", LongType()),
    StructField("_ingested_at", TimestampType()),
]

CUSTOMERS = StructType(
    [
        StructField("customer_id", LongType()),
        # Standardised country, or "Multiple" when one delivery lists the customer with
        # several countries.
        StructField("country", StringType()),
        StructField("countries", StringType()),  # every country listed, sorted, comma-separated
        *LINEAGE,
    ]
)

PRODUCTS = StructType(
    [
        StructField("stock_code", StringType()),
        # Price history reads bottom to top: 1 = bottom row of the stock code (oldest),
        # n = top row (current).
        StructField("version", IntegerType()),
        StructField("description", StringType()),  # product name: first real name from the top
        StructField("description_raw", StringType()),  # this row's own description
        StructField("unit_price", DecimalType(10, 2)),
        # MERCHANDISE | SHIPPING | ADJUSTMENT (discount, manual) | NON_REVENUE (fees, bad debt,
        # samples, vouchers)
        StructField("product_type", StringType()),
        *LINEAGE,
    ]
)

ORDERS = StructType(
    [
        StructField("order_line_id", StringType()),
        StructField("invoice_no", StringType()),
        StructField("stock_code", StringType()),
        StructField("quantity", IntegerType()),
        StructField("invoice_ts", TimestampType()),
        StructField("customer_id", LongType()),
        StructField("transaction_type", StringType()),  # SALE | CANCELLATION | ADJUSTMENT
        *LINEAGE,
    ]
)
