"""
Bronze schemas: the source columns exactly as delivered (all strings, in file order)
plus ingestion metadata. Nothing is typed or cleaned here.
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

METADATA = [
    StructField("_source_file", StringType()),  # landing file the row came from
    StructField("_row_id", LongType()),  # increasing within a file: keeps the row order
    StructField("_ingested_at", TimestampType()),
]

CUSTOMERS = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("country", StringType()),
        *METADATA,
    ]
)

PRODUCTS = StructType(
    [
        StructField("stock_code", StringType()),
        StructField("description", StringType()),
        StructField("unit_price", StringType()),
        *METADATA,
    ]
)

ORDERS = StructType(
    [
        StructField("invoice_no", StringType()),
        StructField("stock_code", StringType()),
        StructField("quantity", StringType()),
        StructField("invoice_date", StringType()),
        StructField("customer_id", StringType()),
        *METADATA,
    ]
)
