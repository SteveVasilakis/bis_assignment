from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================
# STAR SCHEMA
# ============================================================

DIM_CUSTOMER = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("country", StringType()),  # "Multiple" when listed with several
        StructField("countries", StringType()),
    ]
)

DIM_PRODUCT = StructType(
    [
        StructField("product_key", LongType()),
        StructField("stock_code", StringType()),
        StructField("version", IntegerType()),
        StructField("description", StringType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField(
            "product_type", StringType()
        ),  # MERCHANDISE | SHIPPING | ADJUSTMENT | NON_REVENUE
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
    ]
)

FACT_SALES = StructType(
    [
        StructField("order_line_id", StringType()),
        StructField("invoice_no", StringType()),
        StructField("invoice_ts", TimestampType()),
        StructField("customer_id", LongType()),
        StructField("product_key", LongType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("line_amount", DecimalType(18, 2)),
        StructField("transaction_type", StringType()),
        StructField("is_revenue", BooleanType()),
    ]
)

# ============================================================
# ANALYTICS
# ============================================================

TOP_COUNTRIES_BY_CUSTOMERS = StructType(
    [
        StructField("rank", IntegerType()),
        StructField("country", StringType()),
        StructField("customers", LongType()),
    ]
)

REVENUE_BY_COUNTRY = StructType(
    [
        StructField("rank", IntegerType()),
        StructField("country", StringType()),  # "Unknown" = orders without a known customer
        StructField("revenue", DecimalType(18, 2)),
        StructField("revenue_share_pct", DoubleType()),
    ]
)

PRICE_VS_VOLUME = StructType(
    [
        StructField("price_band", StringType()),
        StructField("products", LongType()),
        StructField("units_sold", LongType()),
        StructField("avg_units_per_product", DoubleType()),
        StructField("price_volume_correlation", DoubleType()),  # Pearson over all products by the help of AI
    ]
)

TOP_PRICE_DROPS = StructType(
    [
        StructField("rank", IntegerType()),
        StructField("stock_code", StringType()),
        StructField("description", StringType()),
        StructField("month", StringType()),
        StructField("price_before", DecimalType(10, 2)),
        StructField("price_after", DecimalType(10, 2)),
        StructField("price_drop", DecimalType(10, 2)),
    ]
)
