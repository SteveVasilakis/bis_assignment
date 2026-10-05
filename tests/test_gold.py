# AI-assisted: written with the help of an AI coding assistant (Claude Code).
"""Gold dimensions and the sales fact on tiny silver DataFrames."""

from datetime import datetime
from decimal import Decimal

from lakehouse.tables.gold.dim_customer import GoldDimCustomer
from lakehouse.tables.gold.dim_product import GoldDimProduct
from lakehouse.tables.gold.fact_sales import GoldFactSales
from lakehouse.tables.silver import schemas as silver
from lakehouse.tables.silver.customers import SilverCustomers
from lakehouse.tables.silver.orders import SilverOrders
from lakehouse.tables.silver.products import SilverProducts

TS = datetime(2024, 1, 1)
D = Decimal


def build(table_cls, spark, config, inputs):
    table = table_cls(spark, config)
    return table.enforce_schema(table.run(inputs))


def test_dim_customer_keeps_one_row_per_customer_without_lineage(spark, config):
    rows = [  # customer_id, country, countries
        (1, "United Kingdom", "United Kingdom"),
        (2, "Multiple", "France, Spain"),
    ]
    customers = spark.createDataFrame([(*r, "c.csv", 0, TS) for r in rows], silver.CUSTOMERS)
    df = build(GoldDimCustomer, spark, config, {SilverCustomers: customers})

    assert df.columns == ["customer_id", "country", "countries"]  # lineage columns dropped
    assert sorted(tuple(r) for r in df.collect()) == [
        (1, "United Kingdom", "United Kingdom"),
        (2, "Multiple", "France, Spain"),
    ]


def dim_product(spark, config):
    rows = [  # stock_code, version (1 = oldest), description, raw, price, product_type
        ("A", 1, "PRODUCT A", None, D("2.00"), "MERCHANDISE"),
        ("A", 2, "PRODUCT A", "check", D("3.00"), "MERCHANDISE"),
        ("A", 3, "PRODUCT A", "PRODUCT A", D("1.50"), "MERCHANDISE"),
        ("BANK CHARGES", 1, "Bank Charges", "Bank Charges", D("4.00"), "NON_REVENUE"),
        ("C", 2, "PRODUCT C", "PRODUCT C", D("4.00"), "MERCHANDISE"),  # version 1 quarantined
    ]
    products = spark.createDataFrame([(*r, "p.csv", 0, TS) for r in rows], silver.PRODUCTS)
    return build(GoldDimProduct, spark, config, {SilverProducts: products})


def test_dim_product_dates_versions_one_month_apart(spark, config):
    got = {
        (r["stock_code"], r["version"]): (str(r["valid_from"]), str(r["valid_to"]), r["is_current"])
        for r in dim_product(spark, config).collect()
    }
    assert got == {
        ("A", 1): ("1900-01-01 00:00:00", "2011-11-01 00:00:00", False),
        ("A", 2): ("2011-11-01 00:00:00", "2011-12-01 00:00:00", False),
        ("A", 3): ("2011-12-01 00:00:00", "9999-12-31 00:00:00", True),
        ("BANK CHARGES", 1): ("1900-01-01 00:00:00", "9999-12-31 00:00:00", True),
        # The oldest remaining version is valid from the beginning of time.
        ("C", 2): ("1900-01-01 00:00:00", "9999-12-31 00:00:00", True),
    }


def test_fact_sales_uses_price_valid_at_invoice_time(spark, config):
    rows = [  # invoice, stock code, qty, invoice time, customer, type
        ("1", "A", 6, datetime(2010, 12, 1, 8), 1, "SALE"),
        ("2", "A", 4, datetime(2011, 11, 20), 2, "SALE"),
        ("3", "A", 10, datetime(2011, 12, 5), None, "SALE"),
        ("C4", "A", -2, datetime(2011, 12, 6), 2, "CANCELLATION"),
        ("C5", "BANK CHARGES", -1, datetime(2011, 12, 7), 1, "CANCELLATION"),
        ("6", "A", -3, datetime(2011, 12, 7), None, "ADJUSTMENT"),
    ]
    orders = spark.createDataFrame(
        [(f"line{i}", *r, "o.csv", i, TS) for i, r in enumerate(rows)], silver.ORDERS
    )
    fact = build(
        GoldFactSales,
        spark,
        config,
        {SilverOrders: orders, GoldDimProduct: dim_product(spark, config)},
    )
    got = {
        r["invoice_no"]: (r["unit_price"], r["line_amount"], r["is_revenue"])
        for r in fact.collect()
    }
    assert got == {
        "1": (D("2.00"), D("12.00"), True),  # oldest price
        "2": (D("3.00"), D("12.00"), True),
        "3": (D("1.50"), D("15.00"), True),  # current price; no customer is still a sale
        "C4": (D("1.50"), D("-3.00"), True),  # cancellations reduce revenue
        "C5": (D("4.00"), D("-4.00"), False),  # a fee is a cost
        "6": (D("1.50"), D("-4.50"), False),  # stock adjustment
    }
