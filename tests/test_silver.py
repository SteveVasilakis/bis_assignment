# AI-assisted: written with the help of an AI coding assistant (Claude Code).
"""Each silver table's run() on tiny bronze DataFrames."""

from datetime import datetime

from lakehouse.tables.bronze import schemas as bronze
from lakehouse.tables.bronze.customers import BronzeCustomers
from lakehouse.tables.bronze.orders import BronzeOrders
from lakehouse.tables.bronze.products import BronzeProducts
from lakehouse.tables.silver.customers import SilverCustomers
from lakehouse.tables.silver.orders import SilverOrders
from lakehouse.tables.silver.products import SilverProducts

DAY1, DAY2 = datetime(2024, 1, 1), datetime(2024, 1, 2)


def bronze_df(spark, schema, rows, ingested_at=DAY1, file="f.csv"):
    """Rows hold the source columns only; metadata is added in order."""
    return spark.createDataFrame([(*r, file, i, ingested_at) for i, r in enumerate(rows)], schema)


def build(table_cls, spark, config, inputs):
    table = table_cls(spark, config)
    return table.enforce_schema(table.run(inputs))


def test_customers(spark, config):
    older = bronze_df(spark, bronze.CUSTOMERS, [("1", "USA")], DAY1, "old.csv")
    rows = [
        (" 1 ", " EIRE "),  # the newer delivery supersedes 'USA'
        ("2", "Spain"),
        ("2", "France"),  # same id, another country: one row, country "Multiple"
        ("4", "Germany"),
        ("4", "Germany"),  # plain duplicate: one row
        (None, "Italy"),  # NULL key: kept so DQ can quarantine it
    ]
    new = bronze_df(spark, bronze.CUSTOMERS, rows, DAY2, "new.csv")
    df = build(SilverCustomers, spark, config, {BronzeCustomers: older.unionByName(new)})
    got = sorted(((r["customer_id"] or -1), r["country"], r["countries"]) for r in df.collect())
    assert got == [
        (-1, "Italy", "Italy"),
        (1, "Ireland", "Ireland"),
        (2, "Multiple", "France, Spain"),
        (4, "Germany", "Germany"),
    ]


def test_products_price_history_reads_bottom_to_top(spark, config):
    old = bronze_df(spark, bronze.PRODUCTS, [("85123A", "OLD NAME", "9.99")], DAY1, "old.csv")
    rows = [
        ("85123A", " WHITE HEART ", "2.00"),  # top row: current price
        ("85123A", "damaged", "3"),
        ("85123a", "", "1.5"),  # bottom row: oldest price
        ("POST", "POSTAGE", "10"),
        ("gift_0001_10", "Voucher", "10"),
    ]
    new = bronze_df(spark, bronze.PRODUCTS, rows, DAY2, "new.csv")
    df = build(SilverProducts, spark, config, {BronzeProducts: old.unionByName(new)})
    got = {
        (r["stock_code"], r["version"]): (r["description"], str(r["unit_price"]), r["product_type"])
        for r in df.collect()
    }
    # Only the latest snapshot; lower-case code merged; the name comes from the capitals row.
    assert got == {
        ("85123A", 3): ("WHITE HEART", "2.00", "MERCHANDISE"),
        ("85123A", 2): ("WHITE HEART", "3.00", "MERCHANDISE"),
        ("85123A", 1): ("WHITE HEART", "1.50", "MERCHANDISE"),
        ("POST", 1): ("POSTAGE", "10.00", "SHIPPING"),
        ("GIFT_0001_10", 1): ("Voucher", "10.00", "NON_REVENUE"),
    }


def test_orders(spark, config):
    rows = [
        ("536365", "85123a", "6", "12/1/2010 8:26", "17850"),
        ("536365", "85123A", "6", "12/1/2010 8:26", "17850"),  # duplicate once normalised
        ("C536379", "D", "-1", "12/1/2010 9:41", "14527"),
        ("536380", "22423", "-3", "1/2/2011 10:00", None),
        ("536381", "22423", "2", "15/5/2011 14:48", "13047"),  # day-first date
    ]
    df = build(SilverOrders, spark, config, {BronzeOrders: bronze_df(spark, bronze.ORDERS, rows)})
    got = sorted(
        (
            r["invoice_no"],
            r["stock_code"],
            r["quantity"],
            str(r["invoice_ts"]),
            r["transaction_type"],
        )
        for r in df.collect()
    )
    assert got == [
        ("536365", "85123A", 6, "2010-12-01 08:26:00", "SALE"),
        ("536380", "22423", -3, "2011-01-02 10:00:00", "ADJUSTMENT"),
        ("536381", "22423", 2, "None", "SALE"),  # NULL date: quarantined by invoice_ts_valid
        ("C536379", "D", -1, "2010-12-01 09:41:00", "CANCELLATION"),
    ]
