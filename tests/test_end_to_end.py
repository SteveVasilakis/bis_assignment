# AI-assisted: written with the help of an AI coding assistant (Claude Code).
"""The whole pipeline on small CSV files with the same kinds of problems as the real data."""

from decimal import Decimal

import lakehouse.tables  # noqa: F401  (registers the tables)
from lakehouse.core.pipeline import Pipeline
from tests.conftest import write_csv

D = Decimal

CUSTOMERS = (
    [f"{i},United Kingdom" for i in range(1, 61)]
    + [f"{i},Germany" for i in range(61, 65)]
    + ["65,EIRE", "66,EIRE", "67,France", "68,France", "68,Spain", ",Italy"]
)
PRODUCTS = [  # a stock code's rows read bottom (oldest) to top (current)
    "85123A,WHITE HEART ,2.00",
    "85123A,damaged,3.00",
    "85123a,,1.50",
    "22423,CAKESTAND,5.00",
    "POST,POSTAGE,10.00",
    "BANK CHARGES,Bank Charges,4.00",
    "99999,FREE THING,0.00",  # only price is 0: quarantined, the product has no valid price
]
ORDERS_HEADER = "InvoiceNo,StockCode,Quantity,InvoiceDate,CustomerID"
ORDERS = [
    "536365,85123A,6,12/1/2010 8:26,1",  # oldest price 1.50 -> 9.00 (UK)
    "536366,22423,2,11/15/2011 10:00,61",  # 10.00 (Germany)
    "536367,85123A,4,11/20/2011 10:00,65",  # 3.00 -> 12.00 (Ireland)
    "536368,85123A,10,12/5/2011 9:00,67",  # current price 2.00 -> 20.00 (France)
    "C536369,85123A,-2,12/6/2011 9:00,67",  # -4.00 (France)
    "536370,POST,1,12/6/2011 9:00,1",  # delivery 10.00 (UK)
    "536365,85123A,6,12/1/2010 8:26,1",  # exact duplicate of the first line
    "536371,22423,1,12/7/2011 9:00,",  # no customer: 5.00 (Unknown)
    "536372,22423,1,15/5/2011 14:48,61",  # day-first date: quarantined
    "536373,85123a,3,12/8/2011 10:00,68",  # customer with two countries: 6.00 (Multiple)
    "C536374,BANK CHARGES,-1,12/8/2011 11:00,1",  # a fee: not revenue
    "536375,22423,-5,12/8/2011 12:00,",  # stock adjustment: not revenue
    "536380,99999,2,11/1/2011 10:00,2",  # no valid price: quarantined in fact_sales
] + [f"537{i:03d},22423,1,11/1/2011 10:00,2" for i in range(100)]  # 100 x 5.00 (UK)


def test_pipeline_end_to_end(spark, config):
    def table(name):
        return spark.table(f"{config.catalog}.{name}")

    write_csv(config.landing_dir, "customers_1.csv", "CustomerID,Country", CUSTOMERS)
    write_csv(config.landing_dir, "products_1.csv", "StockCode,Description,UnitPrice", PRODUCTS)
    write_csv(config.landing_dir, "orders_1.csv", ORDERS_HEADER, ORDERS)
    pipeline = Pipeline(spark, config)
    pipeline.run(run_id="run-1")

    # Layers: duplicates collapse; a blank customer id, a bad date, a zero price and the
    # order line that has no price because of it are quarantined.
    assert table("silver.customers").count() == 68
    assert table("silver.products").count() == 6
    assert table("silver.orders").count() == 111
    assert table("gold.fact_sales").count() == 110
    quarantine = sorted(
        (r["table_name"].split(".", 1)[1], r["failed_rules"][0])
        for r in table("dq.quarantine").collect()
    )
    assert quarantine == [
        ("gold.fact_sales", "product_found"),
        ("silver.customers", "customer_id_not_null"),
        ("silver.orders", "invoice_ts_valid"),
        ("silver.products", "unit_price_positive"),
    ]
    log = {
        r["table_name"]: (r["status"], r["rows_total"], r["rows_quarantined"])
        for r in table("ops.run_log").collect()
    }
    assert log["bronze.orders"] == ("SUCCESS", 113, 0)
    assert log["silver.orders"] == ("SUCCESS", 112, 1)
    assert log["gold.fact_sales"] == ("SUCCESS", 111, 1)

    # Partitioning: hidden Iceberg partitions on the date columns.
    def partition_fields(name):
        partitions = spark.table(f"{config.catalog}.{name}.partitions")
        return [f.name for f in partitions.schema["partition"].dataType.fields]

    assert partition_fields("bronze.orders") == ["_ingested_at_day"]
    assert partition_fields("silver.orders") == ["invoice_ts_month"]
    assert partition_fields("gold.fact_sales") == ["invoice_ts_month"]

    # Analytics.
    revenue = {r["country"]: r["revenue"] for r in table("gold.revenue_by_country").collect()}
    assert revenue == {
        "United Kingdom": D("519.00"),  # 9 + 10 delivery + 100 x 5
        "France": D("16.00"),
        "Ireland": D("12.00"),
        "Germany": D("10.00"),
        "Multiple": D("6.00"),
        "Unknown": D("5.00"),
    }
    top = {
        r["country"]: (r["rank"], r["customers"])
        for r in table("gold.top_countries_by_customers").collect()
    }
    assert top == {
        "United Kingdom": (1, 60),
        "Germany": (2, 4),
        "Ireland": (3, 2),
        "France": (4, 1),  # tied: both rank 4 (dense_rank)
        "Multiple": (4, 1),
    }
    drops = [(r["stock_code"], r["price_drop"]) for r in table("gold.top_price_drops").collect()]
    assert drops == [("85123A", D("1.00"))]  # 3.00 -> 2.00 in 2011-12

    # Re-running is idempotent; a new hourly file is picked up and a replayed line merges.
    write_csv(
        config.landing_dir,
        "orders_2.csv",
        ORDERS_HEADER,
        ["536376,22423,4,12/9/2011 11:00,2", "536365,85123A,6,12/1/2010 8:26,1"],
    )
    pipeline.run(run_id="run-2")
    assert table("bronze.orders").count() == 115
    assert table("silver.orders").count() == 112
