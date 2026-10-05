<!-- AI-assisted: written with the help of an AI coding assistant (Claude Code). -->

# BIS assignment: retail data pipeline

A Spark pipeline that loads the `customers`, `products` and `orders` CSV files into
bronze, silver and gold Iceberg tables and answers the four questions of the exercise.

- [docs/data_analysis.md](docs/data_analysis.md): what I found in the data, assumptions,
  ad-hoc queries and results
- [docs/data_model.md](docs/data_model.md): layers, star schema and lineage diagrams
- [docs/data_quality.md](docs/data_quality.md): data quality rules, quarantine and run log

## How it works

Each table is a small class that declares its schema, its inputs and its rules, and
implements `run()`. Defining the class registers it in its layer (bronze, silver or gold).
The pipeline builds the tables in dependency order:

```python
class SilverOrders(SilverTable):
    table_name = "orders"
    schema = schemas.ORDERS          # the output is cast to this schema
    rules = rules.ORDERS             # SQL conditions, true = good row
    dependencies = (BronzeOrders,)   # inputs, which also decide the build order
    merge_keys = ("order_line_id",)  # MERGE into the Iceberg table on this key

    def run(self, inputs):
        ...
```

- **Bronze** reads only the CSV files it hasn't loaded yet and stores every column as text.
- **Silver** cleans and types the data, applies the rules, and merges into the table on its key.
- **Gold** is rebuilt every run: `dim_customer`, `dim_product` (price history, SCD type 2),
  `fact_sales` and one table per question.

Rows that fail a rule go to `dq.quarantine`, rule counts to `dq.results`, and every table
build to `ops.run_log`. If a table fails, the tables depending on it are skipped, and the
rest still runs.

## Running it

You need Java 17 (Spark 3.5 doesn't support Java 21), Python 3.10+ and
[uv](https://docs.astral.sh/uv/).

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
uv sync --extra dev
uv run python -m lakehouse --env local
```

The first run downloads the Iceberg Spark runtime from Maven. The pipeline reads the CSVs
from `assignment/` and writes the warehouse to `data/warehouse/` (git-ignored). Running it
again is safe: loaded files are skipped and silver merges. To simulate an hourly delivery,
add a file such as `assignment/orders_2.csv` and run again. After a schema change, delete
`data/warehouse/` and run again.

To look at the results:

```python
from lakehouse.config import get_config
from lakehouse.spark import create_spark

spark = create_spark(get_config("local"))
spark.table("lakehouse.gold.revenue_by_country").orderBy("rank").show()
spark.table("lakehouse.ops.run_log").show(truncate=False)
spark.table("lakehouse.dq.quarantine").show(truncate=False)
```

## The four questions

| Question | Table | How |
|----------|-------|-----|
| Top 10 countries by number of customers | `gold.top_countries_by_customers` | customers per country |
| Revenue distribution by country | `gold.revenue_by_country` | revenue at the price valid on the invoice date; orders without a customer as `Unknown` |
| Average unit price vs sales volume | `gold.price_vs_volume` | one row per product: its average unit price next to its units sold |
| Top 3 products by price drop in the last month | `gold.top_price_drops` | price before December 2011 vs at its end, from the product price history |

The products file has no dates, so the price history is dated by assumption. See
[docs/data_analysis.md](docs/data_analysis.md#assumptions) for this and the other
assumptions, and the [results](docs/data_analysis.md#results).

## Tests

```bash
uv run pytest --cov
uv run ruff check src tests
```

A few unit tests for the rules and the silver and gold transformations, plus one test that
runs the whole pipeline on small CSV files. Each test uses its own temporary Iceberg catalog.

## Layout

```text
src/lakehouse/
├── __main__.py       python -m lakehouse --env local
├── config.py         settings per environment
├── spark.py          Spark session with the Iceberg catalog
├── core/             layers, Table base classes, data quality, run log, pipeline
└── tables/
    ├── bronze/       one module per source + schemas
    ├── silver/       one module per entity + schemas and rules
    └── gold/         dimensions, fact, one module per question + schemas and rules
tests/
docs/
```

## Use of AI

Parts of this project were written with the help of an AI coding assistant (Claude Code),
as the exercise allows:

- all markdown documentation, including this README;
- big part of the tests;
- docstrings and missing type annotations across `src/`.
