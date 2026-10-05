<!-- AI-assisted: written with the help of an AI coding assistant (Claude Code). -->

# Data quality and logging

## How it works

For every table (`Pipeline.build`):

1. `run()` computes the data, cast to the table's schema.
2. Each row is checked against the table's rules.
3. Rule counts go to `dq.results`; rows failing an `error` rule go to `dq.quarantine`.
4. The other rows are written to the table.

A rule is a SQL condition that is true for a good row. `error` quarantines the row, `warn`
only counts it. Bronze has no rules. If a table fails, the tables depending on it are skipped.

## Rules

| Table | Rule | Severity | Catches |
|-------|------|----------|---------|
| silver.customers | customer_id_not_null | error | missing or non-numeric id |
| | country_not_null | error | missing country |
| | country_unambiguous | warn | customer with several countries |
| | country_is_specific | warn | `Unknown`, `European Community` |
| silver.products | stock_code_not_null, version_not_null | error | missing key |
| | unit_price_not_null | error | missing price |
| | unit_price_positive | error | price of 0 or less |
| silver.orders | order_line_id_not_null | error | missing key |
| | invoice_no_not_null, stock_code_not_null | error | missing values |
| | invoice_ts_valid | error | missing or unparseable date |
| | quantity_valid | error | missing or zero quantity |
| | customer_id_not_null | warn | no customer |
| gold.dim_product | valid_range, unit_price_not_null | error | broken price history |
| gold.fact_sales | product_found, line_amount_not_null | error | line without a valid price |
| gold reports | customers_positive, country_not_null, revenue_not_null, avg_unit_price_not_null, is_a_drop | error | sanity checks |

## Run log

`ops.run_log` has one row per table per run: status (SUCCESS, FAILED, SKIPPED), row counts
and, for a failed or skipped table, the reason. Ingested files are in bronze
(`_source_file`, `_ingested_at`).

## Queries

```sql
-- last run
SELECT table_name, status, rows_total, rows_published, rows_quarantined, message
FROM lakehouse.ops.run_log
WHERE run_id = (SELECT MAX(run_id) FROM lakehouse.ops.run_log);

-- failed rules in the last run
SELECT table_name, rule, severity, failed_rows FROM lakehouse.dq.results
WHERE run_id = (SELECT MAX(run_id) FROM lakehouse.dq.results) AND failed_rows > 0;

-- quarantined rows of one rule
SELECT record FROM lakehouse.dq.quarantine WHERE ARRAY_CONTAINS(failed_rules, 'invoice_ts_valid');

-- ingested files
SELECT _source_file, _ingested_at, COUNT(*) FROM lakehouse.bronze.orders
GROUP BY _source_file, _ingested_at;
```

## Proposed rules

- Freshness: no new orders file for 2 hours, or no customers/products file for a day.
- Volume: a file much smaller or larger than usual.
- Referential: order stock codes and customers exist in products and customers.
- Price jumps: a price change above 50 %.
