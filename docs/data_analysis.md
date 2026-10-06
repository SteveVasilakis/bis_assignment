<!-- AI-assisted: written with the help of an AI coding assistant (Claude Code). -->

# Data analysis

| File | Delivered | Rows | Columns |
|------|-----------|-----:|---------|
| customers.csv | daily | 4,389 | CustomerID, Country |
| products.csv | daily | 5,635 | StockCode, Description, UnitPrice |
| orders.csv | hourly | 541,909 | InvoiceNo, StockCode, Quantity, InvoiceDate, CustomerID |

## Findings

### Customers

- 9 rows without CustomerID: quarantined.
- 8 customers listed twice with different countries: kept once as `Multiple`, with both
  countries in `countries`.
- `EIRE`, `RSA`, `USA`, `Unspecified` renamed to Ireland, South Africa, United States, Unknown.
- 90 % of customers are in the UK.

### Products

- Several prices per stock code (up to 8) and no dates. The rows of a stock code are
  together, and the top row is the real product name in 93 % of cases.
- 960 blank descriptions and about 630 warehouse notes (`check`, `damaged`, `?`, ...):
  kept as prices; the product name comes from the row in capitals.
- 17 stock codes in lower case: upper-cased.
- 10 prices of 0: treated as missing and quarantined. 3 products have no other price, so
  their 641 order lines are quarantined in `fact_sales`.
- Prices range from 0 to 5 and look randomly generated.
- Stock codes that aren't goods:

  | product_type | Stock codes | In revenue |
  |---|---|---|
  | SHIPPING | POST, DOT, C2 | yes |
  | ADJUSTMENT | D (discount), M (manual) | yes |
  | NON_REVENUE | BANK CHARGES, AMAZONFEE, CRUK, B (bad debt), S (samples), gift vouchers | no |
  | MERCHANDISE | everything else | yes |

### Orders

- 135,080 lines (25 %) without CustomerID, mostly web-shop sales: kept, reported as `Unknown`.
- 5,429 exact duplicate lines: collapsed into one.
- 2 unparseable dates (`15/5/2011 14:48`, `1/11/2011 12:90`): quarantined.
- 9,288 cancellations (`C` invoices, negative quantity): reduce revenue.
- 1,336 negative quantities on normal invoices and 3 `A` invoices: stock adjustments, not
  revenue.
- Stock codes match products only after upper-casing.
- Orders run from 2010-12-01 to 2011-12-09.

## Assumptions

- **A1.** A stock code's rows are its price history, bottom = oldest, top = current. Each
  change is one month after the previous one; the current price is valid from the month of
  the latest order (December 2011).
- **A2.** Revenue = goods and delivery charged to customers, minus cancellations and discounts,
  at the price valid on the invoice date. Fees, bad debt, samples, vouchers and stock
  adjustments are excluded.
- **A3.** "Last month" is the latest month with orders: December 2011.
- **A4.** Price drop = price before the month minus price at the end of the month.
- **A5.** Average unit price = average over a product's price history. Sales volume = units
  sold minus units cancelled.
- **A6.** Orders without a customer are sales, reported as `Unknown`.
- **A7.** Customers with two countries are reported as `Multiple`.
- **A8.** Exact duplicate lines are one line delivered twice. The same product twice on one
  invoice with different quantities is two purchases.
- **A9.** A price of 0 means a missing price.

## Ad-hoc queries

```sql
-- customers listed with more than one country
SELECT customer_id, COLLECT_SET(country) FROM lakehouse.bronze.customers
WHERE TRIM(customer_id) <> '' GROUP BY customer_id HAVING COUNT(*) > 1;

-- descriptions that are notes instead of names
SELECT TRIM(description), COUNT(*) FROM lakehouse.bronze.products
WHERE description RLIKE '[a-z]' GROUP BY 1 ORDER BY 2 DESC;

-- unparseable dates
SELECT invoice_date FROM lakehouse.bronze.orders
WHERE TO_TIMESTAMP(invoice_date, 'M/d/yyyy H:mm') IS NULL;

-- exact duplicate order lines
SELECT COUNT(*) - COUNT(DISTINCT invoice_no, stock_code, quantity, invoice_date, customer_id)
FROM lakehouse.bronze.orders;
```

## Results

**Q1. Top 10 countries by customers** (`dense_rank`, so ties share a rank):

| Rank | Countries | Customers |
|---:|---|---:|
| 1 | United Kingdom | 3,950 |
| 2 | Germany | 95 |
| 3 | France | 87 |
| 4 | Spain | 29 |
| 5 | Belgium | 22 |
| 6 | Portugal, Switzerland | 19 |
| 7 | Italy | 15 |
| 8 | Finland | 12 |
| 9 | Norway | 10 |
| 10 | Austria, Channel Islands, Netherlands | 9 |

**Q2. Revenue by country** (top 10 of 38):

| Country | Revenue | Share |
|---|---:|---:|
| United Kingdom | 10,145,747.50 | 74.29 % |
| Unknown | 1,217,470.20 | 8.91 % |
| Netherlands | 515,649.03 | 3.78 % |
| Ireland | 342,028.87 | 2.50 % |
| Germany | 308,747.70 | 2.26 % |
| France | 279,813.45 | 2.05 % |
| Australia | 204,591.50 | 1.50 % |
| Sweden | 92,716.54 | 0.68 % |
| Switzerland | 71,474.21 | 0.52 % |
| Spain | 68,134.79 | 0.50 % |

**Q3. Average unit price vs units sold:** one row per product (3,940). Products below the
median price (2.53) sell 1,400 units on average, products above it 1,319, and the best
sellers come from every price level. There is no real relationship.

**Q4. Top 3 price drops in December 2011:**

| Stock code | Description | Before | After | Drop |
|---|---|---:|---:|---:|
| 85198 | ASSORTED FARMYARD ANIMALS IN BUCKET | 4.94 | 0.09 | 4.85 |
| 16207B | PINK HEART RED HANDBAG | 4.83 | 0.07 | 4.76 |
| 79063D | RETRO PILL BOX, REVOLUTIONARY | 4.81 | 0.16 | 4.65 |

## Open questions

- The one-month spacing of price changes (A1) is an assumption; only the order is in the data.
- Products files are treated as full snapshots. If they are incremental, silver products
  needs a different merge.
