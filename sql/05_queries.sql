-- ============================================================================
-- 05_queries.sql
-- Task 2: "Most sold product in the last month"
--
-- Definition chosen (primary query):
--   * "Sold"      = line items with line_status = 'DELIVERED'
--                   (cancelled / pending / returned items were never sold)
--   * "Last month" = the previous *calendar* month
--                   (e.g. run on 2026-04-20 → March 2026 = [2026-03-01, 2026-04-01))
--   * "Most sold"  = highest sum(quantity); ties broken by revenue then SKU
--
-- Two alternative definitions are included at the bottom for comparison.
-- ============================================================================

USE orders_demo;

-- ----------------------------------------------------------------------------
-- PRIMARY QUERY — most sold product in the previous calendar month
-- Reads directly from the gold_product_monthly_sales mart (no aggregation at
-- read time; the mart has already done the GROUP BY).
-- ----------------------------------------------------------------------------
SELECT
    product_id, sku, product_name, category,
    units_delivered    AS units_sold,
    revenue_delivered  AS revenue,
    distinct_orders
FROM gold_product_monthly_sales
WHERE month_start = CAST(DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_DATE(), -1)) AS DATE)
ORDER BY units_delivered DESC, revenue_delivered DESC, sku
LIMIT 1;

-- ----------------------------------------------------------------------------
-- Equivalent answer computed on-the-fly from the atomic fact.
-- Kept as documentation / fallback — it must produce the same result as the
-- mart-based query above.
-- ----------------------------------------------------------------------------
WITH last_month AS (
    SELECT
        DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_DATE(), -1)) AS start_ts,
        DATE_TRUNC('MONTH', CURRENT_DATE())                 AS end_ts
)
SELECT
    f.product_id, f.sku, f.product_name, f.category,
    SUM(f.quantity)            AS units_sold,
    SUM(f.line_total)          AS revenue,
    COUNT(DISTINCT f.order_id) AS distinct_orders
FROM gold_fact_order_items f
CROSS JOIN last_month lm
WHERE f.order_date  >= lm.start_ts
  AND f.order_date   < lm.end_ts
  AND f.line_status  = 'DELIVERED'
GROUP BY f.product_id, f.sku, f.product_name, f.category
ORDER BY units_sold DESC, revenue DESC, f.sku
LIMIT 1;

-- ----------------------------------------------------------------------------
-- Same pattern at weekly and daily grain (one-row lookups on the marts)
-- ----------------------------------------------------------------------------

-- Most sold product last week (Mon–Sun)
SELECT
    sku, product_name, category,
    units_delivered, revenue_delivered, distinct_orders
FROM gold_product_weekly_sales
WHERE week_start = CAST(DATE_TRUNC('WEEK', CURRENT_DATE() - INTERVAL 7 DAYS) AS DATE)
ORDER BY units_delivered DESC, revenue_delivered DESC
LIMIT 1;

-- Most sold product yesterday
SELECT
    sku, product_name, category,
    units_delivered, revenue_delivered, distinct_orders
FROM gold_product_daily_sales
WHERE order_day = CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY units_delivered DESC, revenue_delivered DESC
LIMIT 1;

-- Demo-friendly version (pick a day we know has data in the seed): 2026-03-06
SELECT sku, product_name, units_delivered, revenue_delivered
FROM gold_product_daily_sales
WHERE order_day = DATE '2026-03-06'
ORDER BY units_delivered DESC, revenue_delivered DESC
LIMIT 5;

-- ----------------------------------------------------------------------------
-- Same query, showing the full leaderboard (useful for verification)
-- ----------------------------------------------------------------------------
WITH last_month AS (
    SELECT
        DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_DATE(), -1)) AS start_ts,
        DATE_TRUNC('MONTH', CURRENT_DATE())                 AS end_ts
)
SELECT
    f.sku,
    f.product_name,
    f.category,
    SUM(f.quantity)       AS units_sold,
    SUM(f.line_total)     AS revenue
FROM gold_fact_order_items f
CROSS JOIN last_month lm
WHERE f.order_date  >= lm.start_ts
  AND f.order_date   < lm.end_ts
  AND f.line_status  = 'DELIVERED'
GROUP BY f.sku, f.product_name, f.category
ORDER BY units_sold DESC, revenue DESC;

-- ----------------------------------------------------------------------------
-- Alternate 1: "last month" = trailing 30 days (rolling window)
--   Useful if the business wants "the last 30 days" rather than the previous
--   calendar month.
-- ----------------------------------------------------------------------------
SELECT
    f.sku,
    f.product_name,
    SUM(f.quantity)   AS units_sold,
    SUM(f.line_total) AS revenue
FROM gold_fact_order_items f
WHERE f.order_date  >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  AND f.line_status  = 'DELIVERED'
GROUP BY f.sku, f.product_name
ORDER BY units_sold DESC, revenue DESC
LIMIT 1;

-- ----------------------------------------------------------------------------
-- Alternate 2: ranked by revenue, not quantity
--   Some businesses care about "most sold" in dollars rather than units.
-- ----------------------------------------------------------------------------
WITH last_month AS (
    SELECT
        DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_DATE(), -1)) AS start_ts,
        DATE_TRUNC('MONTH', CURRENT_DATE())                 AS end_ts
)
SELECT
    f.sku,
    f.product_name,
    SUM(f.line_total)  AS revenue,
    SUM(f.quantity)    AS units_sold
FROM gold_fact_order_items f
CROSS JOIN last_month lm
WHERE f.order_date  >= lm.start_ts
  AND f.order_date   < lm.end_ts
  AND f.line_status  = 'DELIVERED'
GROUP BY f.sku, f.product_name
ORDER BY revenue DESC, units_sold DESC
LIMIT 1;

-- ----------------------------------------------------------------------------
-- Sanity checks (bonus): sample the derived order-status view
-- ----------------------------------------------------------------------------
SELECT order_status, COUNT(*) AS n_orders
FROM gold_vw_order_status
GROUP BY order_status
ORDER BY n_orders DESC;

-- A completed order: every line DELIVERED
SELECT * FROM gold_vw_order_status WHERE order_status = 'COMPLETED' LIMIT 5;

-- A partially-shipped order (the one with a CANCELLED or SHIPPED line)
SELECT * FROM gold_vw_order_status WHERE order_status = 'PARTIALLY_SHIPPED' LIMIT 5;
