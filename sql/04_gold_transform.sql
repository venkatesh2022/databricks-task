-- ============================================================================
-- 04_gold_transform.sql
-- Silver → Gold: dims, a denormalized fact, and a derived order-status view.
-- Runtime implementation used by the notebook and Jobs is PySpark: pipeline/transforms.py
-- This is what analysts and BI tools read.
-- ============================================================================

USE orders_demo;

-- ---------- gold_dim_customer ----------
CREATE OR REPLACE TABLE gold_dim_customer
USING DELTA
AS
SELECT
    customer_id,
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name)  AS full_name,
    email,
    phone,
    created_at                             AS customer_since
FROM silver_customers;

-- ---------- gold_dim_product ----------
CREATE OR REPLACE TABLE gold_dim_product
USING DELTA
AS
SELECT
    product_id,
    sku,
    product_name,
    category,
    unit_price                             AS current_list_price,
    created_at                             AS product_introduced_at
FROM silver_products;

-- ---------- gold_fact_order_items ----------
-- Star-schema fact at line-item grain. Denormalized for fast analytics:
-- carries order_date + customer key + product attributes so most queries
-- don't need to join back to the dims.
CREATE OR REPLACE TABLE gold_fact_order_items
USING DELTA
AS
SELECT
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    p.sku,
    p.product_name,
    p.category,
    o.order_date,
    DATE_TRUNC('MONTH', o.order_date)      AS order_month,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    oi.line_status,
    oi.status_updated_at
FROM silver_order_items oi
JOIN silver_orders    o USING (order_id)
JOIN silver_products  p USING (product_id);

-- ---------- gold_product_daily_sales ----------
-- Pre-aggregated sales mart at (product × day) grain.
-- Reads are index-lookups for the "most sold product" query family.
CREATE OR REPLACE TABLE gold_product_daily_sales USING DELTA AS
SELECT
    f.product_id, f.sku, f.product_name, f.category,
    CAST(f.order_date AS DATE)                                                      AS order_day,
    SUM(CASE WHEN f.line_status = 'DELIVERED' THEN f.quantity   ELSE 0 END)         AS units_delivered,
    SUM(CASE WHEN f.line_status = 'DELIVERED' THEN f.line_total ELSE 0 END)         AS revenue_delivered,
    SUM(CASE WHEN f.line_status = 'SHIPPED'   THEN f.quantity   ELSE 0 END)         AS units_shipped,
    SUM(CASE WHEN f.line_status = 'PENDING'   THEN f.quantity   ELSE 0 END)         AS units_pending,
    SUM(CASE WHEN f.line_status = 'CANCELLED' THEN f.quantity   ELSE 0 END)         AS units_cancelled,
    SUM(CASE WHEN f.line_status = 'RETURNED'  THEN f.quantity   ELSE 0 END)         AS units_returned,
    COUNT(DISTINCT f.order_id)                                                      AS distinct_orders,
    current_timestamp()                                                             AS _refreshed_at
FROM gold_fact_order_items f
GROUP BY f.product_id, f.sku, f.product_name, f.category, CAST(f.order_date AS DATE);

-- ---------- gold_product_weekly_sales ----------
-- Week starts on Monday (Spark DATE_TRUNC('WEEK', ...) convention).
CREATE OR REPLACE TABLE gold_product_weekly_sales USING DELTA AS
SELECT
    f.product_id, f.sku, f.product_name, f.category,
    CAST(DATE_TRUNC('WEEK', f.order_date) AS DATE)                                  AS week_start,
    SUM(CASE WHEN f.line_status = 'DELIVERED' THEN f.quantity   ELSE 0 END)         AS units_delivered,
    SUM(CASE WHEN f.line_status = 'DELIVERED' THEN f.line_total ELSE 0 END)         AS revenue_delivered,
    SUM(CASE WHEN f.line_status = 'SHIPPED'   THEN f.quantity   ELSE 0 END)         AS units_shipped,
    SUM(CASE WHEN f.line_status = 'PENDING'   THEN f.quantity   ELSE 0 END)         AS units_pending,
    SUM(CASE WHEN f.line_status = 'CANCELLED' THEN f.quantity   ELSE 0 END)         AS units_cancelled,
    SUM(CASE WHEN f.line_status = 'RETURNED'  THEN f.quantity   ELSE 0 END)         AS units_returned,
    COUNT(DISTINCT f.order_id)                                                      AS distinct_orders,
    current_timestamp()                                                             AS _refreshed_at
FROM gold_fact_order_items f
GROUP BY f.product_id, f.sku, f.product_name, f.category,
         CAST(DATE_TRUNC('WEEK', f.order_date) AS DATE);

-- ---------- gold_product_monthly_sales ----------
-- This is the primary mart for the Task-2 "most sold in last month" query.
CREATE OR REPLACE TABLE gold_product_monthly_sales USING DELTA AS
SELECT
    f.product_id, f.sku, f.product_name, f.category,
    CAST(DATE_TRUNC('MONTH', f.order_date) AS DATE)                                 AS month_start,
    SUM(CASE WHEN f.line_status = 'DELIVERED' THEN f.quantity   ELSE 0 END)         AS units_delivered,
    SUM(CASE WHEN f.line_status = 'DELIVERED' THEN f.line_total ELSE 0 END)         AS revenue_delivered,
    SUM(CASE WHEN f.line_status = 'SHIPPED'   THEN f.quantity   ELSE 0 END)         AS units_shipped,
    SUM(CASE WHEN f.line_status = 'PENDING'   THEN f.quantity   ELSE 0 END)         AS units_pending,
    SUM(CASE WHEN f.line_status = 'CANCELLED' THEN f.quantity   ELSE 0 END)         AS units_cancelled,
    SUM(CASE WHEN f.line_status = 'RETURNED'  THEN f.quantity   ELSE 0 END)         AS units_returned,
    COUNT(DISTINCT f.order_id)                                                      AS distinct_orders,
    current_timestamp()                                                             AS _refreshed_at
FROM gold_fact_order_items f
GROUP BY f.product_id, f.sku, f.product_name, f.category,
         CAST(DATE_TRUNC('MONTH', f.order_date) AS DATE);

-- ---------- gold_vw_order_status ----------
-- Derived order-level status from line items.
-- An order is COMPLETED only when every line is DELIVERED.
CREATE OR REPLACE VIEW gold_vw_order_status AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    COUNT(*)                                                 AS total_line_items,
    SUM(CASE WHEN oi.line_status = 'DELIVERED'  THEN 1 ELSE 0 END) AS delivered_count,
    SUM(CASE WHEN oi.line_status = 'SHIPPED'    THEN 1 ELSE 0 END) AS shipped_count,
    SUM(CASE WHEN oi.line_status = 'PENDING'    THEN 1 ELSE 0 END) AS pending_count,
    SUM(CASE WHEN oi.line_status = 'CANCELLED'  THEN 1 ELSE 0 END) AS cancelled_count,
    SUM(CASE WHEN oi.line_status = 'RETURNED'   THEN 1 ELSE 0 END) AS returned_count,
    CASE
        WHEN SUM(CASE WHEN oi.line_status = 'DELIVERED' THEN 1 ELSE 0 END) = COUNT(*)
             THEN 'COMPLETED'
        WHEN SUM(CASE WHEN oi.line_status = 'CANCELLED' THEN 1 ELSE 0 END) = COUNT(*)
             THEN 'CANCELLED'
        WHEN SUM(CASE WHEN oi.line_status IN ('SHIPPED','DELIVERED') THEN 1 ELSE 0 END) > 0
             THEN 'PARTIALLY_SHIPPED'
        WHEN SUM(CASE WHEN oi.line_status = 'PENDING' THEN 1 ELSE 0 END) = COUNT(*)
             THEN 'PENDING'
        ELSE 'IN_PROGRESS'
    END AS order_status
FROM silver_orders     o
JOIN silver_order_items oi USING (order_id)
GROUP BY o.order_id, o.customer_id, o.order_date;
