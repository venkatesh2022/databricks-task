-- ============================================================================
-- 03_silver_transform.sql
-- Bronze → Silver: type-cast, deduplicate on natural keys, drop orphan rows.
-- Runtime implementation used by the notebook and Jobs is PySpark: pipeline/transforms.py
-- All Silver tables are idempotently MERGEd so re-running is safe.
-- ============================================================================

USE orders_demo;

-- ---------- silver_customers ----------
CREATE OR REPLACE TABLE silver_customers (
    customer_id   BIGINT       NOT NULL,
    first_name    STRING,
    last_name     STRING,
    email         STRING       NOT NULL,
    phone         STRING,
    created_at    TIMESTAMP,
    _updated_ts   TIMESTAMP
) USING DELTA;

MERGE INTO silver_customers t
USING (
    SELECT
        CAST(customer_id AS BIGINT)                 AS customer_id,
        first_name,
        last_name,
        LOWER(TRIM(email))                          AS email,
        phone,
        CAST(created_at AS TIMESTAMP)               AS created_at,
        MAX(_ingest_ts)                             AS _updated_ts
    FROM bronze_customers
    WHERE customer_id IS NOT NULL
      AND email       IS NOT NULL
    GROUP BY customer_id, first_name, last_name, LOWER(TRIM(email)), phone, CAST(created_at AS TIMESTAMP)
) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ---------- silver_products ----------
CREATE OR REPLACE TABLE silver_products (
    product_id    BIGINT          NOT NULL,
    sku           STRING          NOT NULL,
    product_name  STRING,
    category      STRING,
    unit_price    DECIMAL(10,2),
    created_at    TIMESTAMP,
    _updated_ts   TIMESTAMP
) USING DELTA;

MERGE INTO silver_products t
USING (
    SELECT
        CAST(product_id AS BIGINT)          AS product_id,
        UPPER(TRIM(sku))                    AS sku,
        product_name,
        category,
        CAST(unit_price AS DECIMAL(10,2))   AS unit_price,
        CAST(created_at AS TIMESTAMP)       AS created_at,
        MAX(_ingest_ts)                     AS _updated_ts
    FROM bronze_products
    WHERE product_id IS NOT NULL
      AND sku        IS NOT NULL
    GROUP BY product_id, UPPER(TRIM(sku)), product_name, category,
             CAST(unit_price AS DECIMAL(10,2)), CAST(created_at AS TIMESTAMP)
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ---------- silver_orders ----------
-- FK check: customer must exist in silver_customers, else we drop (and could
-- log to a quarantine table in production).
CREATE OR REPLACE TABLE silver_orders (
    order_id          BIGINT      NOT NULL,
    customer_id       BIGINT      NOT NULL,
    order_date        TIMESTAMP,
    shipping_address  STRING,
    _updated_ts       TIMESTAMP
) USING DELTA;

MERGE INTO silver_orders t
USING (
    SELECT
        CAST(o.order_id AS BIGINT)          AS order_id,
        CAST(o.customer_id AS BIGINT)       AS customer_id,
        CAST(o.order_date AS TIMESTAMP)     AS order_date,
        o.shipping_address,
        MAX(o._ingest_ts)                   AS _updated_ts
    FROM bronze_orders o
    JOIN silver_customers c
      ON CAST(o.customer_id AS BIGINT) = c.customer_id   -- FK enforce
    WHERE o.order_id IS NOT NULL
    GROUP BY CAST(o.order_id AS BIGINT), CAST(o.customer_id AS BIGINT),
             CAST(o.order_date AS TIMESTAMP), o.shipping_address
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ---------- silver_order_items ----------
-- line_total is computed here so downstream never has to.
CREATE OR REPLACE TABLE silver_order_items (
    order_item_id       BIGINT           NOT NULL,
    order_id            BIGINT           NOT NULL,
    product_id          BIGINT           NOT NULL,
    quantity            INT,
    unit_price          DECIMAL(10,2),
    line_total          DECIMAL(12,2),
    line_status         STRING,
    status_updated_at   TIMESTAMP,
    _updated_ts         TIMESTAMP
) USING DELTA;

MERGE INTO silver_order_items t
USING (
    SELECT
        CAST(oi.order_item_id AS BIGINT)                                          AS order_item_id,
        CAST(oi.order_id       AS BIGINT)                                         AS order_id,
        CAST(oi.product_id     AS BIGINT)                                         AS product_id,
        CAST(oi.quantity       AS INT)                                            AS quantity,
        CAST(oi.unit_price     AS DECIMAL(10,2))                                  AS unit_price,
        CAST(oi.quantity AS INT) * CAST(oi.unit_price AS DECIMAL(10,2))           AS line_total,
        UPPER(TRIM(oi.line_status))                                               AS line_status,
        CAST(oi.status_updated_at AS TIMESTAMP)                                   AS status_updated_at,
        MAX(oi._ingest_ts)                                                        AS _updated_ts
    FROM bronze_order_items oi
    JOIN silver_orders   o ON CAST(oi.order_id   AS BIGINT) = o.order_id       -- FK to order
    JOIN silver_products p ON CAST(oi.product_id AS BIGINT) = p.product_id     -- FK to product
    WHERE oi.order_item_id IS NOT NULL
      AND UPPER(TRIM(oi.line_status)) IN ('PENDING','SHIPPED','DELIVERED','CANCELLED','RETURNED')
    GROUP BY oi.order_item_id, oi.order_id, oi.product_id,
             oi.quantity, oi.unit_price, oi.line_status, oi.status_updated_at
) s
ON t.order_item_id = s.order_item_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
