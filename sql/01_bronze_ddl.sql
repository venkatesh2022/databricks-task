-- ============================================================================
-- 01_bronze_ddl.sql
-- Raw landing tables — all columns STRING, no constraints.
-- Simulates files arriving from an OLTP source; we preserve exactly what landed.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS orders_bronze;
CREATE SCHEMA IF NOT EXISTS orders_silver;
CREATE SCHEMA IF NOT EXISTS orders_gold;
USE orders_bronze;

CREATE OR REPLACE TABLE bronze_customers (
    customer_id       STRING,
    first_name        STRING,
    last_name         STRING,
    email             STRING,
    phone             STRING,
    address_line1     STRING,
    city              STRING,
    state             STRING,
    postal_code       STRING,
    country           STRING,
    customer_segment  STRING,   -- RETAIL | WHOLESALE | VIP
    loyalty_tier      STRING,   -- BRONZE | SILVER | GOLD | PLATINUM
    is_active         STRING,   -- true | false
    created_at        STRING,
    updated_at        STRING,
    _ingest_ts        TIMESTAMP
) USING DELTA;

CREATE OR REPLACE TABLE bronze_products (
    product_id       STRING,
    sku              STRING,
    product_name     STRING,
    category         STRING,
    description      STRING,
    unit_price       STRING,   -- current list price
    cost_price       STRING,   -- supplier cost, for margin reporting
    weight_kg        STRING,
    supplier_sku     STRING,
    is_discontinued  STRING,   -- true | false
    created_at       STRING,
    updated_at       STRING,
    _ingest_ts       TIMESTAMP
) USING DELTA;

CREATE OR REPLACE TABLE bronze_orders (
    order_id          STRING,
    customer_id       STRING,
    order_date        STRING,
    order_channel     STRING,   -- WEB | MOBILE | STORE | PHONE
    payment_method    STRING,   -- CARD | PAYPAL | WIRE | COD
    payment_status    STRING,   -- PAID | PENDING | REFUNDED | FAILED
    currency          STRING,   -- USD | EUR | GBP
    shipping_address  STRING,
    shipping_cost     STRING,
    discount_amount   STRING,
    tax_amount        STRING,
    total_amount      STRING,
    coupon_code       STRING,   -- nullable
    notes             STRING,
    _ingest_ts        TIMESTAMP
) USING DELTA;

CREATE OR REPLACE TABLE bronze_order_items (
    order_item_id        STRING,
    order_id             STRING,
    product_id           STRING,
    quantity             STRING,
    unit_price           STRING,
    discount_amount      STRING,
    warehouse_id         STRING,   -- W-SEA | W-ATL | W-LON | W-NYC
    shipment_id          STRING,   -- nullable FK to bronze_shipments
    line_status          STRING,   -- PENDING|SHIPPED|DELIVERED|CANCELLED|RETURNED
    status_updated_at    STRING,
    shipped_at           STRING,   -- nullable
    delivered_at         STRING,   -- nullable
    cancelled_at         STRING,   -- nullable
    returned_at          STRING,   -- nullable
    cancellation_reason  STRING,   -- nullable
    return_reason        STRING,   -- nullable
    _ingest_ts           TIMESTAMP
) USING DELTA;

CREATE OR REPLACE TABLE bronze_shipments (
    shipment_id          STRING,
    order_id             STRING,
    carrier              STRING,   -- UPS | FEDEX | USPS | DHL
    tracking_number      STRING,
    shipped_at           STRING,
    estimated_delivery   STRING,
    actual_delivery      STRING,   -- nullable until delivered
    shipping_status      STRING,   -- IN_TRANSIT | DELIVERED | LOST
    _ingest_ts           TIMESTAMP
) USING DELTA;
