# Logical Architecture — Medallion on Databricks

The solution is organized as a classic **Bronze → Silver → Gold** lakehouse, with each layer stored as **Delta Lake** in its own schema on Databricks Free/Community Edition:

- `orders_bronze`
- `orders_silver`
- `orders_gold`

## Why Medallion for this workload?

Even with a tiny synthetic dataset, the layering demonstrates the production-grade pattern an interviewer expects:

| Concern                                      | Lives in |
|----------------------------------------------|----------|
| Immutable audit of what landed                | Bronze   |
| Type enforcement, deduplication, FK validation| Silver   |
| Business semantics, derived fields, serving  | Gold     |

Each layer has **one job**, which makes failures localized and reprocessing cheap.

## Layer detail

### Bronze — raw, append-only (`orders_bronze`)
- Tables: `orders_bronze.bronze_customers`, `orders_bronze.bronze_products`, `orders_bronze.bronze_orders`, `orders_bronze.bronze_order_items`
- All columns `STRING` — preserves exactly what the source sent, even if malformed
- One extra column per table: `_ingest_ts TIMESTAMP` (when the row was loaded)
- No PK/FK enforcement
- **Purpose**: reproducibility. Any downstream bug can be re-run from Bronze without re-fetching the source.

### Silver — cleaned, typed, conformed (`orders_silver`)
- Tables: `orders_silver.silver_customers`, `orders_silver.silver_products`, `orders_silver.silver_orders`, `orders_silver.silver_order_items`
- Proper types (`BIGINT`, `DECIMAL`, `TIMESTAMP`)
- Rows de-duplicated on natural keys (email, sku, order_item_id)
- `MERGE INTO` from Bronze handles late-arriving / updated rows
- FK-consistent: orphan line items (product or order missing) are filtered out and logged
- **Purpose**: the canonical, query-ready normalized model. This is where the ERD applies.

### Gold — business marts, derived fields (`orders_gold`)
Two kinds of Gold tables:

**Dimensional (Kimball) — for ad-hoc analysis:**
- `orders_gold.gold_dim_customer`, `orders_gold.gold_dim_product`
- `orders_gold.gold_fact_order_items` — denormalized star-schema fact at line-item grain (carries `order_date`, `order_month`, `line_total`, customer & product attributes)
- `orders_gold.gold_vw_order_status` — view deriving order-header status (`COMPLETED`, `PARTIALLY_SHIPPED`, `PENDING`, `CANCELLED`) from line items

**Pre-aggregated sales marts — for BI dashboards and the Task-2 query:**
- `orders_gold.gold_product_daily_sales`   — `(product × day)` grain
- `orders_gold.gold_product_weekly_sales`  — `(product × ISO-week-start)` grain
- `orders_gold.gold_product_monthly_sales` — `(product × month-start)` grain

All three marts carry the same status-broken-out measures: `units_delivered`, `revenue_delivered`, `units_shipped`, `units_pending`, `units_cancelled`, `units_returned`, plus `distinct_orders` and `_refreshed_at`. The "most sold product" query becomes a **single-row lookup** on the relevant mart instead of a runtime GROUP BY.

**Purpose**: Gold is where business semantics live. Dashboards read from the pre-aggregated marts (fast, cheap, consistent across users); analysts drop down to the atomic fact when they need flexibility the aggregates don't offer.

## Diagram

See `diagrams/architecture.mmd`:

```
Source CSV/OLTP  →  Bronze (raw)  →  Silver (typed + clean)  →  Gold (dims, facts, views)  →  Consumers (BI / SQL / ML)
```

## Storage & compute choices

- **Delta Lake** on all layers — ACID, time travel (`VERSION AS OF`) lets us roll back a bad transform, and cheap `MERGE` simplifies upserts.
- **Separate schemas per medallion layer** — `orders_bronze`, `orders_silver`, and `orders_gold` keep raw, conformed, and serving data isolated while still staying simple enough for Free/Community Edition.
- **No partitioning on this demo** — tables are small. In production, `gold_fact_order_items` would be partitioned (or Z-ORDERed in Delta) by `order_month` since every business query filters on time.
- **Idempotent DDL** — every `CREATE` uses `CREATE OR REPLACE` and every load uses `MERGE`, so re-running the whole notebook is safe.

## Scheduling (production shape)

In production the layers would each be a **Databricks Workflow / Lakeflow Job** task (or streaming pipeline) on a cadence:

| Layer  | Trigger                                                         |
|--------|-----------------------------------------------------------------|
| Bronze | File-arrival trigger / Auto Loader / CDC from OLTP              |
| Silver | Every 15 min, streaming or batch MERGE from Bronze (PySpark here) |
| Gold   | Every hour (fact/dim rebuild) or continuous via Lakeflow Pipelines |

This repo includes **`databricks.yml`** — a two-task Job (Silver then Gold Python file tasks) you can deploy with **Databricks Asset Bundles**. The bundle parameterizes `bronze_schema`, `silver_schema`, and `gold_schema`, while the **notebook** still runs Bronze + seed + Silver + Gold end-to-end for the interview demo.
