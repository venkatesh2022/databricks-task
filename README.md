# Orders Data Model — Interview Task

Solution for the scenario:
> Customers place orders. Each order has multiple line items (different products, quantities, statuses). When the complete order is delivered, it is marked Completed. Track status at the item level.

Deliverables:
- **Task 1** — Data model + ER relationships (`data_model.md`, `diagrams/erd.mmd`)
- **Task 2** — "Most sold product in the last month" query (`sql/05_queries.sql`)
- Medallion-layered logical architecture on Databricks (`architecture.md`, `diagrams/architecture.mmd`)

## Repository layout

```
thota/
├── README.md                      # this file
├── databricks.yml                 # Asset Bundle: Workflow / Lakeflow Job (Silver + Gold tasks)
├── data_model.md                  # entities, keys, relationships, design choices
├── architecture.md                # Bronze / Silver / Gold layout on Databricks
├── pipeline/
│   ├── transforms.py              # PySpark: Bronze→Silver, Silver→Gold (Delta MERGE / marts)
│   ├── run_silver_job.py          # Job entrypoint: Silver only
│   └── run_gold_job.py            # Job entrypoint: Gold only (depends on Silver in the Job DAG)
├── diagrams/
│   ├── erd.mmd                    # ER diagram (Mermaid)
│   └── architecture.mmd           # Medallion architecture diagram (Mermaid)
├── sql/
│   ├── 01_bronze_ddl.sql          # raw landing tables
│   ├── 02_bronze_seed.sql         # synthetic demo data
│   ├── 03_silver_transform.sql    # reference SQL (mirrors transforms.py)
│   ├── 04_gold_transform.sql      # reference SQL (mirrors transforms.py)
│   └── 05_queries.sql             # "most sold product in last month" + variants
└── notebook.ipynb                 # Databricks notebook: Bronze in SQL; Silver/Gold via PySpark
```

## Run on Databricks Free / Community Edition

**Recommended:** clone this folder as a **Databricks Repo** so `notebook.ipynb` and the `pipeline/` package live side by side (imports use the notebook’s parent directory).

1. Sign in at <https://community.cloud.databricks.com> (or Free Edition).
2. Add the repo (or upload the whole project folder, not only the `.ipynb`).
3. Attach to a running cluster (classic Spark 3.x+ with Delta; Community Edition does not support serverless for Python notebooks in all cases).
4. **Run All**. The notebook will:
   - create schema `orders_demo`
   - build **Bronze** with SQL, then **Silver** and **Gold** with **PySpark** (`pipeline/transforms.py`)
   - seed demo data
   - run the "most sold product" queries and print results

Silver/Gold are idempotent (`CREATE OR REPLACE` / `MERGE` / overwrite facts) — re-running is safe.

### Workflow / Lakeflow Job (optional)

`databricks.yml` defines a two-task Job: `run_silver_job.py` then `run_gold_job.py`. **Bronze must already exist** (run the notebook through the seed cells, or your own ingest). Deploy with the Databricks CLI:

```bash
databricks bundle deploy
databricks bundle run orders_medallion
```

Adjust `node_type_id` / `spark_version` for your workspace, or attach an `existing_cluster_id` in the Job UI after import.

## Demo dataset

- 8 customers, 10 products (3 categories), 25 orders, ~70 line items
- Orders span **Feb–Apr 2026**; "last month" resolves to **March 2026** when run on or after 2026-04-01
- Line-item statuses: `PENDING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `RETURNED`
- Order header status is **derived**, not stored, so it can never drift from the line items

## The "most sold product in the last month" query

Defined as: the product with the highest total **delivered** quantity in the previous **calendar month**, tie-broken by revenue. See `sql/05_queries.sql` for the primary query and two alternate definitions (trailing-30-days, revenue-based).
