# Data Model — Customer / Order / Line Item / Product

## 1. Entities

### 1.1 `customer`
One row per person or business that can place orders.

| Column         | Type            | Key  | Notes                              |
|----------------|-----------------|------|------------------------------------|
| `customer_id`  | `BIGINT`        | PK   | Surrogate key                      |
| `first_name`   | `STRING`        |      |                                    |
| `last_name`    | `STRING`        |      |                                    |
| `email`        | `STRING`        | UK   | Unique; natural key for upserts    |
| `phone`        | `STRING`        |      |                                    |
| `created_at`   | `TIMESTAMP`     |      | When the customer was registered   |

### 1.2 `product`
One row per sellable SKU.

| Column         | Type            | Key  | Notes                              |
|----------------|-----------------|------|------------------------------------|
| `product_id`   | `BIGINT`        | PK   | Surrogate key                      |
| `sku`          | `STRING`        | UK   | Human-readable natural key         |
| `product_name` | `STRING`        |      |                                    |
| `category`     | `STRING`        |      | Electronics / Accessories / …      |
| `unit_price`   | `DECIMAL(10,2)` |      | Current catalog price              |
| `created_at`   | `TIMESTAMP`     |      |                                    |

### 1.3 `order`
Order **header** — one row per order placed by a customer. **Order-level status is derived**, not stored.

| Column             | Type        | Key  | Notes                                |
|--------------------|-------------|------|--------------------------------------|
| `order_id`         | `BIGINT`    | PK   |                                      |
| `customer_id`      | `BIGINT`    | FK → `customer.customer_id`          |
| `order_date`       | `TIMESTAMP` |      | When the order was placed            |
| `shipping_address` | `STRING`    |      | Snapshot at time of order            |

### 1.4 `order_item`
Order **line item** — the product/quantity/status grain. This is the fact-level entity.

| Column              | Type            | Key  | Notes                                                |
|---------------------|-----------------|------|------------------------------------------------------|
| `order_item_id`     | `BIGINT`        | PK   |                                                      |
| `order_id`          | `BIGINT`        | FK → `order.order_id`                                       |
| `product_id`        | `BIGINT`        | FK → `product.product_id`                                   |
| `quantity`          | `INT`           |      | Units ordered of this product                        |
| `unit_price`        | `DECIMAL(10,2)` |      | Snapshot of price at order time (not current price)  |
| `line_total`        | `DECIMAL(12,2)` |      | Generated: `quantity * unit_price`                   |
| `line_status`       | `STRING`        |      | See status enum below                                |
| `status_updated_at` | `TIMESTAMP`     |      | Last state change                                    |

#### Line-item status enum
`PENDING` → `SHIPPED` → `DELIVERED`
with terminal branches `CANCELLED` and `RETURNED`.

## 2. Relationships

| From       | To           | Cardinality | Verb                                |
|------------|--------------|-------------|-------------------------------------|
| customer   | order        | 1 : 0..N    | a customer *places* orders          |
| order      | order_item   | 1 : 1..N    | an order *contains* line items      |
| product    | order_item   | 1 : 0..N    | a product *is referenced by* items  |

Notes on cardinality:
- `order → order_item` is **1 : 1..N** (never zero). An order with no items is meaningless and should be rejected at ingest.
- `customer → order` and `product → order_item` are **1 : 0..N**. A new customer or product has no orders yet.

## 3. Why `order.status` is **derived**, not stored

The prompt says: *"When the complete order is delivered, it is marked as Completed."*

Two ways to implement that:

| Option                                 | Pros                            | Cons                                             |
|----------------------------------------|---------------------------------|--------------------------------------------------|
| **A. Store `order.status` column**     | Fast reads                      | Must be kept consistent with line items via trigger / job — denormalized, drifts on failure |
| **B. Derive from `order_item.line_status`** (chosen) | Single source of truth, cannot drift | Slightly more expensive read — mitigated by materialized view / Gold table |

This design picks **B**. The Gold layer exposes a view `vw_order_status` that computes:

```
order_status =
  CASE
    WHEN every line is DELIVERED                 THEN 'COMPLETED'
    WHEN every line is CANCELLED                 THEN 'CANCELLED'
    WHEN any line is SHIPPED or DELIVERED        THEN 'PARTIALLY_SHIPPED'
    WHEN any line is PENDING                     THEN 'PENDING'
    ELSE                                              'IN_PROGRESS'
  END
```

If a stored status is required by a downstream consumer, the same logic can be materialized into `gold_dim_order.order_status` by a scheduled job — but the view remains the source of truth.

## 4. Other design decisions

- **Price snapshot on the line item** (`unit_price`) — product catalog prices change. Revenue must be computed from the snapshot, not from `product.unit_price` at read time.
- **Surrogate keys everywhere** — `customer_id`, `product_id`, `order_id`, `order_item_id` are all surrogate `BIGINT`s. Natural keys (`email`, `sku`) are kept as unique constraints for upsert logic but are never joined on.
- **`line_total` as a generated column** — avoids divergence between stored and computed revenue. Delta supports `GENERATED ALWAYS AS`.
- **No soft-delete flag on items** — cancellation is just a status. Keeps the state machine simple.
- **Status history is out of scope** for this model. If audit is required, add `order_item_status_history (order_item_id, from_status, to_status, changed_at, changed_by)`; the current model keeps only the latest state.

## 5. ER diagram

See `diagrams/erd.mmd` (Mermaid source). Rendered form:

```
CUSTOMER ||--o{ ORDER : places
ORDER    ||--|{ ORDER_ITEM : contains
PRODUCT  ||--o{ ORDER_ITEM : referenced by
```
