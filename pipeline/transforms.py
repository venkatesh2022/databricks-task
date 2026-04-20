"""
Bronze → Silver → Gold transforms in PySpark + Delta.

Mirrors sql/03_silver_transform.sql and sql/04_gold_transform.sql so the notebook,
SQL files, and Workflow / Lakeflow Jobs tasks stay logically equivalent.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_LINE_STATUSES = ("PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")
DEFAULT_BRONZE_SCHEMA = "orders_bronze"
DEFAULT_SILVER_SCHEMA = "orders_silver"
DEFAULT_GOLD_SCHEMA = "orders_gold"


def run_silver(
    spark: SparkSession,
    bronze_schema: str = DEFAULT_BRONZE_SCHEMA,
    silver_schema: str = DEFAULT_SILVER_SCHEMA,
) -> None:
    """Typed, deduped Silver tables from Bronze via Delta MERGE (idempotent)."""
    _create_schema(spark, silver_schema)
    _merge_silver_customers(spark, bronze_schema, silver_schema)
    _merge_silver_products(spark, bronze_schema, silver_schema)
    _merge_silver_orders(spark, bronze_schema, silver_schema)
    _merge_silver_order_items(spark, bronze_schema, silver_schema)


def run_gold(
    spark: SparkSession,
    silver_schema: str = DEFAULT_SILVER_SCHEMA,
    gold_schema: str = DEFAULT_GOLD_SCHEMA,
) -> None:
    """Dims, fact, marts, and derived order-status view from Silver."""
    _create_schema(spark, gold_schema)

    sc = spark.table(f"{silver_schema}.silver_customers")
    sp = spark.table(f"{silver_schema}.silver_products")
    so = spark.table(f"{silver_schema}.silver_orders")
    oi = spark.table(f"{silver_schema}.silver_order_items")

    dim_customer = sc.select(
        "customer_id",
        "first_name",
        "last_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
        "email",
        "phone",
        F.col("created_at").alias("customer_since"),
    )

    dim_product = sp.select(
        "product_id",
        "sku",
        "product_name",
        "category",
        F.col("unit_price").alias("current_list_price"),
        F.col("created_at").alias("product_introduced_at"),
    )

    fact = (
        oi.join(so, "order_id")
        .join(sp, "product_id")
        .select(
            F.col("order_item_id"),
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("sku"),
            F.col("product_name"),
            F.col("category"),
            F.col("order_date"),
            F.date_trunc("month", F.col("order_date")).alias("order_month"),
            F.col("quantity"),
            F.col("unit_price"),
            F.col("line_total"),
            F.col("line_status"),
            F.col("status_updated_at"),
        )
    )

    _overwrite_delta(fact, f"{gold_schema}.gold_fact_order_items")

    dim_customer.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{gold_schema}.gold_dim_customer")

    dim_product.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{gold_schema}.gold_dim_product")

    g = spark.table(f"{gold_schema}.gold_fact_order_items")

    _write_sales_marts(gold_schema, g)

    _create_order_status_view(spark, gold_schema, so, oi)


def _overwrite_delta(df: DataFrame, full_table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(full_table_name)


def _create_schema(spark: SparkSession, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _merge_silver_customers(
    spark: SparkSession, bronze_schema: str, silver_schema: str
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silver_schema}.silver_customers (
            customer_id BIGINT NOT NULL, first_name STRING, last_name STRING,
            email STRING NOT NULL, phone STRING, created_at TIMESTAMP, _updated_ts TIMESTAMP
        ) USING DELTA
        """
    )
    bc = spark.table(f"{bronze_schema}.bronze_customers").where(
        F.col("customer_id").isNotNull() & F.col("email").isNotNull()
    )
    s = (
        bc.groupBy(
            F.col("customer_id").cast("long").alias("customer_id"),
            "first_name",
            "last_name",
            F.lower(F.trim(F.col("email"))).alias("email"),
            "phone",
            F.to_timestamp("created_at").alias("created_at"),
        )
        .agg(F.max("_ingest_ts").alias("_updated_ts"))
    )
    _merge_into(
        spark,
        silver_schema,
        "silver_customers",
        s,
        "t.customer_id = s.customer_id",
    )


def _merge_silver_products(
    spark: SparkSession, bronze_schema: str, silver_schema: str
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silver_schema}.silver_products (
            product_id BIGINT NOT NULL, sku STRING NOT NULL, product_name STRING,
            category STRING, unit_price DECIMAL(10,2), created_at TIMESTAMP, _updated_ts TIMESTAMP
        ) USING DELTA
        """
    )
    bp = spark.table(f"{bronze_schema}.bronze_products").where(
        F.col("product_id").isNotNull() & F.col("sku").isNotNull()
    )
    s = (
        bp.groupBy(
            F.col("product_id").cast("long").alias("product_id"),
            F.upper(F.trim(F.col("sku"))).alias("sku"),
            "product_name",
            "category",
            F.col("unit_price").cast("decimal(10,2)"),
            F.to_timestamp("created_at").alias("created_at"),
        )
        .agg(F.max("_ingest_ts").alias("_updated_ts"))
    )
    _merge_into(
        spark,
        silver_schema,
        "silver_products",
        s,
        "t.product_id = s.product_id",
    )


def _merge_silver_orders(
    spark: SparkSession, bronze_schema: str, silver_schema: str
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silver_schema}.silver_orders (
            order_id BIGINT NOT NULL, customer_id BIGINT NOT NULL,
            order_date TIMESTAMP, shipping_address STRING, _updated_ts TIMESTAMP
        ) USING DELTA
        """
    )
    bo = spark.table(f"{bronze_schema}.bronze_orders").alias("o")
    sc = spark.table(f"{silver_schema}.silver_customers").alias("c")
    s = (
        bo.join(
            sc,
            F.col("o.customer_id").cast("long") == F.col("c.customer_id"),
            "inner",
        )
        .where(F.col("o.order_id").isNotNull())
        .groupBy(
            F.col("o.order_id").cast("long").alias("order_id"),
            F.col("o.customer_id").cast("long").alias("customer_id"),
            F.to_timestamp("o.order_date").alias("order_date"),
            F.col("o.shipping_address"),
        )
        .agg(F.max(F.col("o._ingest_ts")).alias("_updated_ts"))
    )
    _merge_into(spark, silver_schema, "silver_orders", s, "t.order_id = s.order_id")


def _merge_silver_order_items(
    spark: SparkSession, bronze_schema: str, silver_schema: str
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silver_schema}.silver_order_items (
            order_item_id BIGINT NOT NULL, order_id BIGINT NOT NULL, product_id BIGINT NOT NULL,
            quantity INT, unit_price DECIMAL(10,2), line_total DECIMAL(12,2),
            line_status STRING, status_updated_at TIMESTAMP, _updated_ts TIMESTAMP
        ) USING DELTA
        """
    )
    b = spark.table(f"{bronze_schema}.bronze_order_items").alias("oi")
    so = spark.table(f"{silver_schema}.silver_orders").alias("o")
    sp = spark.table(f"{silver_schema}.silver_products").alias("p")

    joined = (
        b.join(
            so,
            F.col("oi.order_id").cast("long") == F.col("o.order_id"),
            "inner",
        )
        .join(
            sp,
            F.col("oi.product_id").cast("long") == F.col("p.product_id"),
            "inner",
        )
        .select(
            F.col("oi.order_item_id").alias("order_item_id"),
            F.col("oi.order_id").alias("order_id"),
            F.col("oi.product_id").alias("product_id"),
            F.col("oi.quantity").alias("quantity"),
            F.col("oi.unit_price").alias("unit_price"),
            F.col("oi.line_status").alias("line_status"),
            F.col("oi.status_updated_at").alias("status_updated_at"),
            F.col("oi._ingest_ts").alias("_ingest_ts"),
        )
        .where(F.col("order_item_id").isNotNull())
        .where(
            F.upper(F.trim(F.col("line_status"))).isin(list(VALID_LINE_STATUSES))
        )
    )

    g = joined.groupBy(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_status",
        "status_updated_at",
    ).agg(F.max("_ingest_ts").alias("_updated_ts"))

    s = g.select(
        F.col("order_item_id").cast("long").alias("order_item_id"),
        F.col("order_id").cast("long").alias("order_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        (
            F.col("quantity").cast("int")
            * F.col("unit_price").cast("decimal(10,2)")
        ).cast("decimal(12,2)").alias("line_total"),
        F.upper(F.trim(F.col("line_status"))).alias("line_status"),
        F.to_timestamp("status_updated_at").alias("status_updated_at"),
        F.col("_updated_ts"),
    )

    _merge_into(
        spark,
        silver_schema,
        "silver_order_items",
        s,
        "t.order_item_id = s.order_item_id",
    )


def _merge_into(
    spark: SparkSession,
    schema: str,
    table: str,
    source: DataFrame,
    condition: str,
) -> None:
    full = f"{schema}.{table}"
    tmp = f"_tmp_merge_{table}"
    source.createOrReplaceTempView(tmp)
    spark.sql(
        f"""
        MERGE INTO {full} t
        USING (SELECT * FROM {tmp}) s
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(tmp)


def _status_sum(status: str):
    return F.sum(F.when(F.col("line_status") == status, F.col("quantity")).otherwise(0))


def _write_sales_marts(schema: str, f: DataFrame) -> None:
    order_day = F.to_date("order_date")
    week_start = F.to_date(F.date_trunc("week", F.col("order_date")))
    month_start = F.to_date(F.date_trunc("month", F.col("order_date")))
    refreshed = F.current_timestamp().alias("_refreshed_at")

    base_cols = [
        "product_id",
        "sku",
        "product_name",
        "category",
    ]

    def mart_measures():
        return [
            _status_sum("DELIVERED").alias("units_delivered"),
            F.sum(
                F.when(
                    F.col("line_status") == "DELIVERED",
                    F.col("line_total"),
                ).otherwise(0)
            ).alias("revenue_delivered"),
            _status_sum("SHIPPED").alias("units_shipped"),
            _status_sum("PENDING").alias("units_pending"),
            _status_sum("CANCELLED").alias("units_cancelled"),
            _status_sum("RETURNED").alias("units_returned"),
            F.countDistinct("order_id").alias("distinct_orders"),
            refreshed,
        ]

    daily = f.groupBy(*base_cols, order_day.alias("order_day")).agg(*mart_measures())
    _overwrite_delta(daily, f"{schema}.gold_product_daily_sales")

    weekly = f.groupBy(*base_cols, week_start.alias("week_start")).agg(
        *mart_measures()
    )
    _overwrite_delta(weekly, f"{schema}.gold_product_weekly_sales")

    monthly = f.groupBy(*base_cols, month_start.alias("month_start")).agg(
        *mart_measures()
    )
    _overwrite_delta(monthly, f"{schema}.gold_product_monthly_sales")


def _create_order_status_view(
    spark: SparkSession, schema: str, so: DataFrame, oi: DataFrame
) -> None:
    j = so.join(oi, "order_id")
    delivered = F.when(F.col("line_status") == "DELIVERED", 1).otherwise(0)
    shipped = F.when(F.col("line_status") == "SHIPPED", 1).otherwise(0)
    pending = F.when(F.col("line_status") == "PENDING", 1).otherwise(0)
    cancelled = F.when(F.col("line_status") == "CANCELLED", 1).otherwise(0)
    returned = F.when(F.col("line_status") == "RETURNED", 1).otherwise(0)
    ship_or_del = F.when(
        F.col("line_status").isin("SHIPPED", "DELIVERED"), 1
    ).otherwise(0)

    agg = j.groupBy("order_id", "customer_id", "order_date").agg(
        F.count("*").alias("total_line_items"),
        F.sum(delivered).alias("delivered_count"),
        F.sum(shipped).alias("shipped_count"),
        F.sum(pending).alias("pending_count"),
        F.sum(cancelled).alias("cancelled_count"),
        F.sum(returned).alias("returned_count"),
        F.sum(ship_or_del).alias("_ship_del_lines"),
    )

    vw = agg.select(
        "order_id",
        "customer_id",
        "order_date",
        "total_line_items",
        "delivered_count",
        "shipped_count",
        "pending_count",
        "cancelled_count",
        "returned_count",
        F.when(
            F.col("delivered_count") == F.col("total_line_items"), F.lit("COMPLETED")
        )
        .when(
            F.col("cancelled_count") == F.col("total_line_items"),
            F.lit("CANCELLED"),
        )
        .when(F.col("_ship_del_lines") > 0, F.lit("PARTIALLY_SHIPPED"))
        .when(
            F.col("pending_count") == F.col("total_line_items"), F.lit("PENDING")
        )
        .otherwise(F.lit("IN_PROGRESS"))
        .alias("order_status"),
    )

    vw.createOrReplaceTempView("_tmp_gold_vw_order_status")
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {schema}.gold_vw_order_status AS
        SELECT * FROM _tmp_gold_vw_order_status
        """
    )
    spark.catalog.dropTempView("_tmp_gold_vw_order_status")
