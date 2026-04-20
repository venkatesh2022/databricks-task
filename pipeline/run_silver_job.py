"""
Databricks Job / Lakeflow Workflow task: Bronze → Silver (PySpark).

Prerequisite: Bronze tables exist in the Bronze schema and are populated.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

try:
    from pipeline.transforms import run_silver
except ModuleNotFoundError:
    from transforms import run_silver


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dbricks_task")
    parser.add_argument("--bronze-schema", default="orders_bronze")
    parser.add_argument("--silver-schema", default="orders_silver")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark = SparkSession.builder.getOrCreate()
    run_silver(
        spark,
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
    )
