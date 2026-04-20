"""
Databricks Job / Lakeflow Workflow task: Silver → Gold (PySpark).

Prerequisite: Silver tables exist in the Silver schema (run `run_silver_job.py` first).
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

try:
    from pipeline.transforms import run_gold
except ModuleNotFoundError:
    from transforms import run_gold


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="dbricks_task")
    parser.add_argument("--silver-schema", default="orders_silver")
    parser.add_argument("--gold-schema", default="orders_gold")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark = SparkSession.builder.getOrCreate()
    run_gold(
        spark,
        catalog=args.catalog,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
