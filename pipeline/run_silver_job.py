"""
Databricks Job / Lakeflow Workflow task: Bronze → Silver (PySpark).

Prerequisite: Bronze tables exist in the Bronze schema and are populated.
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from transforms import run_silver  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-schema", default="orders_bronze")
    parser.add_argument("--silver-schema", default="orders_silver")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark = SparkSession.builder.getOrCreate()
    run_silver(
        spark,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
    )
