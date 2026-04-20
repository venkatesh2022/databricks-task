"""
Databricks Job / Lakeflow Workflow task: Bronze → Silver (PySpark).

Prerequisite: schema `orders_demo` exists and Bronze tables are populated.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from transforms import run_silver  # noqa: E402

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run_silver(spark)
