"""
Databricks Job / Lakeflow Workflow task: Silver → Gold (PySpark).

Prerequisite: Silver tables populated (run `run_silver_job.py` first).
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from transforms import run_gold  # noqa: E402

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run_gold(spark)
