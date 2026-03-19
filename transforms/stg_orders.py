# Databricks notebook source
"""
Task 1a — Staging / cleaning layer: orders

Source  : Bronze  ->  data/raw_orders.csv (Reading data from volume)
Output  : Silver  ->  top_notch_solutions.staging.stg_orders (Delta)

Transformations applied:
1. Rename amount_usd       ->  order_amount
2. Normalise status        ->  lowercase + trimmed
3. Cast order_date         ->  DateType
4. Cast order_amount       ->  DoubleType
5. Exclude rows where order_amount IS NULL or = 0
6. Append loaded_at        ->  current UTC timestamp

Design principles:
- transform() is a pure function (no I/O)
- from_csv_read_transform_write() is the I/O boundary: read raw, call transform(), write Delta.
"""

# COMMAND ----------

# Imports
from __future__ import annotations
from pyspark.sql import functions as F
import logging
from common import from_csv_read_transform_write
from pyspark.sql.types import DoubleType

# COMMAND ----------

# Other configurations
logging.basicConfig(level=logging.INFO, force=True)

# COMMAND ----------

# Transform
def transform(raw: DataFrame) -> DataFrame:
    """Apply staging transformations to a raw orders DataFrame.

    Args:
        raw: DataFrame loaded directly from raw_orders.csv (all columns as
             StringType when inferSchema=False).

    Returns:
        Cleaned orders DataFrame ready for the Silver layer.
    """
    return (
        raw
        # 1. Rename
        .withColumnRenamed("amount_usd", "order_amount")
        # 2. Normalise status (handles mixed-case e.g. "COMPLETED")
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        # 3 & 4. Explicit casts — invalid values become NULL, caught below
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("order_amount", F.col("order_amount").cast(DoubleType()))
        # 5. Exclude NULL / zero amounts
        .filter(
            F.col("order_amount").isNotNull()
            & (F.col("order_amount") != 0)
        )
        # 6. Audit timestamp
        .withColumn("loaded_at", F.current_timestamp())
    )

# COMMAND ----------

# Entrypoint
if __name__ == "__main__":
    # While running this notebook as a job, these are passed as parameters. When running interactively, default values are provided for convenience.
    dbutils.widgets.text("source_path", "/Volumes/top_notch_solutions/raw/customers_and_orders_data/raw_orders.csv")
    dbutils.widgets.text("target_table", "top_notch_solutions.staging.stg_orders")        
    source_path  = dbutils.widgets.get("source_path")                  
    target_table = dbutils.widgets.get("target_table")                 

    stg_df = from_csv_read_transform_write(source_path, target_table)
    logging.info(f"stg_orders: {stg_df.count()} rows written to {target_table}")