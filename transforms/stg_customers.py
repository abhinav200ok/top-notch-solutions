# Databricks notebook source
"""
Task 1b — Staging / cleaning layer: customers

Source  : Bronze  ->  data/raw_customers.csv (Reading data from volume)
Output  : Silver  ->  top_notch_solutions.staging.stg_customers (Delta)

Transformations applied:
1. Combine first_name + last_name -> full_name
2. Keep all remaining source columns as-is
3. Append loaded_at (current UTC timestamp) 

Design principles:
- transform() is a pure function (no I/O)
- Code reuse: Helper function from_csv_read_transform_write()
- Column order is made explicit in select() so the output schema is
  self-documenting and stable regardless of CSV column order.
"""

# COMMAND ----------

# Imports
import logging
from pyspark.sql import DataFrame, functions as F
from common import from_csv_read_transform_write

# COMMAND ----------

# Other configurations
logging.basicConfig(level=logging.INFO, force=True)

# COMMAND ----------

# Transform
def transform(raw: DataFrame) -> DataFrame:
    """Apply staging transformations to a raw customers DataFrame.

    Args:
        raw: DataFrame loaded directly from raw_customers.csv.

    Returns:
        Cleaned customers DataFrame ready for the Silver layer.
    """
    return (
        raw
        # 1. Combine name parts into a single readable column
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        # 2 & 3. Explicit output schema — first_name / last_name retired and loaded_at added
        # Keep all columns as-is including signup_date
        .select(
            "customer_id",
            "full_name",
            "email",
            "signup_date",
            "tier",
            F.current_timestamp().alias("loaded_at"),
        )
    )

# COMMAND ----------

# Entrypoint
if __name__ == "__main__":
    # While running this notebook as a job, these are passed as parameters. When running interactively, default values are provided for convenience.
    dbutils.widgets.text("source_path", "/Volumes/top_notch_solutions/raw/customers_and_orders_data/raw_customers.csv")
    dbutils.widgets.text("target_table", "top_notch_solutions.staging.stg_customers")
    source_path  = dbutils.widgets.get("source_path")
    target_table = dbutils.widgets.get("target_table")

    stg_df = from_csv_read_transform_write(spark, transform, source_path, target_table)
    logging.info(f"stg_customers: {stg_df.count()} rows written to {target_table}")