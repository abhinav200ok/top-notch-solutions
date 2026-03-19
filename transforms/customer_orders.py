# Databricks notebook source
"""
Task 2b — Customer summary mart (aggregation layer)

Source  : Gold intermediate  ->  top_notch_solutions.intermediate.int_orders_enriched
Output  : Gold mart          ->  top_notch_solutions.intermediate.customer_orders (Delta)

Output schema (one row per customer):
customer_id       — key
full_name         — from enriched orders
tier              — customer tier
total_orders      — count of all orders (completed + pending)
completed_orders  — count where is_completed = TRUE
total_revenue     — sum of order_amount for completed orders only
avg_order_value   — avg order_amount for completed orders, rounded to 2 d.p.
first_order_date  — earliest order_date across all orders
last_order_date   — latest order_date across all orders

Filter: only customers with at least one completed order.

Key aggregation decisions:
- total_orders counts every row in int_orders_enriched (completed + pending)
- total_revenue and avg_order_value use conditional aggregation to restrict to completed orders only.
- Only F.round() is applied to avg_order_value; revenue is left unrounded to
  preserve precision for downstream consumers.
"""


# COMMAND ----------

# Imports
from __future__ import annotations
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# Other configurations
logging.basicConfig(level=logging.INFO, force=True)

# COMMAND ----------

# Transform

def transform(int_orders: DataFrame) -> DataFrame:
    """Aggregate enriched orders to one row per customer.

    Args:
        int_orders: Enriched orders DataFrame

    Returns:
        Customer summary mart DataFrame, one row per customer with ≥1
        completed order.
    """
    completed_amount = F.when(F.col("is_completed"), F.col("order_amount"))

    return (
        int_orders
        .groupBy("customer_id", "full_name", "tier")
        .agg(
            F.count("order_id")
             .alias("total_orders"),

            F.sum(F.when(F.col("is_completed"), F.lit(1)).otherwise(F.lit(0)))
             .alias("completed_orders"),

            F.sum(completed_amount)
             .alias("total_revenue"),

            F.round(F.avg(completed_amount), 2)
             .alias("avg_order_value"),

            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        # Only customers with at least one completed order
        .filter(F.col("completed_orders") > 0)
    )

# COMMAND ----------

# I/O

def from_dataframe_read_transform_write(
    int_orders: DataFrame,
    target_table: str,
) -> DataFrame:
    """Aggregate, persist to Delta, return the mart DataFrame.

    Args:
        int_orders:   Enriched orders DataFrame.
        target_table: Fully-qualified Delta table name.

    Returns:
        Customer summary mart DataFrame (already written to Delta).
    """
    mart = transform(int_orders)
    (
        mart.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(target_table)
    )
    return mart


# COMMAND ----------

# Entrypoint

if __name__ == "__main__":
    dbutils.widgets.text("int_orders_table", "top_notch_solutions.intermediate.int_orders_enriched")
    dbutils.widgets.text("target_table",     "top_notch_solutions.intermediate.customer_orders")    
    int_orders_table = dbutils.widgets.get("int_orders_table")              
    target_table     = dbutils.widgets.get("target_table")                  
    int_orders_df = spark.table(int_orders_table)
    mart_df = from_dataframe_read_transform_write(int_orders_df, target_table)
    logging.info(f"customer_orders: {mart_df.count()} rows written to {target_table}")