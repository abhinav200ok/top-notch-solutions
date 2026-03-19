# Databricks notebook source
"""
Task 2a — Enrichment layer: join orders to customers

Source  : Silver  ->  stg_orders  +  stg_customers
Output  : Gold    ->  top_notch_solutions.intermediate.int_orders_enriched (Delta)

Transformations applied:
1. Filter stg_orders to 'completed' and 'pending' statuses only
2. Inner-join with stg_customers on customer_id to bring in full_name, tier
3. Derive boolean is_completed = (status == 'completed')
4. Project explicit output columns

Join type rationale:
An INNER join is used for our use-case: any order whose customer_id has no match
in stg_customers is excluded rather than propagated as a row with NULL customer
attributes, which would produce misleading downstream aggregations.

Design principles:
- transform() is a pure function
- _STATUS_ALLOWLIST and _OUTPUT_COLUMNS are easy to update and self-documenting.
"""

# COMMAND ----------

# Imports
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# Other configurations
logging.basicConfig(level=logging.INFO, force=True)

# COMMAND ----------

# Constants

_STATUS_ALLOWLIST = ["completed", "pending"]

_OUTPUT_COLUMNS = [
    "order_id",
    "customer_id",
    "full_name",
    "tier",
    "order_date",
    "status",
    "order_amount",
    "country_code",
    "is_completed",
]

# COMMAND ----------

# Transform

def transform(stg_orders: DataFrame, stg_customers: DataFrame) -> DataFrame:
    """Enrich cleaned orders with customer attributes.

    Args:
        stg_orders:    Cleaned orders DataFrame (output of stg_orders.transform).
        stg_customers: Cleaned customers DataFrame (output of stg_customers.transform).

    Returns:
        Enriched orders DataFrame containing only completed / pending statuses,
        joined to customer full_name and tier, with is_completed flag derived.
    """
    customer_attrs = stg_customers.select("customer_id", "full_name", "tier")

    return (
        stg_orders
        .filter(F.col("status").isin(_STATUS_ALLOWLIST))
        .join(customer_attrs, on="customer_id", how="inner")
        .withColumn("is_completed", F.col("status") == F.lit("completed"))
        .select(*_OUTPUT_COLUMNS)
    )

# COMMAND ----------

# I/O

def from_dataframes_read_transform_write(
    stg_orders: DataFrame,
    stg_customers: DataFrame,
    target_table: str,
) -> DataFrame:
    """Apply enrichment, persist to Delta, return enriched DataFrame.

    Args:
        stg_orders:    Cleaned orders DataFrame.
        stg_customers: Cleaned customers DataFrame.
        target_table:  Fully-qualified Delta table name.

    Returns:
        Enriched DataFrame (already written to Delta).
    """
    enriched = transform(stg_orders, stg_customers)
    (
        enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(target_table)
    )
    return enriched

# COMMAND ----------

# Entrypoint

if __name__ == "__main__":
    dbutils.widgets.text("stg_orders_table", "top_notch_solutions.staging.stg_orders")
    dbutils.widgets.text("stg_customers_table", "top_notch_solutions.staging.stg_customers")
    dbutils.widgets.text("target_table", "top_notch_solutions.intermediate.int_orders_enriched")
    stg_orders_table    = dbutils.widgets.get("stg_orders_table")    
    stg_customers_table = dbutils.widgets.get("stg_customers_table") 
    target_table        = dbutils.widgets.get("target_table")        

    stg_orders_df    = spark.table(stg_orders_table)
    stg_customers_df = spark.table(stg_customers_table)
    enriched_df = from_dataframes_read_transform_write(stg_orders_df, stg_customers_df, target_table)
    logging.info(f"int_orders_enriched: {enriched_df.count()} rows written to {target_table}")