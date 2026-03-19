# Databricks notebook source
"""
Task 3 — Data quality checks

Each check is a plain Python function that accepts a DataFrame
and raises DataQualityError if a violation is found.  Passing checks print a [PASS] line and failing checks raise immediately
with the violation count and a sample of offending rows.

Checks implemented:
  1. stg_orders.order_id         — unique and never null
  2. stg_orders.customer_id      — never null
  3. stg_orders.status           — always in {completed, pending, cancelled, refunded}
  4. stg_orders.order_amount     — never null
  5. customer_orders.customer_id — unique and never null
  6. customer_orders.total_revenue — never null
  7. customer_orders.total_revenue — always > 0

Design principles:
- Each check is independently callable.
- _assert_no_violations() is the single enforcement primitive: any query that
  returns rows is treated as a failure.
- DataQualityError is a distinct exception type so orchestration code can
  differentiate transient failures (worth retrying) from data failures (not).
- run_all_checks() aggregates all checks and is the single entry point for the
  pipeline; it fails fast on the first violation.
"""

# COMMAND ----------

# Imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# Custom exception

class DataQualityError(Exception):
    """Raised when a data quality assertion is violated.

    Distinguishable from generic exceptions so that orchestrators can skip
    retries — a data quality failure is deterministic, not transient.
    """

# COMMAND ----------

# Enforcement primitive

def _assert_no_violations(df: DataFrame, check_name: str, message: str) -> None:
    """Fail if *df* contains any rows (i.e. violations were found).

    Args:
        df:          DataFrame whose rows represent violations.
        check_name:  Short identifier shown in PASS / FAIL output.
        message:     Description of what the violation means.

    Raises:
        DataQualityError: If violation is found.
    """
    count = df.count()
    if count > 0:
        sample = df.limit(5).toPandas().to_string(index=False)
        raise DataQualityError(
            f"\n[FAIL] {check_name}\n"
            f"       {message}\n"
            f"       Violation count : {count}\n"
            f"       Sample rows     :\n{sample}"
        )
    print(f"[PASS] {check_name}")

# COMMAND ----------

# stg_orders checks (1 – 4)

def check_stg_orders_order_id_unique_not_null(stg_orders: DataFrame) -> None:
    """Check 1 — order_id is unique and never null."""
    # 1a. Null check
    _assert_no_violations(
        stg_orders.filter(F.col("order_id").isNull()),
        "stg_orders / order_id NOT NULL",
        "order_id contains NULL values",
    )
    # 1b. Uniqueness check
    dupes = (
        stg_orders
        .groupBy("order_id")
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
    )
    _assert_no_violations(
        dupes,
        "stg_orders / order_id UNIQUE",
        "order_id contains duplicate values",
    )


def check_stg_orders_customer_id_not_null(stg_orders: DataFrame) -> None:
    """Check 2 — customer_id is never null."""
    _assert_no_violations(
        stg_orders.filter(F.col("customer_id").isNull()),
        "stg_orders / customer_id NOT NULL",
        "customer_id is NULL",
    )


def check_stg_orders_status_in_allowlist(stg_orders: DataFrame) -> None:
    """Check 3 — status is always one of: completed, pending, cancelled, refunded."""
    _ALLOWED_STATUSES = ["completed", "pending", "cancelled", "refunded"]
    _assert_no_violations(
        stg_orders.filter(~F.col("status").isin(_ALLOWED_STATUSES)),
        "stg_orders / status IN ('completed','pending','cancelled','refunded')",
        "status contains an unexpected value, which should be one of: completed, pending, cancelled, refunded.",
    )


def check_stg_orders_order_amount_not_null(stg_orders: DataFrame) -> None:
    """Check 4 — order_amount is never null."""
    _assert_no_violations(
        stg_orders.filter(F.col("order_amount").isNull()),
        "stg_orders / order_amount NOT NULL",
        "order_amount is NULL",
    )

# COMMAND ----------

# customer_orders checks (5 – 7)

def check_customer_orders_customer_id_unique_not_null(customer_orders: DataFrame) -> None:
    """Check 5 — customer_id is unique and never null."""
    _assert_no_violations(
        customer_orders.filter(F.col("customer_id").isNull()),
        "customer_orders / customer_id NOT NULL",
        "customer_id is NULL in the mart",
    )
    dupes = (
        customer_orders
        .groupBy("customer_id")
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
    )
    _assert_no_violations(
        dupes,
        "customer_orders / customer_id UNIQUE",
        "customer_id is duplicated",
    )


def check_customer_orders_total_revenue_not_null(customer_orders: DataFrame) -> None:
    """Check 6 — total_revenue is never null."""
    _assert_no_violations(
        customer_orders.filter(F.col("total_revenue").isNull()),
        "customer_orders / total_revenue NOT NULL",
        "total_revenue is NULL",
    )


def check_customer_orders_total_revenue_positive(customer_orders: DataFrame) -> None:
    """Check 7 — no customer has total_revenue of 0 or less."""
    _assert_no_violations(
        customer_orders.filter(F.col("total_revenue") <= 0),
        "customer_orders / total_revenue > 0",
        "total_revenue is 0 or negative",
    )

# COMMAND ----------

# Run all checks

def run_all_checks(stg_orders: DataFrame, customer_orders: DataFrame) -> None:
    """Execute all quality checks. Raises DataQualityError on the first violation encountered."""
    print("=" * 60)
    print("Running data quality checks…")
    print("=" * 60)

    # stg_orders checks
    check_stg_orders_order_id_unique_not_null(stg_orders)
    check_stg_orders_customer_id_not_null(stg_orders)
    check_stg_orders_status_in_allowlist(stg_orders)
    check_stg_orders_order_amount_not_null(stg_orders)

    # customer_orders checks
    check_customer_orders_customer_id_unique_not_null(customer_orders)
    check_customer_orders_total_revenue_not_null(customer_orders)
    check_customer_orders_total_revenue_positive(customer_orders)

    print("=" * 60)
    print("All 7 quality checks passed.")
    print("=" * 60)

# COMMAND ----------

# Entrypoint

if __name__ == "__main__":
    dbutils.widgets.text("stg_orders_table",      "top_notch_solutions.staging.stg_orders")
    dbutils.widgets.text("customer_orders_table", "top_notch_solutions.intermediate.customer_orders")
    stg_orders_table      = dbutils.widgets.get("stg_orders_table")
    customer_orders_table = dbutils.widgets.get("customer_orders_table")

    run_all_checks(
        stg_orders      = spark.table(stg_orders_table),
        customer_orders = spark.table(customer_orders_table),
    )