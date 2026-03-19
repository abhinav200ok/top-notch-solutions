# COMMAND ----------
# Demonstrating code reusability, in a larger use case, we can create python packages for defining reusable code.

# Imports
from collections.abc import Callable
from pyspark.sql import DataFrame, SparkSession

# I/O
def from_csv_read_transform_write(
    spark_session: SparkSession,
    transform_fn: Callable[[DataFrame], DataFrame],
    source_path: str,
    target_table: str,
) -> DataFrame:
    """Read raw CSV, transform, persist to Delta, return the cleaned DataFrame.

    Args:
        spark_session: Active Spark session used to read the raw CSV.
        transform_fn: Transformation function to apply to the raw DataFrame.
        source_path:  DBFS / ADLS path to raw_customers.csv.
        target_table: Fully-qualified Delta table name (catalog.schema.table).

    Returns:
        The cleaned DataFrame (already written to Delta).
    """
    raw = (
        spark_session.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(source_path)
    )
    stg = transform_fn(raw)
    (
        stg.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(target_table)
    )
    return stg