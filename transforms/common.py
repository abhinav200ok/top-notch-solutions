# COMMAND ----------
# Demonstrating code reusability, in a larger use case, we can create python packages for defining reusable code.
# I/O
def from_csv_read_transform_write(source_path: str, target_table: str) -> DataFrame:
    """Read raw CSV, transform, persist to Delta, return the cleaned DataFrame.

    Args:
        source_path:  DBFS / ADLS path to raw_customers.csv.
        target_table: Fully-qualified Delta table name (catalog.schema.table).

    Returns:
        The cleaned DataFrame (already written to Delta).
    """
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(source_path)
    )
    stg = transform(raw)
    (
        stg.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(target_table)
    )
    return stg