# Orchestration

This directory contains the Terraform configuration for the Databricks job infrastructure used by this repository. HCP Terraform stores state remotely and provides locking.

## Job DAG

The Databricks job defined in `customer_orders_pipeline.tf` runs the following task graph:

```mermaid
flowchart TD
    stg_orders[stg_orders<br>Ingest raw_orders.csv] --> int_orders_enriched[int_orders_enriched<br>Join orders and customers]
    stg_customers[stg_customers<br>Ingest raw_customers.csv] --> int_orders_enriched
    int_orders_enriched --> customer_orders[customer_orders<br>Build customer-level mart]
    stg_orders --> quality_checks[quality_checks<br>Run all DQ assertions]
    customer_orders --> quality_checks
```

## Execution Summary

- `stg_orders` and `stg_customers` start in parallel.
- `int_orders_enriched` waits for both staging tasks.
- `customer_orders` waits for `int_orders_enriched`.
- `quality_checks` waits for both `stg_orders` and `customer_orders`.
