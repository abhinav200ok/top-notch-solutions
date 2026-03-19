locals {
  # Fully-qualified Delta table names used as notebook parameters
  tables = {
    stg_orders          = "${var.top_notch_solutions_catalog}.${var.staging_schema}.stg_orders"
    stg_customers       = "${var.top_notch_solutions_catalog}.${var.staging_schema}.stg_customers"
    int_orders_enriched = "${var.top_notch_solutions_catalog}.${var.intermediate_schema}.int_orders_enriched"
    customer_orders     = "${var.top_notch_solutions_catalog}.${var.intermediate_schema}.customer_orders"
  }

  default_max_retries           = 1
  default_min_retry_interval_ms = 300000 # 5 minutes
}


