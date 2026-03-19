
resource "databricks_job" "customer_orders_pipeline" {
  name               = var.job_name
  performance_target = "PERFORMANCE_OPTIMIZED"

  environment {
    environment_key = var.serverless_environment_key

    spec {
      environment_version = var.serverless_environment_version
    }
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?" # 06:00 UTC daily
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  email_notifications {
    on_success                = [var.owner_email]
    on_failure                = [var.owner_email]
    no_alert_for_skipped_runs = true
  }

  # Prevent concurrent runs — a previous day's run finishing late should not
  # overlap with the next scheduled run.
  max_concurrent_runs = 1

  tags = {
    owner      = var.owner_email
    team       = "data-platform"
    managed_by = "terraform"
  }

  # Ingest raw_orders.csv -> Silver stg_orders Delta table.
  # No upstream dependencies — runs immediately at job start.

  task {
    task_key        = "stg_orders"
    description     = "Ingest raw orders CSV -> Silver Delta table"
    environment_key = var.serverless_environment_key

    notebook_task {
      #TODO: Use direct git references instead of hardcoding the notebook path
      notebook_path = "${var.notebook_base}/transforms/stg_orders"
      base_parameters = {
        source_path  = "${var.raw_base_path}/raw_orders.csv"
        target_table = local.tables.stg_orders
      }
    }

    max_retries               = local.default_max_retries
    min_retry_interval_millis = local.default_min_retry_interval_ms
    retry_on_timeout          = true
  }

  # Ingest raw_customers.csv -> Silver stg_customers Delta table.
  # No upstream dependencies — runs in parallel with stg_orders.

  task {
    task_key        = "stg_customers"
    description     = "Ingest raw customers CSV -> Silver Delta table"
    environment_key = var.serverless_environment_key

    notebook_task {
      notebook_path = "${var.notebook_base}/transforms/stg_customers"
      base_parameters = {
        source_path  = "${var.raw_base_path}/raw_customers.csv"
        target_table = local.tables.stg_customers
      }
    }

    max_retries               = local.default_max_retries
    min_retry_interval_millis = local.default_min_retry_interval_ms
    retry_on_timeout          = true
  }

  # Join stg_orders + stg_customers -> Gold-intermediate enriched orders.
  # Waits for BOTH staging tasks to succeed.

  task {
    task_key        = "int_orders_enriched"
    description     = "Join orders + customers -> Gold-intermediate Delta table"
    environment_key = var.serverless_environment_key

    depends_on {
      task_key = "stg_orders"
    }
    depends_on {
      task_key = "stg_customers"
    }

    notebook_task {
      notebook_path = "${var.notebook_base}/transforms/int_orders_enriched"
      base_parameters = {
        stg_orders_table    = local.tables.stg_orders
        stg_customers_table = local.tables.stg_customers
        target_table        = local.tables.int_orders_enriched
      }
    }

    max_retries               = local.default_max_retries
    min_retry_interval_millis = local.default_min_retry_interval_ms
    retry_on_timeout          = true
  }

  # Aggregate int_orders_enriched -> Gold-mart customer_orders (one row per customer).
  # Requires int_orders_enriched to be complete.

  task {
    task_key        = "customer_orders"
    description     = "Aggregate to customer-level mart -> Gold Delta table"
    environment_key = var.serverless_environment_key

    depends_on {
      task_key = "int_orders_enriched"
    }

    notebook_task {
      notebook_path = "${var.notebook_base}/transforms/customer_orders"
      base_parameters = {
        int_orders_table = local.tables.int_orders_enriched
        target_table     = local.tables.customer_orders
      }
    }

    max_retries               = local.default_max_retries
    min_retry_interval_millis = local.default_min_retry_interval_ms
    retry_on_timeout          = true
  }

  # Assert all 7 DQ rules against stg_orders and customer_orders.
  # Depends on stg_orders (checks 1-4) and customer_orders (checks 5-7).

  task {
    task_key        = "quality_checks"
    description     = "Assert all 7 DQ rules — job fails visibly on any violation"
    environment_key = var.serverless_environment_key

    depends_on {
      task_key = "stg_orders"
    }
    depends_on {
      task_key = "customer_orders"
    }

    notebook_task {
      notebook_path = "${var.notebook_base}/quality/checks"
      base_parameters = {
        stg_orders_table      = local.tables.stg_orders
        customer_orders_table = local.tables.customer_orders
      }
    }

    max_retries      = 0 # non-retriable: data failures are deterministic
    retry_on_timeout = false
  }
}
