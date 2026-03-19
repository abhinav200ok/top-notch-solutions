# Hardcoding values for simplicity, real world usage would be more dynamic
variable "databricks_profile" {
  description = "Optional Databricks CLI profile name from ~/.databrickscfg. Leave null to use the default Databricks authentication resolution."
  type        = string
  default     = "main"
  nullable    = true
}

variable "job_name" {
  description = "Display name for the Databricks job."
  type        = string
  default     = "top-notch-solutions-customer-orders-pipeline"
}

variable "serverless_environment_key" {
  description = "Shared serverless environment key referenced by all notebook tasks in the job."
  type        = string
  default     = "python_serverless_v1"
}

variable "serverless_environment_version" {
  description = "Shared Databricks serverless environment version used by all notebook tasks in the job."
  type        = string
  default     = "1"
}

variable "top_notch_solutions_catalog" {
  description = "Unity Catalog catalog name for the Top Notch Solutions data products"
  type        = string
  default     = "top_notch_solutions"
}

variable "raw_schema" {
  description = "Unity Catalog schema for raw data. Here, we land raw CSV data in Volumes as-is with no transformations."
  type        = string
  default     = "raw"
}

variable "staging_schema" {
  description = "Unity Catalog schema for staging data. Here, we land transformed data before moving to intermediate or final tables."
  type        = string
  default     = "staging"
}

variable "intermediate_schema" {
  description = "Unity Catalog schema for intermediate data. Here, we store data that is partially transformed and ready for final aggregation."
  type        = string
  default     = "intermediate"
}

variable "raw_base_path" {
  description = "Unity Catalog Volume path for raw CSV files."
  type        = string
  default     = "/Volumes/top_notch_solutions/raw/customers_and_orders_data"
}

#TODO: Use direct git references instead of hardcoding the notebook path
variable "notebook_base" {
  description = "Absolute shared Databricks Repos path that contains the project notebooks."
  type        = string
  default     = "/Workspace/top_notch_solutions/top-notch-solutions"
}

variable "owner_email" {
  description = "On-call email address; receives failure and success notifications."
  type        = string
  default     = "abhinav999k@gmail.com"
}
