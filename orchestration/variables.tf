# Hardcoding values for simplicity, real world usage would be more dynamic
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

variable "job_git_repo_url" {
  description = "Public Git repository URL that Databricks Jobs pulls code from."
  type        = string
  default     = "https://github.com/abhinav200ok/top-notch-solutions.git"
}

variable "job_git_provider" {
  description = "Git provider name understood by Databricks Jobs."
  type        = string
  default     = "gitHub"
}

variable "job_git_branch" {
  description = "Git branch that Databricks Jobs should run notebooks from."
  type        = string
  default     = "main"
}

variable "owner_email" {
  description = "On-call email address; receives failure and success notifications."
  type        = string
  default     = "abhinav999k@gmail.com"
}
