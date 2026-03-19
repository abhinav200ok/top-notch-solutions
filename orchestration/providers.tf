terraform {
  required_version = ">= 1.13"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.112"
    }
  }
}

provider "databricks" {
  profile = var.databricks_profile != "" ? var.databricks_profile : null
}