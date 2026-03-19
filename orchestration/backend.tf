# This is my first time using Terraform cloud backend 😅
terraform {
  cloud {

    organization = "top_notch_solutions"
    workspaces {
      name = "customer_orders_pipeline"
    }
  }
}