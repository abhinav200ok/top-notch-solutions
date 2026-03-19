# Top Notch Solutions

<p align="center">
	<img src="assets/images/top_notch_solutions.png" alt="Top Notch Solutions" width="420" />
</p>


Top Notch Solutions is an e-commerce-driven company dedicated to delivering smart retail solutions, business growth, and outstanding customer experiences.

It takes raw CSV files, applies staged transformations, builds a customer-level summary table, and runs data quality checks. Terraform is used to define and deploy the Databricks job.

## What This Project Does

- Reads raw order and customer data from CSV files.
- Cleans and standardizes the raw data into staging tables.
- Joins orders with customer information.
- Builds a customer-level summary table for reporting.
- Runs data quality checks before the pipeline is considered successful.

## Deployment

The Databricks job is defined with Terraform in `orchestration/`.

- Pull requests to `main` run Terraform plan.
- Pushes to `main` run Terraform apply.

For more detail on the orchestration setup and job flow, see `orchestration/README.md`.

## Best Practices Implemented in the Project
- Code reusability in Python scripts and GitHub Actions workflow
- Readable code using docstrings and annotations and consistent structure and naming
- For consistency used stg for staging and int as intermediate as name across the project
- This pipeline is idempotent

## Submission checklist

- [x] `stg_orders` — cleaning logic implemented
- [x] `stg_customers` — cleaning logic implemented
- [x] `int_orders_enriched` — enrichment logic implemented
- [x] `customer_orders` — aggregation logic implemented
- [x] Data quality checks written (assertions 1–7)
- [x] Orchestration described (Task 4)
- [x] (Optional) Stretch findings documented


## Additional Notes
- In each pipeline run the whole data is processed, which makes it idempotent. However for large datasets this is not efficient. If I have to design this for a large dataset, I'll use Spark Autoloader for ingestion into staging layer and then use Delta Merge in the intermediate layer for incremental processing.