# Crypto DLT Pipeline

This folder contains the Python code for the Crypto Market streaming analysis.
This code is intended to be moved into its own repository (e.g. `crypto-dlt-pipeline`) and linked to the Databricks Workspace via **Databricks Repos**.

## Contents

- `producer.py`: Locally runnable script to fetch Binance metadata and upload to Azure `raw` container.
- `dlt_pipeline.py`: Delta Live Tables notebook for processing data from Bronze to Gold.

## Next Steps

1. Create a new repository on GitHub.
2. Initialize it with these files.
3. Update `pipeline_repo_url` in your main Terraform project.
4. Run `terraform apply`.
