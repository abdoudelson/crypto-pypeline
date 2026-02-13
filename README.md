# Crypto DLT Pipeline

This repository contains the Delta Live Tables (DLT) code and data producers for the Crypto Market analysis pipeline. It is designed to be integrated with the [Azure Databricks Lakehouse](https://github.com/abdoudelson/azure-databricks-lakehouse) infrastructure project.

## 🏗️ Architecture

The pipeline follows the **Medallion Architecture** using three separate DLT pipelines for maximum scalability and independent monitoring.

### Components

- **[bronze.py](./scripts/bronze.py)**: Ingests raw JSON ticker data from the Azure Data Lake `raw` container using **Auto Loader**.
- **[silver.py](./scripts/silver.py)**: Performs data typing (casting prices/volumes), timestamp conversion, and deduplication.
- **[gold.py](./scripts/gold.py)**: Aggregates metrics (average price, total volume) over a 5-minute sliding window.
- **[producer.py](./scripts/producer.py)**: A standalone Python script that fetches real-time prices from Binance (via CCXT) and uploads files to Azure Storage.

## ⚙️ Dynamic Configuration

This project uses **parameterized DLT code**. Instead of hardcoding paths or catalog names, it retrieves them at runtime from the Spark configuration:

- `pipelines.storage_account`: The name of your Azure storage account.
- `pipelines.catalog`: The Unity Catalog name.
- `pipelines.target_schema_[bronze|silver]`: Dynamic schema names for cross-table references.

These values are automatically injected by the **Terraform** infrastructure code.

## 🚀 Quick Start

### 1. Prerequisite: Git Credentials

In Databricks, go to **User Settings** -> **Linked Accounts** and add your GitHub Personal Access Token (PAT) with `repo` scope.

### 2. Infrastructure Deployment

In the `azure-databricks-lakehouse` repo:

1. Update `terraform.tfvars`:
   ```hcl
   pipeline_repo_url = "https://github.com/abdoudelson/crypto-pypeline.git"
   ```
2. Run `terraform apply`.

### 3. Data Simulation

Run the producer locally or as a container:

```bash
pip install -r requirements.txt
export AZURE_STORAGE_CONNECTION_STRING="your-connection-string"
python scripts/producer.py
```

### 4. Run Pipelines

In the Databricks UI under **Delta Live Tables**, you will see three new pipelines. Start them in order: **Bronze** -> **Silver** -> **Gold**.
