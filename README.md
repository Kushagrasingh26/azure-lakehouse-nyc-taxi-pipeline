# azure-lakehouse-nyc-taxi-pipeline
# Azure Lakehouse NYC Taxi Pipeline (Bronze → Silver → Gold + dbt)

This repo demonstrates an end-to-end lakehouse pipeline using:
- **PySpark + Delta Lake** for Bronze/Silver/Gold layers
- **dbt (DuckDB)** for analytics marts on top of curated parquet outputs

## Architecture
Bronze (raw) -> Silver (cleaned/typed) -> Gold (aggregations) -> dbt marts

## Setup (Local)
### 1) Create venv & install deps
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

pip install -r requirements.txt
