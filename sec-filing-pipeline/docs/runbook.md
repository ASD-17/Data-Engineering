# Runbook

This document covers how to run the SEC Filing Intelligence Pipeline in both local development mode and production on Azure Databricks.

---

## Local Development

### Prerequisites

- macOS with Homebrew
- Python 3.12
- Java 17 (`brew install openjdk@17`)
- Docker Desktop running
- HuggingFace account with API token

### Install dependencies

```bash
pip3 install -r requirements.txt
```

### Start Kafka

```bash
cd infrastructure/kafka
docker-compose up -d
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sec-filings-raw --partitions 3 --replication-factor 1
```

### Run the pipeline

Open four terminals and run in this order:

Terminal 1 — Bronze writer first so it is subscribed before events are published:
```bash
cd src/streaming
python3 bronze_writer.py
```

Terminal 2 — Producer after bronze writer shows "Streaming query started":
```bash
cd src/ingestion
echo "2026-06-11" > state/last_check_timestamp.txt
python3 edgar_producer.py
```

Wait for bronze writer to show "Batch 1: processing 100 record(s)" then stop both with Ctrl+C.

Terminal 3 — Silver transformer:
```bash
cd src/processing
python3 silver_transformer.py
```

Terminal 4 — Gold enricher:
```bash
export HUGGINGFACE_TOKEN="your_token_here"
cd src/enrichment
python3 gold_enricher.py
```

### Stop Kafka

```bash
cd infrastructure/kafka
docker-compose down
```

---

## Production — Azure Databricks

### Infrastructure

```
Resource Group       sec-pipeline-rg (East US)
Storage Account      secpipelinestorage (ADLS Gen2)
Container            sec-data
Access Connector     sec_pipeline_access_connector
Databricks Workspace sec-pipeline-databricks
Storage Credential   sec_pipeline_credential
External Location    sec_pipeline_external
Catalog              sec_pipeline_catalog
```

### Upload local Delta Lake files to ADLS

Run from your Mac terminal after each local pipeline run:

```bash
az storage blob upload-batch \
  --account-name secpipelinestorage \
  --destination sec-data/bronze.sec_filings_raw \
  --source src/streaming/bronze.sec_filings_raw \
  --auth-mode key

az storage blob upload-batch \
  --account-name secpipelinestorage \
  --destination sec-data/silver.sec_filings_parsed \
  --source src/streaming/silver.sec_filings_parsed \
  --auth-mode key

az storage blob upload-batch \
  --account-name secpipelinestorage \
  --destination sec-data/gold.sec_filings_enriched \
  --source src/streaming/gold.sec_filings_enriched \
  --auth-mode key

az storage blob upload-batch \
  --account-name secpipelinestorage \
  --destination sec-data/gold.company_sentiment_summary \
  --source src/streaming/gold.company_sentiment_summary \
  --auth-mode key
```

### Register tables in Unity Catalog

Run once in a Databricks notebook after uploading new data:

```python
spark.sql("CREATE TABLE IF NOT EXISTS sec_pipeline_catalog.bronze.sec_filings_raw USING DELTA LOCATION 'abfss://sec-data@secpipelinestorage.dfs.core.windows.net/bronze.sec_filings_raw'")
spark.sql("CREATE TABLE IF NOT EXISTS sec_pipeline_catalog.silver.sec_filings_parsed USING DELTA LOCATION 'abfss://sec-data@secpipelinestorage.dfs.core.windows.net/silver.sec_filings_parsed'")
spark.sql("CREATE TABLE IF NOT EXISTS sec_pipeline_catalog.gold.sec_filings_enriched USING DELTA LOCATION 'abfss://sec-data@secpipelinestorage.dfs.core.windows.net/gold.sec_filings_enriched'")
spark.sql("CREATE TABLE IF NOT EXISTS sec_pipeline_catalog.gold.company_sentiment_summary USING DELTA LOCATION 'abfss://sec-data@secpipelinestorage.dfs.core.windows.net/gold.company_sentiment_summary'")
print("All tables registered")
```

### Run dashboard queries

Open Databricks SQL Editor and run queries from `src/dashboard/queries.sql`. All queries use the `sec_pipeline_catalog` prefix and run on the Serverless SQL Warehouse.

---

## Troubleshooting

**Bronze writer shows "Batch 0: no records"**
The producer was started before the bronze writer connected to Kafka. Stop both, restart bronze writer first, wait for "Streaming query started", then start the producer.

**Silver transformer shows 100% quarantine rate**
Check that `filing_type` and `filed_date` fields are populated in Bronze records. Run the producer with the latest edgar_producer.py which uses the submissions API for correct field mapping.

**FinBERT API returns 401**
The HuggingFace token is expired or incorrect. Generate a new token at huggingface.co and export it in the same terminal before running gold_enricher.py.

**FinBERT API DNS resolution fails**
Your ISP DNS may be blocking api-inference.huggingface.co. The correct endpoint is router.huggingface.co. Switch to Google DNS (8.8.8.8) or use a mobile hotspot.

**Kafka connection refused**
Docker Desktop is not running or Kafka container is stopped. Start Docker Desktop and run docker-compose up -d in the infrastructure/kafka directory.

**Delta Lake version mismatch**
This pipeline uses PySpark 4.1.1 and Delta Lake 4.2.0. Do not upgrade either independently as they must be compatible. The PYSPARK_SUBMIT_ARGS environment variable in each script handles the package loading.

---

## Environment Variables

```
HUGGINGFACE_TOKEN    HuggingFace API token for FinBERT inference
```

Never commit this value to the repository. Set it in your terminal session before running gold_enricher.py.