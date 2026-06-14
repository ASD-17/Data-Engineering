# SEC Filing Intelligence Pipeline

A production-grade real-time data engineering pipeline that ingests SEC EDGAR filings, processes them through a Medallion Architecture, and enriches them with FinBERT NLP sentiment analysis and anomaly detection. Built to give compliance analysts and financial researchers actionable intelligence on public company disclosures within minutes of filing.

**Status: Live on Azure Databricks**

---

## Architecture

```
SEC EDGAR API
     |
     v
edgar_producer.py
(Python + Confluent Kafka)
     |
     v
Apache Kafka
sec-filings-raw topic
3 partitions
     |
     v
bronze_writer.py
(Spark Structured Streaming + foreachBatch)
     |
     v
Bronze Layer
sec_pipeline_catalog.bronze.sec_filings_raw
Delta Lake on ADLS Gen2
     |
     v
silver_transformer.py
(PySpark batch + schema validation + quarantine)
     |
     v
Silver Layer
sec_pipeline_catalog.silver.sec_filings_parsed
Delta Lake on ADLS Gen2
     |
     v
gold_enricher.py
(FinBERT NLP + rule-based anomaly detection)
     |
     v
Gold Layer
sec_pipeline_catalog.gold.sec_filings_enriched
sec_pipeline_catalog.gold.company_sentiment_summary
Delta Lake on ADLS Gen2
     |
     v
Databricks SQL Dashboard
6 analytical queries for compliance teams
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Confluent Kafka, SEC EDGAR API |
| Streaming | Apache Spark Structured Streaming, foreachBatch |
| Storage | Delta Lake, Azure Data Lake Storage Gen2 |
| Processing | PySpark, Spark SQL |
| NLP | FinBERT (ProsusAI/finbert) via HuggingFace Inference API |
| Cloud | Azure Databricks, ADLS Gen2, Access Connector |
| Governance | Unity Catalog, External Location, Storage Credential |
| Orchestration | Manual batch (Airflow integration planned) |

---

## Pipeline Layers

**Bronze** — Raw ingestion layer. Consumes JSON events from Kafka via Spark Structured Streaming and writes them as Delta Lake Parquet files. Stores raw filing metadata and document text exactly as received. No transformations applied.

**Silver** — Validated and structured layer. Reads Bronze records, applies schema validation, parses filing metadata, computes word count and document flags, and routes invalid records to a quarantine table. Only records with valid filing type and date proceed.

**Gold** — Business-ready enrichment layer. Runs each filing through FinBERT for financial sentiment scoring (positive, negative, neutral with confidence score 0 to 1). Applies rule-based anomaly detection for going concern language, earnings restatements, CEO departures, legal proceedings, and unusual trading activity. Assigns alert severity and writes to the enriched table and company sentiment summary.

---

## Results

Validated against 100 real SEC filings from June 2026:

- 100 records processed end to end with zero pipeline errors
- 12 anomaly alerts triggered on real filings
- Flagstar Bank (FLG) flagged for sudden CEO change
- Repay Holdings (RPAY) flagged for earnings restatement language
- BeOne Medicines (ONC) flagged for earnings restatement language
- FinBERT sentiment scores ranging from 0.92 to 0.95 confidence

---

## Cloud Infrastructure

```
Resource Group       sec-pipeline-rg          East US
Storage Account      secpipelinestorage        ADLS Gen2, LRS
Container            sec-data
Access Connector     sec_pipeline_access_connector
Databricks Workspace sec-pipeline-databricks   Premium Trial
Storage Credential   sec_pipeline_credential   Managed Identity
External Location    sec_pipeline_external     abfss://sec-data@secpipelinestorage
Unity Catalog        sec_pipeline_catalog
Schemas              bronze, silver, gold
```

---

## Repository Structure

```
sec-filing-pipeline/
  docs/
    business_problem.md
    solution_architecture.md
    data_model.md
    pipeline_flow.md
    runbook.md
  config/
    pipeline_config.yml
  src/
    ingestion/
      edgar_producer.py
    streaming/
      bronze_writer.py
    processing/
      silver_transformer.py
    enrichment/
      gold_enricher.py
    dashboard/
      queries.sql
  infrastructure/
    kafka/
      docker-compose.yml
  requirements.txt
  .gitignore
```

---

## Local Setup

Requires Java 17, Python 3.12, Docker Desktop.

```bash
# Start Kafka
cd infrastructure/kafka && docker-compose up -d

# Run pipeline in order
python3 src/ingestion/edgar_producer.py
python3 src/streaming/bronze_writer.py
python3 src/processing/silver_transformer.py
export HUGGINGFACE_TOKEN="your_token"
python3 src/enrichment/gold_enricher.py
```

---

## Author

Agasya Sandilya Devarasetty
Data Engineer 