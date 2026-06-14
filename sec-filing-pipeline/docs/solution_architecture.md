# Solution Architecture

## Overview

The SEC Filing Intelligence Pipeline is a real-time data engineering system that monitors SEC EDGAR for new public company filings, processes them through a Medallion Architecture, enriches them with NLP-based sentiment analysis, and surfaces anomaly alerts to compliance analysts via a Databricks SQL dashboard.

The pipeline runs in two modes. Local development uses Docker-based Kafka and local Delta Lake files on disk. Production runs on Azure with Databricks managed compute, ADLS Gen2 storage, and Unity Catalog governance.

---

## Architecture Diagram

```
SEC EDGAR API
(efts.sec.gov + data.sec.gov)
         |
         | HTTP polling every 30 seconds
         v
edgar_producer.py
Python process running locally or on VM
Fetches filing metadata and document URLs
Publishes JSON events to Kafka
         |
         | Kafka topic: sec-filings-raw
         | 3 partitions, replication factor 1
         v
Apache Kafka
Docker container (local) or Azure Event Hubs (production)
Durable message retention, offset-based replay
         |
         | Spark Structured Streaming reads from Kafka
         | foreachBatch micro-batches every 30 seconds
         v
bronze_writer.py
Fetches raw document text from SEC Archives
Writes raw records to Delta Lake Bronze
         |
         v
BRONZE LAYER
sec_pipeline_catalog.bronze.sec_filings_raw
Delta Lake on ADLS Gen2
abfss://sec-data@secpipelinestorage.dfs.core.windows.net/bronze.sec_filings_raw
         |
         | PySpark batch job
         | Schema validation and quarantine routing
         v
silver_transformer.py
Parses filing metadata
Validates required fields
Quarantines invalid records
         |
         v
SILVER LAYER
sec_pipeline_catalog.silver.sec_filings_parsed
sec_pipeline_catalog.silver.sec_filings_quarantine
Delta Lake on ADLS Gen2
         |
         | FinBERT NLP via HuggingFace Inference API
         | Rule-based anomaly detection
         v
gold_enricher.py
Scores each filing with FinBERT sentiment model
Detects anomaly flags from filing text
Assigns alert severity
Computes company sentiment summary
         |
         v
GOLD LAYER
sec_pipeline_catalog.gold.sec_filings_enriched
sec_pipeline_catalog.gold.company_sentiment_summary
Delta Lake on ADLS Gen2
         |
         | Databricks SQL Warehouse (Serverless)
         v
DASHBOARD
6 analytical queries
Live filing feed, anomaly alerts, sentiment trends
Filing volume, top risk companies, sentiment distribution
```

---

## Azure Infrastructure

```
Subscription         Azure for Students
Resource Group       sec-pipeline-rg (East US)

Storage
  Account            secpipelinestorage
  Type               ADLS Gen2 (hierarchical namespace enabled)
  Redundancy         LRS
  Container          sec-data

Databricks
  Workspace          sec-pipeline-databricks (East US)
  Tier               Premium Trial (14-day free DBUs)
  Compute            Serverless SQL Warehouse

Identity and Access
  Access Connector   sec_pipeline_access_connector
  Credential         sec_pipeline_credential (Managed Identity)
  External Location  sec_pipeline_external
  URL                abfss://sec-data@secpipelinestorage.dfs.core.windows.net/

Unity Catalog
  Catalog            sec_pipeline_catalog
  Storage location   abfss://sec-data@secpipelinestorage.dfs.core.windows.net/
  Schemas            bronze, silver, gold
  Tables             bronze.sec_filings_raw
                     silver.sec_filings_parsed
                     silver.sec_filings_quarantine
                     gold.sec_filings_enriched
                     gold.company_sentiment_summary
```

---

## Technology Decisions

**Kafka over direct API polling** — Kafka decouples the producer from the consumers. If the bronze writer is down, events are retained in Kafka and replayed on restart. This guarantees no filing is lost even if downstream processing fails. The checkpoint mechanism ensures exactly-once delivery semantics.

**Delta Lake over raw Parquet** — Delta Lake adds ACID transactions, time travel, and schema enforcement on top of Parquet. If a bad batch corrupts a table, we can roll back to a previous version. Schema evolution is handled automatically. Compaction and Z-ordering are available for query optimization.

**Medallion Architecture** — Separating Bronze, Silver, and Gold gives each layer a clear contract. Bronze is a faithful copy of the source. Silver is validated and structured. Gold is business-ready. Each layer can be reprocessed independently if logic changes. Quarantine in Silver means bad data never reaches Gold.

**FinBERT over general-purpose sentiment models** — FinBERT is trained on financial text from Reuters and analyst reports. It understands phrases like going concern, material weakness, and restatement in their financial context. General models like VADER score these as neutral; FinBERT scores them correctly as negative signals.

**Rule-based anomaly detection** — Pattern matching on known risk phrases is auditable. A compliance analyst can see exactly which words triggered the flag. Machine learning anomaly detection would require labeled training data and a model retraining pipeline. Rule-based is explainable, maintainable, and sufficient for the current scope.

**Unity Catalog with custom managed location** — Instead of using Databricks-managed default storage, we configured a custom ADLS Gen2 path as the catalog storage location. This means the underlying Delta Lake files are visible and accessible outside of Databricks. The team retains full ownership of the data independent of the Databricks workspace.

**Serverless SQL Warehouse** — No cluster management. Starts automatically when a query runs. Stops when idle. Charged only for query execution time. For a dashboard with intermittent usage this is significantly cheaper than an always-on cluster.

---

## Data Flow Latency

```
Filing submitted to EDGAR
         |
         | 0 to 30 seconds (Kafka poll interval)
         v
Event published to Kafka
         |
         | 0 to 30 seconds (Spark micro-batch interval)
         v
Written to Bronze
         |
         | Manual trigger in current implementation
         | Airflow schedule planned for production
         v
Written to Silver
         |
         v
Written to Gold with sentiment scores
         |
         v
Available in dashboard

Total end-to-end latency target: under 5 minutes filing to dashboard
```

---

## Security

The pipeline uses Azure Managed Identity for all storage access. No credentials or connection strings are stored in code. The Access Connector for Azure Databricks authenticates to ADLS Gen2 using its system-assigned managed identity, which is granted the Storage Blob Data Contributor role on the storage account. The HuggingFace API token is passed as an environment variable and never committed to the repository.