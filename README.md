# Data Engineering Portfolio

Production grade data engineering projects built with real data, cloud infrastructure, and modern tooling. Each project targets a specific business domain and demonstrates a different architectural pattern.

**GitHub:** github.com/ASD-17
**LinkedIn:** linkedin.com/in/agasya-sandilya-devarasetty

---

## Projects

### Enterprise Data Pipelines

| Project | Domain | Architecture | Tech |
|---|---|---|---|
| [SEC Filing Intelligence Pipeline](./sec-filing-pipeline) | Finance | Real-time Streaming | Python, Apache Kafka, Spark Structured Streaming, Delta Lake, FinBERT NLP, Azure Databricks, Unity Catalog |
| [Healthcare Claims Anomaly Detection](./healthcare-claims-pipeline) | Healthcare | Batch Processing | Python, PySpark, Delta Lake, Isolation Forest, scikit-learn, Azure Databricks, Unity Catalog |

---

## SEC Filing Intelligence Pipeline

Real-time pipeline that ingests SEC EDGAR filings through Kafka, processes them through a Medallion Architecture, and enriches them with FinBERT NLP sentiment analysis and anomaly detection.

```
EDGAR API -> Kafka -> Bronze -> Silver -> Gold -> Databricks SQL Dashboard
```

Key results: 100 real SEC filings processed, 12 anomaly alerts triggered on real companies including Flagstar Bank CEO change and Repay Holdings earnings restatement, 23 unit tests passing.

---

## Healthcare Claims Anomaly Detection Pipeline

Batch pipeline that ingests Medicare claims data, engineers provider behavioral features, and applies rule based detection and Isolation Forest to surface fraudulent providers with explainable alerts.

```
Kaggle CSV -> Bronze -> Silver (feature engineering) -> Fraud Detection -> Gold -> Databricks SQL Dashboard
```

Key results: 558,211 claims processed, 5,410 providers scored, 3,378 fraud alerts generated, Isolation Forest ROC-AUC 0.8878, 27 unit tests passing.

---

## Tech Stack

| Category | Technologies |
|---|---|
| Languages | Python, SQL |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Batch Processing | PySpark, Spark SQL |
| Storage | Delta Lake, Azure Data Lake Storage Gen2, AWS S3 |
| Cloud | Azure Databricks, ADLS Gen2, Unity Catalog |
| ML and NLP | scikit-learn, Isolation Forest, FinBERT, HuggingFace |
| Orchestration | Databricks Jobs, Lakeflow Connect |
| Testing | pytest |