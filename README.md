# Data Engineering Portfolio

Production grade data engineering projects built with real data, cloud infrastructure, and modern tooling. Each project targets a specific business domain and demonstrates a different architectural pattern.

**GitHub:** github.com/ASD-17
**LinkedIn:** linkedin.com/in/agasya-sandilya-devarasetty

---

## Projects

| Project | Domain | Pattern | Tech | Status |
|---|---|---|---|---|
| [SEC Filing Intelligence Pipeline](./sec-filing-pipeline) | Finance | Real-time Streaming | Kafka, Spark Structured Streaming, Delta Lake, FinBERT, Azure Databricks | Live |
| [Healthcare Claims Anomaly Detection](./healthcare-claims-pipeline) | Healthcare | Batch Processing | PySpark, Delta Lake, Isolation Forest, Azure Databricks | Live |

---

## SEC Filing Intelligence Pipeline

Real-time pipeline ingesting SEC EDGAR filings through Kafka into a Medallion Architecture on Azure Databricks. Enriched with FinBERT NLP sentiment scoring and rule based anomaly detection.

```
EDGAR API → Kafka → Bronze → Silver → Gold → Databricks SQL Dashboard
```

```
100 real SEC filings processed
12 anomaly alerts triggered
Flagstar Bank CEO change flagged
Repay Holdings earnings restatement flagged
FinBERT confidence 0.92 to 0.95
23 unit tests passing
```

---

## Healthcare Claims Anomaly Detection Pipeline

Batch pipeline processing 558,211 Medicare claims through a Medallion Architecture. Engineers 14 provider behavioral features and applies Isolation Forest anomaly detection with full explainability.

```
Kaggle CSV → Bronze → Silver (feature engineering) → Fraud Detection → Gold → Databricks SQL Dashboard
```

```
558,211 claims processed
5,410 providers scored
3,379 fraud alerts generated
ROC-AUC 0.8865
Explainable alerts with human readable reasons
Databricks Jobs with 3 task pipeline completing in under 3 minutes
27 unit tests passing
```

---

## Tech Stack

| Category | Technologies |
|---|---|
| Languages | Python, SQL |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Batch Processing | PySpark, Spark SQL |
| Storage | Delta Lake, Azure Data Lake Storage Gen2 |
| Cloud | Azure Databricks, ADLS Gen2, Unity Catalog |
| ML and NLP | scikit-learn, Isolation Forest, FinBERT, HuggingFace |
| Orchestration | Databricks Jobs |
| Testing | pytest |