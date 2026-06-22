# Healthcare Claims Anomaly Detection Pipeline

A production grade batch data engineering pipeline that ingests Medicare claims data, processes it through a Medallion Architecture on Delta Lake, and applies statistical anomaly detection to identify fraudulent providers and suspicious billing patterns. Built to surface high risk cases to investigators automatically so they know where to look instead of reviewing millions of claims manually.

**Status: In Progress**

---

## Architecture

```
Kaggle Dataset
Medicare Provider Fraud Detection
Inpatient, Outpatient, Beneficiary, Provider CSVs
         |
         v
BRONZE LAYER
healthcare_catalog.bronze.inpatient_claims
healthcare_catalog.bronze.outpatient_claims
healthcare_catalog.bronze.beneficiary_data
healthcare_catalog.bronze.provider_labels
Delta Lake on ADLS Gen2
         |
         v
SILVER LAYER
healthcare_catalog.silver.claims_enriched
healthcare_catalog.silver.provider_profiles
Delta Lake on ADLS Gen2
         |
         v
GOLD LAYER
healthcare_catalog.gold.fraud_alerts
healthcare_catalog.gold.provider_risk_scores
Delta Lake on ADLS Gen2
         |
         v
Databricks SQL Dashboard
Provider risk ranking, alert panel, billing anomalies
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source | Kaggle Medicare Provider Fraud Detection Dataset |
| Processing | PySpark, Spark SQL |
| Storage | Delta Lake, Azure Data Lake Storage Gen2 |
| Anomaly Detection | Isolation Forest, Rule Based Detection |
| Cloud | Azure Databricks, ADLS Gen2 |
| Governance | Unity Catalog, External Location |
| Orchestration | Databricks Jobs, Lakeflow Connect |

---

## Pipeline Layers

Bronze stores raw CSV data exactly as received from Kaggle. No transformations applied. Four separate tables for inpatient claims, outpatient claims, beneficiary demographics, and provider fraud labels.

Silver joins and enriches the four Bronze tables into two analytical views. Claims are joined with beneficiary demographics. Providers are aggregated into behavioral profiles with derived features like total claims, average reimbursement, and unique beneficiary count.

Gold applies two detection approaches. Rule based detection flags providers with impossible claim volumes, deceased beneficiary billing, and duplicate claims. Isolation Forest scores each provider against their peer group and surfaces statistical outliers. Both outputs are combined into a ranked alert table for investigators.

---

## Repository Structure

```
healthcare-claims-pipeline/
  config/
    pipeline_config.yml
  data/
    raw/
  docs/
    business_problem.md
    solution_architecture.md
    data_model.md
    pipeline_flow.md
    runbook.md
  src/
    ingestion/
      claims_loader.py
    bronze/
      bronze_writer.py
    processing/
      silver_transformer.py
    anomaly_detection/
      fraud_detector.py
    dashboard/
      queries.sql
  tests/
    test_pipeline.py
  requirements.txt
  .gitignore
```

---

## Local Setup

```bash
pip3 install -r requirements.txt

python3 src/ingestion/claims_loader.py
python3 src/bronze/bronze_writer.py
python3 src/processing/silver_transformer.py
python3 src/anomaly_detection/fraud_detector.py
```

---

## Author

Agasya Sandilya Devarasetty
Data Engineer | Atlanta, GA
github.com/ASD-17