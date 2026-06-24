# Healthcare Claims Anomaly Detection Pipeline

A production grade batch data engineering pipeline that ingests Medicare claims data, processes it through a Medallion Architecture on Delta Lake, and applies rule based and machine learning anomaly detection to identify fraudulent providers. Every alert includes human readable explainability so investigators know exactly why a provider was flagged and how far they deviate from their peers.

---

## The Problem

Medicare loses roughly 90 billion dollars per year to fraud. Fraudulent providers look identical to legitimate ones at the claim level. The difference only becomes visible when you analyze patterns across thousands of claims over time. This pipeline automates that analysis and surfaces the highest risk providers to investigators with specific reasons for each flag.

---

## Architecture

```
Kaggle Dataset
Medicare Provider Fraud Detection
Inpatient, Outpatient, Beneficiary, Provider Label CSVs
         |
         v
BRONZE LAYER
inpatient_claims, outpatient_claims
beneficiary_data, provider_labels
Delta Lake on ADLS Gen2
         |
         v
SILVER LAYER
claims_enriched (558,211 records)
provider_features (5,410 provider behavioral profiles)
Delta Lake on ADLS Gen2
         |
         v
FRAUD DETECTION
Rule Based: duplicate claims, deceased billing, upcoding, impossible volume, weekend billing
ML: Isolation Forest anomaly scoring with ROC-AUC 0.8878
Validation: Precision, Recall, F1, ROC-AUC against ground truth labels
Explainability: human readable reasons for every alert
         |
         v
GOLD LAYER
provider_risk_scores (5,410 scored providers)
fraud_alerts (3,378 ranked alerts)
provider_peer_benchmark (peer group comparisons)
investigator_work_queue (prioritized case queue)
Delta Lake on ADLS Gen2
         |
         v
Databricks SQL Dashboard
Executive summary, provider risk ranking
suspicious billing patterns, peer benchmark
investigator work queue, detection accuracy
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source | Kaggle Medicare Provider Fraud Detection Dataset |
| Processing | PySpark 4.1.1, Spark SQL |
| Storage | Delta Lake 4.2.0, Azure Data Lake Storage Gen2 |
| Anomaly Detection | Isolation Forest (scikit-learn), Rule Based Detection |
| Validation | Precision, Recall, F1, ROC-AUC |
| Cloud | Azure Databricks, ADLS Gen2 |
| Governance | Unity Catalog, External Location |
| Orchestration | Databricks Jobs, Lakeflow Connect |
| Testing | pytest, 27 unit tests passing |

---

## Results

Validated on the full Medicare Provider Fraud Detection dataset:

```
Total providers scored        5,410
Fraud providers in dataset    506 (9.3 percent)
Alerts generated              3,378
Critical alerts               24
High alerts                   791
Medium alerts                 2,563

Isolation Forest ROC-AUC      0.8878
Model correctly separates fraud from non-fraud 89 percent of the time
```

---

## Silver Features

The provider_features table computes behavioral metrics for each provider:

total_claims, total_inpatient_claims, total_outpatient_claims, avg_claim_amount, reimbursement_per_patient, unique_patients, unique_physicians, avg_claim_duration_days, duplicate_claim_ratio, weekend_claim_ratio, high_cost_procedure_ratio, deceased_patient_claims, distinct_diagnosis_codes, distinct_procedure_codes

---

## Explainability

Instead of just a risk score, every alert includes human readable reasons:

```
Provider: PRV51459
Risk Score: 0.8995   Severity: High
Priority Rank: 8

Reasons:
Deceased patient billing detected (52 claims after date of death)
```

```
Provider: PRV55215
Risk Score: 0.9645   Severity: High
Priority Rank: 4

Reasons:
Deceased patient billing detected (31 claims after date of death)
```

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

pytest tests/test_pipeline.py -v
```

---

## Author

Agasya Sandilya Devarasetty
Data Engineer | Atlanta, GA | 
github.com/ASD-17