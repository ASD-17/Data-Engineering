# Solution Architecture

## Overview

The Healthcare Claims Anomaly Detection Pipeline is a batch data engineering system that ingests Medicare claims data from Kaggle, processes it through a Medallion Architecture on Delta Lake, applies statistical and rule based anomaly detection to identify fraudulent providers, and surfaces explainable fraud alerts to investigators through a Databricks SQL dashboard.

Healthcare claims are submitted in bulk by providers at the end of each billing cycle. Processing them in scheduled batches mirrors how real world Medicare fraud detection systems operate at CMS. The pipeline is designed to be simple enough to understand and complex enough to demonstrate production grade data engineering skills.

## Architecture Diagram

```
Kaggle Dataset
Medicare Provider Fraud Detection
Inpatient, Outpatient, Beneficiary, Provider Label CSVs
         |
         | claims_loader.py reads CSVs into Spark DataFrames
         v
BRONZE LAYER
healthcare_catalog.bronze.inpatient_claims
healthcare_catalog.bronze.outpatient_claims
healthcare_catalog.bronze.beneficiary_data
healthcare_catalog.bronze.provider_labels
Delta Lake on ADLS Gen2
Raw data exactly as received
         |
         | silver_transformer.py cleans, joins, engineers features
         v
SILVER LAYER
healthcare_catalog.silver.claims_enriched
healthcare_catalog.silver.provider_features
Delta Lake on ADLS Gen2
Validated, joined, and feature engineered data
         |
         | fraud_detector.py runs rule based and ML detection
         v
FRAUD DETECTION
Rule Based: duplicate claims, deceased billing, upcoding, volume
ML: Isolation Forest anomaly scoring
Validation: Precision, Recall, F1, ROC-AUC against ground truth labels
Explainability: human readable reasons for every alert
         |
         v
GOLD LAYER
healthcare_catalog.gold.provider_risk_scores
healthcare_catalog.gold.fraud_alerts
healthcare_catalog.gold.provider_peer_benchmark
healthcare_catalog.gold.investigator_work_queue
Delta Lake on ADLS Gen2
         |
         | Databricks SQL Warehouse (Serverless)
         v
DASHBOARD
Executive summary, provider risk ranking, geographic fraud map
investigator queue with explainable alerts
```

## Technology Decisions

**Spark batch over streaming** Healthcare claims arrive in bulk. Providers submit monthly or weekly batches to CMS. Processing each file as it arrives in a scheduled batch job matches the cadence of the real world problem.

**Five Bronze tables** The Kaggle dataset has four source files representing different entities. Each is kept as its own Bronze table to preserve source fidelity and make reprocessing easier if downstream logic changes.

**Feature engineering at Silver** The Silver provider features table computes behavioral metrics like duplicate claim ratio, weekend claim ratio, and reimbursement per patient. These derived features are what make fraud detectable. Raw claim data alone is not enough.

**Rule based plus Isolation Forest** Two detection approaches run in parallel. Rule based detection catches known fraud patterns that domain experts have identified. Isolation Forest finds statistical outliers that no explicit rule covers. Together they catch both known and novel fraud patterns.

**Explainability over black box scoring** Every provider alert includes human readable reasons explaining which specific behaviors triggered the flag and how far the provider deviates from peers. A risk score of 92 means nothing to an investigator. Knowing that a provider bills 4.2 times more than peers and has duplicate claims gives them a starting point.

**Validation against ground truth** Because the Kaggle dataset includes fraud labels, the pipeline computes precision, recall, F1, and ROC-AUC scores. This proves the detection logic works and gives a baseline for future improvement.

**Delta Lake OPTIMIZE and ZORDER** Gold tables are optimized and Z-ordered by provider_id. Most dashboard queries filter by provider so Z-ordering reduces the amount of data scanned per query.

**Unity Catalog with custom managed location** All tables are registered in Databricks Unity Catalog backed by a custom ADLS Gen2 storage location. This gives full governance, lineage tracking, and the ability to inspect the underlying Delta files directly.

**Databricks Jobs with Lakeflow Connect** On Azure Databricks the pipeline runs as a four task job on Serverless compute. Lakeflow Connect monitors the ADLS container for new CSV files and triggers the job automatically when new data arrives.

## Azure Infrastructure

```
Resource Group       healthcare-pipeline-rg (East US)
Storage Account      healthcarepipelinestorage (ADLS Gen2)
Container            healthcare-data
Databricks Workspace healthcare-pipeline-databricks (Premium)
Catalog              healthcare_catalog
Schemas              bronze, silver, gold
Compute              Serverless SQL Warehouse
```

## Security

The Kaggle dataset is de-identified for research purposes. Azure Managed Identity handles all storage access. No credentials are stored in code or committed to the repository.