# Runbook

This document covers how to run the Healthcare Claims Anomaly Detection Pipeline locally and in production on Azure Databricks.

## Prerequisites

Python 3.12
Java 17
PySpark 4.1.1
Delta Lake 4.2.0
scikit-learn 1.4.0
Kaggle account to download the dataset
Azure account with Databricks workspace

## Local Setup

### Step 1: Install dependencies

```bash
pip3 install -r requirements.txt
```

### Step 2: Download the dataset

Go to Kaggle and search for Medicare Provider Fraud Detection Analysis. Download the dataset and place all four Train CSV files in data/raw/:

```
data/raw/Train_Inpatientdata-1542865627584.csv
data/raw/Train_Outpatientdata-1542865627584.csv
data/raw/Train_Beneficiarydata-1542865627584.csv
data/raw/Train-1542865627584.csv
```

### Step 3: Run the pipeline

Run each script in order. Each step must complete before the next one starts.

```bash
python3 src/ingestion/claims_loader.py
python3 src/bronze/bronze_writer.py
python3 src/processing/silver_transformer.py
python3 src/anomaly_detection/fraud_detector.py
```

Expected output after each step:

```
claims_loader.py      Inpatient: 40474, Outpatient: 517737,
                      Beneficiary: 138556, Labels: 5410
bronze_writer.py      4 Delta tables written to data/delta/bronze/
silver_transformer.py claims_enriched: 558211 records,
                      provider_features: 5410 providers
fraud_detector.py     Critical: 16, High: 775, Medium: 2588
                      ROC-AUC: 0.8865
```

### Step 4: Run unit tests

```bash
pytest tests/test_pipeline.py -v
```

Expected output: 27 passed in under 15 seconds.

## Cloud Deployment on Azure Databricks

### Azure Infrastructure

```
Resource Group       healthcare-pipeline-rg (East US)
Storage Account      healthpipelinestorage (ADLS Gen2)
Container            healthcare-data
Access Connector     healthcare_pipeline_access_connector
Databricks Workspace data-engineering-databricks (shared, databricks-rg)
Storage Credential   healthcare_pipeline_credential
External Location    healthcare_pipeline_external
Catalog              healthcare_catalog
Schemas              bronze, silver, gold
```

### Unity Catalog Tables

```
healthcare_catalog.bronze.inpatient_claims
healthcare_catalog.bronze.outpatient_claims
healthcare_catalog.bronze.beneficiary_data
healthcare_catalog.bronze.provider_labels
healthcare_catalog.silver.claims_enriched
healthcare_catalog.silver.provider_features
healthcare_catalog.gold.provider_risk_scores
healthcare_catalog.gold.fraud_alerts
healthcare_catalog.gold.provider_peer_benchmark
healthcare_catalog.gold.investigator_work_queue
```

### Databricks Notebooks

All notebooks live in the Databricks workspace under:

```
/Users/agasya.devarasetty@coyotes.usd.edu/healthcare-claims-pipeline/
  bronze/bronze_writer
  processing/silver_transformer
  anomaly_detection/fraud_detector
  dashboard/queries
```

### Databricks Job

Job name: healthcare-claims-pipeline

```
Task 1    bronze_writer       Serverless
Task 2    silver_transformer  Serverless   depends on Task 1
Task 3    fraud_detector      Serverless   depends on Task 2
```

To trigger manually:
1. Go to Workflows in Databricks
2. Click healthcare-claims-pipeline
3. Click Run Now

Full pipeline completes in under 3 minutes on Serverless compute.

### Upload new data

When new CSV files are available, upload them to ADLS and trigger the job:

```bash
az storage blob upload-batch \
  --account-name healthpipelinestorage \
  --destination healthcare-data/raw \
  --source data/raw \
  --auth-mode key
```

Then go to Databricks Workflows and click Run Now on the healthcare-claims-pipeline job.

## Troubleshooting

**claims_loader.py fails with column not found**
Check that you downloaded the Train files not the Test files from Kaggle. The column names differ between the two versions.

**silver_transformer.py fails with CAST_INVALID_INPUT**
The dataset contains NA strings in numeric columns. The transformer uses when/otherwise to handle these. Make sure you are running the latest version of silver_transformer.py.

**silver_transformer.py fails with ClosedByInterruptException**
Spark ran out of memory during the write. The transformer repartitions to 8 partitions before writing. If this still fails, increase driver memory or run on a machine with more RAM.

**fraud_detector.py produces zero critical alerts**
The critical threshold requires both risk score above 0.7 and anomaly count of 2 or more. Check the distribution of risk scores and adjust the thresholds in pipeline_config.yml.

**Databricks job task fails**
Check the logs for the failed task before debugging the next one. Bronze must succeed before Silver runs. Silver must succeed before Gold runs. Delta Lake write atomicity means a failed write leaves the previous table unchanged.