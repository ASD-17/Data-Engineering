# Pipeline Flow

## Overview

The pipeline runs in four sequential steps. Each step reads from the previous layer and writes to the next. No step runs until the previous one completes successfully. This is a batch pipeline  it processes a full dataset at once rather than record by record.

## Step 1: Data Extraction

The Kaggle Medicare Provider Fraud Detection dataset is downloaded manually and placed in the local data/raw/ directory. The dataset contains four CSV files: inpatient claims, outpatient claims, beneficiary data, and provider fraud labels.

claims_loader.py reads each CSV file into a Spark DataFrame using PySpark. It performs an initial schema check to confirm all expected columns are present before passing the data to the next step. If any file is missing or malformed the job stops and logs the error before writing anything.

## Step 2: Bronze Layer

bronze_writer.py receives the four Spark DataFrames from the extraction step and writes each one as a separate Delta Lake table in the Bronze schema.

No transformations are applied at this step. Data types remain as strings exactly as they appear in the source CSV files. The raw values are preserved so that if a downstream transformation has a bug, the original data is always available for reprocessing without going back to Kaggle.

Each Bronze table write uses Delta Lake append mode with schema enforcement enabled. A write timestamp is added to every record so each batch can be traced back to when it was loaded.

The four Bronze tables written are:

healthcare_catalog.bronze.inpatient_claims
healthcare_catalog.bronze.outpatient_claims
healthcare_catalog.bronze.beneficiary_data
healthcare_catalog.bronze.provider_labels

## Step 3: Silver Layer

silver_transformer.py reads all four Bronze tables and performs three operations: cleaning, joining, and feature engineering.

Cleaning casts string columns to their correct types. Dates become date types. Amounts become doubles. Integer counts become integers. Null values are handled with business logic  a missing deductible amount is treated as zero, a missing procedure code is treated as an empty array.

Joining connects the four tables into two unified views. The claims enriched table joins inpatient and outpatient claims with beneficiary demographics so every claim row carries the patient age, chronic condition count, and state alongside the claim financials. The provider profiles table aggregates all claims by provider to compute behavioral metrics like total claims submitted, average reimbursement per claim, and number of distinct beneficiaries served.

Feature engineering creates derived columns that the anomaly detection step needs. Claim duration in days is computed from start and end dates. Beneficiary age is computed from date of birth. Chronic condition count is summed from the ten individual condition flags. A deceased beneficiary flag is set when the date of death field is populated.

The two Silver tables written are:

healthcare_catalog.silver.claims_enriched
healthcare_catalog.silver.provider_profiles

## Step 4: Gold Layer

fraud_detector.py reads the Silver provider profiles table and runs two detection approaches in parallel.

Rule based detection applies a set of explicit business rules to each provider profile. A provider is flagged if their claim volume per beneficiary exceeds a defined threshold. A provider is flagged if their average reimbursement is more than three standard deviations above the mean for all providers. A provider is flagged if claims were submitted for deceased beneficiaries. A provider is flagged if the same claim ID appears more than once. Each rule that fires adds one flag to the provider record.

Statistical detection runs Isolation Forest on the numerical features of each provider profile. Isolation Forest is an unsupervised machine learning algorithm that identifies outliers by measuring how easy it is to isolate a data point from the rest of the dataset. Providers whose billing behavior is difficult to isolate are normal. Providers who are easy to isolate are statistical outliers and receive a high anomaly score.

The two outputs are combined into a final risk score and alert severity level for each provider. Providers with three or more flags or an Isolation Forest score above the critical threshold receive a critical severity label. Providers with two flags or a high anomaly score receive high severity. All others receive medium or none.

The two Gold tables written are:

healthcare_catalog.gold.provider_risk_scores
healthcare_catalog.gold.fraud_alerts

## Step 5: Dashboard

Six SQL queries run against the Gold tables in Databricks SQL to power the investigator dashboard. The dashboard shows a ranked list of high risk providers, a breakdown of which fraud flags are most common, total financial exposure by alert severity, and a comparison of flagged versus clean providers by state and specialty.

## Error Handling

Each step logs the record count before and after processing. If the count drops unexpectedly the job logs a warning. If a step fails entirely it logs the full error and stops without writing partial data to the next layer. Delta Lake write atomicity ensures that a failed write leaves the table unchanged rather than partially written.

## Local Run Order

```
python3 src/ingestion/claims_loader.py
python3 src/bronze/bronze_writer.py
python3 src/processing/silver_transformer.py
python3 src/anomaly_detection/fraud_detector.py
```

## Cloud Run Order

On Azure Databricks the pipeline runs as a Databricks Job with four tasks configured in sequence. Lakeflow Connect monitors the ADLS Gen2 container for new CSV files. When new data arrives Lakeflow Connect triggers the job automatically. Each task runs on the Serverless compute cluster and passes its output path to the next task.