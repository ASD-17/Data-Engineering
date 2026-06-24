# Pipeline Flow

## Overview

The pipeline runs in four sequential steps. Each step reads from the previous layer and writes to the next. No step runs until the previous one completes successfully. This is a batch pipeline that processes a full dataset at once.

## Step 1 Data Extraction

claims_loader.py reads each of the four CSV files from data/raw/ into a Spark DataFrame. Before writing anything it validates that all expected columns are present in each file. If a column is missing the job stops and logs the error. A load timestamp is added to every record before passing to the next step.

Files read:
Train_Inpatientdata-1542865627584.csv
Train_Outpatientdata-1542865627584.csv
Train_Beneficiarydata-1542865627584.csv
Train-1542865627584.csv

## Step 2 Bronze Layer

bronze_writer.py receives the four Spark DataFrames and writes each one as a separate Delta Lake table in the Bronze schema. No transformations are applied. Data types remain as strings. The raw values are preserved so that if a downstream transformation has a bug the original data is always available for reprocessing.

Tables written:
healthcare_catalog.bronze.inpatient_claims
healthcare_catalog.bronze.outpatient_claims
healthcare_catalog.bronze.beneficiary_data
healthcare_catalog.bronze.provider_labels

## Step 3 Silver Layer

silver_transformer.py reads all Bronze tables and runs three operations.

Cleaning casts string columns to correct types. Dates become date types. Amounts become doubles. Null values are handled with business logic. A missing deductible is treated as zero. A missing procedure code becomes an empty array.

Joining connects claims with beneficiary demographics to create claims_enriched. Every claim row carries patient age, chronic condition count, and state. The is_deceased flag is set when the date of death field is populated. The is_weekend_claim flag is set when the claim start date falls on a Saturday or Sunday.

Feature engineering aggregates all claims by provider to build provider_features. This table computes the behavioral metrics that fraud detection depends on: total claims, average reimbursement, duplicate claim ratio, weekend claim ratio, high cost procedure ratio, reimbursement per patient, and deceased patient claims.

Tables written:
healthcare_catalog.silver.claims_enriched
healthcare_catalog.silver.provider_features

## Step 4 Fraud Detection and Gold Layer

fraud_detector.py reads the Silver provider_features table and runs two detection approaches in parallel.

Rule based detection applies explicit business rules to each provider. A provider is flagged for duplicate claims if their duplicate claim ratio exceeds the configured threshold. A provider is flagged for deceased billing if they submitted claims after a patient's date of death. A provider is flagged for impossible volume if their total claims per beneficiary exceeds what a single provider could realistically handle. A provider is flagged for upcoding if their high cost procedure ratio is significantly above the peer average. Each rule that fires adds one flag to the provider record.

Isolation Forest runs on the numerical features of each provider profile. Providers whose billing behavior is easy to isolate from the rest of the dataset receive a high anomaly score. Providers whose behavior looks like their peers receive a low score.

Explainability combines the rule based flags and the Isolation Forest score with the peer benchmark table to generate human readable reasons for each alert. Instead of just a risk score, investigators see exactly which behaviors triggered the flag and how far the provider deviates from their peer group.

Validation computes precision, recall, F1 score, and ROC-AUC against the ground truth fraud labels from the provider_labels Bronze table. This proves the detection logic is working and establishes a baseline.

Tables written:
healthcare_catalog.gold.provider_risk_scores
healthcare_catalog.gold.fraud_alerts
healthcare_catalog.gold.provider_peer_benchmark
healthcare_catalog.gold.investigator_work_queue

## Step 5 Dashboard

Six SQL queries run against the Gold tables in Databricks SQL to power the investigator dashboard. The dashboard shows an executive summary with total alerts and financial exposure, a ranked provider risk table, a geographic fraud map by state, and the investigator work queue with explainable alerts sorted by priority.

## Error Handling

Each step logs the record count before and after processing. If the count drops unexpectedly the job logs a warning. If a step fails entirely it logs the full error and stops without writing partial data to the next layer. Delta Lake write atomicity ensures a failed write leaves the table unchanged.

## Local Run Order

```
python3 src/ingestion/claims_loader.py
python3 src/bronze/bronze_writer.py
python3 src/processing/silver_transformer.py
python3 src/anomaly_detection/fraud_detector.py
```

## Cloud Run Order

On Azure Databricks the pipeline runs as a Databricks Job with four tasks in sequence on Serverless compute. Lakeflow Connect monitors the ADLS Gen2 container and triggers the job automatically when new CSV files arrive.