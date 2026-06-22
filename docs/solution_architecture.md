# Solution Architecture

## Overview

The Healthcare Claims Anomaly Detection Pipeline is a batch data engineering system that ingests Medicare claims data from Kaggle, processes it through a Medallion Architecture on Delta Lake, applies statistical anomaly detection to identify fraudulent providers and suspicious billing patterns, and surfaces high risk cases to investigators through a Databricks SQL dashboard.

Healthcare claims are submitted in bulk by providers at the end of each billing cycle. Processing them in scheduled batches mirrors how real world Medicare fraud detection systems operate at CMS.

## Architecture Diagram

```
Kaggle Dataset
Medicare Provider Fraud Detection
Inpatient, Outpatient, Beneficiary, Provider CSVs
         |
         | claims_loader.py reads CSV files into Spark DataFrames
         v
BRONZE LAYER
healthcare_catalog.bronze.inpatient_claims
healthcare_catalog.bronze.outpatient_claims
healthcare_catalog.bronze.beneficiary_data
healthcare_catalog.bronze.provider_labels
Delta Lake on ADLS Gen2
Raw data exactly as received from source
         |
         | silver_transformer.py cleans, joins, and structures
         v
SILVER LAYER
healthcare_catalog.silver.claims_enriched
healthcare_catalog.silver.provider_profiles
Delta Lake on ADLS Gen2
Validated, joined, and structured claims data
         |
         | fraud_detector.py applies anomaly detection
         v
GOLD LAYER
healthcare_catalog.gold.fraud_alerts
healthcare_catalog.gold.provider_risk_scores
Delta Lake on ADLS Gen2
Scored providers and ranked fraud alerts
         |
         | Databricks SQL Warehouse
         v
DASHBOARD
Provider risk ranking, alert panel, billing anomalies
```

## Technology Decisions

**Spark batch over streaming** — Healthcare claims arrive in bulk. Providers submit monthly or weekly batches to CMS. Processing each file as it arrives in a scheduled batch job matches the cadence of the real world problem. Streaming would add complexity without adding value here.

**Four Bronze tables instead of one** — The Kaggle dataset has four separate CSV files representing different entities. Inpatient claims, outpatient claims, beneficiary demographics, and provider fraud labels each have their own schema and update cadence. Keeping them separate in Bronze preserves the raw source structure and makes debugging easier when something goes wrong upstream.

**Joining at Silver** — The four Bronze tables are joined in the Silver layer to create a unified claims view per provider. This is where the analytical value is created. A claim only becomes meaningful when you know the beneficiary age, the provider specialty, and whether the provider is labeled fraudulent in the training data.

**Rule based plus statistical anomaly detection** — Two detection approaches run in parallel. Rule based detection catches obvious fraud patterns like impossible claim volumes or invalid diagnosis codes. Statistical detection uses Isolation Forest to find providers whose billing behavior is statistically unusual relative to their peer group. Together they catch both known fraud patterns and novel ones.

**Delta Lake on ADLS Gen2** — Delta Lake provides ACID transactions, time travel, and schema enforcement on top of Parquet files stored in Azure Data Lake Storage Gen2. If a processing job corrupts a table, time travel allows rollback to a previous version without reprocessing the source data.

**Unity Catalog** — All tables are registered in Databricks Unity Catalog with a custom managed storage location pointing to ADLS Gen2. This gives full data governance, lineage tracking, and the ability to inspect the underlying Delta files directly in the storage account.

## Azure Infrastructure

```
Resource Group       healthcare-pipeline-rg (East US)
Storage Account      healthcarepipelinestorage (ADLS Gen2)
Container            healthcare-data
Databricks Workspace healthcare-pipeline-databricks
Catalog              healthcare_catalog
Schemas              bronze, silver, gold
```

## Data Flow

```
Step 1   Download Kaggle CSVs to data/raw/ locally
Step 2   claims_loader.py reads CSVs into Spark DataFrames
Step 3   bronze_writer.py writes four Bronze Delta tables
Step 4   silver_transformer.py joins and cleans into Silver
Step 5   fraud_detector.py scores providers and generates alerts
Step 6   Upload Delta files to ADLS Gen2
Step 7   Register tables in Unity Catalog
Step 8   Run dashboard queries in Databricks SQL
```

## Security

The Kaggle dataset is de-identified for research purposes. Azure Managed Identity handles all storage access. No credentials are stored in code or committed to the repository.