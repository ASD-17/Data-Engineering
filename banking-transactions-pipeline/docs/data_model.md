# Data Model

## Overview

The pipeline processes PaySim synthetic banking transactions through
three layers. Bronze stores raw CDC events exactly as Debezium publishes
them from the MySQL binlog. Silver cleans and validates those events into
a single transactions table. Gold builds a Kimball star schema with one
fact table and four dimension tables optimized for analytical queries.

## Source Data

PaySim contains 6.3 million synthetic mobile money transactions. The
data lands in MySQL as the OLTP source with the following structure:

| Column | Type | Description |
|---|---|---|
| step | int | Hour of transaction, 1 to 744 representing 30 days |
| type | varchar | Transaction type: CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| amount | decimal | Transaction amount in local currency |
| nameOrig | varchar | Origin account identifier |
| oldbalanceOrg | decimal | Origin account balance before transaction |
| newbalanceOrig | decimal | Origin account balance after transaction |
| nameDest | varchar | Destination account identifier |
| oldbalanceDest | decimal | Destination account balance before transaction |
| newbalanceDest | decimal | Destination account balance after transaction |
| isFraud | int | Ground truth fraud label, 1 is fraud, 0 is legitimate |
| isFlaggedFraud | int | System flagged fraud, 1 is flagged |

## Bronze Layer

Bronze stores raw CDC events from Kafka exactly as Debezium published
them. No transformations applied. The CDC envelope wraps the original
MySQL row with operation metadata.

### banking_catalog.bronze.transactions_raw

| Column | Type | Description |
|---|---|---|
| op | string | CDC operation: c for insert, u for update, d for delete |
| ts_ms | long | Timestamp of the change in milliseconds |
| before | struct | Row state before the change, null for inserts |
| after | struct | Row state after the change, null for deletes |
| source_db | string | Source database name |
| source_table | string | Source table name |
| kafka_offset | long | Kafka topic offset |
| kafka_partition | int | Kafka partition number |
| load_timestamp | timestamp | When the record was written to Bronze |

The before and after fields contain the full MySQL row as a nested
struct with all columns from the source table.

## Silver Layer

Silver extracts the after state from each CDC event, casts all columns
to correct types, removes duplicates, and handles nulls with business
logic. Only insert and update operations are kept. Deletes are logged
but not propagated to Silver since historical transactions are never
deleted in a banking system.

### banking_catalog.silver.transactions_cleaned

| Column | Type | Description |
|---|---|---|
| transaction_id | string | Unique transaction identifier generated from source fields |
| step | integer | Hour of transaction, 1 to 744 |
| transaction_type | string | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| amount | double | Transaction amount |
| origin_account | string | Origin account identifier |
| origin_balance_before | double | Origin balance before transaction |
| origin_balance_after | double | Origin balance after transaction |
| dest_account | string | Destination account identifier |
| dest_balance_before | double | Destination balance before transaction |
| dest_balance_after | double | Destination balance after transaction |
| is_fraud | boolean | Ground truth fraud label |
| is_flagged_fraud | boolean | System flagged fraud |
| balance_discrepancy | double | Difference between expected and actual balance change |
| processed_timestamp | timestamp | When record was written to Silver |

The balance_discrepancy column is derived in Silver. It computes the
difference between the expected balance change and the actual balance
change. A non-zero discrepancy is a strong fraud signal.

## Gold Layer

Gold contains the Kimball star schema. dbt models build these tables
from Silver. Each table is optimized for analytical queries, not for
storage efficiency. The fact table is narrow and numeric. The dimension
tables carry all descriptive attributes.

### banking_catalog.gold.fact_transactions

One row per transaction. All foreign keys reference dimension tables.

| Column | Type | Description |
|---|---|---|
| transaction_key | string | Surrogate key for the fact row |
| transaction_id | string | Natural key from Silver |
| origin_account_key | string | Foreign key to dim_account |
| dest_account_key | string | Foreign key to dim_account |
| transaction_type_key | string | Foreign key to dim_transaction_type |
| date_key | integer | Foreign key to dim_date |
| amount | double | Transaction amount |
| origin_balance_before | double | Origin balance before |
| origin_balance_after | double | Origin balance after |
| dest_balance_before | double | Destination balance before |
| dest_balance_after | double | Destination balance after |
| balance_discrepancy | double | Derived fraud signal |
| is_fraud | boolean | Ground truth fraud label |
| is_flagged_fraud | boolean | System flagged fraud |
| loaded_timestamp | timestamp | When record was loaded to Gold |

### banking_catalog.gold.dim_account

One row per unique account. Both origin and destination accounts are
stored in the same dimension table with an account_type column to
distinguish them.

| Column | Type | Description |
|---|---|---|
| account_key | string | Surrogate key |
| account_id | string | Natural key from PaySim nameOrig or nameDest |
| account_type | string | Customer account (C prefix) or Merchant account (M prefix) |
| first_seen_step | integer | First hour this account appeared in the dataset |
| total_transactions | integer | Total transactions this account was involved in |

### banking_catalog.gold.dim_transaction_type

One row per transaction type. Five rows total.

| Column | Type | Description |
|---|---|---|
| transaction_type_key | string | Surrogate key |
| transaction_type | string | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| is_fraud_possible | boolean | Whether this type can contain fraud in PaySim |
| description | string | Human readable explanation of the transaction type |

### banking_catalog.gold.dim_date

One row per hour in the 30 day simulation. 744 rows total.

| Column | Type | Description |
|---|---|---|
| date_key | integer | Surrogate key, same as step value |
| step | integer | Hour number, 1 to 744 |
| day_of_simulation | integer | Day number, 1 to 30 |
| hour_of_day | integer | Hour within the day, 0 to 23 |
| week_of_simulation | integer | Week number, 1 to 4 |
| is_weekend | boolean | Whether the hour falls on a weekend |