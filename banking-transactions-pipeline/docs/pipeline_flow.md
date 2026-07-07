# Pipeline Flow

## Overview

The pipeline runs in six sequential stages. Each stage has a clear
input, a clear output, and a clear reason for existing. No stage runs
until the previous one completes successfully.

## Stage 1: Data Loading

The PaySim CSV file contains 6.3 million transactions. A Python loader
script reads the CSV and inserts rows into the MySQL transactions table.
MySQL is configured with binary logging enabled. Every insert Debezium
needs to capture requires binlog to be on before the data lands.

The loader script does not dump all 6.3 million rows at once. It inserts
in batches of 10,000 rows with a small delay between batches. This
simulates a real banking system where transactions arrive continuously
rather than all at once.

```
Local run order:
python3 src/ingestion/mysql_loader.py
```

## Stage 2: CDC via Debezium

Debezium runs as a Kafka Connect plugin inside Docker. It connects to
MySQL, reads the binlog, and converts each committed change into a JSON
event. The event contains the operation type, a timestamp, and the full
row state before and after the change.

Debezium does not poll MySQL. It tails the binlog in real time. The
moment MySQL commits a transaction, Debezium reads the binlog entry and
publishes an event to Kafka. This is why CDC latency is measured in
milliseconds, not minutes.

The Kafka topic is named banking.transactions. Debezium publishes one
message per MySQL row change.

## Stage 3: Bronze Layer

bronze_writer.py runs as a Spark Structured Streaming job. It subscribes
to the banking.transactions Kafka topic and processes events in
micro-batches every 30 seconds using foreachBatch.

Each micro-batch writes raw CDC events to the Bronze Delta table. No
transformations are applied. The CDC envelope is preserved including
the op field, the before state, and the after state. If something goes
wrong downstream, Bronze always has the original event to replay from.

Spark maintains a checkpoint directory to track which Kafka offsets have
been processed. If the job restarts, it picks up exactly where it left
off with no duplicate records and no missed events.

```
Local run order:
python3 src/streaming/bronze_writer.py
```

## Stage 4: Silver Layer

silver_transformer.py runs as a PySpark batch job after Bronze is
populated. It reads Bronze, extracts the after state from each CDC
event, casts all columns to their correct types, and derives the
balance_discrepancy column.

Balance discrepancy is computed as the difference between what the
origin balance should be after the transaction and what it actually is.
In legitimate transactions this is always zero. In fraud transactions
the balances do not add up because the fraudster empties the account
before the system catches the anomaly.

Duplicate events are removed using transaction_id deduplication. CDC
can produce duplicate events during Kafka rebalancing. Silver handles
this so Gold never sees duplicates.

Delete operations from CDC are logged but not written to Silver. In a
banking system transactions are never deleted. A delete event in the
CDC stream indicates a data quality issue in the source.

```
Local run order:
python3 src/processing/silver_transformer.py
```

## Stage 5: Gold Layer via dbt

dbt reads Silver and builds the Kimball star schema. dbt models run in
dependency order automatically. dim tables build first because fact
tables reference them.

Build order:
```
stg_transactions          staging model, minor cleanup from Silver
dim_account               one row per unique account
dim_transaction_type      one row per transaction type, five rows total
dim_date                  one row per hour, 744 rows for 30 days
fact_transactions         one row per transaction with all foreign keys
```

dbt also runs data tests after each model builds. Tests check that
transaction_id is unique and not null in the fact table, that all
foreign keys in the fact table exist in their dimension tables, and
that amount is always greater than zero.

```
Local run order:
cd dbt/banking_dw
dbt run
dbt test
```

## Stage 6: Dashboard

Six SQL queries run against the Gold tables in Databricks SQL. The
dashboard answers the questions a fraud analyst needs every morning:

Total transaction volume by type and day.
Accounts with non-zero balance discrepancy.
Fraud rate by transaction type.
High frequency destination accounts that appear across multiple
flagged transactions.
Hourly transaction volume to spot unusual spikes.
Comparison of flagged versus confirmed fraud by transaction type.

## Error Handling

Each stage logs record counts before and after processing. If the count
drops unexpectedly the job logs a warning. Bronze uses Spark checkpoints
for exactly-once Kafka delivery. Silver uses Delta Lake merge for
idempotent upserts when reprocessing. Gold uses dbt incremental models
so reruns only process new records without rebuilding the entire table.

## Local Run Order

```
docker-compose up -d
python3 src/ingestion/mysql_loader.py
python3 src/streaming/bronze_writer.py
python3 src/processing/silver_transformer.py
cd dbt/banking_dw && dbt run && dbt test
python3 src/dashboard/validate_queries.py
```

## Cloud Run Order

On Azure Databricks the pipeline runs as a Databricks Job with tasks
in sequence on Serverless compute. MySQL runs locally or on an Azure
VM. Debezium and Kafka run in Docker on the same VM. Spark reads from
Kafka and writes directly to ADLS Gen2.