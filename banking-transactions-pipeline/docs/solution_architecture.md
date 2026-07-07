# Solution Architecture

## Overview

The Banking Transactions Data Warehouse is a CDC driven data engineering
pipeline that simulates a production grade banking system. MySQL serves
as the OLTP source storing raw transactions. Debezium captures every
committed change from the MySQL binlog and publishes it to Kafka.
Spark Structured Streaming consumes those events and lands them in
Bronze Delta Lake. PySpark batch jobs clean and validate records into
Silver. dbt models transform Silver into a Kimball star schema in Gold.
Analysts query the final warehouse directly in Databricks SQL.

The pipeline simulates how a real bank moves data from its transactional
system to its analytical warehouse. The source is not a file. It is a
live database that produces changes continuously.

## Architecture Diagram

```
PaySim CSV (6.3 million transactions)
         |
         | Load into MySQL OLTP database
         v
MySQL Database
Transactions table, accounts table
ACID compliant writes
Binlog enabled
         |
         | Debezium reads committed changes from binlog
         | Publishes CDC events as JSON to Kafka
         v
Apache Kafka
banking.transactions topic
CDC events with before and after state
         |
         | Spark Structured Streaming consumes topic
         | foreachBatch writes to Delta Lake
         v
BRONZE LAYER
banking_catalog.bronze.transactions_raw
Delta Lake on ADLS Gen2
Raw CDC events exactly as received from Kafka
         |
         | PySpark batch cleans and validates
         v
SILVER LAYER
banking_catalog.silver.transactions_cleaned
Delta Lake on ADLS Gen2
Validated, typed, deduplicated transactions
         |
         | dbt models build star schema
         v
GOLD LAYER (Kimball Star Schema)
banking_catalog.gold.fact_transactions
banking_catalog.gold.dim_account
banking_catalog.gold.dim_transaction_type
banking_catalog.gold.dim_date
Delta Lake on ADLS Gen2
         |
         | Databricks SQL Warehouse (Serverless)
         v
DASHBOARD
Transaction volume, fraud patterns, account analytics
```

## Technology Decisions

**MySQL as OLTP source** because most mid-size banks and fintechs run
MySQL or a compatible database. The CDC pattern with Debezium and MySQL
binlogs is one of the most common production setups in the industry.
Understanding this stack means you can work at companies that are not
running Databricks natively.

**Debezium over polling** because polling introduces lag and misses
deletes. Debezium reads directly from the MySQL binlog which is written
the moment a transaction commits. Every insert, update, and delete is
captured with zero polling lag. This is the same mechanism used by
Netflix, Uber, and LinkedIn for their CDC pipelines.

**Kafka as the message broker** because Debezium needs a durable message
queue. If the Spark consumer goes down, events do not disappear. They
wait in the Kafka topic until consumed. This decouples the producer from
the consumer and gives the pipeline fault tolerance.

**Spark Structured Streaming over batch for Bronze** because CDC events
are a stream. Each committed MySQL transaction produces an event.
Structured Streaming with foreachBatch processes these events in
micro-batches and writes them to Delta Lake with exactly-once semantics
using checkpointing.

**dbt for the star schema** because dbt is the standard for analytics
engineering. SQL transformations with version control, built-in data
tests, and automatic lineage documentation. The alternative is writing
raw PySpark or SQL jobs with no testing and no dependency management.
dbt makes the transformation layer auditable and maintainable.

**Kimball star schema in Gold** because analysts and BI tools expect
denormalized tables. A flat fact table joined to clean dimension tables
gives sub-second query performance on aggregations. The normalized OLTP
schema in MySQL is optimized for writes. The star schema in Gold is
optimized for reads.

**Delta Lake throughout** because ACID transactions, time travel, and
schema enforcement matter at every layer. Bronze needs append-only
writes. Silver needs upserts when CDC updates arrive. Gold needs
reliable reads for the dashboard.

## Azure Infrastructure

```
Resource Group       banking-pipeline-rg (East US)
Storage Account      bankingpipelinestorage (ADLS Gen2)
Container            banking-data
Databricks Workspace data-engineering-databricks (shared)
Catalog              banking_catalog
Schemas              bronze, silver, gold
Compute              Serverless SQL Warehouse
```

## Data Flow Latency

```
Transaction commits in MySQL
         |
         | Debezium reads binlog
         | under 1 second
         v
Kafka topic
         |
         | Spark micro-batch interval 30 seconds
         v
Bronze Delta Lake
         |
         | PySpark batch job
         v
Silver Delta Lake
         |
         | dbt models
         v
Gold star schema available for query

End to end latency target: under 2 minutes from commit to queryable
```

## Local Setup

MySQL and Kafka run in Docker containers locally. Debezium runs as a
Kafka Connect plugin inside the Kafka container. This mirrors production
without requiring cloud resources during development.

```
docker-compose up               starts MySQL, Zookeeper, Kafka, Debezium
python3 loader.py               loads PaySim CSV into MySQL transactions table
                                Debezium detects inserts via binlog
                                events flow to Kafka automatically
python3 bronze_writer.py        Spark consumes Kafka, writes Bronze
python3 silver_transformer.py   cleans Bronze, writes Silver
dbt run                         builds star schema in Gold
```