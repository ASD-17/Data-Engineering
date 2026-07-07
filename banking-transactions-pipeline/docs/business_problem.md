# Business Problem

This doc explains why this pipeline exists and what problem it solves.
Read this before touching any code.

## The Problem

Banks process millions of transactions every day. Most are legitimate.
A small percentage are fraud. The challenge is not detecting fraud in
isolation. It is detecting it fast enough to matter, at a volume that
makes manual review impossible.

PaySim is a synthetic dataset built by researchers at the University of
Edinburgh to simulate mobile money transactions based on real patterns
from a private African financial dataset. It contains 6.3 million
transactions across 30 days with a labeled fraud column. The fraud rate
is 0.13 percent, which is realistic. Most transaction datasets used in
research have artificially inflated fraud rates that do not reflect
production conditions. This one does not.

The core engineering problem here is not the model. It is the pipeline.
In production, a bank does not hand you a CSV. Transactions land in an
OLTP database in real time. Your analytics and fraud systems need to
consume those changes as they happen, transform them into a queryable
warehouse, and surface aggregated signals to investigators and analysts.
That is what this pipeline builds.

## What We Are Building

A banking transactions data warehouse that simulates a production-grade
CDC pipeline. MySQL stores raw transactions as the OLTP source. Debezium
captures every insert and update from MySQL binlogs and publishes them
to Kafka. Spark Structured Streaming consumes Kafka events and writes
Bronze Delta tables in real time. PySpark batch jobs clean and validate
records into Silver. dbt models build a Kimball star schema in Gold,
giving analysts clean dimension and fact tables to query against.

The final output is a Gold layer star schema in Unity Catalog that an
analyst or fraud team can query directly in Databricks SQL.

## Why This Stack

Every technology choice in this pipeline has a reason.

MySQL as the OLTP source because that is what most mid-size banks and
fintechs actually run. PostgreSQL would work too but MySQL with Debezium
is the more common CDC pattern in the wild.

Debezium because it reads directly from the MySQL binlog. No polling.
No lag introduced by application-level change tracking. You get the
change the moment it is committed to the database.

Kafka because Debezium needs a message broker and Kafka is the standard.
It also gives the pipeline durability. If the Spark consumer goes down,
events do not disappear. They wait in the topic until consumed.

dbt for the star schema because dbt is now standard at every serious
data team. SQL transformations with version control, tests, and lineage
built in. Building it here shows you know how analytics engineering
actually works, not just raw Spark jobs.

Kimball star schema because that is what analysts and BI tools expect.
A flat fact table with clean dimension tables they can join against.
Designed for query performance, not for storage efficiency.

## Dataset

PaySim contains 6.3 million synthetic mobile money transactions across
30 days. Key fields:

step              hour of transaction (1 to 744, representing 30 days)
type              CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER
amount            transaction amount in local currency
nameOrig          origin account identifier
oldbalanceOrg     origin account balance before transaction
newbalanceOrig    origin account balance after transaction
nameDest          destination account identifier
oldbalanceDest    destination account balance before transaction
newbalanceDest    destination account balance after transaction
isFraud           ground truth fraud label (1 is fraud, 0 is legitimate)
isFlaggedFraud    transactions flagged by the system (1 is flagged)

Fraud rate is 0.13 percent across all transaction types, matching real
world conditions. Only TRANSFER and CASH_OUT transaction types contain
fraud cases. CASH_IN, DEBIT, and PAYMENT transactions are always
legitimate in this dataset.

## Success Criteria

The pipeline succeeds when an analyst can open Databricks SQL and answer
these questions without writing a single line of Python or Spark:

How much transaction volume moved through the system today by type.
Which accounts show balance discrepancy patterns that indicate fraud.
What is the fraud rate by transaction type and hour of day.
Which destination accounts appear repeatedly across flagged transactions.

These are the questions the Gold star schema is designed to answer.