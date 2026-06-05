# Solution Architecture — SEC Filing Intelligence Pipeline

## Overview

This pipeline is built around one core requirement: a compliance analyst or
researcher needs to know about a significant SEC filing within sixty seconds
of it being published. Everything in this architecture exists to serve that
requirement.

The system has five layers. Each layer has one job. They are loosely coupled,
meaning if one layer has a problem, the others keep running.

```
Layer 1   Ingestion        SEC EDGAR API -> Kafka Producer
Layer 2   Streaming        Kafka -> Spark Structured Streaming
Layer 3   Enrichment       FinBERT NLP Sentiment + Anomaly Detection
Layer 4   Storage          Delta Lake on Azure (Bronze, Silver, Gold)
Layer 5   Visualization    Databricks Dashboard
```

---

## Layer 1: Ingestion

### What It Does

A Python producer polls the SEC EDGAR Full Text Search API at regular intervals.
When a new filing appears, the producer publishes an event to a Kafka topic
called `sec-filings-raw`. That event contains the filing metadata: company name,
ticker, filing type, date, and the URL to the full document.

### Why Kafka and Not Direct Processing

The simplest approach would be to poll the API and immediately process each
filing. The problem with that approach is reliability.

If the processing layer is slow or temporarily down, filings pile up with
nowhere to go. Events get missed. There is no way to replay them.

Kafka acts as a durable buffer. It holds every event until the consumer
confirms it was processed successfully. If Spark goes down at 2am and comes
back at 3am, it picks up exactly where it left off. Nothing is lost.

That guarantee is not optional in a financial data system.

### Why Not AWS SQS or Azure Service Bus

Kafka was chosen over managed queue services for two reasons.

First, Kafka retains messages for a configurable period even after they are
consumed. This means the pipeline can replay historical filings for testing,
backfilling, or debugging without hitting the EDGAR API again.

Second, Kafka integrates natively with Spark Structured Streaming through the
built-in Kafka source connector. The code is cleaner and the throughput is
significantly higher than polling a message queue.

### Technology

```
Language        Python 3.11
Library         confluent-kafka
Topic           sec-filings-raw
Partitions      3
Retention       7 days
```

---

## Layer 2: Streaming

### What It Does

A Spark Structured Streaming job consumes events from the `sec-filings-raw`
Kafka topic. For each event it fetches the full filing document from the EDGAR
URL, parses the raw XML, and extracts structured fields.

Fields extracted at this stage:

```
company_name        string
ticker              string
filing_type         string     10-K, 10-Q, 8-K
filed_date          timestamp
period_of_report    date
filing_url          string
raw_text            string     full document text, cleaned
word_count          integer
```

### Why Spark Structured Streaming and Not Flink or Kinesis

Spark was chosen because the rest of the storage layer uses Delta Lake, which
has native first-class support in Spark. Writing from a Spark stream directly
to Delta Lake requires no additional connectors or custom serializers.

Flink is a valid alternative with lower latency at extreme scale. For this
pipeline processing hundreds of filings per day rather than millions, Spark
is the right choice. It is also the technology most data engineering teams
at mid-to-large companies already run.

Kinesis Data Analytics was ruled out because it locks the pipeline into AWS.
This pipeline runs on Azure.

### Checkpointing

Spark checkpointing is enabled on Azure Blob Storage. This means if the
Spark job restarts, it resumes from the last committed offset in Kafka. No
duplicate processing. No missed events.

### Technology

```
Engine          Apache Spark 3.5
Mode            Structured Streaming
Trigger         ProcessingTime 30 seconds
Checkpoint      Azure Blob Storage
Output          Delta Lake Bronze layer
```

---

## Layer 3: Enrichment

### What It Does

After the raw text is extracted, two enrichment jobs run on the Silver layer.

The first is sentiment analysis using FinBERT. The model reads the filing text
and assigns a sentiment score: positive, negative, or neutral. It also produces
a confidence score between 0 and 1.

The second is anomaly detection. A set of rule-based flags are applied to each
filing looking for patterns that historically precede significant market events.

Anomaly flags applied:

```
sudden_ceo_change           CEO or CFO named in 8-K resignation context
earnings_restatement        Keywords indicating prior earnings were incorrect
going_concern               Auditor language questioning business continuity
legal_proceedings           Significant new litigation disclosed
sentiment_drop              Sentiment score 40% lower than company 3-month average
options_volume_spike        Unusual options activity in 48 hours before filing
```

### Why FinBERT and Not VADER or TextBlob

VADER and TextBlob are general purpose sentiment models. They were trained on
social media and news text. Financial and legal language is structurally
different. A sentence like "the company faces material uncertainty regarding
future operations" is strongly negative in financial context. General models
score it as neutral.

FinBERT was trained specifically on financial documents including SEC filings,
earnings call transcripts, and financial news. Its accuracy on this type of
text is significantly higher. Independent benchmarks show F1 scores above 0.85
on financial sentiment tasks compared to 0.65 for VADER on the same datasets.

### Technology

```
Model           ProsusAI/finbert (HuggingFace)
Framework       PyTorch + Transformers
Batch Size      16 documents per inference call
Anomaly Logic   Rule-based Python, configurable thresholds
```

---

## Layer 4: Storage

### What It Does

All data is stored in Delta Lake on Azure Data Lake Storage Gen2. The storage
follows the medallion architecture with three layers.

### Bronze Layer

Raw filings exactly as received from EDGAR. No transformations. No cleaning.
This is the permanent record of what was ingested and when. If any downstream
processing has a bug, the data can always be reprocessed from Bronze.

Schema:

```
ingestion_timestamp     timestamp
kafka_offset            long
filing_url              string
raw_payload             string     full XML document
source                  string     edgar_api
```

### Silver Layer

Cleaned, parsed, and structured filings. Bad records are quarantined to a
separate error table. This is the layer Spark reads for enrichment.

Schema:

```
filing_id               string     generated UUID
company_name            string
ticker                  string
filing_type             string
filed_date              timestamp
period_of_report        date
raw_text                string
word_count              integer
processed_timestamp     timestamp
```

### Gold Layer

Business-ready data. Sentiment scores, anomaly flags, and aggregated company
metrics. This is what the Databricks dashboard reads.

Schema:

```
filing_id               string
company_name            string
ticker                  string
filing_type             string
filed_date              timestamp
sentiment_label         string     positive, negative, neutral
sentiment_score         float
anomaly_flags           array
anomaly_count           integer
company_avg_sentiment   float      rolling 90-day average
sentiment_delta         float      deviation from company average
alert_triggered         boolean
```

### Why Delta Lake and Not Plain Parquet

Plain Parquet files on Azure Blob Storage would be cheaper and simpler. Delta
Lake adds three things that matter for this use case.

ACID transactions mean that if a Spark job fails mid-write, the table is not
left in a corrupt state. This is critical when processing financial data where
partial records are worse than no records.

Time travel means every version of every table is queryable. If the anomaly
detection logic is updated, historical filings can be rescored without touching
the Bronze or Silver layers.

Schema enforcement prevents bad data from silently corrupting downstream tables.
A filing missing a required field fails loudly at the Silver write, not silently
at the Gold read.

### Technology

```
Storage         Azure Data Lake Storage Gen2
Format          Delta Lake
Compute         Azure Databricks
Catalog         Unity Catalog for table governance
```

---

## Layer 5: Visualization

### What It Does

A Databricks dashboard surfaces the Gold layer data to end users. Three views
are available.

The live feed shows every filing processed in the last 24 hours with sentiment
label and anomaly flag status.

The company sentiment trend shows a rolling 90-day sentiment history for any
company, with filing events marked on the timeline.

The anomaly alert panel shows all filings that triggered one or more anomaly
flags, sorted by severity and recency.

### Technology

```
Platform        Databricks SQL
Dashboard       Databricks built-in dashboard
Refresh         Every 60 seconds
Access          Role-based via Unity Catalog
```

---

## End-to-End Data Flow

```
SEC EDGAR API
     |
     | new filing published
     v
Kafka Producer (Python)
     |
     | event published to sec-filings-raw topic
     v
Kafka Topic
     |
     | consumed every 30 seconds
     v
Spark Structured Streaming
     |
     | raw payload written immediately
     v
Delta Lake Bronze
     |
     | parsed and cleaned
     v
Delta Lake Silver
     |
     | FinBERT scoring + anomaly detection
     v
Delta Lake Gold
     |
     | queried every 60 seconds
     v
Databricks Dashboard
     |
     v
Compliance Analyst sees alert
Total time from filing to alert: under 60 seconds
```

---

## What Was Not Built and Why

A machine learning model for anomaly detection was considered but ruled out
for V1. Rule-based anomaly detection is explainable, which matters in a
compliance context. If a compliance analyst asks why a filing was flagged,
the answer needs to be specific and auditable, not "the model scored it 0.73."
A trained model will be evaluated for V2 once a labeled dataset of confirmed
anomalies is accumulated from V1 production data.

A REST API layer for external consumers was also considered. It was deferred
to V2 in favor of getting the core pipeline stable first. The Gold layer
Delta tables serve as the data contract for now.
