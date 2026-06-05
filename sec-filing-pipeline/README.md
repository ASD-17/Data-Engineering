# SEC Filing Intelligence Pipeline

A real-time data pipeline that ingests SEC EDGAR filings the moment they are
published, scores them for financial sentiment using FinBERT, detects anomalies,
and surfaces compliance alerts on a Databricks dashboard within 60 seconds.

Built on Apache Kafka, Spark Structured Streaming, Delta Lake on Azure, and
Databricks. All data is real, free, and sourced from the US government.

---

## What It Does

Every public company in the US is legally required to report significant events
to the SEC. When a CEO resigns, a merger is announced, or earnings are restated,
that filing goes live on EDGAR within minutes. This pipeline captures that event,
extracts meaning from the unstructured legal text, and flags anything unusual
before a human analyst would have finished reading the first paragraph.

---

## Architecture

```
SEC EDGAR API
     |
     | new filing published
     v
Kafka Producer (Python)
     |
     | event to sec-filings-raw topic
     v
Kafka (3 partitions, 7-day retention)
     |
     | consumed every 30 seconds
     v
Spark Structured Streaming
     |
     v
Delta Lake Bronze       raw XML, append-only, permanent record
     |
     v
Delta Lake Silver       parsed, cleaned, structured
     |
     v
FinBERT NLP + Anomaly Detection
     |
     v
Delta Lake Gold         sentiment scores, anomaly flags, alerts
     |
     v
Databricks Dashboard    compliance analyst sees alert in under 60 seconds
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, SEC EDGAR API, Apache Kafka |
| Streaming | Apache Spark Structured Streaming |
| NLP | FinBERT (ProsusAI/finbert) |
| Storage | Delta Lake, Azure Data Lake Storage Gen2 |
| Orchestration | Databricks Workflows |
| Visualization | Databricks SQL Dashboard |

---

## Filing Types Supported

| Filing | Description | Version |
|--------|-------------|---------|
| 8-K | Breaking news — CEO change, merger, restatement | V1 |
| 10-K | Annual financial report | V1 |
| 10-Q | Quarterly financial report | V1 |
| S-1 | IPO filing | V2 planned |
| DEF 14A | Proxy statement | V2 planned |

---

## Anomaly Detection Flags

The pipeline checks every filing against six anomaly conditions:

```
sudden_ceo_change          CEO or CFO named in resignation context
earnings_restatement       Prior earnings being corrected
going_concern              Auditor questioning business continuity
legal_proceedings          Significant new litigation disclosed
sentiment_drop             Sentiment 40% below company 90-day average
options_volume_spike       Unusual options activity in 48 hours before filing
```

Alert severity is assigned as critical, high, medium, or low based on
the number and type of flags triggered.

---

## Project Structure

```
sec-filing-pipeline/
    src/
        ingestion/
            edgar_producer.py       Kafka producer, polls EDGAR API
        streaming/
            bronze_writer.py        Spark job, Kafka to Bronze Delta
        processing/
            silver_transformer.py   Spark job, Bronze to Silver
        enrichment/
            gold_enricher.py        FinBERT scoring + anomaly detection
        dashboard/
            queries.sql             Databricks SQL dashboard queries
    infrastructure/
        kafka/
            docker-compose.yml      Local Kafka setup
    config/
        pipeline_config.yml         All configuration in one place
    docs/
        business_problem.md         Why this pipeline exists
        solution_architecture.md    How it is built and why
        data_model.md               Bronze, Silver, Gold schemas
        pipeline_flow.md            Step by step data flow
        runbook.md                  How to operate in production
    tests/
        unit/
        integration/
    requirements.txt
    .gitignore
```

---

## Getting Started

Clone the repo:

```bash
git clone https://github.com/ASD-17/sec-filing-pipeline.git
cd sec-filing-pipeline
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Set up environment variables:

```bash
cp .env.example .env
# Edit .env with your Azure storage credentials
```

Start Kafka locally:

```bash
cd infrastructure/kafka
docker-compose up -d
```

Start the EDGAR producer:

```bash
cd src/ingestion
python edgar_producer.py
```

See the full setup guide in [docs/runbook.md](docs/runbook.md).

---

## Documentation

| Document | Description |
|----------|-------------|
| [Business Problem](docs/business_problem.md) | Why this pipeline exists |
| [Solution Architecture](docs/solution_architecture.md) | Design decisions and technology choices |
| [Data Model](docs/data_model.md) | Complete Bronze, Silver, Gold schemas |
| [Pipeline Flow](docs/pipeline_flow.md) | Step by step walkthrough of the full pipeline |
| [Runbook](docs/runbook.md) | How to start, stop, monitor, and troubleshoot |

---

## Data Source

All data is sourced from the SEC EDGAR Full Text Search API at
https://efts.sec.gov/LATEST/search-index. No API key required. Free,
real, and government maintained. These are the same filings that analysts
at hedge funds and investment banks read every day.

---

## Status

Pipeline is under active development. Documentation is complete.
Code implementation in progress.

| Component | Status |
|-----------|--------|
| Docs | Complete |
| Kafka Producer | In progress |
| Bronze Writer | In progress |
| Silver Transformer | Planned |
| Gold Enricher | Planned |
| Dashboard | Planned |
