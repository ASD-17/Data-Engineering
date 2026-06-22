# Business Problem — SEC Filing Intelligence Pipeline

## Background

Every public company in the United States is legally required to file financial
documents with the Securities and Exchange Commission (SEC). These filings are
published on a government database called EDGAR (Electronic Data Gathering,
Analysis, and Retrieval).

SEC filing types and their market relevance:

| Filing | What It Means | Why It Matters | Pipeline Version |
|--------|--------------|----------------|-----------------|
| **10-K** | Annual report — full year financial performance | Revenue, profit, risk disclosures, forward guidance | V1 — Core |
| **10-Q** | Quarterly report — 3-month financial snapshot | Earnings beats or misses, updated guidance | V1 — Core |
| **8-K** | Breaking news — something material just happened | CEO resignation, merger, lawsuit, earnings restatement | V1 — Core |
| **S-1** | IPO filing — private company going public for the first time | Signals market entry of high-profile companies like Uber, Airbnb, Reddit | V2 — Planned |
| **DEF 14A** | Proxy statement — shareholder voting agenda before annual meeting | Reveals executive compensation, board elections, M&A votes | V2 — Planned |

**V1 scope: 10-K, 10-Q, 8-K** — these are filed daily by thousands of companies and drive the highest volume of real-time market signals.

**V2 scope: S-1, DEF 14A** — lower frequency but high analytical value. S-1 filings signal IPO market activity. DEF 14A filings reveal governance health and activist investor triggers.

## The Problem

Hedge funds, investment banks, and compliance teams need to process these
filings the moment they are published — not hours later, not at end of day.

A company filing an unexpected 8-K at 9:47 AM can move its stock price within
minutes. Compliance teams at banks like JPMorgan and Goldman Sachs monitor
these filings in real time to detect insider trading signals — specifically,
when unusual options market activity coincides with a new filing.

**The challenge:**

- SEC EDGAR publishes hundreds of filings every single day
- Each filing is an unstructured XML document — raw text, no clean schema
- Relevant signals (sentiment, anomalies, risk keywords) are buried in paragraphs
- Manual review is impossible at this volume and speed
- Missing a signal can mean regulatory fines or missed trading opportunities

## What This Pipeline Solves

The moment a company submits a filing, this pipeline picks it up through Kafka,
processes and enriches it with Spark and FinBERT, stores it in Delta Lake, and
surfaces the result on a Databricks dashboard within sixty seconds.

That covers the full cycle: ingestion, NLP enrichment, anomaly detection,
storage, and visualization. No manual steps. No batch lag.

## Who Uses This

| User | What They Need | How This Pipeline Delivers |
|------|---------------|---------------------------|
| Compliance Analyst | Know immediately when a high-risk 8-K is filed | Real-time alerts with anomaly score |
| Quantitative Researcher | Historical correlation between filing sentiment and stock movement | Gold layer Delta Lake tables |
| Portfolio Manager | Which companies are showing deteriorating financial language | Sentiment trend dashboard |
| Regulatory Team | Audit trail of all filings processed and flagged | Full Bronze layer — raw, immutable records |

## Why This Is Hard to Build

Most engineers either:

- Poll the EDGAR API every few minutes — slow, misses events, not scalable
- Download bulk files overnight — 12+ hour lag, useless for real-time decisions

This pipeline uses Apache Kafka to capture filing events the moment they
appear, Spark Structured Streaming to process them at scale, and FinBERT,
a financial domain NLP model, to extract meaning from unstructured legal text.

Building all three together is what most engineers skip. Kafka alone is a
tutorial. Spark alone is a tutorial. Combining them with financial NLP and
anomaly detection on real government data is an actual system.

## Business Impact (Quantified)

When complete, this pipeline will demonstrate:

- Processing **500+ daily SEC filings** with sub-minute latency
- Sentiment scoring accuracy benchmarked against FinBERT baseline (F1 > 0.85)
- Anomaly detection flagging rate compared against known market events
- End-to-end latency from filing publish to dashboard alert: **< 60 seconds**

## Data Source

All data used in this project is **free, real, and government-published**.

- **SEC EDGAR Full-Text Search API**: `https://efts.sec.gov/LATEST/search-index`
- No API key required
- Updated in real time as companies submit filings
- Covers all US public companies — Apple, JPMorgan, Tesla, and 10,000+ others

This is not simulated data. These are real filings from real companies that
real analysts read every day.

## Future Enhancements (V2 Roadmap)

| Enhancement | Business Value |
|-------------|---------------|
| S-1 IPO filing ingestion | Track private companies entering public markets |
| DEF 14A proxy analysis | Governance scoring, executive pay anomaly detection |
| XBRL financial data extraction | Structured financial metrics directly from filings |
| Multi-company correlation | Detect when filings across a sector show simultaneous risk signals |
