# Pipeline Flow — SEC Filing Intelligence Pipeline

## Overview

This document walks through exactly what happens from the moment a company
submits a filing to the SEC to the moment a compliance analyst sees an alert
on the dashboard. Every step is explained, including what can go wrong and
how the pipeline handles it.

---

## Step 1: Company Submits a Filing to SEC EDGAR

This step happens outside our system entirely.

A public company like Apple or JPMorgan submits a filing through the SEC's
internal portal. Within minutes, that filing appears on the EDGAR public
database and becomes available through the API.

Our pipeline starts the moment the filing is available on EDGAR.

Nothing we build controls or affects this step. Our job starts at Step 2.

---

## Step 2: Kafka Producer Detects the New Filing

File: `src/ingestion/edgar_producer.py`

The Kafka producer is a Python script running continuously. Every 30 seconds
it calls the EDGAR Full Text Search API and asks one question: are there any
filings published since the last time I checked?

The API call looks like this:

```
GET https://efts.sec.gov/LATEST/search-index?q=%22%22&dateRange=custom
    &startdt={last_check_timestamp}&enddt={now}&forms=10-K,10-Q,8-K
```

If new filings exist, the producer creates one Kafka event per filing and
publishes it to the `sec-filings-raw` topic.

Each Kafka event contains:

```json
{
  "ingestion_id": "uuid-generated-here",
  "ingestion_timestamp": "2025-03-15T09:47:23Z",
  "company_name": "Apple Inc.",
  "ticker": "AAPL",
  "cik": "0000320193",
  "filing_type": "8-K",
  "filed_date": "2025-03-15T09:44:00Z",
  "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/...",
  "source": "edgar_api"
}
```

The producer then saves the current timestamp as last_check_timestamp so
the next poll knows where to start from.

What happens if the EDGAR API is down: the producer catches the exception,
logs the failure, waits 60 seconds, and retries. It does not crash. The
last_check_timestamp is not updated until a successful response is received,
so no filings are skipped.

What happens if Kafka is down: the producer catches the connection error,
waits, and retries with exponential backoff. Events are queued in memory
up to a configurable limit. If Kafka is down for an extended period, the
producer logs a critical alert.

---

## Step 3: Kafka Holds the Event

This step requires no code from us. Kafka handles it automatically.

The event sits in the `sec-filings-raw` topic on partition determined by
the company ticker. Events from the same company always go to the same
partition. This guarantees that filings from one company are processed
in the order they were filed.

Kafka retains every event for 7 days regardless of whether it has been
consumed. This means:

- If Spark is down for a few hours, events are waiting when it comes back
- Historical events can be replayed for testing or backfilling
- Multiple consumers can read the same topic independently if needed

---

## Step 4: Spark Reads from Kafka and Writes to Bronze

File: `src/streaming/bronze_writer.py`

A Spark Structured Streaming job reads from `sec-filings-raw` continuously.
Every 30 seconds it processes all events that have arrived since the last
micro-batch.

For each event, Spark does the minimum work possible at this stage. It takes
the raw Kafka event, adds a batch_id and kafka offset information, and writes
it directly to the Bronze Delta table. No parsing. No transformation. Just
store it exactly as received.

This is intentional. Bronze is a safety net. The faster raw data is written
to Bronze, the less chance of losing it.

Spark also fetches the full filing document from the EDGAR URL at this stage
and stores the complete raw XML in the raw_payload column.

If the document fetch fails (EDGAR occasionally has slow responses), the
record is still written to Bronze with raw_payload as null and a fetch_error
flag set to true. A separate retry job handles these records later.

Checkpoint location: `abfss://checkpoints@storageaccount.dfs.core.windows.net/bronze/`

The checkpoint saves the last successfully processed Kafka offset to Azure
Blob Storage. If the Spark job restarts for any reason, it reads the
checkpoint and resumes from exactly where it left off.

---

## Step 5: Bronze to Silver — Parsing and Cleaning

File: `src/processing/silver_transformer.py`

A second Spark job reads new records from Bronze and transforms them into
the Silver layer. This job runs every 5 minutes.

It is a separate job from the Bronze writer deliberately. Keeping ingestion
and transformation decoupled means a parsing bug does not stop new data
from being ingested. Raw data keeps flowing into Bronze while the parsing
issue is fixed.

What this job does for each Bronze record:

First it parses the raw XML document. It extracts company name, CIK, ticker,
filing dates, and the full document text. HTML tags and boilerplate headers
are stripped from the text.

Then it validates the record. Required fields are checked. If company name,
CIK, filing type, or filed date is missing, the record goes to the quarantine
table, not to Silver.

Then it adds the computed fields: word count, has_risk_section flag,
has_forward_looking flag, is_amended flag.

Finally it writes the clean record to `silver.sec_filings_parsed`.

The quarantine table gets a detailed error record for every failure. A daily
alert is sent if quarantine volume exceeds 5% of total ingestion volume.
That threshold is a signal that something systematic is wrong, not just
occasional bad records.

---

## Step 6: Silver to Gold — Enrichment

File: `src/enrichment/gold_enricher.py`

This is the most compute-intensive step. It runs every 15 minutes on new
Silver records.

Two things happen here in sequence.

First, FinBERT sentiment analysis. The raw_text from Silver is passed to the
FinBERT model in batches of 16 documents. FinBERT returns a sentiment label
(positive, negative, neutral) and a confidence score for each document. For
long documents that exceed the FinBERT token limit, the text is split into
chunks, scored individually, and the scores are averaged with the highest
confidence chunk weighted more heavily.

Second, anomaly detection. After sentiment scoring, the anomaly detection
logic runs against each record. It checks for the six anomaly flags defined
in the solution architecture. Each flag is checked independently. The results
are collected into the anomaly_flags array.

Then the historical context is added. The job looks up the company's rolling
90-day average sentiment score from `gold.company_sentiment_summary` and
computes the sentiment_delta for this filing.

Finally alert_severity is assigned based on this logic:

```
critical    anomaly_count >= 3 or going_concern flag present
high        anomaly_count == 2 or sentiment_delta <= -0.4
medium      anomaly_count == 1 or sentiment_delta <= -0.2
low         alert_triggered = true but no above conditions met
none        alert_triggered = false
```

The enriched record is written to `gold.sec_filings_enriched`.

---

## Step 7: Dashboard Refresh

File: Databricks SQL Dashboard (no custom code)

The Databricks dashboard queries the Gold layer every 60 seconds. Three
panels refresh automatically.

The live feed panel queries:

```sql
SELECT company_name, ticker, filing_type, filed_date,
       sentiment_label, sentiment_score, anomaly_count,
       alert_severity, alert_triggered
FROM gold.sec_filings_enriched
WHERE filed_date >= current_timestamp() - interval 24 hours
ORDER BY filed_date DESC
```

The anomaly alert panel queries:

```sql
SELECT company_name, ticker, filing_type, filed_date,
       anomaly_flags, alert_severity, sentiment_delta
FROM gold.sec_filings_enriched
WHERE alert_triggered = true
AND filed_date >= current_timestamp() - interval 7 days
ORDER BY alert_severity DESC, filed_date DESC
```

The company trend panel queries `gold.company_sentiment_summary` for
the selected company and renders a 90-day sentiment timeline.

---

## Step 8: Compliance Analyst Sees the Alert

A compliance analyst at their workstation sees a new row appear in the
anomaly alert panel. The filing is from a mid-size pharmaceutical company.
The alert severity is high. The anomaly flags show earnings_restatement
and sentiment_drop. The sentiment delta is minus 0.47, meaning this filing
is significantly more negative than anything this company has filed in the
last 90 days.

The analyst clicks the filing URL in the dashboard. The original EDGAR
document opens. The analyst reads the specific sections flagged and decides
whether to escalate to the compliance team.

Total time from the company submitting the filing to the analyst seeing
the alert: under 60 seconds.

---

## Error Handling Summary

```
Step                    Failure Scenario                    Recovery
----                    ----------------                    --------
EDGAR API poll          API returns error or timeout        Retry with backoff, skip tick
Document fetch          Slow or failed response             Write null to Bronze, retry job
Kafka publish           Kafka unavailable                   In-memory queue, retry with backoff
Spark Bronze write      Job crash                           Resume from checkpoint on restart
XML parsing             Malformed document                  Write to quarantine, continue batch
FinBERT inference       Model error on a document           Log error, write with null scores
Gold write              Schema mismatch                     Fail loudly, alert on-call engineer
```

---

## Latency Budget

The 60-second end-to-end target is allocated across steps as follows:

```
Step                                Target Latency
----                                --------------
EDGAR API poll interval             0 to 30 seconds   (depends on when filing appears)
Kafka publish                       under 1 second
Spark Bronze micro-batch            30 seconds
Bronze to Silver processing         under 10 seconds per record
FinBERT inference                   under 15 seconds per batch
Gold write and dashboard refresh    under 5 seconds
Total worst case                    under 91 seconds
Total typical case                  under 60 seconds
```

The worst case slightly exceeds 60 seconds when a filing appears immediately
after a poll cycle completes. The 60-second target is the typical case and
the number used in all documentation and benchmarks.
