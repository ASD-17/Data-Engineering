# Runbook — SEC Filing Intelligence Pipeline

## What This Document Is

A runbook is the document an engineer opens at 2am when something breaks.
It answers three questions: what is broken, why it broke, and how to fix it.

This runbook covers how to start, stop, monitor, and troubleshoot every
component of the SEC Filing Intelligence Pipeline.

---

## Components Overview

```
Component               Technology              Where It Runs
---------               ----------              -------------
Kafka Broker            Apache Kafka            Docker (local) / Azure Event Hubs (prod)
EDGAR Producer          Python script           Azure VM / local machine
Bronze Writer           Spark Streaming job     Databricks cluster
Silver Transformer      Spark batch job         Databricks cluster
Gold Enricher           Spark batch job         Databricks cluster
Delta Lake Storage      Delta Lake              Azure Data Lake Storage Gen2
Dashboard               Databricks SQL          Databricks workspace
```

---

## Starting the Pipeline

Start components in this exact order. Starting them out of order will cause
connection errors.

### Step 1: Start Kafka

Local development:

```bash
cd infrastructure/kafka
docker-compose up -d
```

Verify Kafka is running:

```bash
docker-compose ps
# kafka and zookeeper containers should show status "Up"
```

Check the topic exists:

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
# sec-filings-raw should appear in the list
```

If the topic does not exist, create it:

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic sec-filings-raw \
  --partitions 3 \
  --replication-factor 1
```

### Step 2: Start the EDGAR Producer

```bash
cd src/ingestion
python edgar_producer.py
```

Expected output when running correctly:

```
2025-03-15 09:47:00 INFO  Producer started. Polling EDGAR every 30 seconds.
2025-03-15 09:47:00 INFO  Last check timestamp: 2025-03-15T09:46:30Z
2025-03-15 09:47:30 INFO  EDGAR API returned 3 new filings
2025-03-15 09:47:30 INFO  Published event: AAPL 8-K to sec-filings-raw partition 1
2025-03-15 09:47:30 INFO  Published event: MSFT 10-Q to sec-filings-raw partition 0
2025-03-15 09:47:30 INFO  Published event: JPM 8-K to sec-filings-raw partition 2
```

If you see no output after 60 seconds, check the EDGAR API directly:

```bash
curl "https://efts.sec.gov/LATEST/search-index?q=%22%22&forms=8-K" | python -m json.tool
```

### Step 3: Start Spark Streaming Jobs on Databricks

In the Databricks workspace:

1. Open the Workflows tab
2. Start the job named `sec-bronze-writer`
3. Wait for status to show Running (usually takes 2 to 3 minutes to start)
4. Start the job named `sec-silver-transformer`
5. Start the job named `sec-gold-enricher`

All three jobs should show status Running before proceeding.

### Step 4: Verify End-to-End Flow

Wait 5 minutes after starting all components, then run this verification
query in Databricks SQL:

```sql
SELECT
  COUNT(*) as bronze_count,
  MAX(ingestion_timestamp) as latest_ingestion
FROM bronze.sec_filings_raw
WHERE ingestion_timestamp >= current_timestamp() - interval 10 minutes
```

If bronze_count is 0 after 10 minutes, something is wrong. See the
troubleshooting section.

---

## Stopping the Pipeline

Stop in reverse order to avoid data loss.

### Step 1: Stop Spark Jobs

In Databricks Workflows, stop jobs in this order:
1. Stop `sec-gold-enricher`
2. Stop `sec-silver-transformer`
3. Stop `sec-bronze-writer`

Wait for each job to show status Stopped before stopping the next one.

### Step 2: Stop the EDGAR Producer

Press Ctrl+C in the terminal running the producer. The producer handles
the interrupt gracefully and logs a clean shutdown message.

### Step 3: Stop Kafka

```bash
cd infrastructure/kafka
docker-compose down
```

Do not use docker-compose down -v unless you want to delete all Kafka data.
The -v flag removes volumes which means all unprocessed events in the topic
are lost permanently.

---

## Monitoring

### What to Check Daily

Run this query every morning to confirm the pipeline processed filings
overnight:

```sql
SELECT
  filing_type,
  COUNT(*) as total_filings,
  SUM(CASE WHEN alert_triggered THEN 1 ELSE 0 END) as alerts_triggered,
  AVG(sentiment_score) as avg_sentiment,
  MAX(enrichment_timestamp) as latest_enrichment
FROM gold.sec_filings_enriched
WHERE filed_date >= current_date() - interval 1 day
GROUP BY filing_type
ORDER BY filing_type
```

Expected results on a normal trading day:

```
filing_type    total_filings    alerts_triggered    avg_sentiment
10-K           5 to 20          0 to 3              0.45 to 0.65
10-Q           10 to 40         0 to 5              0.45 to 0.65
8-K            50 to 150        5 to 20             0.35 to 0.55
```

If total_filings is 0 for any filing type on a trading day, the pipeline
is not running correctly.

### What to Check if Something Looks Wrong

Check the quarantine table for parsing failures:

```sql
SELECT
  error_type,
  COUNT(*) as error_count,
  MAX(error_timestamp) as latest_error
FROM silver.sec_filings_quarantine
WHERE error_timestamp >= current_date() - interval 1 day
GROUP BY error_type
ORDER BY error_count DESC
```

A quarantine rate above 5% of total Bronze volume is a warning sign.

Check the Kafka consumer lag to see if Spark is keeping up with ingestion:

```bash
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group sec-bronze-writer
```

The LAG column should be under 10. If it is growing, Spark is falling behind.

---

## Troubleshooting

### Problem: EDGAR Producer is running but no events in Kafka

Possible causes and checks:

Check if EDGAR API is returning results:

```bash
curl "https://efts.sec.gov/LATEST/search-index?q=%22%22&forms=8-K" | python -m json.tool
```

If the API returns an error or empty results, EDGAR may be experiencing
downtime. This happens occasionally, usually resolved within 30 minutes.
Check SEC system status at https://www.sec.gov/cgi-bin/browse-edgar.

Check the last_check_timestamp file:

```bash
cat src/ingestion/state/last_check_timestamp.txt
```

If this timestamp is in the future or very old, delete the file and restart
the producer. It will reset to 1 hour ago and reprocess recent filings.

### Problem: Bronze table is empty or not updating

Check if the Spark bronze-writer job is actually running in Databricks.
A job can show Running status but be stuck on initialization.

Check the Spark job logs in Databricks for this error:

```
Could not find checkpoint. Starting from beginning of Kafka topic.
```

This is not an error. It means the job is starting fresh. Wait two
micro-batch cycles (60 seconds) and check Bronze again.

Check Kafka has messages waiting:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic sec-filings-raw \
  --from-beginning \
  --max-messages 5
```

If this prints events, Kafka is fine and the issue is with Spark reading
from it. Check the Databricks cluster has the correct Kafka bootstrap
server address in the job configuration.

### Problem: Silver records are not appearing after Bronze records exist

The Silver transformer runs every 5 minutes. Wait one full cycle before
investigating.

If Silver is still empty after 10 minutes:

Check the quarantine table. If all records are going to quarantine, there
is a parsing bug. Look at the error_message column to identify the cause.

```sql
SELECT error_type, error_message, raw_payload
FROM silver.sec_filings_quarantine
ORDER BY error_timestamp DESC
LIMIT 5
```

The most common cause is an EDGAR API response format change. The XML
structure occasionally changes slightly. The raw_payload in the quarantine
record shows exactly what the parser received.

### Problem: Gold records have null sentiment scores

FinBERT failed to score those documents. Check the Gold enricher logs
in Databricks for:

```
WARN  FinBERT inference failed for filing_id xyz: CUDA out of memory
```

If this appears, the Databricks cluster running the enricher does not have
enough GPU memory. Switch to CPU inference by setting USE_GPU=false in the
job environment variables. It will be slower but will not fail.

If the logs show:

```
ERROR  HuggingFace model not found: ProsusAI/finbert
```

The model needs to be downloaded. Run this in a Databricks notebook:

```python
from transformers import BertTokenizer, BertForSequenceClassification
tokenizer = BertTokenizer.from_pretrained("ProsusAI/finbert")
model = BertForSequenceClassification.from_pretrained("ProsusAI/finbert")
```

### Problem: Dashboard shows no data

First confirm the Gold table has recent data:

```sql
SELECT MAX(enrichment_timestamp) as latest
FROM gold.sec_filings_enriched
```

If this returns a timestamp from more than 30 minutes ago, the pipeline
upstream is the issue, not the dashboard.

If the Gold table is current but the dashboard shows nothing, the dashboard
queries may have a permissions issue. In Databricks SQL, verify the dashboard
service principal has SELECT permission on the gold schema:

```sql
GRANT SELECT ON SCHEMA gold TO `dashboard-service-principal`
```

---

## Common Kafka Commands Reference

Check all topics:

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Check messages in a topic:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic sec-filings-raw \
  --from-beginning \
  --max-messages 10
```

Check consumer group lag:

```bash
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group sec-bronze-writer
```

Delete and recreate a topic (use only in development):

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic sec-filings-raw

docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic sec-filings-raw \
  --partitions 3 --replication-factor 1
```

---

## Reprocessing Historical Filings

If the Silver or Gold layer needs to be rebuilt, do not touch Bronze.
Bronze is the source of truth.

To reprocess everything from Bronze:

```bash
# Step 1: Clear Silver and Gold tables
# Run in Databricks SQL
TRUNCATE TABLE silver.sec_filings_parsed;
TRUNCATE TABLE gold.sec_filings_enriched;
TRUNCATE TABLE gold.company_sentiment_summary;

# Step 2: Run the Silver transformer in backfill mode
cd src/processing
python silver_transformer.py --mode backfill --source bronze

# Step 3: Run the Gold enricher in backfill mode
cd src/enrichment
python gold_enricher.py --mode backfill --source silver
```

Backfill mode processes all records rather than only new ones. Depending
on how much data is in Bronze, this can take several hours.

---

## Environment Variables Reference

```
Variable                    Description                         Required
--------                    -----------                         --------
KAFKA_BOOTSTRAP_SERVERS     Kafka broker address                Yes
KAFKA_TOPIC                 Topic name, default sec-filings-raw Yes
EDGAR_POLL_INTERVAL         Seconds between API polls, default 30  No
AZURE_STORAGE_ACCOUNT       Azure storage account name          Yes
AZURE_STORAGE_KEY           Azure storage account key           Yes
DELTA_LAKE_PATH             Base path for Delta tables          Yes
USE_GPU                     Enable GPU for FinBERT, default true   No
FINBERT_BATCH_SIZE          Documents per inference batch, default 16  No
QUARANTINE_ALERT_THRESHOLD  Max quarantine rate before alert, default 0.05  No
```

All environment variables should be stored in a .env file locally and
in Databricks job environment variables in production. Never hardcode
credentials in source code.
