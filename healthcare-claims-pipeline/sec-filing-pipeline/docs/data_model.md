# Data Model — SEC Filing Intelligence Pipeline

## Overview

This pipeline uses a medallion architecture with three Delta Lake layers.
Each layer has a specific job and a specific audience.

Bronze is for engineers. It is the raw, unmodified record of everything
ingested. It never changes after it is written.

Silver is for processing. It is the cleaned and structured version that
enrichment jobs read from and write to.

Gold is for business users. It is the final output that the Databricks
dashboard reads. Compliance analysts and researchers never touch Bronze
or Silver directly.

---

## Bronze Layer — Raw Ingestion

Table name: `bronze.sec_filings_raw`

This table is append-only. No updates. No deletes. Every record written
here stays here permanently. It is the source of truth for everything
downstream. If Silver or Gold ever needs to be rebuilt from scratch,
Bronze is where it starts.

```
Column                  Type            Description
----------------------  --------------  ------------------------------------
ingestion_id            string          UUID generated at ingestion time
ingestion_timestamp     timestamp       When this record was written to Bronze
kafka_offset            long            Kafka partition offset for this event
kafka_partition         integer         Kafka partition number
filing_url              string          Full URL to the EDGAR filing document
raw_payload             string          Complete raw XML document as received
filing_type             string          10-K, 10-Q, 8-K (from Kafka event)
source                  string          Always "edgar_api" in V1
batch_id                string          Groups all filings from one Spark micro-batch
```

Why kafka_offset and kafka_partition are stored here: if there is ever a
question about whether a filing was ingested or processed in the right order,
these two columns let engineers trace exactly where in the Kafka stream each
record came from. Without them, debugging ingestion gaps is guesswork.

Why raw_payload stores the full XML: downstream parsing logic will change
over time. Storing the raw document means historical filings can always be
re-parsed with updated logic without going back to the EDGAR API.

Partition strategy: partitioned by filing_type and date(ingestion_timestamp).
This means querying all 10-K filings from a specific month reads only the
relevant partition, not the entire table.

---

## Silver Layer — Cleaned and Structured

Table name: `silver.sec_filings_parsed`

This is where raw XML becomes structured data. Bad records that cannot be
parsed are written to a separate quarantine table instead of failing the
entire batch.

```
Column                  Type            Description
----------------------  --------------  ------------------------------------
filing_id               string          UUID, primary key for this filing
ingestion_id            string          Foreign key back to Bronze record
company_name            string          Full legal company name
ticker                  string          Stock ticker symbol
cik                     string          SEC Central Index Key, unique per company
filing_type             string          10-K, 10-Q, 8-K
filed_date              timestamp       Date company submitted the filing
period_of_report        date            Period the filing covers
fiscal_year_end         string          Company fiscal year end month
raw_text                string          Full document text, HTML tags removed
word_count              integer         Total words in raw_text
has_risk_section        boolean         Whether filing contains a risk factors section
has_forward_looking     boolean         Whether filing contains forward-looking statements
processed_timestamp     timestamp       When this record was written to Silver
is_amended              boolean         Whether this is an amended filing (10-K/A, etc)
```

Why cik is stored separately from ticker: some companies change their ticker
symbol over time. The CIK is the permanent, unique identifier assigned by the
SEC that never changes. Joining on CIK rather than ticker avoids data
mismatches when companies rebrand or merge.

Why has_risk_section and has_forward_looking are boolean flags: these are
fast filter columns. The anomaly detection layer queries them before running
FinBERT. A filing with no risk section and no forward-looking statements is
much less likely to contain meaningful sentiment signal. Flagging them early
saves compute.

Error quarantine table: `silver.sec_filings_quarantine`

```
Column                  Type            Description
----------------------  --------------  ------------------------------------
ingestion_id            string          Points back to the Bronze record
error_timestamp         timestamp       When the parse failure occurred
error_type              string          xml_parse_error, missing_field, etc
error_message           string          Full exception message
raw_payload             string          Copy of the raw document that failed
retry_count             integer         How many times this record has been retried
```

Having a quarantine table rather than just dropping bad records is important
in a financial data context. Every filing that comes in should be accounted
for. If ten filings failed to parse in a day, a compliance team needs to know
which ten and why.

Partition strategy: partitioned by filing_type and date(filed_date).

---

## Gold Layer — Business Ready

Table name: `gold.sec_filings_enriched`

This is the final output. Every record here has been through FinBERT sentiment
scoring and anomaly detection. This is what the dashboard reads.

```
Column                      Type            Description
--------------------------  --------------  ------------------------------------
filing_id                   string          Foreign key to Silver
company_name                string
ticker                      string
cik                         string
filing_type                 string
filed_date                  timestamp
period_of_report            date
sentiment_label             string          positive, negative, neutral
sentiment_score             float           FinBERT confidence score 0.0 to 1.0
sentiment_category          string          strong_positive, mild_positive, neutral,
                                            mild_negative, strong_negative
word_count                  integer
anomaly_flags               array<string>   List of triggered anomaly flag names
anomaly_count               integer         Number of flags triggered
company_avg_sentiment       float           Rolling 90-day average sentiment score
sentiment_delta             float           Difference from company average
alert_triggered             boolean         True if any anomaly flag was triggered
alert_severity              string          low, medium, high, critical
enrichment_timestamp        timestamp       When enrichment completed
dashboard_visible           boolean         False for records under manual review
```

Why sentiment_category exists alongside sentiment_score: a score of 0.51
positive and a score of 0.95 positive are both labeled "positive" but mean
very different things. The category column bins the score into five meaningful
groups so dashboard filters work naturally without requiring analysts to think
in decimal ranges.

Why company_avg_sentiment and sentiment_delta are stored here: a single
negative filing from a company that always files negatively is not interesting.
A filing that is dramatically more negative than that company's own historical
average is very interesting. Storing the delta at enrichment time means the
dashboard query is a simple filter, not a window function computed at read time.

Why alert_severity has four levels: not all anomalies are equal. A single
risk keyword appearing in a 10-K is low severity. An 8-K with a going concern
flag and a 60% sentiment drop from the company average on the same day as an
options volume spike is critical. The severity level tells the compliance
analyst where to look first.

Aggregate table: `gold.company_sentiment_summary`

This table is recomputed daily. It gives a company-level view of sentiment
trends over time. The dashboard uses this for the trend charts.

```
Column                      Type            Description
--------------------------  --------------  ------------------------------------
cik                         string
company_name                string
ticker                      string
summary_date                date
filings_count_30d           integer         Filings processed in last 30 days
avg_sentiment_30d           float           Average sentiment score last 30 days
avg_sentiment_90d           float           Average sentiment score last 90 days
sentiment_trend             string          improving, stable, deteriorating
total_alerts_30d            integer         Alerts triggered in last 30 days
last_filing_date            timestamp
last_filing_type            string
last_sentiment_label        string
high_risk_flag              boolean         True if deteriorating trend + alerts > 3
```

Partition strategy: partitioned by date(filed_date) and filing_type.

---

## Data Lineage

Every record in Gold can be traced back to its Silver record and from there
back to its Bronze record using filing_id and ingestion_id. This is the
audit trail.

```
gold.sec_filings_enriched.filing_id
        |
        v
silver.sec_filings_parsed.filing_id
        |
        v (via ingestion_id)
bronze.sec_filings_raw.ingestion_id
        |
        v
Kafka offset + partition
        |
        v
Original EDGAR filing URL
```

If a compliance team ever asks "show me exactly what was in the original
filing that triggered this alert", that question can be answered in three
table joins.

---

## Schema Evolution

Delta Lake supports schema evolution. When new fields are added to any layer,
existing records are not broken. New columns are added with null values for
historical records.

The rule for this pipeline is that Bronze schema never changes. If the EDGAR
API changes its response format, a new column is added to Bronze and the old
column is kept. Silver and Gold schemas can evolve as new enrichment fields
are added.

---

## Data Retention

```
Layer       Retention       Reason
-------     ---------       -------
Bronze      Indefinite      Permanent audit record, source of truth
Silver      2 years         Operational processing layer
Gold        2 years         Dashboard and analytics layer
Quarantine  90 days         Investigation and retry window
```
