import json
import time
import uuid
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import yaml
from confluent_kafka import Producer
from dotenv import load_dotenv
from loguru import logger

# ─────────────────────────────────────────
# Load environment and config
# ─────────────────────────────────────────

load_dotenv()

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    config["kafka"]["bootstrap_servers"]
)
KAFKA_TOPIC             = config["kafka"]["topic"]
EDGAR_BASE_URL          = config["edgar"]["base_url"]
POLL_INTERVAL           = config["edgar"]["poll_interval_seconds"]
FILING_TYPES            = config["edgar"]["filing_types"]
STATE_FILE              = Path(__file__).parent / "state" / "last_check_timestamp.txt"
USER_AGENT              = "AgasyaDevarasetty asrkgd@gmail.com"

# ─────────────────────────────────────────
# Kafka delivery callback
# ─────────────────────────────────────────

def delivery_report(err, msg):
    """Called by Kafka once per message to confirm delivery or report failure."""
    if err:
        logger.error(f"Delivery failed for topic {msg.topic()}: {err}")
    else:
        logger.info(
            f"Delivered to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

# ─────────────────────────────────────────
# State management
# Saves and loads the last poll timestamp
# so we never re-process already-seen filings
# ─────────────────────────────────────────

def load_last_check_timestamp() -> str:
    """
    Read the last successful poll timestamp from disk.
    If no state file exists (first run), default to 1 hour ago
    so we pick up any recent filings immediately.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    if STATE_FILE.exists():
        ts = STATE_FILE.read_text().strip()
        logger.info(f"Resuming from last check timestamp: {ts}")
        return ts

    default_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    logger.info(f"No state file found. Starting from: {default_ts}")
    return default_ts


def save_last_check_timestamp(ts: str):
    """Persist the current poll timestamp to disk."""
    STATE_FILE.write_text(ts)


# ─────────────────────────────────────────
# EDGAR API
# ─────────────────────────────────────────

def fetch_new_filings(since_timestamp: str) -> list[dict]:
    """
    Call the SEC EDGAR Full Text Search API and return
    all filings of the configured types published since
    the given timestamp.

    Returns a list of raw filing dicts from the API response.
    Returns an empty list if the API call fails.
    """
    forms_param = ",".join(FILING_TYPES)
    params = {
        "q":         "annual report",
        "dateRange": "custom",
        "startdt":   since_timestamp[:10],
        "enddt":     datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "forms":     forms_param,
    }

    try:
        headers = {"User-Agent": "AgasyaDevarasetty asrkgd@gmail.com"}
        response = requests.get(EDGAR_BASE_URL, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])
        logger.info(f"EDGAR API returned {len(hits)} new filing(s)")
        return hits

    except requests.exceptions.Timeout:
        logger.warning("EDGAR API request timed out. Will retry next poll cycle.")
        return []

    except requests.exceptions.RequestException as e:
        logger.error(f"EDGAR API request failed: {e}")
        return []


# ─────────────────────────────────────────
# Event builder
# Converts a raw EDGAR API hit into a
# clean Kafka event payload
# ─────────────────────────────────────────

def get_filing_details(cik_numeric: str, accession_clean: str) -> dict:
    """
    Fetch company name and primary document filename from the EDGAR
    submissions API. This is the authoritative source for both fields.

    The submissions API URL format:
    https://data.sec.gov/submissions/CIK0000320193.json

    CIK must be zero-padded to 10 digits.
    We look through recent filings to find the matching accession number
    and return the primary document filename and company name.
    """
    try:
        cik_padded = cik_numeric.zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        headers = {"User-Agent": USER_AGENT}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        company_name = data.get("name", "")
        ticker = ""
        tickers = data.get("tickers", [])
        if tickers:
            ticker = tickers[0]

        # Find primary document for this accession number
        recent = data.get("filings", {}).get("recent", {})
        accession_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        primary_doc = ""
        for i, acc in enumerate(accession_numbers):
            acc_clean = acc.replace("-", "")
            if acc_clean == accession_clean:
                if i < len(primary_docs):
                    primary_doc = primary_docs[i]
                break

        return {
            "company_name": company_name,
            "ticker": ticker,
            "primary_doc": primary_doc
        }
    except Exception as e:
        logger.warning(f"Could not fetch submissions for CIK {cik_numeric}: {e}")
        return {}


def build_kafka_event(hit: dict) -> dict:
    """
    Extract the fields we need from one EDGAR API result
    and build a structured Kafka event.

    Uses the EDGAR submissions API to get the authoritative
    company name, ticker, and primary document filename.
    The ingestion_id is a UUID generated here so every event
    has a unique, traceable identifier from the moment it enters
    the pipeline.
    """
    source = hit.get("_source", {})
    file_id = hit.get("_id", "")

    # Filing type from root_forms list
    root_forms = source.get("root_forms", [])
    filing_type = root_forms[0] if root_forms else source.get("form_type", "")

    # Filed date
    filed_date = source.get("file_date", "")

    # Extract accession number and CIK from _id field
    # _id looks like "0001651308-26-000017:bgne-20260611.htm"
    filing_url = ""
    cik_numeric = ""
    company_name = ""
    ticker = ""

    if file_id and ":" in file_id:
        accession = file_id.split(":")[0]
        accession_clean = accession.replace("-", "")
        # First 10 digits of accession number = filer CIK
        cik_numeric = str(int(accession_clean[:10])) if accession_clean else ""

        # Get company name, ticker, and primary doc from submissions API
        # Small delay to respect SEC rate limits (10 requests/second max)
        time.sleep(0.15)
        details = get_filing_details(cik_numeric, accession_clean)
        company_name = details.get("company_name", "")
        ticker = details.get("ticker", "")
        primary_doc = details.get("primary_doc", "")

        # Build URL using primary document filename from submissions API
        if primary_doc and cik_numeric:
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_numeric}/{accession_clean}/{primary_doc}"
        elif cik_numeric:
            # Fallback to index page which always exists
            filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_numeric}&type={filing_type}&dateb=&owner=include&count=1"

    return {
        "ingestion_id":        str(uuid.uuid4()),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "company_name":        company_name,
        "ticker":              ticker,
        "cik":                 cik_numeric,
        "filing_type":         filing_type,
        "filed_date":          filed_date,
        "period_of_report":    source.get("period_of_report", ""),
        "filing_url":          filing_url,
        "source":              "edgar_api",
    }


# ─────────────────────────────────────────
# Main producer loop
# ─────────────────────────────────────────

def run_producer():
    """
    Main loop. Connects to Kafka, then continuously polls
    EDGAR for new filings and publishes each one as a Kafka event.

    Runs forever until interrupted with Ctrl+C.
    """
    logger.info("Starting SEC EDGAR Kafka Producer")
    logger.info(f"Kafka broker   : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka topic    : {KAFKA_TOPIC}")
    logger.info(f"Filing types   : {FILING_TYPES}")
    logger.info(f"Poll interval  : {POLL_INTERVAL}s")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    last_check_ts = load_last_check_timestamp()

    try:
        while True:
            current_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            logger.info(f"Polling EDGAR for filings since {last_check_ts}")

            filings = fetch_new_filings(last_check_ts)

            for hit in filings:
                event     = build_kafka_event(hit)
                filing_id = event["ingestion_id"]
                ticker    = event["ticker"] or "UNKNOWN"
                f_type    = event["filing_type"]

                producer.produce(
                    topic     = KAFKA_TOPIC,
                    key       = ticker.encode("utf-8"),
                    value     = json.dumps(event).encode("utf-8"),
                    callback  = delivery_report,
                )

                logger.info(f"Published event: {ticker} {f_type} | id={filing_id}")

            # Flush ensures all buffered messages are sent before sleeping
            producer.flush()

            # Only advance the timestamp if we got a successful API response
            if filings is not None:
                save_last_check_timestamp(current_ts)
                last_check_ts = current_ts

            logger.info(f"Sleeping {POLL_INTERVAL}s until next poll")
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
        producer.flush()


if __name__ == "__main__":
    run_producer()