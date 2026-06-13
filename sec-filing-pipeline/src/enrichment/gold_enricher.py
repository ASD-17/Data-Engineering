import os
import uuid
import yaml
import requests
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, udf, when, avg,
    window, desc, count, concat
)
from pyspark.sql.types import (
    StringType, FloatType, BooleanType,
    ArrayType, IntegerType
)
from delta import configure_spark_with_delta_pip
from loguru import logger

# ─────────────────────────────────────────
# Load config
# ─────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SILVER_PATH     = str(Path(__file__).parent.parent / "streaming" / "silver.sec_filings_parsed")
GOLD_PATH       = str(Path(__file__).parent.parent / "streaming" / "gold.sec_filings_enriched")
SUMMARY_PATH    = str(Path(__file__).parent.parent / "streaming" / "gold.company_sentiment_summary")

HF_TOKEN        = os.getenv("HUGGINGFACE_TOKEN", "")
HF_API_URL      = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"
HF_HEADERS      = {"Authorization": f"Bearer {HF_TOKEN}"}

# ─────────────────────────────────────────
# FinBERT via HuggingFace Inference API
# ─────────────────────────────────────────

def score_sentiment_api(text: str) -> tuple:
    """
    Call HuggingFace Inference API to run FinBERT on the filing text.

    FinBERT is a BERT model trained specifically on financial text.
    It returns three scores — positive, negative, neutral — that sum to 1.
    We take the label with the highest score as the sentiment.

    Returns (label, score) where label is positive/negative/neutral
    and score is the confidence between 0 and 1.

    Falls back to neutral if the API call fails.
    """
    if not text or len(text.strip()) == 0:
        return ("neutral", 0.5)

    # FinBERT has a 512 token limit
    # We take the first 1000 characters which covers the key sections
    truncated = text[:1000].strip()

    try:
        response = requests.post(
            HF_API_URL,
            headers=HF_HEADERS,
            json={"inputs": truncated},
            timeout=30
        )

        if response.status_code == 503:
            # Model is loading — wait and return neutral for now
            logger.warning("FinBERT model loading. Returning neutral.")
            return ("neutral", 0.5)

        if response.status_code != 200:
            logger.warning(f"FinBERT API returned {response.status_code}. Using neutral.")
            return ("neutral", 0.5)

        results = response.json()

        # API returns list of list of dicts
        if isinstance(results, list) and len(results) > 0:
            scores = results[0] if isinstance(results[0], list) else results
            best = max(scores, key=lambda x: x["score"])
            label = best["label"].lower()
            score = round(best["score"], 4)
            return (label, score)

        return ("neutral", 0.5)

    except Exception as e:
        logger.warning(f"FinBERT API call failed: {e}. Using neutral.")
        return ("neutral", 0.5)


def get_sentiment_label(text):
    label, _ = score_sentiment_api(text)
    return label

def get_sentiment_score(text):
    _, score = score_sentiment_api(text)
    return score


# Register UDFs
sentiment_label_udf = udf(get_sentiment_label, StringType())
sentiment_score_udf = udf(get_sentiment_score, FloatType())


# ─────────────────────────────────────────
# Anomaly Detection
# Rule-based flags applied to each filing
# ─────────────────────────────────────────

ANOMALY_PATTERNS = {
    "going_concern": [
        "going concern", "substantial doubt", "ability to continue",
        "doubt about ability"
    ],
    "earnings_restatement": [
        "restate", "restatement", "material weakness",
        "accounting error", "prior period"
    ],
    "sudden_ceo_change": [
        "chief executive officer resigned", "ceo resigned",
        "president resigned", "chief financial officer resigned",
        "cfo resigned", "departure of"
    ],
    "legal_proceedings": [
        "securities class action", "derivative lawsuit",
        "regulatory investigation", "SEC investigation",
        "DOJ investigation", "criminal charges"
    ],
    "options_volume_spike": [
        "unusual trading", "unusual volume", "trading activity"
    ]
}


def detect_anomalies(text: str, filing_type: str) -> list:
    """
    Check the filing text for known red flag patterns.

    Each pattern category represents a different type of risk signal.
    Returns a list of flag names that were triggered.

    Rule-based detection is used here because it is explainable.
    If a compliance analyst asks why a filing was flagged,
    the answer is specific and auditable.
    """
    if not text:
        return []

    text_lower = text.lower()
    triggered = []

    for flag_name, patterns in ANOMALY_PATTERNS.items():
        for pattern in patterns:
            if pattern.lower() in text_lower:
                triggered.append(flag_name)
                break

    return triggered


def get_anomaly_flags(text):
    flags = detect_anomalies(text, "")
    return flags

def get_anomaly_count(text):
    return len(detect_anomalies(text, ""))


anomaly_flags_udf = udf(get_anomaly_flags, ArrayType(StringType()))
anomaly_count_udf = udf(get_anomaly_count, IntegerType())


# ─────────────────────────────────────────
# Alert severity
# ─────────────────────────────────────────

def assign_severity(anomaly_count: int, sentiment_label: str,
                    has_going_concern: bool) -> str:
    """
    Assign an alert severity level based on the combination of
    anomaly flags and sentiment score.

    critical  3+ anomaly flags or going concern language present
    high      2 anomaly flags or very negative sentiment
    medium    1 anomaly flag or negative sentiment
    low       alert triggered but no major flags
    none      no flags triggered
    """
    if has_going_concern or anomaly_count >= 3:
        return "critical"
    if anomaly_count >= 2 or sentiment_label == "negative":
        return "high"
    if anomaly_count >= 1:
        return "medium"
    return "none"


def get_alert_severity(anomaly_count, sentiment_label, flags):
    has_going_concern = False
    if flags:
        has_going_concern = "going_concern" in flags
    return assign_severity(anomaly_count or 0, sentiment_label or "neutral",
                           has_going_concern)

def get_alert_triggered(anomaly_count, sentiment_label):
    if anomaly_count and anomaly_count > 0:
        return True
    if sentiment_label == "negative":
        return True
    return False


severity_udf      = udf(get_alert_severity, StringType())
alert_triggered_udf = udf(get_alert_triggered, BooleanType())


# ─────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────

def create_spark_session() -> SparkSession:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
        "io.delta:delta-spark_2.13:4.2.0 pyspark-shell"
    )
    builder = (
        SparkSession.builder
        .appName(config["spark"]["app_name"] + "-gold")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark


# ─────────────────────────────────────────
# Main enricher
# ─────────────────────────────────────────

def run_gold_enricher():
    """
    Batch job that reads Silver records, runs FinBERT sentiment
    scoring and anomaly detection, assigns alert severity,
    and writes results to the Gold layer.

    Gold is the final business-ready layer. Compliance analysts
    and researchers query this layer directly via the dashboard.
    """
    logger.info("Starting Gold Enricher")
    logger.info(f"Silver path  : {SILVER_PATH}")
    logger.info(f"Gold path    : {GOLD_PATH}")
    logger.info(f"HF Token set : {'yes' if HF_TOKEN else 'NO - set HUGGINGFACE_TOKEN'}")

    spark = create_spark_session()

    # Read Silver records
    try:
        silver_df = spark.read.format("delta").load(SILVER_PATH)
        total = silver_df.count()
        logger.info(f"Read {total} record(s) from Silver")
    except Exception as e:
        logger.error(f"Could not read Silver table: {e}")
        return

    if total == 0:
        logger.info("No records in Silver. Run silver_transformer.py first.")
        return

    # Step 1-4: Run enrichment on driver (not Spark workers)
    # HuggingFace API calls must run on the driver where network is available
    # Collect Silver records to driver, enrich, then create enriched DataFrame
    logger.info("Running FinBERT sentiment scoring via HuggingFace API...")
    logger.info("Running anomaly detection...")

    silver_rows = silver_df.collect()
    enriched_rows = []

    for row in silver_rows:
        text = row.raw_text or ""
        label, score = score_sentiment_api(text)
        flags = detect_anomalies(text, row.filing_type or "")
        anomaly_count = len(flags)
        alert_triggered = get_alert_triggered(anomaly_count, label)
        severity = get_alert_severity(anomaly_count, label, flags)

        if score >= 0.8:
            category = f"strong_{label}"
        elif score >= 0.6:
            category = f"mild_{label}"
        else:
            category = "neutral"

        enriched_rows.append({
            "filing_id":            row.filing_id,
            "ingestion_id":         row.ingestion_id,
            "company_name":         row.company_name,
            "ticker":               row.ticker,
            "cik":                  row.cik,
            "filing_type":          row.filing_type,
            "filed_date":           row.filed_date,
            "period_of_report":     row.period_of_report,
            "word_count":           row.word_count,
            "sentiment_label":      label,
            "sentiment_score":      float(score),
            "sentiment_category":   category,
            "anomaly_flags":        flags,
            "anomaly_count":        anomaly_count,
            "alert_triggered":      alert_triggered,
            "alert_severity":       severity,
            "enrichment_timestamp": datetime.now(timezone.utc).isoformat(),
        })

    gold_df = spark.createDataFrame(enriched_rows)

    # Select Gold schema columns
    gold_output_df = gold_df.select(
        "filing_id",
        "ingestion_id",
        "company_name",
        "ticker",
        "cik",
        "filing_type",
        "filed_date",
        "period_of_report",
        "word_count",
        "sentiment_label",
        "sentiment_score",
        "sentiment_category",
        "anomaly_flags",
        "anomaly_count",
        "alert_triggered",
        "alert_severity",
        "enrichment_timestamp"
    )

    # Step 5: Write to Gold
    gold_output_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(GOLD_PATH)

    alert_count = gold_output_df.filter(col("alert_triggered")).count()
    critical_count = gold_output_df.filter(col("alert_severity") == "critical").count()

    logger.info(f"Wrote {total} record(s) to Gold")
    logger.info(f"Alerts triggered : {alert_count}")
    logger.info(f"Critical alerts  : {critical_count}")

    # Step 6: Company sentiment summary
    logger.info("Computing company sentiment summary...")
    summary_df = gold_output_df.groupBy("cik", "company_name", "ticker") \
        .agg(
            count("filing_id").alias("total_filings"),
            avg("sentiment_score").alias("avg_sentiment"),
            count(when(col("alert_triggered"), True)).alias("total_alerts"),
            count(when(col("alert_severity") == "critical", True))
                .alias("critical_alerts")
        ).withColumn(
            "summary_timestamp", current_timestamp()
        )

    summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(SUMMARY_PATH)

    logger.info("Gold enricher complete.")

    # Print sample results
    logger.info("Sample Gold records:")
    gold_output_df.select(
        "filing_type", "filed_date", "sentiment_label",
        "sentiment_score", "anomaly_count", "alert_severity"
    ).show(10, truncate=False)


if __name__ == "__main__":
    run_gold_enricher()