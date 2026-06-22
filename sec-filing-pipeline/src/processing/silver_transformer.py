import os
import re
import uuid
import yaml
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, udf, lit, current_timestamp, length, split, size,
    when, regexp_replace, trim, lower, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, TimestampType
)
from delta import configure_spark_with_delta_pip
from loguru import logger

# ─────────────────────────────────────────
# Load config
# ─────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

BRONZE_TABLE      = config["delta"]["bronze_table"]
SILVER_TABLE      = config["delta"]["silver_table"]
QUARANTINE_TABLE  = config["delta"]["quarantine_table"]

# ─────────────────────────────────────────
# XML Parser
# Extracts structured fields from raw EDGAR
# XML document stored in Bronze raw_payload
# ─────────────────────────────────────────

def parse_filing_document(raw_payload: str) -> dict:
    """
    Parse the raw XML/HTML document from EDGAR and extract
    the clean text content.

    EDGAR documents contain a mix of XML headers, HTML content,
    and plain text. We strip all tags and extract only the
    readable text for downstream NLP processing.

    Returns a dict with extracted fields.
    """
    if not raw_payload:
        return {
            "raw_text":           None,
            "word_count":         0,
            "has_risk_section":   False,
            "has_forward_looking": False,
            "parse_error":        "empty_payload"
        }

    try:
        # Remove XML/HTML tags
        clean_text = re.sub(r'<[^>]+>', ' ', raw_payload)

        # Remove excessive whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()

        # Remove special characters but keep punctuation
        clean_text = re.sub(r'[^\w\s\.,;:!?\-\(\)]', ' ', clean_text)

        # Word count
        word_count = len(clean_text.split()) if clean_text else 0

        # Check for risk factors section
        # SEC requires 10-K and 10-Q to have a risk factors section
        has_risk = bool(re.search(
            r'risk\s+factors|item\s+1a',
            clean_text.lower()
        ))

        # Check for forward-looking statements disclaimer
        # Companies include this when making future projections
        has_forward = bool(re.search(
            r'forward.looking\s+statements?|future\s+results|'
            r'may\s+differ\s+materially',
            clean_text.lower()
        ))

        return {
            "raw_text":            clean_text[:500000],  # cap at 500k chars
            "word_count":          word_count,
            "has_risk_section":    has_risk,
            "has_forward_looking": has_forward,
            "parse_error":         None
        }

    except Exception as e:
        return {
            "raw_text":            None,
            "word_count":          0,
            "has_risk_section":    False,
            "has_forward_looking": False,
            "parse_error":         str(e)
        }


def extract_raw_text(raw_payload):
    result = parse_filing_document(raw_payload)
    return result.get("raw_text")

def extract_word_count(raw_payload):
    result = parse_filing_document(raw_payload)
    return result.get("word_count", 0)

def extract_has_risk(raw_payload):
    result = parse_filing_document(raw_payload)
    return result.get("has_risk_section", False)

def extract_has_forward(raw_payload):
    result = parse_filing_document(raw_payload)
    return result.get("has_forward_looking", False)

def extract_parse_error(raw_payload):
    result = parse_filing_document(raw_payload)
    return result.get("parse_error")

def check_is_amended(filing_type):
    if not filing_type:
        return False
    return filing_type.endswith("/A")


# Register UDFs
extract_raw_text_udf    = udf(extract_raw_text,    StringType())
extract_word_count_udf  = udf(extract_word_count,  IntegerType())
extract_has_risk_udf    = udf(extract_has_risk,    BooleanType())
extract_has_forward_udf = udf(extract_has_forward, BooleanType())
extract_parse_error_udf = udf(extract_parse_error, StringType())
check_is_amended_udf    = udf(check_is_amended,    BooleanType())


# ─────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────

def create_spark_session() -> SparkSession:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 "
        "pyspark-shell"
    )

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
        "io.delta:delta-spark_2.13:4.2.0 pyspark-shell"
    )

    builder = (
        SparkSession.builder
        .appName(config["spark"]["app_name"] + "-silver")
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
# Validation
# Required fields for a valid Silver record
# ─────────────────────────────────────────

REQUIRED_FIELDS = ["ingestion_id", "company_name", "filing_type", "filed_date"]

def split_valid_quarantine(df: DataFrame):
    """
    Split the DataFrame into two:
    - valid_df: records that pass all required field checks
    - quarantine_df: records missing critical fields

    We never drop bad records. Every filing that enters Bronze
    must be accounted for — either in Silver or in quarantine.
    """
    # A record is valid if all required fields are non-null and non-empty
    # A record is valid if it has the core metadata fields
    # parse_error only fails validation if raw_payload was present but failed to parse
    # null raw_payload (fetch failed) is acceptable - record still goes to Silver
    validity_condition = (
        col("ingestion_id").isNotNull() &
        col("filing_type").isNotNull() & (trim(col("filing_type")) != "") &
        col("filed_date").isNotNull()
    )

    valid_df      = df.filter(validity_condition)
    quarantine_df = df.filter(~validity_condition)

    return valid_df, quarantine_df


# ─────────────────────────────────────────
# Main transformer
# ─────────────────────────────────────────

def run_silver_transformer():
    """
    Batch job that reads unprocessed Bronze records,
    parses and cleans each filing document, validates
    required fields, and writes clean records to Silver.

    Bad records go to the quarantine table with a full
    error description so they can be investigated and retried.

    This job is intentionally separate from the Bronze writer.
    Keeping ingestion and transformation decoupled means a
    parsing bug does not stop new data from being ingested.
    """
    logger.info("Starting Silver Transformer")
    logger.info(f"Bronze table     : {BRONZE_TABLE}")
    logger.info(f"Silver table     : {SILVER_TABLE}")
    logger.info(f"Quarantine table : {QUARANTINE_TABLE}")

    spark = create_spark_session()

    # Read all Bronze records
    # In production this would use a watermark or processed flag
    # to read only new records since last run
    try:
        bronze_path = str(Path(__file__).parent.parent / "streaming" / "bronze.sec_filings_raw")
        bronze_df = spark.read.format("delta").load(bronze_path)
        total_bronze = bronze_df.count()
        logger.info(f"Read {total_bronze} record(s) from Bronze")
    except Exception as e:
        logger.error(f"Could not read Bronze table: {e}")
        logger.info("Bronze table may be empty. Run edgar_producer.py first.")
        return

    if total_bronze == 0:
        logger.info("No records in Bronze. Nothing to process.")
        return

    # Step 1: Parse XML documents
    logger.info("Parsing filing documents...")
    parsed_df = bronze_df.withColumn(
        "raw_text",            extract_raw_text_udf(col("raw_payload"))
    ).withColumn(
        "word_count",          extract_word_count_udf(col("raw_payload"))
    ).withColumn(
        "has_risk_section",    extract_has_risk_udf(col("raw_payload"))
    ).withColumn(
        "has_forward_looking", extract_has_forward_udf(col("raw_payload"))
    ).withColumn(
        "parse_error",         extract_parse_error_udf(col("raw_payload"))
    ).withColumn(
        "is_amended",          check_is_amended_udf(col("filing_type"))
    )

    # Step 2: Add Silver metadata
    parsed_df = parsed_df.withColumn(
        "filing_id",            lit(None).cast(StringType())
    ).withColumn(
        "processed_timestamp",  current_timestamp()
    )

    # Generate filing_id as UUID for each row
    generate_uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
    parsed_df = parsed_df.withColumn("filing_id", generate_uuid_udf())

    # Step 3: Split valid and quarantine records
    valid_df, quarantine_df = split_valid_quarantine(parsed_df)

    valid_count      = valid_df.count()
    quarantine_count = quarantine_df.count()

    logger.info(f"Valid records    : {valid_count}")
    logger.info(f"Quarantine records: {quarantine_count}")

    # Step 4: Write valid records to Silver
    if valid_count > 0:
        silver_df = valid_df.select(
            "filing_id",
            "ingestion_id",
            "company_name",
            "ticker",
            "cik",
            "filing_type",
            "filed_date",
            "period_of_report",
            "raw_text",
            "word_count",
            "has_risk_section",
            "has_forward_looking",
            "is_amended",
            "processed_timestamp"
        )

        silver_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(str(Path(__file__).parent.parent / "streaming" / "silver.sec_filings_parsed"))

        logger.info(f"Wrote {valid_count} record(s) to Silver")

    # Step 5: Write quarantine records
    if quarantine_count > 0:
        quarantine_out_df = quarantine_df.select(
            "ingestion_id",
            current_timestamp().alias("error_timestamp"),
            lit("silver_transform").alias("error_type"),
            col("parse_error").alias("error_message"),
            "raw_payload",
            lit(1).alias("retry_count")
        )

        quarantine_out_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(str(Path(__file__).parent.parent / "streaming" / "silver.sec_filings_quarantine"))

        logger.warning(
            f"Sent {quarantine_count} record(s) to quarantine. "
            f"Check {QUARANTINE_TABLE} for details."
        )

    # Alert if quarantine rate is too high
    if total_bronze > 0:
        quarantine_rate = quarantine_count / total_bronze
        threshold = config["anomaly"]["quarantine_alert_threshold"]
        if quarantine_rate > threshold:
            logger.error(
                f"Quarantine rate {quarantine_rate:.1%} exceeds "
                f"threshold {threshold:.1%}. Investigate immediately."
            )

    logger.info("Silver transformer complete.")


if __name__ == "__main__":
    run_silver_transformer()