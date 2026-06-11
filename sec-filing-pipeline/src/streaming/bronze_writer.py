import os
import uuid
import requests
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)
from delta import configure_spark_with_delta_pip
from loguru import logger

# ─────────────────────────────────────────
# Load config
# ─────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    config["kafka"]["bootstrap_servers"]
)
KAFKA_TOPIC         = config["kafka"]["topic"]
TRIGGER_INTERVAL    = config["spark"]["trigger_interval"]
CHECKPOINT_PATH     = config["spark"]["bronze_checkpoint"]
BRONZE_TABLE        = config["delta"]["bronze_table"]

# ─────────────────────────────────────────
# Kafka event schema
# This is the structure of every JSON event
# that edgar_producer.py publishes to Kafka
# ─────────────────────────────────────────

KAFKA_EVENT_SCHEMA = StructType([
    StructField("ingestion_id",        StringType(),  True),
    StructField("ingestion_timestamp", StringType(),  True),
    StructField("company_name",        StringType(),  True),
    StructField("ticker",              StringType(),  True),
    StructField("cik",                 StringType(),  True),
    StructField("filing_type",         StringType(),  True),
    StructField("filed_date",          StringType(),  True),
    StructField("period_of_report",    StringType(),  True),
    StructField("filing_url",          StringType(),  True),
    StructField("source",              StringType(),  True),
])

# ─────────────────────────────────────────
# Document fetcher
# Downloads the raw XML filing from EDGAR
# ─────────────────────────────────────────

def fetch_raw_document(filing_url):
    """
    Fetch the full XML document from the EDGAR filing URL.
    Returns the raw text content or None if the fetch fails.
    """
    if not filing_url:
        return None

    headers = {"User-Agent": "AgasyaDevarasetty asrkgd@gmail.com"}

    try:
        response = requests.get(filing_url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text

    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch document from {filing_url}: {e}")
        return None


fetch_document_udf = udf(fetch_raw_document, StringType())

# ─────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Create a Spark session with Delta Lake and Kafka support.
    Kafka connector is passed via PYSPARK_SUBMIT_ARGS so it is
    available before the session starts — this is required for
    streaming sources to be recognized correctly.
    """
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,io.delta:delta-spark_2.13:4.2.0 "
        "pyspark-shell"
    )

    builder = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
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
# Bronze writer
# ─────────────────────────────────────────

def process_batch(batch_df, batch_id: int):
    """
    Called by Spark for every micro-batch of Kafka events.
    Parses JSON, fetches XML document, adds metadata,
    appends to Bronze Delta table.
    Bronze is append-only — never updated or deleted.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: no records, skipping")
        return

    record_count = batch_df.count()
    logger.info(f"Batch {batch_id}: processing {record_count} record(s)")

    # Parse JSON payload from Kafka bytes
    parsed_df = batch_df.select(
        from_json(
            col("value").cast("string"),
            KAFKA_EVENT_SCHEMA
        ).alias("event"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    ).select(
        "event.*",
        "kafka_partition",
        "kafka_offset",
    )

    # Fetch raw XML document for each filing
    enriched_df = parsed_df.withColumn(
        "raw_payload",
        fetch_document_udf(col("filing_url"))
    )

    # Add pipeline metadata
    bronze_df = enriched_df.withColumn(
        "batch_id", lit(str(uuid.uuid4()))
    ).withColumn(
        "bronze_write_timestamp", current_timestamp()
    ).withColumn(
        "fetch_success",
        col("raw_payload").isNotNull()
    )

    # Write to Bronze Delta table — append only
    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(BRONZE_TABLE)
    )

    success_count = bronze_df.filter(col("fetch_success")).count()
    failed_count  = record_count - success_count

    logger.info(
        f"Batch {batch_id}: wrote {record_count} record(s) to Bronze "
        f"({success_count} with document, {failed_count} without)"
    )


# ─────────────────────────────────────────
# Main streaming job
# ─────────────────────────────────────────

def run_bronze_writer():
    """
    Start the Spark Structured Streaming job.
    Reads from Kafka topic sec-filings-raw.
    Triggers every 30 seconds.
    Checkpoints progress so restarts resume cleanly.
    """
    logger.info("Starting Bronze Writer — Spark Structured Streaming")
    logger.info(f"Kafka broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka topic   : {KAFKA_TOPIC}")
    logger.info(f"Bronze table  : {BRONZE_TABLE}")
    logger.info(f"Checkpoint    : {CHECKPOINT_PATH}")

    spark = create_spark_session()

    # Read stream from Kafka
    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    logger.info("Kafka stream connected. Starting micro-batch processing.")

    query = (
        kafka_stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(f"Streaming query started. Query ID: {query.id}")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Bronze writer stopped by user.")
        query.stop()


if __name__ == "__main__":
    run_bronze_writer()