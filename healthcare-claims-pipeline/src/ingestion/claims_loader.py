import os
import sys
import yaml
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
from loguru import logger

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

RAW_PATH         = Path(__file__).parent.parent.parent / config["data"]["raw_path"]
INPATIENT_FILE   = config["data"]["inpatient_file"]
OUTPATIENT_FILE  = config["data"]["outpatient_file"]
BENEFICIARY_FILE = config["data"]["beneficiary_file"]
LABELS_FILE      = config["data"]["provider_labels_file"]

EXPECTED_COLUMNS = config["columns"]


# ─────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Create a local Spark session for batch processing.

    shuffle_partitions is set to 8 to match the number of CPU cores
    on a typical development machine. In production on Databricks
    this is managed automatically by the cluster configuration.
    """
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages io.delta:delta-spark_2.13:4.2.0 pyspark-shell"
    )

    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master(config["spark"]["master"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config["spark"]["log_level"])
    logger.info("Spark session created successfully")
    return spark


# ─────────────────────────────────────────
# Schema validation
# ─────────────────────────────────────────

def validate_columns(df: DataFrame, expected: list, table_name: str) -> bool:
    """
    Validate that all expected columns are present in the DataFrame.

    If a column is missing the pipeline stops immediately rather than
    writing corrupt data to Bronze. It is better to fail fast here
    than to discover missing columns three steps later in Silver.
    """
    actual = set(df.columns)
    expected_set = set(expected)
    missing = expected_set - actual

    if missing:
        logger.error(
            f"{table_name}: missing columns {sorted(missing)}. "
            f"Check that the correct CSV file is in data/raw/"
        )
        return False

    logger.info(f"{table_name}: schema validation passed ({len(df.columns)} columns)")
    return True


# ─────────────────────────────────────────
# CSV loaders
# ─────────────────────────────────────────

def load_inpatient(spark: SparkSession) -> DataFrame:
    """
    Load inpatient claims CSV into a Spark DataFrame.

    Inpatient claims represent hospital admissions. Each row is one
    hospital stay with admission date, discharge date, diagnosis codes,
    procedure codes, and reimbursement amount.
    """
    path = str(RAW_PATH / INPATIENT_FILE)
    logger.info(f"Loading inpatient claims from {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

    if not validate_columns(df, EXPECTED_COLUMNS["inpatient"], "inpatient_claims"):
        sys.exit(1)

    df = df.withColumn("load_timestamp", current_timestamp())
    logger.info(f"Loaded {df.count()} inpatient claims")
    return df


def load_outpatient(spark: SparkSession) -> DataFrame:
    """
    Load outpatient claims CSV into a Spark DataFrame.

    Outpatient claims represent doctor visits and procedures where
    the patient was not admitted to the hospital. Same provider and
    beneficiary structure as inpatient but without admission dates.
    """
    path = str(RAW_PATH / OUTPATIENT_FILE)
    logger.info(f"Loading outpatient claims from {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

    if not validate_columns(df, EXPECTED_COLUMNS["outpatient"], "outpatient_claims"):
        sys.exit(1)

    df = df.withColumn("load_timestamp", current_timestamp())
    logger.info(f"Loaded {df.count()} outpatient claims")
    return df


def load_beneficiary(spark: SparkSession) -> DataFrame:
    """
    Load beneficiary demographics CSV into a Spark DataFrame.

    Beneficiary data contains patient KYC information including
    date of birth, date of death, chronic condition flags for eleven
    conditions, and annual reimbursement totals. The date of death
    field is critical for detecting claims filed for deceased patients.
    """
    path = str(RAW_PATH / BENEFICIARY_FILE)
    logger.info(f"Loading beneficiary data from {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

    if not validate_columns(df, EXPECTED_COLUMNS["beneficiary"], "beneficiary_data"):
        sys.exit(1)

    df = df.withColumn("load_timestamp", current_timestamp())
    logger.info(f"Loaded {df.count()} beneficiary records")
    return df


def load_provider_labels(spark: SparkSession) -> DataFrame:
    """
    Load provider fraud labels CSV into a Spark DataFrame.

    Provider labels contain two columns: Provider ID and PotentialFraud
    which is either Yes or No. This is the ground truth used to validate
    the anomaly detection model. A provider labeled Yes was confirmed
    fraudulent through CMS investigation.
    """
    path = str(RAW_PATH / LABELS_FILE)
    logger.info(f"Loading provider labels from {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

    if not validate_columns(df, EXPECTED_COLUMNS["provider_labels"], "provider_labels"):
        sys.exit(1)

    df = df.withColumn("load_timestamp", current_timestamp())

    total     = df.count()
    fraud     = df.filter(df.PotentialFraud == "Yes").count()
    non_fraud = total - fraud

    logger.info(f"Loaded {total} provider labels: {fraud} fraud, {non_fraud} non-fraud")
    return df


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

def run_claims_loader():
    """
    Load all four source CSV files into Spark DataFrames and validate
    schema before passing to the Bronze writer.

    All four DataFrames are returned as a dictionary so bronze_writer.py
    can access them by name without importing this module directly.
    """
    logger.info("Starting Claims Loader")
    logger.info(f"Raw data path : {RAW_PATH}")

    spark = create_spark_session()

    inpatient_df       = load_inpatient(spark)
    outpatient_df      = load_outpatient(spark)
    beneficiary_df     = load_beneficiary(spark)
    provider_labels_df = load_provider_labels(spark)

    logger.info("All four source files loaded and validated successfully")
    logger.info(f"Inpatient claims    : {inpatient_df.count()}")
    logger.info(f"Outpatient claims   : {outpatient_df.count()}")
    logger.info(f"Beneficiary records : {beneficiary_df.count()}")
    logger.info(f"Provider labels     : {provider_labels_df.count()}")

    return {
        "inpatient":       inpatient_df,
        "outpatient":      outpatient_df,
        "beneficiary":     beneficiary_df,
        "provider_labels": provider_labels_df,
        "spark":           spark
    }


if __name__ == "__main__":
    run_claims_loader()