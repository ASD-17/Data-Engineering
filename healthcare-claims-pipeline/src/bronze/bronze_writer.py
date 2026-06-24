import os
import sys
import yaml
from pathlib import Path

from pyspark.sql import DataFrame
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))
from claims_loader import run_claims_loader

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

DELTA_PATH       = Path(__file__).parent.parent.parent / config["data"]["delta_path"]
WRITE_MODE       = config["bronze"]["write_mode"]

INPATIENT_PATH   = str(DELTA_PATH / "bronze" / "inpatient_claims")
OUTPATIENT_PATH  = str(DELTA_PATH / "bronze" / "outpatient_claims")
BENEFICIARY_PATH = str(DELTA_PATH / "bronze" / "beneficiary_data")
LABELS_PATH      = str(DELTA_PATH / "bronze" / "provider_labels")


# ─────────────────────────────────────────
# Writers
# ─────────────────────────────────────────

def write_bronze_table(df: DataFrame, path: str, table_name: str) -> None:
    """
    Write a DataFrame to a Delta Lake Bronze table.

    Bronze tables store raw data exactly as received from the source.
    No transformations are applied here. The write mode is overwrite
    so each pipeline run produces a clean Bronze layer from the source
    files without accumulating duplicates.

    mergeSchema is enabled to handle minor schema changes between
    Kaggle dataset versions without breaking the write.
    """
    count = df.count()
    logger.info(f"Writing {count} records to {table_name}")

    (
        df.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("mergeSchema", "true")
        .save(path)
    )

    logger.info(f"Wrote {count} records to {table_name}")
    logger.info(f"Path: {path}")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

def run_bronze_writer() -> None:
    """
    Load all four source DataFrames from claims_loader and write
    each one as a Delta Lake table in the Bronze layer.

    Bronze is the raw landing zone. Data is written exactly as it
    comes from the CSV files with only a load_timestamp added by
    the loader. No business logic runs here. If the Silver or Gold
    layers need to be reprocessed with new logic, Bronze never needs
    to change.
    """
    logger.info("Starting Bronze Writer")
    logger.info(f"Delta path  : {DELTA_PATH}")
    logger.info(f"Write mode  : {WRITE_MODE}")

    data = run_claims_loader()

    write_bronze_table(data["inpatient"],       INPATIENT_PATH,   "bronze.inpatient_claims")
    write_bronze_table(data["outpatient"],      OUTPATIENT_PATH,  "bronze.outpatient_claims")
    write_bronze_table(data["beneficiary"],     BENEFICIARY_PATH, "bronze.beneficiary_data")
    write_bronze_table(data["provider_labels"], LABELS_PATH,      "bronze.provider_labels")

    logger.info("Bronze Writer complete")
    logger.info("Tables written:")
    logger.info(f"  bronze.inpatient_claims   -> {INPATIENT_PATH}")
    logger.info(f"  bronze.outpatient_claims  -> {OUTPATIENT_PATH}")
    logger.info(f"  bronze.beneficiary_data   -> {BENEFICIARY_PATH}")
    logger.info(f"  bronze.provider_labels    -> {LABELS_PATH}")


if __name__ == "__main__":
    run_bronze_writer()