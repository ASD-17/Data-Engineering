import os
import sys
import yaml
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, to_date, datediff, months_between,
    count, countDistinct, sum as spark_sum, avg, stddev,
    collect_list, array_distinct, size, coalesce,
    current_timestamp, dayofweek
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))
from claims_loader import create_spark_session

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

DELTA_PATH = Path(__file__).parent.parent.parent / config["data"]["delta_path"]

BRONZE_INPATIENT   = str(DELTA_PATH / "bronze" / "inpatient_claims")
BRONZE_OUTPATIENT  = str(DELTA_PATH / "bronze" / "outpatient_claims")
BRONZE_BENEFICIARY = str(DELTA_PATH / "bronze" / "beneficiary_data")
BRONZE_LABELS      = str(DELTA_PATH / "bronze" / "provider_labels")

SILVER_CLAIMS    = str(DELTA_PATH / "silver" / "claims_enriched")
SILVER_FEATURES  = str(DELTA_PATH / "silver" / "provider_features")

WRITE_MODE = config["silver"]["write_mode"]
MAX_CLAIM_DURATION = config["silver"]["max_claim_duration_days"]

HIGH_COST_PROCEDURES = {"4154", "1761", "3995", "0106", "3310", "3722", "5119"}


# ─────────────────────────────────────────
# Step 1: Build claims_enriched
# ─────────────────────────────────────────

def build_claims_enriched(
    inpatient_df: DataFrame,
    outpatient_df: DataFrame,
    beneficiary_df: DataFrame
) -> DataFrame:
    """
    Join inpatient and outpatient claims with beneficiary demographics
    to produce one unified claims table.

    Inpatient and outpatient claims are tagged with a claim_type column
    before being unioned so downstream logic can distinguish them.
    Both are then joined to beneficiary data so every claim row carries
    patient age, chronic condition count, and deceased status.

    The is_deceased flag is critical for one of the core fraud rules:
    claims submitted after a patient's date of death. The is_weekend_claim
    flag captures whether the claim was filed on a Saturday or Sunday,
    which is a known fraud signal when unusually high across a provider.
    """
    logger.info("Building claims_enriched")

    # Clean and cast inpatient
    ip = (
        inpatient_df
        .withColumn("claim_type", lit("inpatient"))
        .withColumn("claim_start_date", F.try_to_date(col("ClaimStartDt"), "yyyy-MM-dd"))
        .withColumn("claim_end_date",   to_date(col("ClaimEndDt"),   "yyyy-MM-dd"))
        .withColumn("admission_date",   to_date(col("AdmissionDt"),  "yyyy-MM-dd"))
        .withColumn("discharge_date",   to_date(col("DischargeDt"),  "yyyy-MM-dd"))
        .withColumn("reimbursement_amount",
            coalesce(when(col("InscClaimAmtReimbursed") == "NA", lit(None)).otherwise(col("InscClaimAmtReimbursed")).cast(DoubleType()), lit(0.0)))
        .withColumn("deductible_paid",
            coalesce(when(col("DeductibleAmtPaid") == "NA", lit(None)).otherwise(col("DeductibleAmtPaid")).cast(DoubleType()), lit(0.0)))
        .withColumn("claim_duration_days",
            when(
                col("claim_end_date").isNotNull() & col("claim_start_date").isNotNull(),
                datediff(col("claim_end_date"), col("claim_start_date"))
            ).otherwise(lit(0)).cast(IntegerType()))
        .withColumn("attending_physician", coalesce(col("AttendingPhysician"), lit("UNKNOWN")))
        .select(
            col("BeneID").alias("beneficiary_id"),
            col("ClaimID").alias("claim_id"),
            col("Provider").alias("provider_id"),
            "claim_type",
            "claim_start_date",
            "claim_end_date",
            "admission_date",
            "reimbursement_amount",
            "deductible_paid",
            "claim_duration_days",
            "attending_physician",
            col("ClmDiagnosisCode_1").alias("primary_diagnosis_code"),
            col("ClmProcedureCode_1").alias("primary_procedure_code"),
        )
    )

    # Clean and cast outpatient
    op = (
        outpatient_df
        .withColumn("claim_type", lit("outpatient"))
        .withColumn("claim_start_date", F.try_to_date(col("ClaimStartDt"), "yyyy-MM-dd"))
        .withColumn("claim_end_date",   to_date(col("ClaimEndDt"),   "yyyy-MM-dd"))
        .withColumn("reimbursement_amount",
            coalesce(when(col("InscClaimAmtReimbursed") == "NA", lit(None)).otherwise(col("InscClaimAmtReimbursed")).cast(DoubleType()), lit(0.0)))
        .withColumn("deductible_paid",
            coalesce(when(col("DeductibleAmtPaid") == "NA", lit(None)).otherwise(col("DeductibleAmtPaid")).cast(DoubleType()), lit(0.0)))
        .withColumn("claim_duration_days", lit(0).cast(IntegerType()))
        .withColumn("attending_physician", coalesce(col("AttendingPhysician"), lit("UNKNOWN")))
        .withColumn("admission_date", lit(None).cast("date"))
        .select(
            col("BeneID").alias("beneficiary_id"),
            col("ClaimID").alias("claim_id"),
            col("Provider").alias("provider_id"),
            "claim_type",
            "claim_start_date",
            "claim_end_date",
            "admission_date",
            "reimbursement_amount",
            "deductible_paid",
            "claim_duration_days",
            "attending_physician",
            col("ClmDiagnosisCode_1").alias("primary_diagnosis_code"),
            col("ClmProcedureCode_1").alias("primary_procedure_code"),
        )
    )

    # Union inpatient and outpatient
    claims = ip.union(op)

    # Clean beneficiary
    bene = (
        beneficiary_df
        .withColumn("dob", F.try_to_date(col("DOB"), "yyyy-MM-dd"))
        .withColumn("dod", F.try_to_date(col("DOD"), "yyyy-MM-dd"))
        .withColumn("is_deceased", col("dod").isNotNull().cast(BooleanType()))
        .withColumn("beneficiary_age",
            when(col("dob").isNotNull(),
                (months_between(F.current_date(), col("dob")) / 12).cast(IntegerType())
            ).otherwise(lit(0)))
        .withColumn("chronic_condition_count",
            (
                coalesce(when(col("ChronicCond_Alzheimer") == "NA", lit(None)).otherwise(col("ChronicCond_Alzheimer")).cast(IntegerType()),    lit(0)) +
                coalesce(when(col("ChronicCond_Heartfailure") == "NA", lit(None)).otherwise(col("ChronicCond_Heartfailure")).cast(IntegerType()), lit(0)) +
                coalesce(when(col("ChronicCond_KidneyDisease") == "NA", lit(None)).otherwise(col("ChronicCond_KidneyDisease")).cast(IntegerType()),lit(0)) +
                coalesce(when(col("ChronicCond_Cancer") == "NA", lit(None)).otherwise(col("ChronicCond_Cancer")).cast(IntegerType()),       lit(0)) +
                coalesce(when(col("ChronicCond_ObstrPulmonary") == "NA", lit(None)).otherwise(col("ChronicCond_ObstrPulmonary")).cast(IntegerType()),lit(0)) +
                coalesce(when(col("ChronicCond_Depression") == "NA", lit(None)).otherwise(col("ChronicCond_Depression")).cast(IntegerType()),   lit(0)) +
                coalesce(when(col("ChronicCond_Diabetes") == "NA", lit(None)).otherwise(col("ChronicCond_Diabetes")).cast(IntegerType()),     lit(0)) +
                coalesce(when(col("ChronicCond_IschemicHeart") == "NA", lit(None)).otherwise(col("ChronicCond_IschemicHeart")).cast(IntegerType()),lit(0)) +
                coalesce(when(col("ChronicCond_Osteoporasis") == "NA", lit(None)).otherwise(col("ChronicCond_Osteoporasis")).cast(IntegerType()), lit(0)) +
                coalesce(when(col("ChronicCond_rheumatoidarthritis") == "NA", lit(None)).otherwise(col("ChronicCond_rheumatoidarthritis")).cast(IntegerType()), lit(0)) +
                coalesce(when(col("ChronicCond_stroke") == "NA", lit(None)).otherwise(col("ChronicCond_stroke")).cast(IntegerType()),       lit(0))
            )
        )
        .select(
            col("BeneID").alias("beneficiary_id"),
            "dob",
            "dod",
            "is_deceased",
            "beneficiary_age",
            "chronic_condition_count",
            col("Gender").alias("beneficiary_gender"),
            col("State").alias("beneficiary_state"),
        )
    )

    # Join claims with beneficiary demographics
    enriched = (
        claims
        .join(bene, on="beneficiary_id", how="left")
        .withColumn("is_weekend_claim",
            dayofweek(col("claim_start_date")).isin([1, 7]).cast(BooleanType()))
        .withColumn("processed_timestamp", current_timestamp())
    )

    total = enriched.count()
    logger.info(f"claims_enriched: {total} records")
    return enriched


# ─────────────────────────────────────────
# Step 2: Build provider_features
# ─────────────────────────────────────────

def build_provider_features(
    claims_enriched: DataFrame,
    provider_labels_df: DataFrame
) -> DataFrame:
    """
    Aggregate all claims by provider to compute behavioral metrics.

    These features are what make fraud detectable. A single claim from
    a provider tells you nothing. Knowing that a provider submitted
    40,000 claims for 200 patients, with a 35 percent duplicate rate
    and 12 claims for deceased patients, tells you everything.

    Each metric captures a specific fraud signal:

    duplicate_claim_ratio: legitimate providers rarely submit the same
    claim twice. A high ratio suggests systematic billing fraud.

    weekend_claim_ratio: most medical procedures happen on weekdays.
    Unusually high weekend billing suggests fabricated claims.

    high_cost_procedure_ratio: upcoding is when a provider bills for
    a more expensive procedure than was actually performed. A provider
    with a disproportionately high ratio of high cost procedures is
    a strong upcoding signal.

    deceased_patient_claims: any claim filed after a patient's date of
    death is fraudulent by definition. Even one such claim is a red flag.

    reimbursement_per_patient: divides total reimbursement by unique
    patients. Fraudulent providers tend to extract far more per patient
    than legitimate ones.
    """
    logger.info("Building provider_features")

    high_cost_set = list(HIGH_COST_PROCEDURES)

    features = (
        claims_enriched
        .groupBy("provider_id")
        .agg(
            count("claim_id").alias("total_claims"),
            countDistinct("beneficiary_id").alias("unique_patients"),
            countDistinct("attending_physician").alias("unique_physicians"),
            spark_sum("reimbursement_amount").alias("total_reimbursement"),
            avg("reimbursement_amount").alias("avg_claim_amount"),
            avg("claim_duration_days").alias("avg_claim_duration_days"),
            avg("chronic_condition_count").alias("avg_chronic_conditions"),
            countDistinct("primary_diagnosis_code").alias("distinct_diagnosis_codes"),
            countDistinct("primary_procedure_code").alias("distinct_procedure_codes"),

            # Inpatient vs outpatient split
            spark_sum(when(col("claim_type") == "inpatient",  1).otherwise(0)).alias("total_inpatient_claims"),
            spark_sum(when(col("claim_type") == "outpatient", 1).otherwise(0)).alias("total_outpatient_claims"),

            # Duplicate claim ratio
            (
                (count("claim_id") - countDistinct("claim_id")).cast(DoubleType()) /
                count("claim_id").cast(DoubleType())
            ).alias("duplicate_claim_ratio"),

            # Weekend claim ratio
            (
                spark_sum(when(col("is_weekend_claim") == True, 1).otherwise(0)).cast(DoubleType()) /
                count("claim_id").cast(DoubleType())
            ).alias("weekend_claim_ratio"),

            # High cost procedure ratio
            (
                spark_sum(
                    when(col("primary_procedure_code").isin(high_cost_set), 1).otherwise(0)
                ).cast(DoubleType()) /
                count("claim_id").cast(DoubleType())
            ).alias("high_cost_procedure_ratio"),

            # Deceased patient claims
            spark_sum(
                when(col("is_deceased") == True, 1).otherwise(0)
            ).alias("deceased_patient_claims"),
        )
        .withColumn("reimbursement_per_patient",
            when(col("unique_patients") > 0,
                col("total_reimbursement") / col("unique_patients").cast(DoubleType())
            ).otherwise(lit(0.0))
        )
        .withColumn("duplicate_claim_ratio",
            coalesce(col("duplicate_claim_ratio"), lit(0.0)))
        .withColumn("weekend_claim_ratio",
            coalesce(col("weekend_claim_ratio"), lit(0.0)))
        .withColumn("high_cost_procedure_ratio",
            coalesce(col("high_cost_procedure_ratio"), lit(0.0)))
    )

    # Join fraud labels
    labels = (
        provider_labels_df
        .withColumn("is_fraud",
            when(col("PotentialFraud") == "Yes", True).otherwise(False))
        .select(
            col("Provider").alias("provider_id"),
            "is_fraud"
        )
    )

    features = (
        features
        .join(labels, on="provider_id", how="left")
        .withColumn("is_fraud", coalesce(col("is_fraud"), lit(False)))
        .withColumn("processed_timestamp", current_timestamp())
    )

    total      = features.count()
    fraud      = features.filter(col("is_fraud") == True).count()
    non_fraud  = total - fraud

    logger.info(f"provider_features: {total} providers ({fraud} fraud, {non_fraud} non-fraud)")
    return features


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

def run_silver_transformer() -> None:
    logger.info("Starting Silver Transformer")
    logger.info(f"Bronze path : {DELTA_PATH / 'bronze'}")
    logger.info(f"Silver path : {DELTA_PATH / 'silver'}")

    spark = create_spark_session()

    # Read Bronze tables
    logger.info("Reading Bronze tables")
    inpatient_df       = spark.read.format("delta").load(BRONZE_INPATIENT)
    outpatient_df      = spark.read.format("delta").load(BRONZE_OUTPATIENT)
    beneficiary_df     = spark.read.format("delta").load(BRONZE_BENEFICIARY)
    provider_labels_df = spark.read.format("delta").load(BRONZE_LABELS)

    logger.info(f"Bronze inpatient    : {inpatient_df.count()} records")
    logger.info(f"Bronze outpatient   : {outpatient_df.count()} records")
    logger.info(f"Bronze beneficiary  : {beneficiary_df.count()} records")
    logger.info(f"Bronze labels       : {provider_labels_df.count()} records")

    # Build Silver tables
    claims_enriched   = build_claims_enriched(inpatient_df, outpatient_df, beneficiary_df)
    provider_features = build_provider_features(claims_enriched, provider_labels_df)

    # Write Silver tables
    logger.info("Writing silver.claims_enriched")
    (
        claims_enriched
        .repartition(8)
        .write
        .format("delta")
        .mode(WRITE_MODE)
        .option("mergeSchema", "true")
        .save(SILVER_CLAIMS)
    )
    logger.info(f"Wrote claims_enriched to {SILVER_CLAIMS}")

    logger.info("Writing silver.provider_features")
    (
        provider_features.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("mergeSchema", "true")
        .save(SILVER_FEATURES)
    )
    logger.info(f"Wrote provider_features to {SILVER_FEATURES}")

    logger.info("Silver Transformer complete")
    logger.info("Sample provider features:")
    provider_features.select(
        "provider_id",
        "total_claims",
        "unique_patients",
        "duplicate_claim_ratio",
        "weekend_claim_ratio",
        "deceased_patient_claims",
        "is_fraud"
    ).show(10, truncate=False)


if __name__ == "__main__":
    run_silver_transformer()