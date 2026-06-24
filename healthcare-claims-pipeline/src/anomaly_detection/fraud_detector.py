import os
import sys
import uuid
import yaml
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, udf,
    array, concat_ws, current_timestamp, row_number,
    avg as spark_avg, stddev as spark_stddev
)
from pyspark.sql.types import StringType, DoubleType, ArrayType, BooleanType
from pyspark.sql.window import Window

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler
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

SILVER_FEATURES         = str(DELTA_PATH / "silver" / "provider_features")
GOLD_RISK_SCORES        = str(DELTA_PATH / "gold" / "provider_risk_scores")
GOLD_FRAUD_ALERTS       = str(DELTA_PATH / "gold" / "fraud_alerts")
GOLD_PEER_BENCHMARK     = str(DELTA_PATH / "gold" / "provider_peer_benchmark")
GOLD_WORK_QUEUE         = str(DELTA_PATH / "gold" / "investigator_work_queue")

WRITE_MODE = config["gold"]["write_mode"]

AD = config["anomaly_detection"]
DUPLICATE_THRESHOLD      = AD["duplicate_claim_ratio_threshold"]
WEEKEND_THRESHOLD        = AD["weekend_claim_ratio_threshold"]
HIGH_COST_THRESHOLD      = AD["high_cost_procedure_ratio_threshold"]
HIGH_VOLUME_THRESHOLD    = AD["high_claim_volume_threshold"]
REIMB_STD_THRESHOLD      = AD["high_reimbursement_std_threshold"]
CRITICAL_SCORE_THRESHOLD = AD["critical_risk_score_threshold"]
HIGH_SCORE_THRESHOLD     = AD["high_risk_score_threshold"]
MEDIUM_SCORE_THRESHOLD   = AD["medium_risk_score_threshold"]
CONTAMINATION            = AD["isolation_forest_contamination"]
N_ESTIMATORS             = AD["isolation_forest_n_estimators"]
RANDOM_STATE             = AD["isolation_forest_random_state"]

# Features used for Isolation Forest
FEATURE_COLS = [
    "total_claims",
    "avg_claim_amount",
    "unique_patients",
    "duplicate_claim_ratio",
    "weekend_claim_ratio",
    "high_cost_procedure_ratio",
    "reimbursement_per_patient",
    "deceased_patient_claims",
    "distinct_diagnosis_codes",
    "distinct_procedure_codes",
]


# ─────────────────────────────────────────
# Step 1: Rule based detection
# ─────────────────────────────────────────

def apply_rule_based_flags(df: DataFrame, peer_stats: dict) -> DataFrame:
    """
    Apply explicit business rules to flag suspicious providers.

    Each rule targets a specific fraud pattern that domain experts
    have identified in Medicare data. Rules are applied independently
    so a provider can trigger multiple flags.

    The peer_stats dict contains the mean and standard deviation of
    reimbursement_per_patient across all providers. This is used to
    flag providers who are statistical outliers on reimbursement
    without needing the full Isolation Forest model.
    """
    avg_reimb  = peer_stats["avg_reimbursement_per_patient"]
    std_reimb  = peer_stats["std_reimbursement_per_patient"]
    high_reimb = avg_reimb + (REIMB_STD_THRESHOLD * std_reimb)

    flagged = (
        df
        .withColumn("flag_duplicate_claims",
            (col("duplicate_claim_ratio") > DUPLICATE_THRESHOLD).cast(BooleanType()))
        .withColumn("flag_deceased_billing",
            (col("deceased_patient_claims") > 0).cast(BooleanType()))
        .withColumn("flag_high_volume",
            (col("total_claims") > HIGH_VOLUME_THRESHOLD * col("unique_patients")).cast(BooleanType()))
        .withColumn("flag_upcoding",
            (col("high_cost_procedure_ratio") > HIGH_COST_THRESHOLD).cast(BooleanType()))
        .withColumn("flag_weekend_billing",
            (col("weekend_claim_ratio") > WEEKEND_THRESHOLD).cast(BooleanType()))
        .withColumn("flag_high_reimbursement",
            (col("reimbursement_per_patient") > high_reimb).cast(BooleanType()))
        .withColumn("anomaly_count",
            (
                col("flag_duplicate_claims").cast("int") +
                col("flag_deceased_billing").cast("int") +
                col("flag_high_volume").cast("int") +
                col("flag_upcoding").cast("int") +
                col("flag_weekend_billing").cast("int") +
                col("flag_high_reimbursement").cast("int")
            )
        )
    )

    logger.info(f"Rule based flags applied. Providers with at least one flag: "
                f"{flagged.filter(col('anomaly_count') > 0).count()}")
    return flagged


# ─────────────────────────────────────────
# Step 2: Isolation Forest
# ─────────────────────────────────────────

def run_isolation_forest(df: DataFrame) -> DataFrame:
    """
    Run Isolation Forest on provider behavioral features to detect
    statistical outliers.

    Isolation Forest works by randomly partitioning the feature space.
    Anomalous providers are isolated in fewer partitions than normal
    ones because their behavior is distinct from the majority.

    The model runs on the Spark driver using Pandas since sklearn
    does not distribute across Spark workers. At 5,410 providers
    this fits comfortably in memory.

    The raw Isolation Forest score is inverted and scaled to 0 to 1
    where 1 is most anomalous. This makes it easier to combine with
    rule based flags and present to investigators.
    """
    logger.info("Running Isolation Forest")

    pdf = df.select(["provider_id"] + FEATURE_COLS + ["is_fraud"]).toPandas()
    pdf[FEATURE_COLS] = pdf[FEATURE_COLS].fillna(0)

    scaler = StandardScaler()
    X = scaler.fit_transform(pdf[FEATURE_COLS])

    model = IsolationForest(
        contamination=CONTAMINATION,
        n_estimators=N_ESTIMATORS,
        random_state=RANDOM_STATE
    )
    model.fit(X)

    raw_scores = model.decision_function(X)
    # Invert and scale to 0 to 1 where 1 is most anomalous
    min_score = raw_scores.min()
    max_score = raw_scores.max()
    scaled_scores = 1 - (raw_scores - min_score) / (max_score - min_score)

    pdf["risk_score"] = scaled_scores

    # Validate against ground truth
    predictions = (scaled_scores >= (1 - CONTAMINATION)).astype(int)
    y_true = pdf["is_fraud"].astype(int)

    precision = precision_score(y_true, predictions, zero_division=0)
    recall    = recall_score(y_true, predictions, zero_division=0)
    f1        = f1_score(y_true, predictions, zero_division=0)
    roc_auc   = roc_auc_score(y_true, scaled_scores)

    logger.info(f"Isolation Forest Metrics:")
    logger.info(f"  Precision : {precision:.4f}")
    logger.info(f"  Recall    : {recall:.4f}")
    logger.info(f"  F1 Score  : {f1:.4f}")
    logger.info(f"  ROC-AUC   : {roc_auc:.4f}")

    scores_df = pdf[["provider_id", "risk_score"]].copy()

    spark = SparkSession.getActiveSession()
    scores_spark = spark.createDataFrame(scores_df)

    result = df.join(scores_spark, on="provider_id", how="left")
    return result, {"precision": precision, "recall": recall, "f1": f1, "roc_auc": roc_auc}


# ─────────────────────────────────────────
# Step 3: Alert severity and explainability
# ─────────────────────────────────────────

def assign_severity(df: DataFrame) -> DataFrame:
    """
    Assign alert severity based on risk score and anomaly count.

    Critical requires both a high risk score and multiple flags.
    This reduces false positives by requiring evidence from both
    the statistical model and the rule based checks.
    """
    return df.withColumn("alert_severity",
        when(
            (col("risk_score") >= CRITICAL_SCORE_THRESHOLD) & (col("anomaly_count") >= 2),
            lit("critical")
        ).when(
            (col("risk_score") >= HIGH_SCORE_THRESHOLD) | (col("anomaly_count") >= 2),
            lit("high")
        ).when(
            (col("risk_score") >= MEDIUM_SCORE_THRESHOLD) | (col("anomaly_count") == 1),
            lit("medium")
        ).otherwise(lit("none"))
    )


def build_top_reasons(df: DataFrame) -> DataFrame:
    """
    Generate human readable explanation for each provider alert.

    Instead of showing investigators a black box risk score, the
    top_reasons column explains exactly which behaviors triggered
    the alert. This gives investigators a concrete starting point
    for their review and makes the alerts actionable.

    Example output:
    Deceased patient billing detected (5 claims after date of death),
    Weekend claim ratio 0.38 above threshold 0.30,
    High reimbursement per patient vs peer group
    """
    return df.withColumn("top_reasons",
        concat_ws(", ",
            when(col("flag_deceased_billing") == True,
                F.concat(lit("Deceased patient billing detected ("),
                         col("deceased_patient_claims").cast(StringType()),
                         lit(" claims after date of death)"))),
            when(col("flag_duplicate_claims") == True,
                F.concat(lit("Duplicate claim ratio "),
                         spark_round(col("duplicate_claim_ratio"), 2).cast(StringType()),
                         lit(" above threshold "),
                         lit(str(DUPLICATE_THRESHOLD)))),
            when(col("flag_high_volume") == True,
                F.concat(lit("Claim volume "),
                         col("total_claims").cast(StringType()),
                         lit(" exceeds expected maximum for "),
                         col("unique_patients").cast(StringType()),
                         lit(" patients"))),
            when(col("flag_upcoding") == True,
                F.concat(lit("High cost procedure ratio "),
                         spark_round(col("high_cost_procedure_ratio"), 2).cast(StringType()),
                         lit(" above threshold "),
                         lit(str(HIGH_COST_THRESHOLD)))),
            when(col("flag_weekend_billing") == True,
                F.concat(lit("Weekend claim ratio "),
                         spark_round(col("weekend_claim_ratio"), 2).cast(StringType()),
                         lit(" above threshold "),
                         lit(str(WEEKEND_THRESHOLD)))),
            when(col("flag_high_reimbursement") == True,
                lit("High reimbursement per patient vs peer group")),
        )
    )


# ─────────────────────────────────────────
# Step 4: Build Gold tables
# ─────────────────────────────────────────

def build_provider_risk_scores(df: DataFrame, metrics: dict) -> DataFrame:
    return (
        df
        .withColumn("risk_score",    spark_round(col("risk_score"), 4))
        .withColumn("risk_label",
            when(col("alert_severity") == "critical", lit("Critical"))
            .when(col("alert_severity") == "high",     lit("High"))
            .when(col("alert_severity") == "medium",   lit("Medium"))
            .otherwise(lit("Low")))
        .withColumn("precision", lit(round(metrics["precision"], 4)))
        .withColumn("recall",    lit(round(metrics["recall"],    4)))
        .withColumn("f1_score",  lit(round(metrics["f1"],        4)))
        .withColumn("roc_auc",   lit(round(metrics["roc_auc"],   4)))
        .withColumn("scored_timestamp", current_timestamp())
        .select(
            "provider_id", "risk_score", "risk_label",
            "flag_duplicate_claims", "flag_deceased_billing",
            "flag_high_volume", "flag_upcoding",
            "flag_weekend_billing", "flag_high_reimbursement",
            "anomaly_count", "alert_severity", "top_reasons",
            "is_fraud", "precision", "recall", "f1_score", "roc_auc",
            "scored_timestamp"
        )
    )


def build_fraud_alerts(risk_scores_df: DataFrame) -> DataFrame:
    alert_udf = udf(lambda: str(uuid.uuid4()), StringType())
    window = Window.orderBy(col("risk_score").desc())
    return (
        risk_scores_df
        .filter(col("alert_severity") != "none")
        .withColumn("alert_id",    alert_udf())
        .withColumn("priority_rank", row_number().over(window))
        .withColumn("alert_timestamp", current_timestamp())
        .select(
            "alert_id", "provider_id", "priority_rank",
            "alert_severity", "risk_score", "top_reasons",
            "anomaly_count", "is_fraud", "alert_timestamp"
        )
    )


def build_peer_benchmark(features_df: DataFrame, risk_scores_df: DataFrame) -> DataFrame:
    """
    Compare each provider against peer group averages.

    The benchmark table powers the explainability layer by showing
    exactly how far each provider deviates from their peers on key
    metrics. A provider billing 4.2x more than peers is a specific,
    actionable data point for investigators.
    """
    peer_avgs = features_df.agg(
        spark_avg("total_claims").alias("peer_avg_total_claims"),
        spark_avg("avg_claim_amount").alias("peer_avg_claim_amount"),
        spark_avg("reimbursement_per_patient").alias("peer_avg_reimbursement_per_patient"),
        spark_avg("weekend_claim_ratio").alias("peer_avg_weekend_ratio"),
        spark_avg("duplicate_claim_ratio").alias("peer_avg_duplicate_ratio"),
    ).collect()[0]

    return (
        features_df
        .join(risk_scores_df.select("provider_id", "risk_score", "alert_severity"), on="provider_id", how="left")
        .withColumn("peer_avg_total_claims",            lit(round(peer_avgs["peer_avg_total_claims"], 2)))
        .withColumn("peer_avg_claim_amount",            lit(round(peer_avgs["peer_avg_claim_amount"], 2)))
        .withColumn("peer_avg_reimbursement_per_patient", lit(round(peer_avgs["peer_avg_reimbursement_per_patient"], 2)))
        .withColumn("peer_avg_weekend_ratio",           lit(round(peer_avgs["peer_avg_weekend_ratio"], 4)))
        .withColumn("peer_avg_duplicate_ratio",         lit(round(peer_avgs["peer_avg_duplicate_ratio"], 4)))
        .withColumn("claims_vs_peer_ratio",
            spark_round(col("total_claims") / lit(max(peer_avgs["peer_avg_total_claims"], 0.001)), 2))
        .withColumn("reimbursement_vs_peer_ratio",
            spark_round(col("reimbursement_per_patient") / lit(max(peer_avgs["peer_avg_reimbursement_per_patient"], 0.001)), 2))
        .withColumn("benchmark_timestamp", current_timestamp())
        .select(
            "provider_id", "risk_score", "alert_severity",
            "total_claims", "peer_avg_total_claims", "claims_vs_peer_ratio",
            "avg_claim_amount", "peer_avg_claim_amount",
            "reimbursement_per_patient", "peer_avg_reimbursement_per_patient", "reimbursement_vs_peer_ratio",
            "weekend_claim_ratio", "peer_avg_weekend_ratio",
            "duplicate_claim_ratio", "peer_avg_duplicate_ratio",
            "benchmark_timestamp"
        )
    )


def build_investigator_work_queue(fraud_alerts_df: DataFrame, features_df: DataFrame) -> DataFrame:
    return (
        fraud_alerts_df
        .join(
            features_df.select("provider_id", "total_claims", "total_reimbursement", "unique_patients"),
            on="provider_id", how="left"
        )
        .withColumn("status",         lit("open"))
        .withColumn("assigned_to",    lit(None).cast(StringType()))
        .withColumn("queue_timestamp", current_timestamp())
        .select(
            col("alert_id").alias("queue_id"),
            "provider_id", "priority_rank", "alert_severity",
            "risk_score", "top_reasons", "total_claims",
            "total_reimbursement", "unique_patients",
            "anomaly_count", "status", "assigned_to",
            "is_fraud", "queue_timestamp"
        )
    )


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

def run_fraud_detector() -> None:
    logger.info("Starting Fraud Detector")
    logger.info(f"Silver path : {DELTA_PATH / 'silver'}")
    logger.info(f"Gold path   : {DELTA_PATH / 'gold'}")

    spark = create_spark_session()

    features_df = spark.read.format("delta").load(SILVER_FEATURES)
    total = features_df.count()
    logger.info(f"Read {total} provider profiles from Silver")

    # Compute peer stats for rule based detection
    peer_stats_row = features_df.agg(
        spark_avg("reimbursement_per_patient").alias("avg_reimbursement_per_patient"),
        spark_stddev("reimbursement_per_patient").alias("std_reimbursement_per_patient")
    ).collect()[0]

    peer_stats = {
        "avg_reimbursement_per_patient": float(peer_stats_row["avg_reimbursement_per_patient"] or 0),
        "std_reimbursement_per_patient": float(peer_stats_row["std_reimbursement_per_patient"] or 0),
    }

    # Apply detection
    flagged_df             = apply_rule_based_flags(features_df, peer_stats)
    scored_df, metrics     = run_isolation_forest(flagged_df)
    scored_df              = assign_severity(scored_df)
    scored_df              = build_top_reasons(scored_df)

    # Build Gold tables
    risk_scores_df         = build_provider_risk_scores(scored_df, metrics)
    fraud_alerts_df        = build_fraud_alerts(risk_scores_df)
    peer_benchmark_df      = build_peer_benchmark(features_df, risk_scores_df)
    work_queue_df          = build_investigator_work_queue(fraud_alerts_df, features_df)

    # Write Gold tables
    logger.info("Writing Gold tables")

    risk_scores_df.write.format("delta").mode(WRITE_MODE).option("mergeSchema", "true").save(GOLD_RISK_SCORES)
    logger.info(f"Wrote provider_risk_scores: {risk_scores_df.count()} records")

    fraud_alerts_df.write.format("delta").mode(WRITE_MODE).option("mergeSchema", "true").save(GOLD_FRAUD_ALERTS)
    logger.info(f"Wrote fraud_alerts: {fraud_alerts_df.count()} records")

    peer_benchmark_df.write.format("delta").mode(WRITE_MODE).option("mergeSchema", "true").save(GOLD_PEER_BENCHMARK)
    logger.info(f"Wrote provider_peer_benchmark: {peer_benchmark_df.count()} records")

    work_queue_df.write.format("delta").mode(WRITE_MODE).option("mergeSchema", "true").save(GOLD_WORK_QUEUE)
    logger.info(f"Wrote investigator_work_queue: {work_queue_df.count()} records")

    logger.info("Fraud Detector complete")

    critical = risk_scores_df.filter(col("alert_severity") == "critical").count()
    high     = risk_scores_df.filter(col("alert_severity") == "high").count()
    medium   = risk_scores_df.filter(col("alert_severity") == "medium").count()

    logger.info(f"Alert summary: {critical} critical, {high} high, {medium} medium")
    logger.info(f"ML Metrics: Precision={metrics['precision']:.4f}, Recall={metrics['recall']:.4f}, "
                f"F1={metrics['f1']:.4f}, ROC-AUC={metrics['roc_auc']:.4f}")

    logger.info("Sample fraud alerts:")
    work_queue_df.select(
        "provider_id", "priority_rank", "alert_severity",
        "risk_score", "top_reasons"
    ).orderBy("priority_rank").show(10, truncate=False)


if __name__ == "__main__":
    run_fraud_detector()