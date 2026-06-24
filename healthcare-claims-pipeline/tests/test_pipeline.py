"""
Healthcare Claims Anomaly Detection Pipeline -- Unit Tests

Tests cover the three most critical functions in the pipeline:
- validate_columns: schema validation before Bronze write
- apply_rule_based_flags: fraud flag detection logic
- build_top_reasons: explainable alert generation

Run with: pytest tests/test_pipeline.py -v
"""
import os
import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "ingestion"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "anomaly_detection"))


# ─────────────────────────────────────────
# Tests for validate_columns
# ─────────────────────────────────────────

class TestValidateColumns:

    def setup_method(self):
        from claims_loader import validate_columns
        self.validate = validate_columns

    def test_all_columns_present_returns_true(self):
        mock_df = MagicMock()
        mock_df.columns = ["BeneID", "ClaimID", "Provider", "InscClaimAmtReimbursed"]
        result = self.validate(mock_df, ["BeneID", "ClaimID", "Provider", "InscClaimAmtReimbursed"], "test_table")
        assert result is True

    def test_missing_column_returns_false(self):
        mock_df = MagicMock()
        mock_df.columns = ["BeneID", "ClaimID", "Provider"]
        result = self.validate(mock_df, ["BeneID", "ClaimID", "Provider", "InscClaimAmtReimbursed"], "test_table")
        assert result is False

    def test_multiple_missing_columns_returns_false(self):
        mock_df = MagicMock()
        mock_df.columns = ["BeneID"]
        result = self.validate(mock_df, ["BeneID", "ClaimID", "Provider", "InscClaimAmtReimbursed"], "test_table")
        assert result is False

    def test_extra_columns_still_passes(self):
        mock_df = MagicMock()
        mock_df.columns = ["BeneID", "ClaimID", "Provider", "InscClaimAmtReimbursed", "ExtraColumn"]
        result = self.validate(mock_df, ["BeneID", "ClaimID", "Provider", "InscClaimAmtReimbursed"], "test_table")
        assert result is True

    def test_empty_expected_columns_returns_true(self):
        mock_df = MagicMock()
        mock_df.columns = ["BeneID", "ClaimID"]
        result = self.validate(mock_df, [], "test_table")
        assert result is True

    def test_provider_labels_schema_passes(self):
        mock_df = MagicMock()
        mock_df.columns = ["Provider", "PotentialFraud", "load_timestamp"]
        result = self.validate(mock_df, ["Provider", "PotentialFraud"], "provider_labels")
        assert result is True

    def test_provider_labels_missing_fraud_column_fails(self):
        mock_df = MagicMock()
        mock_df.columns = ["Provider", "load_timestamp"]
        result = self.validate(mock_df, ["Provider", "PotentialFraud"], "provider_labels")
        assert result is False


# ─────────────────────────────────────────
# Tests for apply_rule_based_flags
# ─────────────────────────────────────────

class TestApplyRuleBasedFlags:

    def setup_method(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages io.delta:delta-spark_2.13:4.2.0 pyspark-shell"
        )
        from pyspark.sql import SparkSession
        self.spark = (
            SparkSession.builder
            .appName("test_fraud_detector")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        from fraud_detector import apply_rule_based_flags
        self.apply_flags = apply_rule_based_flags

    def _make_provider(self, **kwargs):
        defaults = {
            "provider_id": "PRV99999",
            "total_claims": 50,
            "unique_patients": 40,
            "total_reimbursement": 100000.0,
            "avg_claim_amount": 2000.0,
            "reimbursement_per_patient": 2500.0,
            "unique_physicians": 5,
            "avg_claim_duration_days": 3.0,
            "avg_chronic_conditions": 2.0,
            "duplicate_claim_ratio": 0.0,
            "weekend_claim_ratio": 0.1,
            "high_cost_procedure_ratio": 0.1,
            "deceased_patient_claims": 0,
            "distinct_diagnosis_codes": 10,
            "distinct_procedure_codes": 5,
            "total_inpatient_claims": 20,
            "total_outpatient_claims": 30,
            "is_fraud": False,
        }
        defaults.update(kwargs)
        return self.spark.createDataFrame([defaults])

    def _peer_stats(self, avg=2000.0, std=500.0):
        return {
            "avg_reimbursement_per_patient": avg,
            "std_reimbursement_per_patient": std
        }

    def test_clean_provider_has_no_flags(self):
        df = self._make_provider()
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_duplicate_claims"]   == False
        assert result["flag_deceased_billing"]   == False
        assert result["flag_high_volume"]        == False
        assert result["flag_upcoding"]           == False
        assert result["flag_weekend_billing"]    == False
        assert result["flag_high_reimbursement"] == False
        assert result["anomaly_count"]           == 0

    def test_duplicate_claims_flag_triggered(self):
        df = self._make_provider(duplicate_claim_ratio=0.10)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_duplicate_claims"] == True

    def test_duplicate_claims_below_threshold_not_flagged(self):
        df = self._make_provider(duplicate_claim_ratio=0.04)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_duplicate_claims"] == False

    def test_deceased_billing_flag_triggered(self):
        df = self._make_provider(deceased_patient_claims=5)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_deceased_billing"] == True

    def test_deceased_billing_zero_not_flagged(self):
        df = self._make_provider(deceased_patient_claims=0)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_deceased_billing"] == False

    def test_high_volume_flag_triggered(self):
        df = self._make_provider(total_claims=5000, unique_patients=10)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_high_volume"] == True

    def test_weekend_billing_flag_triggered(self):
        df = self._make_provider(weekend_claim_ratio=0.50)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_weekend_billing"] == True

    def test_weekend_billing_below_threshold_not_flagged(self):
        df = self._make_provider(weekend_claim_ratio=0.20)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_weekend_billing"] == False

    def test_upcoding_flag_triggered(self):
        df = self._make_provider(high_cost_procedure_ratio=0.60)
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["flag_upcoding"] == True

    def test_high_reimbursement_flag_triggered(self):
        df = self._make_provider(reimbursement_per_patient=5000.0)
        result = self.apply_flags(df, self._peer_stats(avg=2000.0, std=500.0)).collect()[0]
        assert result["flag_high_reimbursement"] == True

    def test_multiple_flags_anomaly_count_correct(self):
        df = self._make_provider(
            duplicate_claim_ratio=0.10,
            deceased_patient_claims=3,
            weekend_claim_ratio=0.50
        )
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["anomaly_count"] == 3

    def test_anomaly_count_zero_for_clean_provider(self):
        df = self._make_provider()
        result = self.apply_flags(df, self._peer_stats()).collect()[0]
        assert result["anomaly_count"] == 0


# ─────────────────────────────────────────
# Tests for build_top_reasons
# ─────────────────────────────────────────

class TestBuildTopReasons:

    def setup_method(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages io.delta:delta-spark_2.13:4.2.0 pyspark-shell"
        )
        from pyspark.sql import SparkSession
        self.spark = (
            SparkSession.builder
            .appName("test_top_reasons")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        from fraud_detector import build_top_reasons
        self.build_reasons = build_top_reasons

    def _make_flagged(self, **kwargs):
        defaults = {
            "provider_id": "PRV99999",
            "flag_duplicate_claims":   False,
            "flag_deceased_billing":   False,
            "flag_high_volume":        False,
            "flag_upcoding":           False,
            "flag_weekend_billing":    False,
            "flag_high_reimbursement": False,
            "duplicate_claim_ratio":   0.0,
            "weekend_claim_ratio":     0.0,
            "high_cost_procedure_ratio": 0.0,
            "deceased_patient_claims": 0,
            "total_claims":            50,
            "unique_patients":         40,
            "anomaly_count":           0,
        }
        defaults.update(kwargs)
        return self.spark.createDataFrame([defaults])

    def test_no_flags_returns_empty_reasons(self):
        df = self._make_flagged()
        result = self.build_reasons(df).collect()[0]
        assert result["top_reasons"] is None or result["top_reasons"] == ""

    def test_deceased_billing_reason_included(self):
        df = self._make_flagged(flag_deceased_billing=True, deceased_patient_claims=12)
        result = self.build_reasons(df).collect()[0]
        assert "Deceased patient billing detected" in result["top_reasons"]
        assert "12" in result["top_reasons"]

    def test_duplicate_claims_reason_included(self):
        df = self._make_flagged(flag_duplicate_claims=True, duplicate_claim_ratio=0.15)
        result = self.build_reasons(df).collect()[0]
        assert "Duplicate claim ratio" in result["top_reasons"]

    def test_weekend_billing_reason_included(self):
        df = self._make_flagged(flag_weekend_billing=True, weekend_claim_ratio=0.45)
        result = self.build_reasons(df).collect()[0]
        assert "Weekend claim ratio" in result["top_reasons"]

    def test_high_volume_reason_included(self):
        df = self._make_flagged(flag_high_volume=True, total_claims=500, unique_patients=5)
        result = self.build_reasons(df).collect()[0]
        assert "Claim volume" in result["top_reasons"]

    def test_upcoding_reason_included(self):
        df = self._make_flagged(flag_upcoding=True, high_cost_procedure_ratio=0.65)
        result = self.build_reasons(df).collect()[0]
        assert "High cost procedure ratio" in result["top_reasons"]

    def test_high_reimbursement_reason_included(self):
        df = self._make_flagged(flag_high_reimbursement=True)
        result = self.build_reasons(df).collect()[0]
        assert "High reimbursement per patient vs peer group" in result["top_reasons"]

    def test_multiple_flags_all_reasons_included(self):
        df = self._make_flagged(
            flag_deceased_billing=True,
            deceased_patient_claims=5,
            flag_duplicate_claims=True,
            duplicate_claim_ratio=0.12,
            flag_weekend_billing=True,
            weekend_claim_ratio=0.40
        )
        result = self.build_reasons(df).collect()[0]
        assert "Deceased patient billing detected" in result["top_reasons"]
        assert "Duplicate claim ratio" in result["top_reasons"]
        assert "Weekend claim ratio" in result["top_reasons"]


