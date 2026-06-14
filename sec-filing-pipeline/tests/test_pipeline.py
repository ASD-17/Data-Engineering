"""
SEC Filing Intelligence Pipeline -- Unit Tests

Tests cover the three most critical functions in the pipeline:
- build_kafka_event: field extraction and URL construction from EDGAR API response
- detect_anomalies: rule-based anomaly flag detection from filing text
- score_sentiment_api: FinBERT API call handling including fallback behavior

Run with: pytest tests/test_pipeline.py -v
"""

import pytest
from unittest.mock import patch, MagicMock


# ─────────────────────────────────────────
# Tests for detect_anomalies
# ─────────────────────────────────────────

class TestDetectAnomalies:

    def setup_method(self):
        from src.enrichment.gold_enricher import detect_anomalies
        self.detect = detect_anomalies

    def test_going_concern_detected(self):
        text = "There is substantial doubt about the company's ability to continue as a going concern."
        flags = self.detect(text, "10-K")
        assert "going_concern" in flags

    def test_earnings_restatement_detected(self):
        text = "The company announced a restatement of its previously reported financial results due to a material weakness."
        flags = self.detect(text, "8-K")
        assert "earnings_restatement" in flags

    def test_sudden_ceo_change_detected(self):
        text = "The board announced that the Chief Executive Officer resigned effective immediately."
        flags = self.detect(text, "8-K")
        assert "sudden_ceo_change" in flags

    def test_legal_proceedings_detected(self):
        text = "The company is subject to an SEC investigation into its accounting practices."
        flags = self.detect(text, "10-K")
        assert "legal_proceedings" in flags

    def test_clean_filing_no_flags(self):
        text = "The company reported quarterly revenue of 500 million dollars, in line with analyst expectations."
        flags = self.detect(text, "10-Q")
        assert flags == []

    def test_empty_text_returns_no_flags(self):
        flags = self.detect("", "8-K")
        assert flags == []

    def test_none_text_returns_no_flags(self):
        flags = self.detect(None, "8-K")
        assert flags == []

    def test_multiple_flags_detected(self):
        text = """
        There is substantial doubt about the company's ability to continue as a going concern.
        The Chief Executive Officer resigned effective immediately.
        The company is subject to an SEC investigation.
        """
        flags = self.detect(text, "10-K")
        assert "going_concern" in flags
        assert "sudden_ceo_change" in flags
        assert "legal_proceedings" in flags
        assert len(flags) == 3

    def test_case_insensitive_detection(self):
        text = "SUBSTANTIAL DOUBT ABOUT THE ABILITY TO CONTINUE AS A GOING CONCERN."
        flags = self.detect(text, "10-K")
        assert "going_concern" in flags


# ─────────────────────────────────────────
# Tests for score_sentiment_api
# ─────────────────────────────────────────

class TestScoreSentimentApi:

    def setup_method(self):
        from src.enrichment.gold_enricher import score_sentiment_api
        self.score = score_sentiment_api

    def test_empty_text_returns_neutral(self):
        label, score = self.score("")
        assert label == "neutral"
        assert score == 0.5

    def test_none_text_returns_neutral(self):
        label, score = self.score(None)
        assert label == "neutral"
        assert score == 0.5

    def test_api_success_positive(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [[
            {"label": "positive", "score": 0.9556},
            {"label": "neutral",  "score": 0.0272},
            {"label": "negative", "score": 0.0171}
        ]]
        with patch("src.enrichment.gold_enricher.requests.post", return_value=mock_response):
            label, score = self.score("The company reported strong earnings growth.")
            assert label == "positive"
            assert score == 0.9556

    def test_api_success_negative(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [[
            {"label": "negative", "score": 0.8821},
            {"label": "neutral",  "score": 0.0912},
            {"label": "positive", "score": 0.0267}
        ]]
        with patch("src.enrichment.gold_enricher.requests.post", return_value=mock_response):
            label, score = self.score("Going concern doubt raised by auditors.")
            assert label == "negative"
            assert score == 0.8821

    def test_api_503_returns_neutral_fallback(self):
        mock_response = MagicMock()
        mock_response.status_code = 503
        with patch("src.enrichment.gold_enricher.requests.post", return_value=mock_response):
            label, score = self.score("Some filing text here.")
            assert label == "neutral"
            assert score == 0.5

    def test_api_exception_returns_neutral_fallback(self):
        with patch("src.enrichment.gold_enricher.requests.post", side_effect=Exception("Connection error")):
            label, score = self.score("Some filing text here.")
            assert label == "neutral"
            assert score == 0.5

    def test_score_is_float(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [[
            {"label": "neutral", "score": 0.9473},
        ]]
        with patch("src.enrichment.gold_enricher.requests.post", return_value=mock_response):
            label, score = self.score("Quarterly results in line with expectations.")
            assert isinstance(score, float)


# ─────────────────────────────────────────
# Tests for build_kafka_event
# ─────────────────────────────────────────

class TestBuildKafkaEvent:

    def setup_method(self):
        from src.ingestion.edgar_producer import build_kafka_event
        self.build = build_kafka_event

    def test_filing_type_extracted_from_root_forms(self):
        hit = {
            "_id": "0001651308-26-000017:bgne-20260611.htm",
            "_source": {
                "root_forms": ["8-K"],
                "file_date": "2026-06-11"
            }
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={}):
            event = self.build(hit)
            assert event["filing_type"] == "8-K"

    def test_filed_date_extracted(self):
        hit = {
            "_id": "0001651308-26-000017:bgne-20260611.htm",
            "_source": {
                "root_forms": ["10-K"],
                "file_date": "2026-06-11"
            }
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={}):
            event = self.build(hit)
            assert event["filed_date"] == "2026-06-11"

    def test_company_name_from_submissions_api(self):
        hit = {
            "_id": "0001651308-26-000017:bgne-20260611.htm",
            "_source": {
                "root_forms": ["8-K"],
                "file_date": "2026-06-11"
            }
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={
            "company_name": "BeOne Medicines Ltd.",
            "ticker": "ONC",
            "primary_doc": "bgne-20260611.htm"
        }):
            event = self.build(hit)
            assert event["company_name"] == "BeOne Medicines Ltd."
            assert event["ticker"] == "ONC"

    def test_filing_url_constructed_correctly(self):
        hit = {
            "_id": "0001651308-26-000017:bgne-20260611.htm",
            "_source": {
                "root_forms": ["8-K"],
                "file_date": "2026-06-11"
            }
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={
            "company_name": "BeOne Medicines Ltd.",
            "ticker": "ONC",
            "primary_doc": "bgne-20260611.htm"
        }):
            event = self.build(hit)
            expected_url = "https://www.sec.gov/Archives/edgar/data/1651308/000165130826000017/bgne-20260611.htm"
            assert event["filing_url"] == expected_url

    def test_ingestion_id_is_uuid(self):
        import uuid
        hit = {
            "_id": "0001651308-26-000017:bgne-20260611.htm",
            "_source": {"root_forms": ["8-K"], "file_date": "2026-06-11"}
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={}):
            event = self.build(hit)
            uuid.UUID(event["ingestion_id"])

    def test_source_is_edgar_api(self):
        hit = {
            "_id": "0001651308-26-000017:bgne-20260611.htm",
            "_source": {"root_forms": ["8-K"], "file_date": "2026-06-11"}
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={}):
            event = self.build(hit)
            assert event["source"] == "edgar_api"

    def test_empty_id_returns_empty_url(self):
        hit = {
            "_id": "",
            "_source": {"root_forms": ["8-K"], "file_date": "2026-06-11"}
        }
        with patch("src.ingestion.edgar_producer.get_filing_details", return_value={}):
            event = self.build(hit)
            assert event["filing_url"] == ""
