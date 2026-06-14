-- =============================================================================
-- SEC Filing Intelligence Pipeline -- Dashboard Queries
-- Target: Databricks SQL Dashboard
-- Layer: Gold (gold.sec_filings_enriched, gold.company_sentiment_summary)
-- Author: Agasya Sandilya Devarasetty
-- =============================================================================


-- =============================================================================
-- Query 1: Live Filing Feed
-- Purpose: Real-time feed of the most recent SEC filings with sentiment scores
-- and alert severity. This is the first panel analysts see every morning.
-- Refreshes automatically as new filings flow through the pipeline.
-- =============================================================================

SELECT
    filing_type,
    company_name,
    ticker,
    filed_date,
    sentiment_label,
    ROUND(sentiment_score, 4)   AS sentiment_score,
    anomaly_count,
    alert_severity,
    enrichment_timestamp
FROM gold.sec_filings_enriched
ORDER BY enrichment_timestamp DESC
LIMIT 50;


-- =============================================================================
-- Query 2: Anomaly Alert Panel
-- Purpose: Surfaces all filings that triggered an alert, grouped by severity.
-- Compliance teams use this to prioritize which filings to investigate first.
-- Critical alerts require same-day review. High alerts within 24 hours.
-- =============================================================================

SELECT
    alert_severity,
    company_name,
    ticker,
    filing_type,
    filed_date,
    anomaly_flags,
    anomaly_count,
    sentiment_label,
    ROUND(sentiment_score, 4)   AS sentiment_score
FROM gold.sec_filings_enriched
WHERE alert_triggered = TRUE
ORDER BY
    CASE alert_severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
        ELSE 5
    END,
    filed_date DESC;


-- =============================================================================
-- Query 3: Company Sentiment Trend (Rolling 90 Days)
-- Purpose: Shows how sentiment is trending for each company over the last
-- 90 days. A company moving from positive to negative over multiple filings
-- is an early warning signal for analysts to investigate.
-- =============================================================================

SELECT
    company_name,
    ticker,
    filed_date,
    sentiment_label,
    ROUND(sentiment_score, 4)       AS sentiment_score,
    ROUND(AVG(sentiment_score) OVER (
        PARTITION BY ticker
        ORDER BY filed_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4)                         AS rolling_7day_avg_sentiment,
    anomaly_count,
    alert_severity
FROM gold.sec_filings_enriched
WHERE filed_date >= DATE_SUB(CURRENT_DATE(), 90)
    AND ticker IS NOT NULL
    AND ticker != ''
ORDER BY ticker, filed_date DESC;


-- =============================================================================
-- Query 4: Filing Volume by Type and Date
-- Purpose: Daily count of filings broken down by type (10-K, 10-Q, 8-K).
-- Powers the bar chart showing filing activity over time.
-- Sudden spikes in 8-K volume often indicate market-moving events.
-- =============================================================================

SELECT
    filed_date,
    filing_type,
    COUNT(*)                                AS filing_count,
    SUM(COUNT(*)) OVER (
        PARTITION BY filed_date
    )                                       AS total_filings_that_day,
    ROUND(AVG(sentiment_score), 4)          AS avg_sentiment,
    SUM(CASE WHEN alert_triggered THEN 1 ELSE 0 END) AS alerts_triggered
FROM gold.sec_filings_enriched
WHERE filed_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY filed_date, filing_type
ORDER BY filed_date DESC, filing_count DESC;


-- =============================================================================
-- Query 5: Top Risk Companies
-- Purpose: Ranks companies by total number of anomaly flags triggered across
-- all their filings. Companies at the top of this list warrant deeper research.
-- Uses the pre-aggregated company sentiment summary table for fast performance.
-- =============================================================================

SELECT
    company_name,
    ticker,
    total_filings,
    total_alerts,
    critical_alerts,
    ROUND(avg_sentiment, 4)                 AS avg_sentiment,
    ROUND(total_alerts / total_filings, 4)  AS alert_rate_per_filing
FROM gold.company_sentiment_summary
WHERE total_filings > 0
ORDER BY total_alerts DESC, critical_alerts DESC
LIMIT 25;


-- =============================================================================
-- Query 6: Sentiment Distribution
-- Purpose: Breakdown of positive, negative, and neutral filings across the
-- entire dataset. Powers the pie chart on the summary dashboard.
-- A rising share of negative filings across the market is a macro risk signal.
-- =============================================================================

SELECT
    sentiment_label,
    COUNT(*)                                            AS filing_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(sentiment_score), 4)                      AS avg_confidence_score
FROM gold.sec_filings_enriched
GROUP BY sentiment_label
ORDER BY filing_count DESC;