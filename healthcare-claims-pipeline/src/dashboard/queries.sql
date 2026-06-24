-- =============================================================================
-- Healthcare Claims Anomaly Detection Pipeline -- Dashboard Queries
-- Target: Databricks SQL Dashboard
-- Layer: Gold (healthcare_catalog.gold)
-- =============================================================================


-- =============================================================================
-- Query 1: Executive Summary
-- Purpose: High level snapshot of fraud alerts across the entire dataset.
-- This is the first panel investigators see every morning. It answers
-- how many providers are flagged, how much financial exposure exists,
-- and what the model performance looks like.
-- =============================================================================

SELECT
    COUNT(*)                                                    AS total_providers_scored,
    SUM(CASE WHEN alert_severity = 'critical' THEN 1 ELSE 0 END) AS critical_alerts,
    SUM(CASE WHEN alert_severity = 'high'     THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN alert_severity = 'medium'   THEN 1 ELSE 0 END) AS medium_alerts,
    SUM(CASE WHEN alert_severity = 'none'     THEN 1 ELSE 0 END) AS no_alerts,
    SUM(CASE WHEN is_fraud = true             THEN 1 ELSE 0 END) AS known_fraud_providers,
    ROUND(AVG(precision), 4)                                    AS model_precision,
    ROUND(AVG(recall), 4)                                       AS model_recall,
    ROUND(AVG(f1_score), 4)                                     AS model_f1,
    ROUND(AVG(roc_auc), 4)                                      AS model_roc_auc
FROM healthcare_catalog.gold.provider_risk_scores;


-- =============================================================================
-- Query 2: Provider Risk Ranking
-- Purpose: Ranked list of all flagged providers ordered by risk score.
-- Investigators use this to decide which providers to investigate first.
-- Each row includes the human readable reasons for the alert so investigators
-- know exactly what triggered the flag before they start their review.
-- =============================================================================

SELECT
    f.priority_rank,
    f.provider_id,
    f.alert_severity,
    ROUND(f.risk_score, 4)                                      AS risk_score,
    f.anomaly_count,
    f.top_reasons,
    f.total_claims,
    ROUND(f.total_reimbursement, 2)                             AS total_reimbursement,
    f.unique_patients,
    f.is_fraud,
    f.alert_timestamp
FROM healthcare_catalog.gold.investigator_work_queue f
ORDER BY f.priority_rank ASC
LIMIT 100;


-- =============================================================================
-- Query 3: Suspicious Billing Patterns
-- Purpose: Deep dive into which specific fraud flags are most common
-- across the flagged provider population. Shows investigators which
-- fraud patterns are most prevalent in the current dataset.
-- =============================================================================

SELECT
    'Deceased Patient Billing'      AS fraud_pattern,
    SUM(CASE WHEN flag_deceased_billing    = true THEN 1 ELSE 0 END) AS providers_flagged
FROM healthcare_catalog.gold.provider_risk_scores
UNION ALL
SELECT
    'Duplicate Claims'              AS fraud_pattern,
    SUM(CASE WHEN flag_duplicate_claims    = true THEN 1 ELSE 0 END) AS providers_flagged
FROM healthcare_catalog.gold.provider_risk_scores
UNION ALL
SELECT
    'High Claim Volume'             AS fraud_pattern,
    SUM(CASE WHEN flag_high_volume         = true THEN 1 ELSE 0 END) AS providers_flagged
FROM healthcare_catalog.gold.provider_risk_scores
UNION ALL
SELECT
    'Upcoding'                      AS fraud_pattern,
    SUM(CASE WHEN flag_upcoding            = true THEN 1 ELSE 0 END) AS providers_flagged
FROM healthcare_catalog.gold.provider_risk_scores
UNION ALL
SELECT
    'Weekend Billing Anomaly'       AS fraud_pattern,
    SUM(CASE WHEN flag_weekend_billing     = true THEN 1 ELSE 0 END) AS providers_flagged
FROM healthcare_catalog.gold.provider_risk_scores
UNION ALL
SELECT
    'High Reimbursement vs Peers'   AS fraud_pattern,
    SUM(CASE WHEN flag_high_reimbursement  = true THEN 1 ELSE 0 END) AS providers_flagged
FROM healthcare_catalog.gold.provider_risk_scores
ORDER BY providers_flagged DESC;


-- =============================================================================
-- Query 4: Provider Peer Benchmark
-- Purpose: Shows how each flagged provider compares to the peer group average.
-- This powers the explainability layer. Instead of just showing a risk score,
-- investigators see that a provider bills 4.2x more than peers or has a
-- reimbursement rate 3.1x above the average.
-- =============================================================================

SELECT
    b.provider_id,
    b.alert_severity,
    ROUND(b.risk_score, 4)                                          AS risk_score,
    b.total_claims,
    b.peer_avg_total_claims,
    ROUND(b.claims_vs_peer_ratio, 2)                                AS claims_vs_peer_ratio,
    ROUND(b.reimbursement_per_patient, 2)                           AS reimbursement_per_patient,
    b.peer_avg_reimbursement_per_patient,
    ROUND(b.reimbursement_vs_peer_ratio, 2)                         AS reimbursement_vs_peer_ratio,
    ROUND(b.weekend_claim_ratio, 4)                                 AS weekend_claim_ratio,
    b.peer_avg_weekend_ratio,
    ROUND(b.duplicate_claim_ratio, 4)                               AS duplicate_claim_ratio,
    b.peer_avg_duplicate_ratio
FROM healthcare_catalog.gold.provider_peer_benchmark b
WHERE b.alert_severity IN ('critical', 'high')
ORDER BY b.risk_score DESC
LIMIT 50;


-- =============================================================================
-- Query 5: Investigator Work Queue
-- Purpose: Prioritized list of open cases for investigator review.
-- Shows only open cases sorted by priority rank. Investigators work
-- through this list top to bottom. Each case includes the top reasons
-- so they know what to look for before pulling the full claim history.
-- =============================================================================

SELECT
    queue_id,
    priority_rank,
    provider_id,
    alert_severity,
    ROUND(risk_score, 4)                                            AS risk_score,
    top_reasons,
    total_claims,
    ROUND(total_reimbursement, 2)                                   AS total_reimbursement,
    unique_patients,
    anomaly_count,
    status,
    assigned_to,
    is_fraud,
    queue_timestamp
FROM healthcare_catalog.gold.investigator_work_queue
WHERE status = 'open'
ORDER BY priority_rank ASC;


-- =============================================================================
-- Query 6: Fraud Detection Accuracy by Severity
-- Purpose: Breaks down model accuracy at each alert severity level.
-- Shows what percentage of providers flagged at each severity level
-- are actually confirmed fraudsters. Helps investigators calibrate
-- how much trust to place in each severity level.
-- =============================================================================

SELECT
    alert_severity,
    COUNT(*)                                                        AS total_providers,
    SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)               AS confirmed_fraud,
    ROUND(
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) * 100.0 /
        COUNT(*), 2
    )                                                               AS fraud_hit_rate_pct,
    ROUND(AVG(risk_score), 4)                                       AS avg_risk_score,
    ROUND(AVG(anomaly_count), 2)                                    AS avg_anomaly_count
FROM healthcare_catalog.gold.provider_risk_scores
GROUP BY alert_severity
ORDER BY
    CASE alert_severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'none'     THEN 4
    END;