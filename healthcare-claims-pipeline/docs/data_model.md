# Data Model

## Overview

The pipeline processes four source files from the Medicare Provider Fraud Detection dataset on Kaggle. Each file represents a different entity in the healthcare claims ecosystem. They are loaded into the Bronze layer as raw tables, joined and feature engineered in the Silver layer, and aggregated into fraud intelligence tables in the Gold layer.

## Source Data

The Kaggle dataset contains four CSV files.

Inpatient claims cover hospital admissions. Each row represents one hospital stay including admission date, discharge date, diagnosis codes, procedure codes, attending physician, and reimbursement amount.

Outpatient claims cover doctor visits and outpatient procedures. Each row represents one visit including diagnosis codes, procedure codes, provider identifier, and claim amount.

Beneficiary data covers patient demographics. Each row represents one Medicare patient including date of birth, date of death, chronic condition flags for eleven conditions, and annual reimbursement totals.

Provider labels are the fraud ground truth. Each row is a provider identifier with a binary label indicating whether that provider was found to be fraudulent.

## Bronze Layer

Bronze stores raw data exactly as received from the CSV files. No transformations applied. Data types remain as strings. A load timestamp is added to every record.

### healthcare_catalog.bronze.inpatient_claims

BeneID, ClaimID, ClaimStartDt, ClaimEndDt, Provider, InscClaimAmtReimbursed, AttendingPhysician, OperatingPhysician, OtherPhysician, AdmissionDt, ClmAdmitDiagnosisCode, DeductibleAmtPaid, DischargeDt, DiagnosisGroupCode, ClmDiagnosisCode 1 through 10, ClmProcedureCode 1 through 6, load_timestamp

### healthcare_catalog.bronze.outpatient_claims

BeneID, ClaimID, ClaimStartDt, ClaimEndDt, Provider, InscClaimAmtReimbursed, AttendingPhysician, OperatingPhysician, OtherPhysician, ClmDiagnosisCode 1 through 10, ClmProcedureCode 1 through 6, DeductibleAmtPaid, ClmAdmitDiagnosisCode, load_timestamp

### healthcare_catalog.bronze.beneficiary_data

BeneID, DOB, DOD, Gender, Race, RenalDiseaseIndicator, State, County, NoOfMonths_PartACov, NoOfMonths_PartBCov, ChronicCond_Alzheimer, ChronicCond_Heartfailure, ChronicCond_KidneyDisease, ChronicCond_Cancer, ChronicCond_ObstrPulmonary, ChronicCond_Depression, ChronicCond_Diabetes, ChronicCond_IschemicHeart, ChronicCond_Osteoporasis, ChronicCond_rheumatoidarthritis, ChronicCond_stroke, IPAnnualReimbursementAmt, IPAnnualDeductibleAmt, OPAnnualReimbursementAmt, OPAnnualDeductibleAmt, load_timestamp

### healthcare_catalog.bronze.provider_labels

Provider, PotentialFraud, load_timestamp

## Silver Layer

Silver cleans, joins, and engineers features from the Bronze tables. Two tables are produced.

### healthcare_catalog.silver.claims_enriched

One row per claim with beneficiary demographics attached. This table joins inpatient and outpatient claims with beneficiary data so every claim carries patient context alongside the claim financials.

claim_id, claim_type (inpatient or outpatient), provider_id, beneficiary_id, claim_start_date, claim_end_date, claim_duration_days, reimbursement_amount, deductible_paid, attending_physician, diagnosis_codes (array), procedure_codes (array), beneficiary_age, beneficiary_gender, beneficiary_state, chronic_condition_count, is_deceased, is_weekend_claim, processed_timestamp

### healthcare_catalog.silver.provider_features

One row per provider aggregating all their claims into a behavioral profile. This is the table that feeds directly into fraud detection.

| Column | Description |
|---|---|
| provider_id | Provider identifier |
| total_claims | Total claims submitted across inpatient and outpatient |
| total_inpatient_claims | Inpatient claims count |
| total_outpatient_claims | Outpatient claims count |
| total_reimbursement | Total amount reimbursed |
| avg_claim_amount | Average reimbursement per claim |
| reimbursement_per_patient | Total reimbursement divided by unique patients |
| unique_patients | Distinct beneficiaries served |
| unique_physicians | Distinct physicians associated with provider |
| avg_claim_duration_days | Average hospital stay length |
| avg_chronic_conditions | Average chronic conditions per patient |
| duplicate_claim_ratio | Proportion of duplicate claim IDs |
| weekend_claim_ratio | Proportion of claims filed on weekends |
| high_cost_procedure_ratio | Proportion of claims with high cost procedure codes |
| distinct_diagnosis_codes | Unique diagnosis codes used |
| distinct_procedure_codes | Unique procedure codes used |
| deceased_patient_claims | Claims filed after beneficiary date of death |
| is_fraud | Ground truth fraud label from provider labels |
| processed_timestamp | When profile was computed |

## Gold Layer

Gold contains the outputs of fraud detection. These four tables are queried directly by the dashboard and used by investigators.

### healthcare_catalog.gold.provider_risk_scores

One row per provider with fraud risk score, anomaly flags, and explainability reasons.

| Column | Description |
|---|---|
| provider_id | Provider identifier |
| risk_score | Isolation Forest anomaly score 0 to 1 |
| risk_label | Critical, high, medium, or low |
| flag_duplicate_claims | Duplicate claim ratio above threshold |
| flag_deceased_billing | Claims submitted after patient death |
| flag_high_volume | Claim volume impossible for one provider |
| flag_upcoding | High cost procedure ratio above peer group |
| flag_weekend_billing | Weekend claim ratio above threshold |
| flag_high_reimbursement | Reimbursement per patient above peer group |
| anomaly_count | Total flags triggered |
| alert_severity | Critical, high, medium, or none |
| top_reasons | Human readable explanation of top flags |
| is_fraud | Ground truth label for validation |
| precision | Model precision at this threshold |
| recall | Model recall at this threshold |
| scored_timestamp | When score was computed |

### healthcare_catalog.gold.fraud_alerts

One row per provider alert ordered by risk score for investigator review.

alert_id, provider_id, alert_severity, risk_score, top_reasons, total_claims, total_reimbursement, unique_patients, anomaly_count, is_fraud, alert_timestamp

### healthcare_catalog.gold.provider_peer_benchmark

One row per provider comparing their metrics against the average of all providers. This powers the explainability layer by showing how far each provider deviates from peers.

provider_id, total_claims, peer_avg_total_claims, claims_vs_peer_ratio, avg_claim_amount, peer_avg_claim_amount, amount_vs_peer_ratio, reimbursement_per_patient, peer_avg_reimbursement_per_patient, reimbursement_vs_peer_ratio, weekend_claim_ratio, peer_avg_weekend_ratio, benchmark_timestamp

### healthcare_catalog.gold.investigator_work_queue

One row per provider flagged for investigation, ordered by priority. This is the operational table investigators open every morning.

queue_id, provider_id, priority_rank, alert_severity, risk_score, top_reasons, total_claims, total_reimbursement, days_since_first_claim, assigned_to, status (open, in review, closed), queue_timestamp