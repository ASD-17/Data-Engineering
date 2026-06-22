# Data Model

## Overview

The pipeline processes four source files from the Medicare Provider Fraud Detection dataset on Kaggle. Each file represents a different entity in the healthcare claims ecosystem. They are kept separate in the Bronze layer to preserve source fidelity, joined and enriched in the Silver layer to create analytical views, and aggregated in the Gold layer to produce fraud alerts and provider risk scores.

## Source Data

The Kaggle dataset contains four CSV files:

Inpatient claims cover hospital admissions. Each row represents one hospital stay including admission date, discharge date, diagnosis codes, procedure codes, attending physician, and reimbursement amount.

Outpatient claims cover doctor visits, lab tests, and outpatient procedures. Each row represents one visit including diagnosis codes, procedure codes, provider identifier, and claim amount.

Beneficiary data covers patient demographics. Each row represents one Medicare patient including age, gender, state, chronic condition flags for ten common conditions, and annual reimbursement totals.

Provider labels are the fraud ground truth. Each row is a provider identifier with a binary label indicating whether that provider was found to be fraudulent through investigation.

## Bronze Layer

Bronze stores raw data exactly as received from the source files. No transformations are applied. Schema is inferred from the CSV files and stored as Delta Lake tables.

### healthcare_catalog.bronze.inpatient_claims

| Column | Type | Description |
|---|---|---|
| BeneID | string | Beneficiary identifier |
| ClaimID | string | Unique claim identifier |
| ClaimStartDt | string | Admission date |
| ClaimEndDt | string | Discharge date |
| Provider | string | Provider identifier |
| InscClaimAmtReimbursed | string | Amount reimbursed |
| AttendingPhysician | string | Attending physician code |
| OperatingPhysician | string | Operating physician code |
| OtherPhysician | string | Other physician code |
| AdmissionDt | string | Hospital admission date |
| ClmAdmitDiagnosisCode | string | Primary admission diagnosis |
| DeductibleAmtPaid | string | Deductible paid by patient |
| ClmDiagnosisCode 1 to 10 | string | Diagnosis codes |
| ClmProcedureCode 1 to 6 | string | Procedure codes |

### healthcare_catalog.bronze.outpatient_claims

| Column | Type | Description |
|---|---|---|
| BeneID | string | Beneficiary identifier |
| ClaimID | string | Unique claim identifier |
| ClaimStartDt | string | Claim start date |
| ClaimEndDt | string | Claim end date |
| Provider | string | Provider identifier |
| InscClaimAmtReimbursed | string | Amount reimbursed |
| AttendingPhysician | string | Attending physician code |
| ClmDiagnosisCode 1 to 10 | string | Diagnosis codes |
| ClmProcedureCode 1 to 6 | string | Procedure codes |

### healthcare_catalog.bronze.beneficiary_data

| Column | Type | Description |
|---|---|---|
| BeneID | string | Beneficiary identifier |
| DOB | string | Date of birth |
| DOD | string | Date of death if applicable |
| Gender | string | Gender |
| Race | string | Race |
| State | string | State code |
| County | string | County code |
| ChronicCond Alzheimer | string | Alzheimer flag |
| ChronicCond Heartfailure | string | Heart failure flag |
| ChronicCond KidneyDisease | string | Kidney disease flag |
| ChronicCond Cancer | string | Cancer flag |
| ChronicCond ObstrPulmonary | string | COPD flag |
| ChronicCond Depression | string | Depression flag |
| ChronicCond Diabetes | string | Diabetes flag |
| ChronicCond IschemicHeart | string | Ischemic heart disease flag |
| ChronicCond Osteoporasis | string | Osteoporosis flag |
| ChronicCond rheumatoidArthritis | string | Rheumatoid arthritis flag |
| ChronicCond stroke | string | Stroke flag |
| IPAnnualReimbursementAmt | string | Annual inpatient reimbursement |
| OPAnnualReimbursementAmt | string | Annual outpatient reimbursement |

### healthcare_catalog.bronze.provider_labels

| Column | Type | Description |
|---|---|---|
| Provider | string | Provider identifier |
| PotentialFraud | string | Yes or No fraud label |

## Silver Layer

Silver joins the four Bronze tables into two analytical views. Data types are cast to their correct types. Null handling is applied. Derived features are computed to support anomaly detection.

### healthcare_catalog.silver.claims_enriched

One row per claim combining claim details with beneficiary demographics.

| Column | Type | Description |
|---|---|---|
| claim_id | string | Unique claim identifier |
| claim_type | string | Inpatient or outpatient |
| provider_id | string | Provider identifier |
| beneficiary_id | string | Beneficiary identifier |
| claim_start_date | date | Claim start date |
| claim_end_date | date | Claim end date |
| claim_duration_days | integer | Days between start and end |
| reimbursement_amount | double | Amount reimbursed |
| deductible_paid | double | Deductible paid by patient |
| attending_physician | string | Attending physician code |
| diagnosis_codes | array of string | All diagnosis codes |
| procedure_codes | array of string | All procedure codes |
| beneficiary_age | integer | Age derived from DOB |
| beneficiary_gender | string | Gender |
| beneficiary_state | string | State code |
| chronic_condition_count | integer | Number of chronic conditions |
| is_deceased | boolean | Whether beneficiary is deceased |
| processed_timestamp | timestamp | When record was processed |

### healthcare_catalog.silver.provider_profiles

One row per provider aggregating all their claims into a behavioral profile.

| Column | Type | Description |
|---|---|---|
| provider_id | string | Provider identifier |
| total_claims | integer | Total number of claims submitted |
| total_inpatient_claims | integer | Inpatient claims count |
| total_outpatient_claims | integer | Outpatient claims count |
| total_reimbursement | double | Total amount reimbursed |
| avg_reimbursement_per_claim | double | Average claim amount |
| unique_beneficiaries | integer | Distinct patients served |
| unique_physicians | integer | Distinct physicians associated |
| avg_claim_duration_days | double | Average hospital stay length |
| avg_chronic_conditions | double | Average chronic conditions per patient |
| distinct_diagnosis_codes | integer | Unique diagnosis codes used |
| distinct_procedure_codes | integer | Unique procedure codes used |
| is_fraud | boolean | Ground truth fraud label |
| processed_timestamp | timestamp | When profile was computed |

## Gold Layer

Gold contains the outputs of anomaly detection. These are the tables queried by the dashboard and used by investigators.

### healthcare_catalog.gold.provider_risk_scores

One row per provider with fraud risk scores and anomaly flags.

| Column | Type | Description |
|---|---|---|
| provider_id | string | Provider identifier |
| risk_score | double | Anomaly score from Isolation Forest |
| risk_label | string | High, medium, or low risk |
| flag_high_claim_volume | boolean | Claims per beneficiary above threshold |
| flag_high_reimbursement | boolean | Average reimbursement above peer group |
| flag_short_stay_upcoding | boolean | Short stays billed as long stays |
| flag_duplicate_claims | boolean | Same claim submitted multiple times |
| flag_deceased_beneficiary | boolean | Claims submitted after patient death |
| anomaly_count | integer | Total number of flags triggered |
| alert_severity | string | Critical, high, medium, or none |
| is_fraud | boolean | Ground truth label for validation |
| scored_timestamp | timestamp | When score was computed |

### healthcare_catalog.gold.fraud_alerts

One row per provider alert ordered by risk score for investigator review.

| Column | Type | Description |
|---|---|---|
| alert_id | string | Unique alert identifier |
| provider_id | string | Provider identifier |
| alert_severity | string | Critical, high, medium |
| risk_score | double | Anomaly score |
| top_flags | array of string | Most significant anomaly flags |
| total_claims | integer | Total claims submitted |
| total_reimbursement | double | Total amount reimbursed |
| unique_beneficiaries | integer | Distinct patients served |
| alert_timestamp | timestamp | When alert was generated |