# Business Problem

## The Problem

Medicare and Medicaid process over one billion claims every year. Hospitals, physicians, pharmacies, and medical equipment suppliers submit these claims to get reimbursed for services provided to patients. The US government paid out roughly 900 billion dollars in Medicare and Medicaid expenditures in 2023. Approximately 10 percent of that, 90 billion dollars, is lost to fraud, waste, and abuse every single year.

The fraud is not obvious. A fraudulent provider looks identical to a legitimate one at the claim level. A physician who bills for procedures that never happened submits claims in the exact same format as a physician who actually performed them. The difference only becomes visible when you analyze patterns across thousands of claims over time. Billing for the same procedure on every single patient regardless of diagnosis. Seeing 50 patients in a single day when a typical physician sees 20. Filing claims for patients who were already deceased. Billing expensive procedures for simple conditions to maximize reimbursement.

Manual review cannot scale to this volume. A team of auditors reviewing claims one by one would take years to work through a single month of Medicare submissions. By the time fraud is detected through manual processes, millions of dollars have already been paid out and recovery is difficult.

## Who Is Affected

The US federal government bears the largest financial impact through the Centers for Medicare and Medicaid Services. Taxpayers fund these programs. Legitimate patients are affected when fraudulent providers consume program resources or bill using stolen identities. Honest healthcare providers face increased regulatory scrutiny and administrative burden because of fraud committed by others in the same system.

## Why This Pipeline Exists

This pipeline ingests Medicare claims data, processes it through a Medallion Architecture, and applies both rule based detection and machine learning anomaly detection to surface suspicious providers automatically. Instead of reviewing every claim, investigators receive a prioritized work queue with explainable alerts.

The explainability piece is critical. A risk score alone tells an investigator nothing. Knowing that a specific provider bills 4.2 times more than peers in the same specialty, has a 35 percent duplicate claim ratio, and filed claims for 12 deceased patients gives investigators a concrete starting point for their review.

The goal is not to replace human investigators. The goal is to tell them where to look and why.

## Data Source

This pipeline uses the Medicare Provider Fraud Detection dataset from Kaggle, which contains real world structured claims data including inpatient claims, outpatient claims, beneficiary demographics, and provider fraud labels. The dataset includes labeled fraud cases, making it possible to validate detection accuracy using precision, recall, F1 score, and ROC-AUC.

## Success Criteria

A successful pipeline surfaces the same fraudulent providers that manual investigation would eventually find, but in minutes rather than months.

Flag providers whose billing patterns deviate significantly from peers in the same region.
Detect duplicate claims, deceased patient billing, impossible claim volumes, and upcoding patterns.
Generate explainable alerts so investigators understand exactly why a provider was flagged.
Validate detection accuracy against known fraud labels using standard ML metrics.
Rank providers by risk so investigators prioritize the highest risk cases first.