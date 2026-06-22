# Business Problem

## The Problem

Medicare and Medicaid process over one billion claims every year. Hospitals, physicians, pharmacies, and medical equipment suppliers submit these claims to get reimbursed for services provided to patients. The US government paid out roughly 900 billion dollars in Medicare and Medicaid expenditures in 2023. Approximately 10 percent of that, 90 billion dollars, is lost to fraud, waste, and abuse every single year.

The fraud is not obvious. A fraudulent provider looks identical to a legitimate one at the claim level. A physician who bills for procedures that never happened submits claims in the exact same format as a physician who actually performed them. The difference only becomes visible when you analyze patterns across thousands of claims over time. Billing for the same procedure on every single patient regardless of diagnosis. Seeing 50 patients in a single day when a typical physician sees 20. Prescribing controlled substances at rates ten times higher than peers in the same specialty.

Manual review cannot scale to this volume. A team of auditors reviewing claims one by one would take years to work through a single month of Medicare submissions. By the time fraud is detected through manual processes, millions of dollars have already been paid out and recovery is difficult.

## Who Is Affected

The US federal government bears the largest financial impact through the Centers for Medicare and Medicaid Services. Taxpayers fund these programs. Legitimate patients are affected when fraudulent providers consume program resources or bill using stolen identities. Honest healthcare providers are affected when fraud drives up regulatory scrutiny and administrative burden across the entire industry.

## Why This Pipeline Exists

This pipeline ingests Medicare claims data, processes it through a Medallion Architecture, and applies statistical anomaly detection to surface suspicious providers and billing patterns automatically. Instead of reviewing every claim, investigators receive a ranked list of high risk providers with specific flags explaining why each one was flagged. Abnormal billing frequency, unusual diagnosis to procedure combinations, statistical outliers relative to peer groups.

The goal is not to replace human investigators. The goal is to tell them where to look.

## Data Source

This pipeline uses the Medicare Provider Fraud Detection dataset from Kaggle, which contains real world structured claims data including inpatient claims, outpatient claims, beneficiary demographics, and provider information. The dataset includes labeled fraud cases, making it possible to validate detection accuracy against known outcomes.

## Success Criteria

A successful pipeline surfaces the same fraudulent providers that manual investigation would eventually find, but in minutes rather than months. Concretely:

Flag providers whose billing patterns deviate significantly from peers in the same specialty and region.
Detect anomalies in diagnosis to procedure combinations that suggest upcoding or unbundling.
Identify beneficiaries whose claim patterns suggest identity theft or unnecessary services.
Generate an alert severity ranking so investigators prioritize the highest risk cases first.