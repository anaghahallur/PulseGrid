# MIMIC-IV Demo Synchronized Cohort — Model Evaluation Report

**Selected Model:** Direct Ridge Regression

## Test Partition Overall Metrics

| Model Name | MAE (Beds) | RMSE (Beds) | WAPE (%) |
| :--- | :---: | :---: | :---: |
| Persistence Baseline | 0.10 | 0.32 | 6.5% |
| Seasonal-Naive Baseline (24h) | 0.21 | 0.45 | 13.0% |
| Direct Ridge Regression | 0.82 | 1.37 | 52.3% |
| Direct HistGradientBoosting | 0.56 | 0.74 | 35.4% |

> [!NOTE]
> In the 100-patient MIMIC-IV cohort, patient volume is low (mean census 1 to 8 beds per ward), so bed status changes are infrequent compared to a full hospital with hundreds of concurrent admissions. As a result, the Persistence Baseline (y_{t+h} = y_t) achieves a lower Test MAE than Direct Ridge.
