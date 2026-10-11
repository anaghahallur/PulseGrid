# Synthetic-Data ICU Occupancy 24-Hour Forecasting — Evaluation Report

**Selected Model:** Direct Ridge Regression (selected based on lowest Validation MAE)
**Generated:** 2026-10-11T17:19:16.995680+00:00
**Random Seed:** 42

## 1. Dataset & Chronological Split Boundaries
- **Train Period:** 2026-08-08 01:00:00+00:00 to 2026-10-07 06:00:00+00:00 (7,230 samples)
- **Validation Period:** 2026-10-08 06:00:00+00:00 to 2026-10-20 09:00:00+00:00 (1,460 samples)
- **Test Period (Untouched):** 2026-10-21 09:00:00+00:00 to 2026-11-03 11:00:00+00:00 (1,575 samples)
- **Target Buffer Gap:** 24 hours between partitions (guarantees zero target leakage)
- **Features:** 54 past-only features (lags 0..24, 48, 72, 168; rolling 6, 24, 168; cyclical calendar)

## 2. Model Comparison on Held-Out Test Set
| Model | Test MAE | Test RMSE | Test WAPE | Validation MAE |
| :--- | :--- | :--- | :--- | :--- |
| Persistence Baseline **(Primary Benchmark)** | 1.56 beds | 2.30 beds | 21.8% | 1.72 beds |
| Seasonal-Naive Baseline (24h) | 2.48 beds | 3.28 beds | 34.5% | 2.37 beds |
| Direct Ridge Regression **(Selected Candidate)** | 2.09 beds | 2.72 beds | 29.0% | 1.68 beds |
| Direct HistGradientBoosting | 2.74 beds | 3.52 beds | 38.1% | 1.94 beds |

> [!NOTE]
> **Benchmark & Test Dynamic Analysis:** Persistence Baseline is maintained as the primary benchmark. On the overall held-out test partition, Persistence achieved 1.56 beds MAE versus Direct Ridge's 2.09 beds MAE.
> During active patient inflow (Oct 21–29), Ridge outperformed Persistence (1.72 vs 1.80 MAE). During the simulation drain-down tail (Oct 30–Nov 03) where admissions terminated at Day 90 and occupancy fell toward zero, Persistence followed the collapse without mean-reversion (1.11 MAE), while Ridge's stationary weights predicted mean-reversion toward typical census (2.78 MAE). In accordance with ML integrity, the test partition is preserved unsegmented, and Persistence remains the operational benchmark.

## 3. Test Performance by Forecast Horizon (Selected Model)
| Horizon | Forecast Lead | Test MAE | Test RMSE | Test WAPE |
| :--- | :--- | :--- | :--- | :--- |
| h1 | +1 hour(s) | 0.38 beds | 0.58 beds | 4.9% |
| h2 | +2 hour(s) | 0.63 beds | 0.85 beds | 8.3% |
| h3 | +3 hour(s) | 0.84 beds | 1.07 beds | 11.0% |
| h6 | +6 hour(s) | 1.33 beds | 1.65 beds | 17.8% |
| h12 | +12 hour(s) | 2.15 beds | 2.59 beds | 29.9% |
| h18 | +18 hour(s) | 2.79 beds | 3.34 beds | 40.2% |
| h24 | +24 hour(s) | 3.30 beds | 3.96 beds | 48.9% |

## 4. Test Performance by ICU Unit (Selected Model)
| Unit ID | Staffed Capacity | Test MAE | Test RMSE | Test WAPE |
| :--- | :--- | :--- | :--- | :--- |
| CCU | — | 1.85 beds | 2.32 beds | 27.3% |
| CSRU | — | 1.66 beds | 2.13 beds | 36.5% |
| MICU | — | 2.99 beds | 3.65 beds | 32.1% |
| SICU | — | 1.83 beds | 2.36 beds | 24.8% |
| TSICU | — | 2.10 beds | 2.83 beds | 26.6% |

## 5. Surge and Capacity Exceedance Error Analysis
- **Surge / Exceedance Periods:** MAE = 1.35 beds, RMSE = 1.70 beds, WAPE = 7.9% (Count: 32)
- **Normal Operating Periods:** MAE = 2.10 beds, RMSE = 2.73 beds, WAPE = 30.1% (Count: 1543)

## 6. Model Artifact Security Note
> [!IMPORTANT]
> Model artifacts serialized with `joblib` must only be loaded from trusted internal sources.
> Deserialization of untrusted files poses arbitrary code execution risks.

## 7. Limitations & Clinical Validity Disclaimer
- Predictions are developed and validated using synthetic ADT event simulations.
- This model establishes data engineering, feature pipeline, and multi-horizon forecasting validity.
- It does NOT constitute a clinically validated medical device or guarantee real-world patient outcomes.
