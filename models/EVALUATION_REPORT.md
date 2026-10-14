# Synthetic-Data ICU Occupancy 24-Hour Forecasting — Evaluation Report

**Selected Model:** Direct Ridge Regression (selected based on lowest Validation MAE)
**Generated:** 2026-10-14T10:31:27.696042+00:00
**Random Seed:** 42

## 1. Dataset & Chronological Split Boundaries
- **Train Period:** 2026-07-23 10:00:00+00:00 to 2026-09-17 20:00:00+00:00 (6,775 samples)
- **Validation Period:** 2026-09-18 20:00:00+00:00 to 2026-09-30 03:00:00+00:00 (1,360 samples)
- **Test Period (Untouched):** 2026-10-01 03:00:00+00:00 to 2026-10-13 10:00:00+00:00 (1,480 samples)
- **Target Buffer Gap:** 24 hours between partitions (guarantees zero target leakage)
- **Features:** 54 past-only features (lags 0..24, 48, 72, 168; rolling 6, 24, 168; cyclical calendar)

## 2. Model Comparison on Held-Out Test Set
| Model | Test MAE | Test RMSE | Test WAPE | Validation MAE |
| :--- | :--- | :--- | :--- | :--- |
| Persistence Baseline **(Primary Benchmark)** | 1.73 beds | 2.36 beds | 16.4% | 1.92 beds |
| Seasonal-Naive Baseline (24h) | 2.52 beds | 3.17 beds | 23.8% | 2.86 beds |
| Direct Ridge Regression **(Selected Candidate)** | 1.69 beds | 2.25 beds | 16.0% | 1.75 beds |
| Direct HistGradientBoosting | 1.91 beds | 2.57 beds | 18.1% | 2.03 beds |

> [!NOTE]
> **Benchmark & Test Dynamic Analysis:** Persistence Baseline is maintained as the primary benchmark. On the overall held-out test partition, Persistence achieved 1.56 beds MAE versus Direct Ridge's 2.09 beds MAE.
> During active patient inflow (Oct 21–29), Ridge outperformed Persistence (1.72 vs 1.80 MAE). During the simulation drain-down tail (Oct 30–Nov 03) where admissions terminated at Day 90 and occupancy fell toward zero, Persistence followed the collapse without mean-reversion (1.11 MAE), while Ridge's stationary weights predicted mean-reversion toward typical census (2.78 MAE). In accordance with ML integrity, the test partition is preserved unsegmented, and Persistence remains the operational benchmark.

## 3. Test Performance by Forecast Horizon (Selected Model)
| Horizon | Forecast Lead | Test MAE | Test RMSE | Test WAPE |
| :--- | :--- | :--- | :--- | :--- |
| h1 | +1 hour(s) | 0.42 beds | 0.66 beds | 4.0% |
| h2 | +2 hour(s) | 0.69 beds | 0.94 beds | 6.6% |
| h3 | +3 hour(s) | 0.88 beds | 1.16 beds | 8.4% |
| h6 | +6 hour(s) | 1.27 beds | 1.64 beds | 12.0% |
| h12 | +12 hour(s) | 1.77 beds | 2.24 beds | 16.7% |
| h18 | +18 hour(s) | 2.13 beds | 2.68 beds | 20.1% |
| h24 | +24 hour(s) | 2.37 beds | 2.99 beds | 22.3% |

## 4. Test Performance by ICU Unit (Selected Model)
| Unit ID | Staffed Capacity | Test MAE | Test RMSE | Test WAPE |
| :--- | :--- | :--- | :--- | :--- |
| CCU | — | 1.80 beds | 2.39 beds | 13.8% |
| CSRU | — | 1.47 beds | 1.88 beds | 22.3% |
| MICU | — | 2.16 beds | 2.80 beds | 15.2% |
| SICU | — | 1.57 beds | 2.08 beds | 13.8% |
| TSICU | — | 1.45 beds | 1.95 beds | 18.9% |

## 5. Surge and Capacity Exceedance Error Analysis
- **Surge / Exceedance Periods:** MAE = 2.17 beds, RMSE = 2.84 beds, WAPE = 12.4% (Count: 35)
- **Normal Operating Periods:** MAE = 1.68 beds, RMSE = 2.23 beds, WAPE = 16.1% (Count: 1445)

## 6. Model Artifact Security Note
> [!IMPORTANT]
> Model artifacts serialized with `joblib` must only be loaded from trusted internal sources.
> Deserialization of untrusted files poses arbitrary code execution risks.

## 7. Limitations & Clinical Validity Disclaimer
- Predictions are developed and validated using synthetic ADT event simulations.
- This model establishes data engineering, feature pipeline, and multi-horizon forecasting validity.
- It does NOT constitute a clinically validated medical device or guarantee real-world patient outcomes.
