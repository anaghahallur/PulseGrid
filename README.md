# ICU Bed Demand Forecasting

A real-time big data streaming, forecasting, and operational capacity planning system for Intensive Care Units (ICUs).

## Overview
This project forecasts ICU bed occupancy for the next 24 hours using historical patient admission, transfer, and discharge (ADT) events.

### Phase Roadmap & Architecture
- **Phase 1: Foundation & Data Pipelines** — Docker Compose infra (Kafka, ZooKeeper, Hadoop NameNode, DataNode), Pydantic ADT schemas, deterministic synthetic event generator, and MIMIC-IV demo CSV ingestion.
- **Phase 2 & 2.1: Streaming Ingestion & Lake Storage** — Kafka producer, Kafka-to-HDFS atomic micro-batch sink (`/raw/icu_events`), dead-letter quarantine, and stateful hourly occupancy reconstruction (`/processed/icu_occupancy_hourly`) with duplicate event deduplication.
- **Phase 3: Machine Learning & Occupancy Forecasting** — Unit-isolated leakage-safe feature engineering (lags, rolling stats, cyclical calendar harmonics), direct multi-horizon regression models, 24-hour target buffer gap, and model evaluation against baselines.
- **Phase 4: Serving & Operational Visuals** — Prediction REST API (FastAPI) with health/readiness probes and interactive clinical capacity dashboard (Streamlit & Plotly).

---

## AI Agent & Contributor Guidance
This repository includes an [`agent/`](file:///c:/building%20projs/bda%20proj/agent) directory containing dedicated architectural guidelines, runbooks, and constraints for AI pair programming:
- [`agent/README.md`](file:///c:/building%20projs/bda%20proj/agent/README.md): Onboarding and project tree map.
- [`agent/ARCHITECTURE.md`](file:///c:/building%20projs/bda%20proj/agent/ARCHITECTURE.md): Component interactions, data flows, and state diagrams.
- [`agent/WORKFLOWS.md`](file:///c:/building%20projs/bda%20proj/agent/WORKFLOWS.md): Step-by-step developer and testing commands.
- [`agent/RULES.md`](file:///c:/building%20projs/bda%20proj/agent/RULES.md): Clinical disclaimers, leakage safeguards, and test standards.

---

## Data Strategy & Provenance
The system strictly distinguishes between two data sources:
1. **Synthetic Data (`SYNTHETIC`):** Primary source for continuous Kafka streaming, surge testing, and ML training (generating 90 days of consistent hospital occupancy across 5 ICU units).
2. **MIMIC-IV Demo Data (`MIMIC_DEMO`):** Open-access subset (100 patients, ODbL license) from PhysioNet used strictly to validate CSV ingestion, schema transformations, and clinical transfer logic without temporal misalignment.

---

## Installation & Setup Guide

### 1. Environment Setup
Create a Python 3.12 virtual environment:
```powershell
# Using uv (recommended)
uv venv .venv --python 3.12
.\.venv\Scripts\Activate.ps1

# Or standard venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Dependency Installation

#### A. Core & Testing Suite
For pipeline ingestion, Kafka streaming, HDFS sinks, and running automated tests:
```powershell
uv pip install -e ".[test]"
# Or: pip install -e ".[test]"
```

#### B. Machine Learning Training & Evaluation
To train and evaluate the Phase 3 multi-horizon forecasting models (`scikit-learn`, `joblib`):
```powershell
uv pip install -e ".[ml,test]"
# Or: pip install -e ".[ml,test]"
```

#### C. Prediction Serving & Dashboard (Phase 4)
To run the FastAPI service and Streamlit dashboard (`fastapi`, `uvicorn`, `streamlit`, `plotly`):
```powershell
uv pip install -e ".[serving]"
# Or: pip install -e ".[serving]"
```

To install all dependency groups at once:
```powershell
uv pip install -e ".[test,ml,serving]"
```

---

## Quickstart Workflow

### 1. Launch Local Big Data Infrastructure
Ensure Docker Desktop is running. Clean clones include default cluster configurations in `docker/hadoop/hadoop.env`:
```powershell
docker compose config
docker compose up -d
docker compose ps
```

### 2. Generate Synthetic Historical Data (90 Days)
Generates 90 days of synthetic ADT events and precomputes hourly census ground truth with a TSICU surge scenario:
```powershell
python scripts/generate_synthetic_data.py --days 90 --seed 42 --surge
```

### 3. Train and Evaluate Forecasting Models
Trains direct multi-horizon models (Ridge, HistGradientBoosting) against Persistence and Seasonal-Naive baselines using chronological splits (70% Train, 15% Val, 15% Test with a 24-hour buffer gap):
```powershell
python scripts/train_eval_forecaster.py --data-path data/synthetic/simulated_hourly_occupancy.parquet --output-dir models --seed 42
```

### 4. Launch FastAPI Prediction Service
Starts the REST API with lifespan-cached model artifacts on port 8000:
```powershell
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```
Interactive API docs available at `http://localhost:8000/docs`.

### 5. Launch Streamlit Operational Dashboard
Starts the interactive visualization dashboard on port 8501:
```powershell
streamlit run src/dashboard/app.py --server.port 8501
```
View dashboard at `http://localhost:8501`.

### 6. (Optional) Ingest MIMIC-IV Clinical Demo Data
Ingests and validates schema alignment against PhysioNet demo records:
```powershell
python scripts/ingest_mimic_data.py --source demo
```

### 7. Audit Assigned Ports
Inspect port assignments and check whether all containers and services are ready:
```powershell
python scripts/check_ports.py
```

### 8. Run Automated Tests
```powershell
pytest -v
```

---

## Port Management & Service Allocation Matrix

This project coordinates multiple distributed infrastructure containers and application servers. All ports are centrally managed via `.env` and `src/config.py`:

| Service / Container | Assigned Port | Protocol / Transport | Purpose | Configuration Key |
| :--- | :--- | :--- | :--- | :--- |
| **Confluent ZooKeeper** | `2181` | TCP / ZK Client | Kafka metadata & broker consensus | `docker-compose.yml` (`2181:2181`) |
| **Apache Kafka Broker** | `9092` | TCP / PLAINTEXT | Real-time ADT event streaming | `KAFKA_BOOTSTRAP_SERVERS` |
| **Hadoop NameNode (WebHDFS)** | `9870` | HTTP / REST | WebHDFS data lake API & DFS UI | `HDFS_NAMENODE_URL` |
| **Hadoop NameNode (RPC)** | `9000` | TCP / Hadoop IPC | Cluster inter-process communication | `HDFS_NAMENODE_RPC` |
| **Hadoop DataNode** | `9864` | HTTP / Stream | Raw/processed block HTTP reads/writes | `docker-compose.yml` (`9864:9864`) |
| **FastAPI Prediction Engine** | `8000` | HTTP / ASGI | Multi-horizon inference API & OpenAPI docs | `FASTAPI_PORT` |
| **Streamlit Dashboard** | `8501` | HTTP / Web | Interactive clinical operational UI | `STREAMLIT_PORT` |

### How to Reassign Ports
If port 8000 or 8501 is occupied by another application on your system, you can reassign them without changing code:
1. Copy `.env.example` to `.env` (if not already done).
2. Update the port variables, e.g.:
   ```dotenv
   FASTAPI_PORT=8080
   STREAMLIT_PORT=8502
   ```
3. Run `python scripts/check_ports.py` to confirm the new ports are free.

---

## Synthetic-Data Forecasting Evaluation (Phase 3)

> [!IMPORTANT]
> **Synthetic Data Disclaimer:** All metrics reported below were evaluated strictly on synthetic simulation data. They validate data engineering, feature generation, and multi-horizon architectural mechanics. They **do not** demonstrate clinical efficacy or real-world patient outcomes.

### Model Comparison
Models were selected based on **Validation MAE** (chronological partition untouched by test data). **Persistence Baseline** is retained as the primary operational benchmark:

| Model | Val MAE (Selection) | Test MAE | Test RMSE | Test WAPE | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Persistence Baseline** ($y_{t+h} = y_t$) | 1.72 beds | **1.56 beds** | **2.30 beds** | **21.8%** | **Primary Benchmark** |
| **Direct Ridge Regression** ($\alpha=10.0$) | **1.68 beds** | 2.09 beds | 2.72 beds | 29.0% | Selected Candidate |
| **Seasonal-Naive Baseline** ($24\text{h}$) | 2.37 beds | 2.48 beds | 3.28 beds | 34.5% | Secondary Baseline |
| **Direct HistGradientBoosting** | 1.94 beds | 2.74 beds | 3.52 beds | 38.1% | Non-linear Candidate |

### Test Performance by Lead Horizon (Direct Ridge Forecaster)
- **+1 hour ($h_1$):** MAE = 0.38 beds | RMSE = 0.58 beds | WAPE = 4.9%
- **+3 hours ($h_3$):** MAE = 0.84 beds | RMSE = 1.07 beds | WAPE = 11.0%
- **+6 hours ($h_6$):** MAE = 1.33 beds | RMSE = 1.65 beds | WAPE = 17.8%
- **+12 hours ($h_{12}$):** MAE = 2.15 beds | RMSE = 2.59 beds | WAPE = 29.9%
- **+24 hours ($h_{24}$):** MAE = 3.30 beds | RMSE = 3.96 beds | WAPE = 48.9%

### Analysis of Test Set Dynamic: Ridge vs. Persistence
- **Active Inflow Period (Oct 21 – Oct 29, 1,035 samples):** During normal ongoing patient arrivals, Direct Ridge Regression outperformed Persistence (**1.72 vs 1.80 MAE**).
- **Simulation Runout Tail (Oct 30 – Nov 03, 540 samples):** Because admissions stopped at Day 90, census collapsed to zero. Persistence followed the collapsing values without mean-reversion (**1.11 MAE**), whereas Ridge's stationary weights predicted mean-reversion toward typical operating census (~9.6 beds), incurring **2.78 MAE** (+1.10 beds positive bias).
- In accordance with rigorous machine learning methodology, the held-out test partition is preserved unsegmented, and **Persistence Baseline is maintained as the primary benchmark**.

