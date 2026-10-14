# ICU Bed Demand Forecasting — Documentation Suite

A comprehensive technical documentation suite for the **ICU Bed Demand Forecasting** system — an end-to-end Big Data Analytics and Operational Capacity Planning platform built with Apache Kafka, Hadoop HDFS, Scikit-learn, FastAPI, and Streamlit.

---

## 📑 Documentation Index

This directory contains the complete, authoritative documentation for the project, structured for software engineers, data architects, faculty members, and academic project evaluators:

| Document | Description | Key Focus Areas |
| :--- | :--- | :--- |
| **[PROJECT_OVERVIEW.md](./PROJECT_OVERVIEW.md)** | High-level project definition and business context | Problem statement, objectives, scope, clinical motivation, target users, and limitations |
| **[SYSTEM_ARCHITECTURE.md](./SYSTEM_ARCHITECTURE.md)** | Architectural design and component interaction | 5-tier architecture, container topology, Mermaid diagrams, protocols, and design trade-offs |
| **[TECHNICAL_DOCUMENTATION.md](./TECHNICAL_DOCUMENTATION.md)** | In-depth engineering reference | Codebase inventory, module-by-module breakdown, function signatures, schemas, and REST APIs |
| **[DATA_PIPELINE.md](./DATA_PIPELINE.md)** | End-to-end data lifecycle | Ingestion, Kafka streaming, HDFS micro-batch sink, stateful census reconstruction, and feature pipeline |
| **[FEATURES_AND_WORKFLOWS.md](./FEATURES_AND_WORKFLOWS.md)** | Feature inventory and operational workflows | Master feature matrix, step-by-step user workflows, UI controls, and error scenarios |
| **[SETUP_AND_EXECUTION.md](./SETUP_AND_EXECUTION.md)** | Developer onboarding and runbook | Prerequisites, Docker setup, virtual environment, commands for macOS/Linux/Windows, troubleshooting |
| **[TESTING_AND_VALIDATION.md](./TESTING_AND_VALIDATION.md)** | Verification strategy and test audit | Unit tests, integration tests, failure recovery, actual test execution results, and testing gaps |
| **[PERFORMANCE_AND_SCALABILITY.md](./PERFORMANCE_AND_SCALABILITY.md)** | Big Data considerations and scaling | Complexity analysis, latency/throughput, memory profiles, bottlenecks, and scale-out roadmap |
| **[SECURITY_AND_LIMITATIONS.md](./SECURITY_AND_LIMITATIONS.md)** | Governance, security, and constraints | HIPAA/PHI controls, input sanitization, deserialization risks, technical debt, and known issues |
| **[FUTURE_ENHANCEMENTS.md](./FUTURE_ENHANCEMENTS.md)** | Research and feature roadmap | Distributed PySpark, continuous Flink streaming, Iceberg lakehouse, Transformers, and FHIR APIs |
| **[PROJECT_REPORT.md](./PROJECT_REPORT.md)** | Formal academic project report | Academic thesis structure: Literature review, methodology, mathematical formulas, results, and evaluation |
| **[DOCUMENTATION_AUDIT.md](./DOCUMENTATION_AUDIT.md)** | Verification audit of this documentation suite | Files inspected, verified facts vs assumptions, test logs, and maintenance checklist |

---

## 🏥 Executive Project Summary

### The Problem
Intensive Care Units (ICUs) operate at high utilization rates (frequently 85–95%) with rigid nurse-to-patient staffing ratios. Reactive bed management causes:
1. **Emergency Department (ED) Boarding:** Critically ill patients waiting hours for an open ICU bed.
2. **Surgical Cancellations:** Elective complex surgeries deferred due to unavailable postoperative critical beds.
3. **Premature Discharges:** Patients stepped down prematurely, driving up readmission rates.

### The Solution
An end-to-end Big Data streaming and predictive analytics architecture that:
- Ingests Patient **Admission, Discharge, and Transfer (ADT)** events in real time via **Apache Kafka**.
- Persists raw event streams immutably into an **Apache Hadoop HDFS** data lake with dead-letter quarantine and atomic micro-batching.
- Reconstructs instantaneous hourly unit census without double-counting using a stateful replay engine.
- Engineers 54 leakage-safe past-only features (lags, rolling stats, diurnal calendar harmonics).
- Projects bed occupancy across **1 to 24 hourly lead horizons** using **Direct Multi-Horizon Ridge Regression** alongside operational baselines.
- Surfaces real-time census trajectories, capacity exceedance warnings, and surge planning via a **FastAPI** backend and an interactive **Streamlit / Plotly** operational dashboard.

```mermaid
flowchart LR
    A["Event Sources<br/>(Simulator & MIMIC Demo)"] -->|"JSON ADT Events"| B["Apache Kafka Broker<br/>(:9092)"]
    B -->|"Micro-Batch Consumer"| C["Kafka-to-HDFS Sink<br/>(Atomic Stage/Rename)"]
    C -->|"Raw Lake"| D["Hadoop HDFS Lake<br/>(/raw/icu_events)"]
    D -->|"Cumulative Replay"| E["Occupancy Calculator<br/>(Deduplication & State)"]
    E -->|"Processed Parquet"| F["HDFS Processed Lake<br/>(/processed/icu_occupancy_hourly)"]
    F -->|"54 Past-Only Features"| G["Direct Ridge Forecaster<br/>(1-24h Horizons)"]
    G -->|"Lifespan Cached Model"| H["FastAPI Engine<br/>(:8000)"]
    H -->|"REST API / Fallback"| I["Streamlit Dashboard<br/>(:8501)"]
    I --> J["Operational Clinical Staff<br/>(Surge Alerts & Capacity Planning)"]
```

---

## 🛠️ Complete Technology Stack

| Layer | Technology | Version | Purpose in Codebase |
| :--- | :--- | :--- | :--- |
| **Event Streaming** | **Apache Kafka** | `7.5.0` (Confluent) | Real-time event broker for `icu-events` topic; encounter-keyed partitioning |
| **Cluster Coordination** | **ZooKeeper** | `7.5.0` (Confluent) | Kafka broker metadata and consensus management |
| **Data Lake Storage** | **Apache Hadoop HDFS** | `3.2.1` (bde2020) | Distributed lake storage for raw event batches and processed hourly census |
| **Data Lake Client** | **WebHDFS REST API** | HTTP/REST | Python client wrapper resolving Docker container redirects to localhost |
| **Lake File Format** | **Apache Parquet (PyArrow)** | `>=15.0.0` | Columnar storage with Snappy compression for processed hourly occupancy series |
| **Data Processing** | **Pandas & NumPy** | `>=2.2.0` / `>=1.26.0` | Cumulative state replay, matrix transformations, time series indexing |
| **Schema Validation** | **Pydantic & Settings** | `>=2.7.0` / `>=2.2.0` | Domain event contracts, clinical routing validation, environment settings |
| **Machine Learning** | **Scikit-learn & Joblib** | `>=1.4.0` / `>=1.3.0` | Direct multi-output Ridge regression, HistGradientBoosting, baselines, serialization |
| **Serving Backend** | **FastAPI & Uvicorn** | `>=0.110.0` / `>=0.28.0` | ASGI REST API with lifespan model verification, probes, and universal validation |
| **Visualization UI** | **Streamlit & Plotly** | `>=1.32.0` / `>=5.20.0` | Interactive capacity dashboard, real-time trajectory curves, surge alerts |
| **Infrastructure** | **Docker Compose** | `v2.x` | Orchestration for ZooKeeper, Kafka, Hadoop NameNode, and DataNode |
| **Testing** | **Pytest & HTTPX** | `>=8.0.0` / `>=0.27.0` | Comprehensive unit and live integration testing suites |

---

## 📋 Monitored ICU Units and Staffed Capacities

The system monitors five distinct Intensive Care Units configured in `src/config.py`:

| Unit ID | Full Unit Designation | Staffed Capacity | Primary Clinical Focus |
| :--- | :--- | :---: | :--- |
| **MICU** | Medical Intensive Care Unit | 24 beds | Severe medical illnesses, sepsis, respiratory failure, acute organ failure |
| **SICU** | Surgical Intensive Care Unit | 20 beds | Postoperative complex surgical cases, elective surgery recoveries |
| **CCU** | Coronary Care Unit | 16 beds | Acute coronary syndrome, heart failure, cardiogenic shock |
| **TSICU** | Trauma-Surgical Intensive Care Unit | 16 beds | Multi-trauma, neuro-trauma, emergency trauma surgeries |
| **CSRU** | Cardiac Surgery Recovery Unit | 18 beds | Post-cardiopulmonary bypass, open-heart valve/CABG surgery recovery |

---

## 📊 Forecasting Performance Benchmark (Phase 3 Evaluation)

> [!IMPORTANT]
> **Synthetic Data Disclaimer:** All figures reported below were evaluated strictly on synthetic simulation data (90 days, 5 units). They demonstrate big data engineering, feature generation, and multi-horizon machine learning mechanics, **not** clinical efficacy on living human patients.

Models were trained on 70% of chronological history (7,230 samples), selected on 15% validation data (1,460 samples), and evaluated on 15% held-out test data (1,575 samples) with a **24-hour target buffer gap** between partitions:

| Model | Selection Criterion (Val MAE) | Held-Out Test MAE | Held-Out Test RMSE | Held-Out Test WAPE | Operational Role |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Persistence Baseline** ($y_{t+h} = y_t$) | 1.72 beds | **1.56 beds** | **2.30 beds** | **21.8%** | **Primary Benchmark** |
| **Direct Ridge Regression** ($\alpha=10.0$) | **1.68 beds** | 2.09 beds | 2.72 beds | 29.0% | **Selected Candidate** |
| **Seasonal-Naive Baseline** ($24\text{h}$) | 2.37 beds | 2.48 beds | 3.28 beds | 34.5% | Secondary Baseline |
| **Direct HistGradientBoosting** | 1.94 beds | 2.74 beds | 3.52 beds | 38.1% | Non-Linear Candidate |

### Test Performance by Forecast Lead Time (Direct Ridge Forecaster)
- **+1 hour ($h_1$):** MAE = **0.38 beds** | RMSE = **0.58 beds** | WAPE = **4.9%**
- **+3 hours ($h_3$):** MAE = **0.84 beds** | RMSE = **1.07 beds** | WAPE = **11.0%**
- **+6 hours ($h_6$):** MAE = **1.33 beds** | RMSE = **1.65 beds** | WAPE = **17.8%**
- **+12 hours ($h_{12}$):** MAE = **2.15 beds** | RMSE = **2.59 beds** | WAPE = **29.9%**
- **+24 hours ($h_{24}$):** MAE = **3.30 beds** | RMSE = **3.96 beds** | WAPE = **48.9%**

---

## ⚡ Quickstart Execution Guide

### 1. Environment Setup
```bash
# Create and activate Python 3.12 virtual environment
python -m venv .venv
source .venv/bin/activate  # macOS / Linux
# .\.venv\Scripts\Activate.ps1  # Windows PowerShell

# Install full dependencies (serving, ml, testing)
pip install -e ".[test,ml,serving]"
```

### 2. Start Distributed Infrastructure (Docker)
```bash
docker compose up -d
python scripts/check_ports.py
```

### 3. Generate Historical Data & Train Model
```bash
# Generate 90-day canonical dataset with TSICU surge scenario
python scripts/generate_synthetic_data.py --days 90 --seed 42 --surge

# Train and serialize multi-horizon forecasting models
python scripts/train_eval_forecaster.py --data-path data/synthetic/simulated_hourly_occupancy.parquet --output-dir models --seed 42
```

### 4. Launch Serving Services
```bash
# Terminal A: Start FastAPI Prediction Service
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal B: Start Streamlit Dashboard
streamlit run src/dashboard/app.py --server.port 8501
```

Access the web interfaces:
- **Streamlit Operational Dashboard:** [http://localhost:8501](http://localhost:8501)
- **FastAPI OpenAPI Interactive Documentation:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Hadoop NameNode WebHDFS UI:** [http://localhost:9870](http://localhost:9870)

---

## 🔍 Detailed Documentation Guide

For comprehensive technical analysis and evaluation, please navigate to the dedicated documents:
- Read **[PROJECT_OVERVIEW.md](./PROJECT_OVERVIEW.md)** for detailed problem background and functional specifications.
- Read **[SYSTEM_ARCHITECTURE.md](./SYSTEM_ARCHITECTURE.md)** for multi-tier component topologies and communication protocols.
- Read **[DATA_PIPELINE.md](./DATA_PIPELINE.md)** for data flow diagrams, atomic staging, and deduplication logic.
- Read **[TECHNICAL_DOCUMENTATION.md](./TECHNICAL_DOCUMENTATION.md)** for full source code inventories and API schemas.
- Read **[PROJECT_REPORT.md](./PROJECT_REPORT.md)** for the complete academic thesis report.
