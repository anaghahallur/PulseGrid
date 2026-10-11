# ICU Bed Demand Forecasting

A real-time big data streaming, forecasting, and operational capacity planning system for Intensive Care Units (ICUs).

## Overview
This project forecasts ICU bed occupancy for the next 24 hours using historical patient admission, transfer, and discharge (ADT) events.

### Core Stack
- **Streaming Ingestion:** Apache Kafka (`icu-events` topic)
- **Data Lake Storage:** Hadoop HDFS (`/raw/icu_events` and `/processed/icu_occupancy_hourly`)
- **Processing & State Tracking:** Python & Pandas (atomic double-counting prevention)
- **Machine Learning:** Scikit-learn multi-step regression models (Phase 4)
- **Prediction Serving:** FastAPI (Phase 5)
- **Operational Visuals:** Streamlit & Plotly (Phase 6)
- **Container Infrastructure:** Docker Compose (Kafka, ZooKeeper, Hadoop NameNode, DataNode)
- **Testing:** Pytest

---

## Data Strategy & Provenance
The system strictly distinguishes between two data sources:
1. **Synthetic Data (`SYNTHETIC`):** Primary source for continuous Kafka streaming, surge testing, and ML training (generating 60+ days of consistent hospital occupancy).
2. **MIMIC-IV Demo Data (`MIMIC_DEMO`):** Open-access subset (100 patients, ODbL license) from PhysioNet used strictly to validate CSV ingestion, schema transformations, and clinical transfer logic without temporal misalignment.

---

## Quickstart Guide

### 1. Environment Setup
```powershell
# Create Python 3.12 virtual environment using uv
uv venv .venv --python 3.12
.\.venv\Scripts\Activate.ps1

# Install pinned dependencies
uv pip install -e ".[test]"
```

### 2. Launch Local Big Data Infrastructure
```powershell
docker compose up -d
```

### 3. Generate Synthetic Historical Data
```powershell
python scripts/generate_synthetic_data.py --days 60 --seed 42 --surge
```

### 4. (Optional) Ingest MIMIC-IV Clinical Demo Data
```powershell
python scripts/ingest_mimic_data.py --source demo
```

### 5. Run Test Suite
```powershell
pytest
```
