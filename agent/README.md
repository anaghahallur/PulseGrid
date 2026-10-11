# AI Agent Guide — ICU Bed Demand Forecasting

Welcome, AI Agent / Contributor! This folder provides the complete context, architectural blueprints, constraints, and operational workflows needed to navigate, develop, test, and extend the **ICU Bed Demand Forecasting** system.

---

## 1. Project Mission & Core Stack
The objective of this project is to ingest patient Admission, Discharge, and Transfer (ADT) streaming events in real time, persist them in a big data lake, calculate hourly bed occupancy without double-counting, and forecast ICU census 1-to-24 hours ahead to provide hospital operations with early warnings for bed shortages.

### Technology Stack
- **Event Streaming:** Apache Kafka (`icu-events` topic), Confluent ZooKeeper
- **Data Lake Storage:** Apache Hadoop HDFS (`/raw/icu_events` and `/processed/icu_occupancy_hourly`) via WebHDFS REST API
- **Data Processing:** Python 3.12, Pandas, Pydantic v2
- **Machine Learning:** Scikit-learn multi-output regression (Direct Ridge, HistGradientBoosting, Persistence Baseline)
- **Serving API:** FastAPI with lifespan startup caching
- **Operational UI:** Streamlit & Plotly
- **Infrastructure:** Docker Compose (`icu-zookeeper`, `icu-kafka`, `icu-namenode`, `icu-datanode`)
- **Testing:** Pytest

---

## 2. Directory Structure Map
```text
.
├── agent/                         # AI agent context, architecture, workflows, and rules
│   ├── README.md                  # This file
│   ├── ARCHITECTURE.md            # Detailed system component interactions
│   ├── WORKFLOWS.md               # Step-by-step developer and testing commands
│   └── RULES.md                   # Critical constraints, leakage prevention, and clinical disclaimers
├── docker/                        # Container configuration
│   └── hadoop/hadoop.env          # NameNode & DataNode environment variables (tracked)
├── docker-compose.yml             # Local Kafka & Hadoop infrastructure definition
├── models/                        # Persisted model artifacts & evaluation reports
│   ├── icu_occupancy_forecaster.joblib  # Trained Direct Ridge forecaster artifact
│   ├── feature_metadata.json      # Complete 54 feature names and split metadata
│   ├── evaluation_report.json     # Machine-readable metrics breakdown
│   └── EVALUATION_REPORT.md       # Human-readable evaluation markdown
├── scripts/                       # CLI automation scripts
│   ├── generate_synthetic_data.py # 90-day reproducible synthetic ADT event generator
│   ├── ingest_mimic_data.py       # PhysioNet MIMIC-IV demo CSV ingestion
│   └── train_eval_forecaster.py   # Chronological ML training & validation CLI
├── src/                           # Core source tree
│   ├── api/                       # FastAPI prediction service (Phase 4)
│   │   ├── main.py                # FastAPI endpoints (/health, /ready, /units, /predict)
│   │   ├── schemas.py             # Pydantic request/response validation models
│   │   └── service.py             # Feature reconstruction, model loading, validation
│   ├── config.py                  # Pydantic BaseSettings environment configuration
│   ├── dashboard/                 # Streamlit interactive dashboard (Phase 4)
│   │   └── app.py                 # Streamlit UI with Plotly charts & offline fallback
│   ├── ml/                        # Machine learning forecasting pipeline (Phase 3)
│   │   ├── features.py            # Unit-isolated past-only feature engineering
│   │   ├── metrics.py             # MAE, RMSE, zero-denominator safe WAPE
│   │   ├── models.py              # Multi-horizon Direct Ridge, Baselines, clipping
│   │   └── pipeline.py            # Chronological splitting (24h buffer gap), selection
│   ├── processing/                # Census calculation & deduplication (Phase 2)
│   │   └── occupancy_calculator.py# Out-of-order, idempotent hourly reconstruction
│   ├── producer/                  # ADT event generators (Phase 1 & 2)
│   │   ├── kafka_producer.py      # Confluent Kafka event publisher
│   │   └── simulator.py           # Non-homogeneous Poisson clinical ADT simulator
│   ├── schema/                    # Pydantic ADT domain models
│   │   ├── events.py              # ICUEvent, EventType, correction flags
│   │   └── occupancy.py           # HourlyOccupancy record
│   └── storage/                   # Storage sinks (Phase 2 & 2.1)
│       ├── hdfs_client.py         # WebHDFS client wrapper
│       └── kafka_sink.py          # Atomic micro-batch Kafka-to-HDFS consumer
└── tests/                         # Pytest automated test suite
    ├── integration/               # Live Kafka, HDFS, and API integration tests
    └── unit/                      # Fast unit tests (schemas, simulator, ML, API)
```

---

## 3. How to Use the Agent Documentation
- Read [`agent/ARCHITECTURE.md`](file:///c:/building%20projs/bda%20proj/agent/ARCHITECTURE.md) to understand data flow and component seams.
- Read [`agent/WORKFLOWS.md`](file:///c:/building%20projs/bda%20proj/agent/WORKFLOWS.md) before executing tasks, reproducing runs, or creating tests.
- Read [`agent/RULES.md`](file:///c:/building%20projs/bda%20proj/agent/RULES.md) to maintain compliance with test standards, temporal leak guards, and data provenance rules.
