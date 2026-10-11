# System Architecture — ICU Bed Demand Forecasting

This document details the architectural layers and data flows across the system.

---

## 1. End-to-End Data Pipeline Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        Sim["ICUSimulator (Poisson arrivals, lognormal LOS)"] -->|ADT Events| Prod["ICUKafkaProducer"]
        Demo["MIMIC-IV Demo Ingester"] -->|Clinical CSVs| Prod
        Prod -->|Topic: icu-events| Kafka["Apache Kafka Broker (:9092)"]
    end

    subgraph Lake["2. Data Lake Layer"]
        Kafka -->|Micro-Batch Consumer| Sink["KafkaToHDFSSink"]
        Sink -->|Atomic Write| RawHDFS["HDFS Raw Zone: /raw/icu_events/YYYY/MM/DD/"]
        Sink -->|Quarantine| DLQ["Dead-Letter Queue / Quarantine"]
    end

    subgraph Processing["3. State Tracking Layer"]
        RawHDFS --> Calc["OccupancyCalculator"]
        Calc -->|Deduplicate & Sort (Version, Timestamp)| State["Active Encounter State Map"]
        State -->|Reconstructed Hourly Census| ProcHDFS["HDFS Processed Zone: /processed/icu_occupancy_hourly/"]
    end

    subgraph ML["4. Machine Learning Layer"]
        ProcHDFS --> Feat["Feature Pipeline (lags, rolling stats, cyclical harmonics)"]
        Feat --> Split["Chronological Split (70% Train, 15% Val, 15% Test with 24h Buffer)"]
        Split --> Ridge["Direct Ridge Forecaster (Selected)"]
        Split --> Pers["Persistence Baseline (Benchmark)"]
        Ridge --> Models["models/icu_occupancy_forecaster.joblib"]
    end

    subgraph Serving["5. Serving & UI Layer"]
        Models --> API["FastAPI Service (:8000)"]
        ProcHDFS --> API
        API --> UI["Streamlit Dashboard (:8501)"]
        UI --> Charts["Plotly Real-Time Trajectory Charts"]
    end
```

---

## 2. Key Component Deep Dives

### A. Streaming Ingestion & Lake Persistence
- **Apache Kafka:** Topic `icu-events`. Messages serialized as JSON Pydantic `ICUEvent`.
- **HDFS Client:** Custom wrapper around WebHDFS REST API (`http://namenode:9870/webhdfs/v1`).
- **Kafka-to-HDFS Sink:** Consumes events in micro-batches, partitions by date, writes temporary `.tmp` files, verifies completeness, and atomically renames to final HDFS paths before committing Kafka offsets. Prevents message loss and premature commits during multi-partition micro-batches.

### B. Stateful Hourly Occupancy Reconstruction
- **Double-Counting Prevention:** An encounter key `(patient_id, encounter_id)` can only occupy at most one unit bed at any given timestamp milestone.
- **Idempotence & Out-of-Order Handling:** Events are sorted deterministically by `(event_timestamp, version, type_priority, event_id)`. Newer event versions supersede older ones; correction events replace originals without duplicated occupancy counts.

### C. Multi-Horizon Forecasting Engine
- **Strategy:** Direct Multi-Output regression for horizons $h \in [1..24]$. Independent linear weights are learned per lead time.
- **Leakage Prevention:** Features use strictly historical observations ($L_0 \dots L_{168}$) and rolling summary statistics closed on the past.
- **Buffer Gaps:** 24-hour buffer gaps between train/val and val/test prevent target overlap.
- **Physical Feasibility:** Predictions are clipped at $\ge 0$ beds (`np.clip(y, 0, None)`), but intentionally not clipped at staffed capacity so operational surges remain visible.

### D. Prediction Serving (FastAPI)
- **Lifespan Context Manager:** Loads model artifact and validates `feature_metadata.json` once on startup, preventing redundant deserialization on each request.
- **Strict History Validation:** Verifies that historical observations are strictly contiguous (1-hour steps). Rejects missing or non-contiguous data with HTTP 422. Never silently zero-fills gaps.
- **Baseline Parity:** Retains `PersistenceBaseline` and `SeasonalNaiveBaseline` for comparative benchmarking.

### E. Operational Dashboard (Streamlit & Plotly)
- Displays current occupancy, staffed capacity, 24-hour forward projections, and capacity-exceedance warnings.
- Graceful offline fallback: if FastAPI is offline, the dashboard falls back to local forecasting logic rather than failing.
- Clear synthetic data labeling.
