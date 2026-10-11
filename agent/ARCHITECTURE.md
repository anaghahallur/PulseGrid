# System Architecture — ICU Bed Demand Forecasting

This document details the architectural layers, data flows, and state handling mechanisms across the system.

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
        Kafka -->|Micro-Batch Consumer (At-Least-Once)| Sink["KafkaToHDFSSink"]
        Sink -->|Atomic Write: stage -> final| RawHDFS["HDFS Raw Zone: /raw/icu_events/YYYY/MM/DD/"]
        Sink -->|Verified Quarantine| DLQ["Dead-Letter Quarantine: /dead_letter/malformed_events/"]
    end

    subgraph Processing["3. State Tracking Layer"]
        RawHDFS --> Calc["OccupancyCalculator (Cumulative Lake Replay)"]
        Calc -->|Deduplicate & Sort (Version, Timestamp)| State["Active Encounter State Map"]
        State -->|Reconstructed Hourly Census| ProcHDFS["HDFS Processed Zone: /processed/icu_occupancy_hourly/"]
    end

    subgraph ML["4. Machine Learning Layer"]
        ProcHDFS --> Feat["Feature Pipeline (54 past-only features)"]
        Feat --> Split["Chronological Split (70% Train, 15% Val, 15% Test with 24h Buffer)"]
        Split --> Ridge["Direct Ridge Forecaster (Selected Candidate)"]
        Split --> Pers["Persistence Baseline (Primary Benchmark)"]
        Ridge --> Models["models/icu_occupancy_forecaster.joblib"]
    end

    subgraph Serving["5. Serving & UI Layer"]
        Models --> API["FastAPI Service (:8000)"]
        ProcHDFS -->|Primary Live Handoff| API
        LocalStore["Local Parquet Fallback Store"] -.->|Offline Benchmark Replay| API
        API --> UI["Streamlit Dashboard (:8501)"]
        UI --> Charts["Plotly Real-Time Trajectory Charts & Surge Alerts"]
    end
```

---

## 2. Key Component Deep Dives

### A. Streaming Ingestion & Lake Persistence
- **Apache Kafka:** Topic `icu-events`. Messages serialized as JSON Pydantic `ICUEvent`.
- **HDFS Client:** Custom wrapper around WebHDFS REST API (`http://namenode:9870/webhdfs/v1`).
- **Kafka-to-HDFS Sink:**
  - Consumes events in micro-batches, partitions by date, writes temporary staging files (`stage_*.jsonl`), and atomically renames them to final paths.
  - Offsets are committed **strictly post-finalization** to guarantee **at-least-once delivery**.
  - **Dead-Letter Quarantine:** Malformed payloads are quarantined to HDFS. The return value is strictly checked; if DLQ write fails, the micro-batch is aborted before committing Kafka offsets.

### B. Stateful Hourly Occupancy Reconstruction
- **Double-Counting Prevention:** An encounter key `(patient_id, encounter_id)` can only occupy at most one unit bed at any given timestamp milestone.
- **Idempotence & Out-of-Order Handling:** Events are sorted deterministically by `(event_timestamp, version, type_priority, event_id)`. Newer event versions supersede older ones; correction events replace originals without duplicated occupancy counts.
- **Transfer-Origin Mismatches:** If a transfer event specifies an origin unit that conflicts with internal tracked location, the patient is relocated to the destination unit from their actual registered unit, preserving hospital-wide bed counts.
- **Windowing Limitation & Cumulative Replay:** `OccupancyCalculator` reconstructs census by replaying cumulative raw events from HDFS. If events are sliced into isolated time windows without pre-window history, stays crossing window boundaries would be omitted. Global lake replay is the chosen architectural model.

### C. HDFS Processed Census to Inference Handoff
- **Primary Source (`HDFS_PROCESSED_LAKE`):** Inference checks `/processed/icu_occupancy_hourly` in HDFS.
  - **Timestamp-Based Dataset Selection:** Datasets are selected via atomic `latest_manifest.json` or by reading candidate files and evaluating maximum data timestamps (`cand_df['timestamp'].max()`). Never relies on naive alphabetical filename sorting.
- **Offline Fallback (`LOCAL_SYNTHETIC_STORE`):** If HDFS is offline or unpopulated, cleanly falls back to canonical local Parquet store (`data/synthetic/simulated_hourly_occupancy.parquet`).
- **Decoupled Data Provenance & Streaming Freshness:**
  - `data_source`: Indicates physical storage source (`HDFS_PROCESSED_LAKE`, `LOCAL_SYNTHETIC_STORE`, `CLIENT_PAYLOAD`).
  - `is_live_stream`: True strictly when data originates from `HDFS_PROCESSED_LAKE`, is not future-dated, and has `source_age_hours <= 2.0`. Mere file existence in HDFS does not claim active live streaming.
  - `source_timestamp`: Newest observation timestamp in the history.
  - `source_age_hours`: Exact hours elapsed since the newest observation. Negative source age is preserved for future scenarios and **never clamped to zero**.
  - `CLOCK_SKEW_TOLERANCE`: 5 minutes (300 seconds) tolerated for minor host clock drift.
  - `simulation_time_label`: Explicitly labels live lake stream, synthetic simulation benchmark, or future scenario.
  - `STALE_STREAM_WARNING`: Raised if HDFS lake data is older than 2 hours.

### D. Multi-Horizon Forecasting Engine
- **Strategy:** Direct Multi-Output regression for horizons $h \in [1..24]$. Independent linear estimators are learned per lead time.
- **Leakage Prevention:** Features use strictly historical observations ($L_0 \dots L_{168}$) and rolling summary statistics closed on the past.
- **Buffer Gaps:** 24-hour buffer gaps between train/val and val/test prevent target overlap.
- **Physical Feasibility:** Predictions are clipped at $\ge 0$ beds (`np.clip(y, 0, None)`), but intentionally not clipped at staffed capacity so operational surges remain visible.

### E. Prediction Serving (FastAPI)
- **Lifespan Startup Verification:** Validates `models/icu_occupancy_forecaster.joblib` and `models/feature_metadata.json` on startup: feature count (54), horizon count (24), estimator dimensions, and training provenance.
- **Universal History Validation:** Validates top-of-hour alignment (`:00:00`), chronological ordering, uniqueness, and exact 1-hour continuity across all model paths (Persistence, Seasonal-Naive, Direct Ridge).
- **Capacity Authority:** Server-side `Settings.icu_unit_capacities` is the baseline authority. Clients may provide an explicit `scenario_capacity` for what-if surge planning. Non-positive capacities are strictly rejected.
- **500 Safety:** Unhandled internal exceptions return a sanitized message without exposing internal stack traces or connection details.
- **CORS Restriction:** Restricted to authorized localhost dashboard endpoints (`8501`, `8000`).

### F. Operational Dashboard (Streamlit & Plotly)
- Displays current occupancy, staffed capacity, 24-hour forward projections, and capacity-exceedance warnings.
- **Prominent Fallback Callout:** Highlights when predictions use a baseline fallback.
- **Provenance & Age Display:** Displays whether data is live streaming lake or synthetic simulation replay with source age.
- **Academic Disclaimer:** Prominently labels all data and forecasts as academic synthetic simulations.
