# Agent Rules & Architectural Constraints

All AI agents and contributors working in this repository MUST adhere to the following rules:

---

## 1. Clinical Reality & Provenance Rules
- **NEVER claim clinical validity:** All models, metrics, and dashboards are evaluated strictly on synthetic ADT event simulations. They establish engineering and ML pipeline validity, NOT real-world medical safety.
- **Always label synthetic data:** All public endpoints, dashboard screens, and reports must clearly designate results as synthetic demonstrations (`SIMULATION_DATA`).
- **Data Provenance & Live vs Replay Distinction:**
  - Clearly distinguish between data provenance (`data_source`: `HDFS_PROCESSED_LAKE`, `LOCAL_SYNTHETIC_STORE`, `CLIENT_PAYLOAD`) and streaming freshness (`is_live_stream`, `source_age_hours`).
  - **Decoupled Live Stream Semantics:** Mere existence of a Parquet file in HDFS does NOT imply active live streaming. `is_live_stream` is True **strictly** when data originates from `HDFS_PROCESSED_LAKE`, is not future-dated, and has `source_age_hours <= 2.0`. Older files are labeled as persisted stale snapshots with `STALE_STREAM_WARNING`.
  - Surface `source_timestamp`, `source_age_hours`, and `simulation_time_label` on all inference responses.
- **Clock-Skew Tolerance & Future-Dated Data Rules:**
  - **Clock-Skew Tolerance:** A 5-minute tolerance (300 seconds) accommodates host/NTP time drift. Origins within 5 minutes in the future are treated as current (`source_age_hours = 0.0`).
  - **Never Clamp Negative Source Age:** When origin is in the future beyond 5 minutes, negative source age is preserved (`source_age_hours < 0`) and NEVER silently clamped to 0.0 or labeled fresh.
  - **Rejection vs Warning:**
    - If `reject_future_data=True`: Rejects with a 422 validation error (`ValueError: Future-dated forecast origin detected`).
    - If `reject_future_data=False`: Emits `FUTURE_DATED_DATA_WARNING`, preserves negative age, and marks `is_live_stream=False`.
- **HDFS Dataset Selection (Data Timestamps & Atomic Manifest):**
  - In `load_history_from_store()`, datasets must NEVER be selected by alphabetical filename sorting (`valid_files[-1]`).
  - Selection resolves the atomic `latest_manifest.json` or inspects actual data timestamps (`cand_df['timestamp'].max()`) across files to guarantee the newest chronological dataset is chosen.
- **Zero Secrets / Protected Health Information (PHI):** Never commit real patient records, PhysioNet credentials, `.env` secrets, or raw clinical datasets.

---

## 2. History Validation & Feature Integrity
- **Universal History Validation:** Apply strict validation across **every model path**, including Persistence and Seasonal-Naive baselines:
  - **Top-of-Hour Alignment:** Timestamps must be exact hours (`:00:00`). Off-hour timestamps are strictly rejected with HTTP 422.
  - **Chronological Sorting & Origin Guarantee:** History is sorted chronologically so the forecast origin $t$ is always the newest valid record.
  - **Strict Uniqueness:** Duplicate timestamps within a unit's history are rejected.
  - **Exact Hourly Continuity:** Time series must have continuous 1-hour intervals. Gaps are rejected; missing observations are **NEVER silently zero-filled**.
  - **Duration Minimums:** Persistence requires $\ge 1$ observation; Seasonal-Naive requires $\ge 25$ observations (24h lag series + origin); Direct Ridge requires $\ge 169$ observations (168h lookback + origin).
- **Zero Lookahead / Future Leakage:** All feature transformations must strictly leverage past observations ($L_0 \dots L_{168}$) and left-closed rolling windows. Never shift negative numbers into features.
- **Chronological Boundary Isolation:** Always use chronological splits with a **24-hour target buffer gap** between train, validation, and test sets.
- **Never Tune Against Held-Out Test Set:** Model selection must be conducted strictly on the validation partition. The test partition must remain untouched.
- **Preserve Baselines:** Always evaluate models against `PersistenceBaseline` ($y_{t+h} = y_t$) and `SeasonalNaiveBaseline`. Persistence is the primary operational benchmark.

---

## 3. Capacity Authority & Model Artifact Validation
- **Capacity Authority:**
  - Server-side `Settings.icu_unit_capacities` is the authoritative baseline capacity for each unit.
  - Explicit scenario capacity (`scenario_capacity`) is supported in `PredictionRequest` for surge or bed-reallocation what-if analysis.
  - Any capacity (scenario or baseline) must be strictly positive ($> 0$). Zero or negative values are rejected.
- **Model Artifact Startup Validation:**
  - `ForecastingService.load_model()` executes at service startup.
  - Validates feature count matches `feature_names` length (54 features).
  - Validates horizon count matches model sub-estimators ($h=1\dots 24$).
  - Validates model class is a verified `BaseForecaster` instance.
  - Validates model sub-estimators match feature input dimensions.
  - Validates training provenance (split boundaries and random seed).

---

## 4. Streaming & Big Data Integrity
- **At-Least-Once Delivery Semantics:** The Kafka-to-HDFS pipeline guarantees **at-least-once delivery** with idempotent downstream deduplication. Never claim exactly-once delivery.
- **Dead-Letter Quarantine Confirmation:** Dead-letter writes to HDFS must be strictly verified before Kafka offsets are committed. If DLQ write fails, the batch is aborted.
- **Idempotent Storage:** Raw events persisted to HDFS must use atomic rename patterns (`stage_*.jsonl` $\to$ `events_*.jsonl`). Offsets in Kafka must only be committed AFTER successful HDFS persistence.
- **Double-Counting Prevention:** An encounter can only occupy one bed at any given hourly snapshot.
- **Unit Boundary Isolation:** Feature generation must group by `unit_id` so that shifts and rolling windows never bleed between distinct hospital wards.
- **Occupancy Reconstruction Windowing Limitation:**
  - `OccupancyCalculator` reconstructs exact census by replaying cumulative raw events from HDFS.
  - *Limitation:* If events are sliced into isolated windows without cumulative history, stays crossing window boundaries (admitted before window start) would not be reflected unless pre-window events are included. Cumulative lake replay is the architectural design choice.

---

## 5. API Safety & Dashboard Presentation
- **500 Response Sanitization:** Unexpected internal exceptions must return a generic, sanitized HTTP 500 error (`"An internal server error occurred while processing the request."`). Stack traces, database strings, or internal exception messages must NEVER be exposed in API responses.
- **CORS Restriction:** Restrict CORS origins to authorized frontend endpoints (`localhost:8501`, `127.0.0.1:8501`, `localhost:8000`).
- **Dashboard Presentation:**
  - Prominently indicate when predictions use a baseline fallback.
  - Clearly label data provenance (HDFS live streaming lake vs static synthetic simulation replay).
  - Prominently display clinical validity disclaimers.

---

## 6. Software Quality & Testing Standards
- **Never Delete Existing Tests or Weaken Assertions:** When adding new features or fixes, add corresponding regression tests; never silence existing test failures.
- **Verify Live Containers Before Integration Tests:** Ensure Docker containers (`icu-zookeeper`, `icu-kafka`, `icu-namenode`, `icu-datanode`) are healthy before running integration suites.
- **Run `graphify update .` after changes:** Keep the AST knowledge graph updated after modifying source files.
