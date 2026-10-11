# Graph Report - bda proj  (2026-10-12)

## Corpus Check
- 41 files · ~25,470 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 442 nodes · 655 edges · 36 communities (24 shown, 12 thin omitted)
- Extraction: 72% EXTRACTED · 28% INFERRED · 0% AMBIGUOUS · INFERRED: 185 edges (avg confidence: 0.77)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `ec8ba9ea`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 34|Community 34]]

## God Nodes (most connected - your core abstractions)
1. `ICUEvent` - 37 edges
2. `HDFSClient` - 28 edges
3. `ForecastingService` - 24 edges
4. `OccupancyCalculator` - 23 edges
5. `get_settings()` - 18 edges
6. `KafkaHDFSSink` - 18 edges
7. `ICUKafkaProducer` - 15 edges
8. `ICUSimulator` - 15 edges
9. `HourlyCensusRecord` - 12 edges
10. `prepare_forecasting_dataset()` - 11 edges

## Surprising Connections (you probably didn't know these)
- `kafka_settings()` --calls--> `get_settings()`  [INFERRED]
  tests/integration/test_pipeline_reliability_audit.py → src/config.py
- `hdfs_client()` --calls--> `HDFSClient`  [INFERRED]
  tests/integration/test_pipeline_reliability_audit.py → src/storage/hdfs_client.py
- `main()` --calls--> `get_settings()`  [INFERRED]
  scripts/check_ports.py → src/config.py
- `main()` --calls--> `get_settings()`  [INFERRED]
  scripts/generate_synthetic_data.py → src/config.py
- `main()` --calls--> `OccupancyCalculator`  [INFERRED]
  scripts/generate_synthetic_data.py → src/processing/occupancy_calculator.py

## Communities (36 total, 12 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.05
Nodes (48): HealthResponse, HorizonPrediction, HourlyCensusRecord, ModelInfoResponse, PredictionRequest, PredictionResponse, Pydantic Request and Response Schemas for the ICU Occupancy Forecasting API. Enf, Service liveness probe response. (+40 more)

### Community 1 - "Community 1"
Cohesion: 0.08
Nodes (33): Validates historical continuity and reconstructs the exact 54 features expected, build_unit_features_and_targets(), prepare_forecasting_dataset(), Feature Engineering and Target Construction for ICU Occupancy Forecasting. Const, Constructs leakage-safe features and targets for a single ICU unit.     All shif, Constructs full forecasting dataset across all ICU units while strictly isolatin, chronological_split(), load_forecaster() (+25 more)

### Community 2 - "Community 2"
Cohesion: 0.09
Nodes (15): ABC, BaseForecaster, DirectHistGradientBoostingForecaster, DirectRandomForestForecaster, DirectRidgeForecaster, Multi-Horizon Forecasting Models for ICU Bed Demand. Implements: 1. Persistence, Direct Multi-Horizon Ridge Regression.     Trains independent regularized linear, Direct Multi-Horizon Histogram Gradient Boosting Regressor.     Fast non-linear (+7 more)

### Community 3 - "Community 3"
Cohesion: 0.08
Nodes (28): create_app(), lifespan(), FastAPI Application Entrypoint for ICU Bed Demand Forecasting. Provides health,, Application lifespan context manager.     Loads and validates the trained model, Factory function to build and configure the FastAPI application., get_forecasting_service(), Inference Service for ICU Bed Demand Forecasting. Loads trained forecasters, val, Provides access to the global ForecastingService instance. (+20 more)

### Community 4 - "Community 4"
Cohesion: 0.08
Nodes (26): OccupancyCalculator, Hourly Occupancy Reconstruction Engine. Reconstructs exact instantaneous unit ce, State machine that processes patient ADT events to reconstruct hourly census., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, :param unit_capacities: Mapping of unit identifier to total bed capacity., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, Reconstructs hourly occupancy time series across all known units.          :para, Unit tests for occupancy calculation and double-counting prevention. (+18 more)

### Community 5 - "Community 5"
Cohesion: 0.09
Nodes (21): client(), Integration tests for FastAPI endpoints using TestClient. Tests: 1. Liveness (/h, Verifies /predict with an unknown unit ID returns HTTP 422., Verifies /predict with missing hourly gaps returns HTTP 422.     Missing observa, Verifies that capacity exceedances generate warnings and exceedance flags., Provides a TestClient with lifespan context executed., Verifies that /health returns 200 with operational status., Verifies that /ready returns 200 and confirms model readiness. (+13 more)

### Community 6 - "Community 6"
Cohesion: 0.1
Nodes (11): List contents of an HDFS directory., Write list of dictionaries as newline-delimited JSON to HDFS., Read newline-delimited JSON file from HDFS into list of dictionaries., Recursively lists all file paths under base_path matching suffix.         Ignore, Read Parquet file from HDFS into a pandas DataFrame., Recursively finds and reads all JSONL files under base_path into a combined list, Serialize a pandas DataFrame to Parquet and write to HDFS., Read Parquet file from HDFS into a pandas DataFrame. (+3 more)

### Community 7 - "Community 7"
Cohesion: 0.11
Nodes (14): ICUSimulator, Deterministic Synthetic ICU Event Generator. Produces high-fidelity patient admi, Samples ICU length of stay from a log-normal distribution.         Clinical ICU, Simulates patient flow through an ICU network with configurable capacities,, Saves simulated events to JSON Lines format.          :param filepath: Target pa, Initialize the simulator.          :param start_date: Starting datetime (UTC)., Computes diurnal and day-of-week arrival rate multiplier.         Hospital admis, main() (+6 more)

### Community 8 - "Community 8"
Cohesion: 0.2
Nodes (15): Generates chronologically ordered ICU events across the simulation window., ICUEvent, Convert event to serialized dictionary with ISO string timestamps and UUID strin, Validated representation of an ICU Patient Transition Event.     Serves as the c, str, Unit tests for ICU ADT event schema validation., test_admission_missing_to_unit_fails(), test_admission_with_from_unit_fails() (+7 more)

### Community 9 - "Community 9"
Cohesion: 0.14
Nodes (10): KafkaHDFSSink, Kafka-to-HDFS Sink Pipeline. Consumes events in bounded micro-batches from Apach, Synchronously commit Kafka consumer offsets., Polls Kafka for messages up to max_messages or until idle_timeout expires,, Polls Kafka for messages up to max_messages or until idle_timeout expires,, Closes the Kafka consumer safely., Closes the Kafka consumer safely., Micro-batch consumer that lands Kafka events into partitioned HDFS lake storage (+2 more)

### Community 10 - "Community 10"
Cohesion: 0.15
Nodes (9): ICUKafkaProducer, Resilient Kafka Event Producer. Publishes ICU ADT events to Kafka topic with par, Publishes an iterable of events with optional rate throttling and event limiting, Wait for all outstanding messages to be delivered., Flush remaining messages and close producer., Publishes serialized ICUEvent records to Apache Kafka with delivery confirmation, Internal callback tracking delivery success and failures., Determines the partition routing key.         - 'encounter': Guarantees all tran (+1 more)

### Community 11 - "Community 11"
Cohesion: 0.15
Nodes (13): BaseSettings, Integration tests for the complete live event ingestion pipeline: Kafka Producer, Verifies that reconstructed occupancy DataFrames are persisted to HDFS as Parque, Verifies that:     1. Published events are consumed by KafkaHDFSSink.     2. Sto, Verifies that malformed, corrupted, or non-JSON messages sent to Kafka     are i, test_kafka_to_hdfs_sink_full_cycle(), test_malformed_event_quarantine_to_dead_letter(), test_occupancy_persistence_to_hdfs_and_readback() (+5 more)

### Community 12 - "Community 12"
Cohesion: 0.14
Nodes (8): HDFSClient, WebHDFS REST API Client. Provides cross-platform access to Hadoop HDFS without r, Delete an HDFS file or directory., Check if an HDFS path exists., Atomically rename a file or directory in HDFS.         Destination must be an ab, Robust WebHDFS REST client with Docker Desktop redirect remapping., Check if NameNode WebHDFS endpoint is reachable., Create directory in HDFS.

### Community 13 - "Community 13"
Cohesion: 0.23
Nodes (12): calculate_mae(), calculate_rmse(), calculate_wape(), compute_metrics(), evaluate_forecasts(), Evaluation Metrics and Error Analysis for ICU Occupancy Forecasting. Computes MA, Root Mean Squared Error., Weighted Absolute Percentage Error:     WAPE = sum(|y_true - y_pred|) / sum(y_tr (+4 more)

### Community 14 - "Community 14"
Cohesion: 0.18
Nodes (12): _create_mock_message(), hdfs_client(), kafka_settings(), Pipeline Reliability Audit Integration Tests for Phase 2.1. Verifies failure sce, Scenario: HDFS finalization succeeds, but process crashes before the Kafka offse, Scenario: A micro-batch arrives with a mix of valid events, malformed JSON,, Scenario: Calculator processes raw HDFS events and persists Parquet.     Process, Scenario: A micro-batch contains events spanning multiple date partitions.     F (+4 more)

### Community 15 - "Community 15"
Cohesion: 0.25
Nodes (10): compute_sha256(), download_file(), main(), parse_mimic_events(), Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads, Parses transfers.csv.gz to extract structured patient ICU transition events., Compute SHA-256 hash of a local file., Download a file with HTTP streaming. (+2 more)

### Community 16 - "Community 16"
Cohesion: 0.28
Nodes (6): Enum, DataSource, EventType, Event schemas and validation rules for ICU Admission, Transfer, and Discharge (A, Categorical types of patient bed events., Provenance tracking for events.

### Community 17 - "Community 17"
Cohesion: 0.4
Nodes (5): Persists selected model and evaluation reports to disk:     - Model artifact: jo, save_pipeline_artifacts(), main(), parse_args(), Standalone CLI script to execute the Phase 3 ICU Occupancy Forecasting Training

### Community 18 - "Community 18"
Cohesion: 0.33
Nodes (5): Integration tests for Kafka streaming and Hadoop HDFS storage., Verify WebHDFS read and write cycle., Verify Kafka producer connection and message emission., test_hdfs_connectivity_and_rw(), test_kafka_producer_publish()

### Community 19 - "Community 19"
Cohesion: 0.33
Nodes (5): Integration tests for Kafka-HDFS failure recovery, offset semantics, and idempot, Simulates an HDFS write/rename failure during batch processing.     Asserts that, Verifies that once a micro-batch is finalized and offsets are committed,     res, test_hdfs_failure_aborts_offset_commit(), test_sink_restart_does_not_duplicate_acknowledged_events()

### Community 20 - "Community 20"
Cohesion: 0.33
Nodes (4): HourlyOccupancy, Hourly occupancy census schemas for ICU units. Represents aggregated ground-trut, Ground-truth census record for a specific ICU unit at a specific hour boundary., Serialize occupancy model to dictionary.

### Community 21 - "Community 21"
Cohesion: 0.5
Nodes (4): check_port(), main(), CLI utility to audit, verify, and report port assignments for ICU Bed Demand For, Returns True if the port is open/listening on the given host.

### Community 22 - "Community 22"
Cohesion: 0.67
Nodes (3): main(), parse_args(), Demonstration script for Phase 2: Live Streaming Ingestion and Kafka-to-HDFS Pip

## Knowledge Gaps
- **195 isolated node(s):** `CLI utility to audit, verify, and report port assignments for ICU Bed Demand For`, `Returns True if the port is open/listening on the given host.`, `Standalone CLI script to generate reproducible synthetic ICU event datasets and`, `Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads`, `Compute SHA-256 hash of a local file.` (+190 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **12 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ICUEvent` connect `Community 8` to `Community 0`, `Community 4`, `Community 7`, `Community 9`, `Community 10`, `Community 11`, `Community 14`, `Community 15`, `Community 16`, `Community 18`, `Community 19`?**
  _High betweenness centrality (0.243) - this node is a cross-community bridge._
- **Why does `ForecastingService` connect `Community 0` to `Community 1`, `Community 2`, `Community 3`?**
  _High betweenness centrality (0.175) - this node is a cross-community bridge._
- **Why does `HDFSClient` connect `Community 12` to `Community 6`, `Community 9`, `Community 11`, `Community 14`, `Community 18`, `Community 19`, `Community 22`?**
  _High betweenness centrality (0.138) - this node is a cross-community bridge._
- **Are the 33 inferred relationships involving `ICUEvent` (e.g. with `OccupancyCalculator` and `ICUKafkaProducer`) actually correct?**
  _`ICUEvent` has 33 INFERRED edges - model-reasoned connections that need verification._
- **Are the 10 inferred relationships involving `HDFSClient` (e.g. with `KafkaHDFSSink` and `main()`) actually correct?**
  _`HDFSClient` has 10 INFERRED edges - model-reasoned connections that need verification._
- **Are the 14 inferred relationships involving `ForecastingService` (e.g. with `BaseForecaster` and `PersistenceBaseline`) actually correct?**
  _`ForecastingService` has 14 INFERRED edges - model-reasoned connections that need verification._
- **Are the 18 inferred relationships involving `OccupancyCalculator` (e.g. with `DataSource` and `EventType`) actually correct?**
  _`OccupancyCalculator` has 18 INFERRED edges - model-reasoned connections that need verification._