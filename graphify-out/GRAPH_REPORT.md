# Graph Report - bda proj  (2026-10-11)

## Corpus Check
- 31 files · ~17,116 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 320 nodes · 484 edges · 24 communities (14 shown, 10 thin omitted)
- Extraction: 71% EXTRACTED · 29% INFERRED · 0% AMBIGUOUS · INFERRED: 142 edges (avg confidence: 0.78)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `3b9f150d`
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

## God Nodes (most connected - your core abstractions)
1. `ICUEvent` - 37 edges
2. `HDFSClient` - 28 edges
3. `OccupancyCalculator` - 23 edges
4. `KafkaHDFSSink` - 18 edges
5. `get_settings()` - 16 edges
6. `ICUKafkaProducer` - 15 edges
7. `ICUSimulator` - 15 edges
8. `prepare_forecasting_dataset()` - 11 edges
9. `DirectRidgeForecaster` - 11 edges
10. `run_training_and_evaluation()` - 11 edges

## Surprising Connections (you probably didn't know these)
- `kafka_settings()` --calls--> `get_settings()`  [INFERRED]
  tests/integration/test_pipeline_reliability_audit.py → src/config.py
- `hdfs_client()` --calls--> `HDFSClient`  [INFERRED]
  tests/integration/test_pipeline_reliability_audit.py → src/storage/hdfs_client.py
- `main()` --calls--> `get_settings()`  [INFERRED]
  scripts/generate_synthetic_data.py → src/config.py
- `main()` --calls--> `OccupancyCalculator`  [INFERRED]
  scripts/generate_synthetic_data.py → src/processing/occupancy_calculator.py
- `parse_mimic_events()` --calls--> `ICUEvent`  [INFERRED]
  scripts/ingest_mimic_data.py → src/schema/events.py

## Communities (24 total, 10 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.05
Nodes (43): Integration tests for the complete live event ingestion pipeline: Kafka Producer, Verifies that reconstructed occupancy DataFrames are persisted to HDFS as Parque, Verifies that:     1. Published events are consumed by KafkaHDFSSink.     2. Sto, Verifies that malformed, corrupted, or non-JSON messages sent to Kafka     are i, test_kafka_to_hdfs_sink_full_cycle(), test_malformed_event_quarantine_to_dead_letter(), test_occupancy_persistence_to_hdfs_and_readback(), Integration tests for Kafka-HDFS failure recovery, offset semantics, and idempot (+35 more)

### Community 1 - "Community 1"
Cohesion: 0.06
Nodes (32): Enum, OccupancyCalculator, Hourly Occupancy Reconstruction Engine. Reconstructs exact instantaneous unit ce, State machine that processes patient ADT events to reconstruct hourly census., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, :param unit_capacities: Mapping of unit identifier to total bed capacity., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, Reconstructs hourly occupancy time series across all known units.          :para (+24 more)

### Community 2 - "Community 2"
Cohesion: 0.08
Nodes (19): ABC, BaseForecaster, DirectHistGradientBoostingForecaster, DirectRandomForestForecaster, DirectRidgeForecaster, PersistenceBaseline, Multi-Horizon Forecasting Models for ICU Bed Demand. Implements: 1. Persistence, Direct Multi-Horizon Ridge Regression.     Trains independent regularized linear (+11 more)

### Community 3 - "Community 3"
Cohesion: 0.1
Nodes (28): _create_mock_message(), hdfs_client(), kafka_settings(), Pipeline Reliability Audit Integration Tests for Phase 2.1. Verifies failure sce, Scenario: HDFS finalization succeeds, but process crashes before the Kafka offse, Scenario: A micro-batch arrives with a mix of valid events, malformed JSON,, Scenario: Calculator processes raw HDFS events and persists Parquet.     Process, Scenario: A micro-batch contains events spanning multiple date partitions.     F (+20 more)

### Community 4 - "Community 4"
Cohesion: 0.09
Nodes (29): build_unit_features_and_targets(), prepare_forecasting_dataset(), Feature Engineering and Target Construction for ICU Occupancy Forecasting. Const, Constructs leakage-safe features and targets for a single ICU unit.     All shif, Constructs full forecasting dataset across all ICU units while strictly isolatin, chronological_split(), load_forecaster(), End-to-End Training, Evaluation, and Serialization Pipeline for ICU Occupancy Fo (+21 more)

### Community 5 - "Community 5"
Cohesion: 0.1
Nodes (11): List contents of an HDFS directory., Write list of dictionaries as newline-delimited JSON to HDFS., Read newline-delimited JSON file from HDFS into list of dictionaries., Recursively lists all file paths under base_path matching suffix.         Ignore, Read Parquet file from HDFS into a pandas DataFrame., Recursively finds and reads all JSONL files under base_path into a combined list, Serialize a pandas DataFrame to Parquet and write to HDFS., Read Parquet file from HDFS into a pandas DataFrame. (+3 more)

### Community 6 - "Community 6"
Cohesion: 0.11
Nodes (14): ICUSimulator, Deterministic Synthetic ICU Event Generator. Produces high-fidelity patient admi, Samples ICU length of stay from a log-normal distribution.         Clinical ICU, Simulates patient flow through an ICU network with configurable capacities,, Saves simulated events to JSON Lines format.          :param filepath: Target pa, Initialize the simulator.          :param start_date: Starting datetime (UTC)., Computes diurnal and day-of-week arrival rate multiplier.         Hospital admis, main() (+6 more)

### Community 7 - "Community 7"
Cohesion: 0.23
Nodes (12): calculate_mae(), calculate_rmse(), calculate_wape(), compute_metrics(), evaluate_forecasts(), Evaluation Metrics and Error Analysis for ICU Occupancy Forecasting. Computes MA, Root Mean Squared Error., Weighted Absolute Percentage Error:     WAPE = sum(|y_true - y_pred|) / sum(y_tr (+4 more)

### Community 8 - "Community 8"
Cohesion: 0.25
Nodes (10): compute_sha256(), download_file(), main(), parse_mimic_events(), Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads, Parses transfers.csv.gz to extract structured patient ICU transition events., Compute SHA-256 hash of a local file., Download a file with HTTP streaming. (+2 more)

### Community 9 - "Community 9"
Cohesion: 0.22
Nodes (5): Synchronously commit Kafka consumer offsets., Polls Kafka for messages up to max_messages or until idle_timeout expires,, Polls Kafka for messages up to max_messages or until idle_timeout expires,, Parses and validates a Kafka message payload.         Returns (valid_event, None, Processes a bounded batch of Kafka messages:         1. Validates and segregates

### Community 10 - "Community 10"
Cohesion: 0.29
Nodes (5): BaseModel, HourlyOccupancy, Hourly occupancy census schemas for ICU units. Represents aggregated ground-trut, Ground-truth census record for a specific ICU unit at a specific hour boundary., Serialize occupancy model to dictionary.

### Community 11 - "Community 11"
Cohesion: 0.4
Nodes (5): Persists selected model and evaluation reports to disk:     - Model artifact: jo, save_pipeline_artifacts(), main(), parse_args(), Standalone CLI script to execute the Phase 3 ICU Occupancy Forecasting Training

### Community 12 - "Community 12"
Cohesion: 0.4
Nodes (4): BaseSettings, Application configuration module using Pydantic Settings. Loads environment vari, System-wide configuration settings with environment override support., Settings

## Knowledge Gaps
- **136 isolated node(s):** `Standalone CLI script to generate reproducible synthetic ICU event datasets and`, `Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads`, `Compute SHA-256 hash of a local file.`, `Download a file with HTTP streaming.`, `Verify downloaded files against SHA256SUMS.txt if available.` (+131 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `chronological_split()` connect `Community 4` to `Community 2`, `Community 3`?**
  _High betweenness centrality (0.376) - this node is a cross-community bridge._
- **Why does `ICUEvent` connect `Community 3` to `Community 0`, `Community 1`, `Community 6`, `Community 8`, `Community 10`?**
  _High betweenness centrality (0.240) - this node is a cross-community bridge._
- **Why does `HDFSClient` connect `Community 0` to `Community 3`, `Community 5`?**
  _High betweenness centrality (0.203) - this node is a cross-community bridge._
- **Are the 33 inferred relationships involving `ICUEvent` (e.g. with `OccupancyCalculator` and `ICUKafkaProducer`) actually correct?**
  _`ICUEvent` has 33 INFERRED edges - model-reasoned connections that need verification._
- **Are the 10 inferred relationships involving `HDFSClient` (e.g. with `KafkaHDFSSink` and `main()`) actually correct?**
  _`HDFSClient` has 10 INFERRED edges - model-reasoned connections that need verification._
- **Are the 18 inferred relationships involving `OccupancyCalculator` (e.g. with `DataSource` and `EventType`) actually correct?**
  _`OccupancyCalculator` has 18 INFERRED edges - model-reasoned connections that need verification._
- **Are the 10 inferred relationships involving `KafkaHDFSSink` (e.g. with `ICUEvent` and `HDFSClient`) actually correct?**
  _`KafkaHDFSSink` has 10 INFERRED edges - model-reasoned connections that need verification._