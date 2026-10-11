# Graph Report - bda proj  (2026-10-11)

## Corpus Check
- 24 files · ~11,342 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 226 nodes · 342 edges · 17 communities (10 shown, 7 thin omitted)
- Extraction: 69% EXTRACTED · 31% INFERRED · 0% AMBIGUOUS · INFERRED: 107 edges (avg confidence: 0.77)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `9e075ec9`
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

## God Nodes (most connected - your core abstractions)
1. `ICUEvent` - 37 edges
2. `HDFSClient` - 28 edges
3. `OccupancyCalculator` - 23 edges
4. `KafkaHDFSSink` - 18 edges
5. `get_settings()` - 16 edges
6. `ICUKafkaProducer` - 15 edges
7. `ICUSimulator` - 15 edges
8. `main()` - 8 edges
9. `test_hdfs_failure_aborts_offset_commit()` - 8 edges
10. `DataSource` - 7 edges

## Surprising Connections (you probably didn't know these)
- `hdfs_client()` --calls--> `HDFSClient`  [INFERRED]
  tests/integration/test_pipeline_reliability_audit.py → src/storage/hdfs_client.py
- `main()` --calls--> `get_settings()`  [INFERRED]
  scripts/generate_synthetic_data.py → src/config.py
- `main()` --calls--> `OccupancyCalculator`  [INFERRED]
  scripts/generate_synthetic_data.py → src/processing/occupancy_calculator.py
- `main()` --calls--> `HDFSClient`  [INFERRED]
  scripts/run_streaming_pipeline.py → src/storage/hdfs_client.py
- `main()` --calls--> `ICUSimulator`  [INFERRED]
  scripts/run_streaming_pipeline.py → src/producer/simulator.py

## Communities (17 total, 7 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.07
Nodes (19): HDFSClient, WebHDFS REST API Client. Provides cross-platform access to Hadoop HDFS without r, List contents of an HDFS directory., Delete an HDFS file or directory., Check if an HDFS path exists., Atomically rename a file or directory in HDFS.         Destination must be an ab, Write list of dictionaries as newline-delimited JSON to HDFS., Read newline-delimited JSON file from HDFS into list of dictionaries. (+11 more)

### Community 1 - "Community 1"
Cohesion: 0.08
Nodes (26): OccupancyCalculator, Hourly Occupancy Reconstruction Engine. Reconstructs exact instantaneous unit ce, State machine that processes patient ADT events to reconstruct hourly census., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, :param unit_capacities: Mapping of unit identifier to total bed capacity., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, Reconstructs hourly occupancy time series across all known units.          :para, Unit tests for occupancy calculation and double-counting prevention. (+18 more)

### Community 2 - "Community 2"
Cohesion: 0.09
Nodes (21): _create_mock_message(), hdfs_client(), Pipeline Reliability Audit Integration Tests for Phase 2.1. Verifies failure sce, Scenario: HDFS finalization succeeds, but process crashes before the Kafka offse, Scenario: A micro-batch arrives with a mix of valid events, malformed JSON,, Scenario: Calculator processes raw HDFS events and persists Parquet.     Process, Scenario: A micro-batch contains events spanning multiple date partitions.     F, test_multi_date_partition_hdfs_failure_aborts_offset_commit() (+13 more)

### Community 3 - "Community 3"
Cohesion: 0.09
Nodes (22): BaseSettings, Integration tests for the complete live event ingestion pipeline: Kafka Producer, Verifies that reconstructed occupancy DataFrames are persisted to HDFS as Parque, Verifies that:     1. Published events are consumed by KafkaHDFSSink.     2. Sto, Verifies that malformed, corrupted, or non-JSON messages sent to Kafka     are i, test_kafka_to_hdfs_sink_full_cycle(), test_malformed_event_quarantine_to_dead_letter(), test_occupancy_persistence_to_hdfs_and_readback() (+14 more)

### Community 4 - "Community 4"
Cohesion: 0.11
Nodes (14): Integration tests for Kafka-HDFS failure recovery, offset semantics, and idempot, Simulates an HDFS write/rename failure during batch processing.     Asserts that, Verifies that once a micro-batch is finalized and offsets are committed,     res, test_hdfs_failure_aborts_offset_commit(), test_sink_restart_does_not_duplicate_acknowledged_events(), ICUKafkaProducer, Resilient Kafka Event Producer. Publishes ICU ADT events to Kafka topic with par, Publishes an iterable of events with optional rate throttling and event limiting (+6 more)

### Community 5 - "Community 5"
Cohesion: 0.11
Nodes (14): ICUSimulator, Deterministic Synthetic ICU Event Generator. Produces high-fidelity patient admi, Samples ICU length of stay from a log-normal distribution.         Clinical ICU, Simulates patient flow through an ICU network with configurable capacities,, Saves simulated events to JSON Lines format.          :param filepath: Target pa, Initialize the simulator.          :param start_date: Starting datetime (UTC)., Computes diurnal and day-of-week arrival rate multiplier.         Hospital admis, main() (+6 more)

### Community 6 - "Community 6"
Cohesion: 0.18
Nodes (17): Generates chronologically ordered ICU events across the simulation window., ICUEvent, Convert event to serialized dictionary with ISO string timestamps and UUID strin, Validated representation of an ICU Patient Transition Event.     Serves as the c, parse_mimic_events(), Parses transfers.csv.gz to extract structured patient ICU transition events., str, Unit tests for ICU ADT event schema validation. (+9 more)

### Community 7 - "Community 7"
Cohesion: 0.14
Nodes (11): BaseModel, Enum, DataSource, EventType, Event schemas and validation rules for ICU Admission, Transfer, and Discharge (A, Categorical types of patient bed events., Provenance tracking for events., HourlyOccupancy (+3 more)

### Community 8 - "Community 8"
Cohesion: 0.31
Nodes (8): compute_sha256(), download_file(), main(), Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads, Compute SHA-256 hash of a local file., Download a file with HTTP streaming., Verify downloaded files against SHA256SUMS.txt if available., verify_checksums()

## Knowledge Gaps
- **99 isolated node(s):** `Standalone CLI script to generate reproducible synthetic ICU event datasets and`, `Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads`, `Compute SHA-256 hash of a local file.`, `Download a file with HTTP streaming.`, `Verify downloaded files against SHA256SUMS.txt if available.` (+94 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ICUEvent` connect `Community 6` to `Community 1`, `Community 2`, `Community 3`, `Community 4`, `Community 5`, `Community 7`?**
  _High betweenness centrality (0.374) - this node is a cross-community bridge._
- **Why does `HDFSClient` connect `Community 0` to `Community 2`, `Community 3`, `Community 4`?**
  _High betweenness centrality (0.276) - this node is a cross-community bridge._
- **Why does `KafkaHDFSSink` connect `Community 2` to `Community 0`, `Community 3`, `Community 4`, `Community 6`?**
  _High betweenness centrality (0.151) - this node is a cross-community bridge._
- **Are the 33 inferred relationships involving `ICUEvent` (e.g. with `OccupancyCalculator` and `ICUKafkaProducer`) actually correct?**
  _`ICUEvent` has 33 INFERRED edges - model-reasoned connections that need verification._
- **Are the 10 inferred relationships involving `HDFSClient` (e.g. with `KafkaHDFSSink` and `main()`) actually correct?**
  _`HDFSClient` has 10 INFERRED edges - model-reasoned connections that need verification._
- **Are the 18 inferred relationships involving `OccupancyCalculator` (e.g. with `DataSource` and `EventType`) actually correct?**
  _`OccupancyCalculator` has 18 INFERRED edges - model-reasoned connections that need verification._
- **Are the 10 inferred relationships involving `KafkaHDFSSink` (e.g. with `ICUEvent` and `HDFSClient`) actually correct?**
  _`KafkaHDFSSink` has 10 INFERRED edges - model-reasoned connections that need verification._