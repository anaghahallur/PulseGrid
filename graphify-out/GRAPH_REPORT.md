# Graph Report - bda proj  (2026-10-11)

## Corpus Check
- 23 files · ~9,670 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 197 nodes · 288 edges · 19 communities (12 shown, 7 thin omitted)
- Extraction: 69% EXTRACTED · 31% INFERRED · 0% AMBIGUOUS · INFERRED: 88 edges (avg confidence: 0.76)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `31bbe077`
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

## God Nodes (most connected - your core abstractions)
1. `ICUEvent` - 30 edges
2. `HDFSClient` - 25 edges
3. `OccupancyCalculator` - 18 edges
4. `get_settings()` - 15 edges
5. `ICUKafkaProducer` - 15 edges
6. `ICUSimulator` - 15 edges
7. `KafkaHDFSSink` - 14 edges
8. `main()` - 8 edges
9. `test_hdfs_failure_aborts_offset_commit()` - 8 edges
10. `DataSource` - 7 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `get_settings()`  [INFERRED]
  scripts/generate_synthetic_data.py → src/config.py
- `main()` --calls--> `OccupancyCalculator`  [INFERRED]
  scripts/generate_synthetic_data.py → src/processing/occupancy_calculator.py
- `parse_mimic_events()` --calls--> `ICUEvent`  [INFERRED]
  scripts/ingest_mimic_data.py → src/schema/events.py
- `main()` --calls--> `get_settings()`  [INFERRED]
  scripts/run_streaming_pipeline.py → src/config.py
- `main()` --calls--> `HDFSClient`  [INFERRED]
  scripts/run_streaming_pipeline.py → src/storage/hdfs_client.py

## Communities (19 total, 7 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.07
Nodes (29): Integration tests for the complete live event ingestion pipeline: Kafka Producer, Verifies that reconstructed occupancy DataFrames are persisted to HDFS as Parque, Verifies that:     1. Published events are consumed by KafkaHDFSSink.     2. Sto, Verifies that malformed, corrupted, or non-JSON messages sent to Kafka     are i, test_kafka_to_hdfs_sink_full_cycle(), test_malformed_event_quarantine_to_dead_letter(), test_occupancy_persistence_to_hdfs_and_readback(), Integration tests for Kafka-HDFS failure recovery, offset semantics, and idempot (+21 more)

### Community 1 - "Community 1"
Cohesion: 0.1
Nodes (19): OccupancyCalculator, Hourly Occupancy Reconstruction Engine. Reconstructs exact instantaneous unit ce, State machine that processes patient ADT events to reconstruct hourly census., Persists reconstructed hourly census DataFrame to HDFS as Parquet         using, :param unit_capacities: Mapping of unit identifier to total bed capacity., Reconstructs hourly occupancy time series across all known units.          :para, Unit tests for occupancy calculation and double-counting prevention., Verifies that feeding duplicate events (with the same event_id)     does not inf (+11 more)

### Community 2 - "Community 2"
Cohesion: 0.12
Nodes (12): Integration tests for Kafka streaming and Hadoop HDFS storage., Verify Kafka producer connection and message emission., test_kafka_producer_publish(), ICUKafkaProducer, Resilient Kafka Event Producer. Publishes ICU ADT events to Kafka topic with par, Publishes an iterable of events with optional rate throttling and event limiting, Wait for all outstanding messages to be delivered., Flush remaining messages and close producer. (+4 more)

### Community 3 - "Community 3"
Cohesion: 0.11
Nodes (14): ICUSimulator, Deterministic Synthetic ICU Event Generator. Produces high-fidelity patient admi, Samples ICU length of stay from a log-normal distribution.         Clinical ICU, Simulates patient flow through an ICU network with configurable capacities,, Saves simulated events to JSON Lines format.          :param filepath: Target pa, Initialize the simulator.          :param start_date: Starting datetime (UTC)., Computes diurnal and day-of-week arrival rate multiplier.         Hospital admis, main() (+6 more)

### Community 4 - "Community 4"
Cohesion: 0.2
Nodes (15): Generates chronologically ordered ICU events across the simulation window., ICUEvent, Convert event to serialized dictionary with ISO string timestamps and UUID strin, Validated representation of an ICU Patient Transition Event.     Serves as the c, str, Unit tests for ICU ADT event schema validation., test_admission_missing_to_unit_fails(), test_admission_with_from_unit_fails() (+7 more)

### Community 5 - "Community 5"
Cohesion: 0.14
Nodes (11): BaseModel, Enum, DataSource, EventType, Event schemas and validation rules for ICU Admission, Transfer, and Discharge (A, Categorical types of patient bed events., Provenance tracking for events., HourlyOccupancy (+3 more)

### Community 6 - "Community 6"
Cohesion: 0.14
Nodes (7): Write list of dictionaries as newline-delimited JSON to HDFS., Read newline-delimited JSON file from HDFS into list of dictionaries., Serialize a pandas DataFrame to Parquet and write to HDFS., Read Parquet file from HDFS into a pandas DataFrame., Remaps internal Docker container hostnames in DataNode redirects to localhost, Write bytes payload to HDFS using 2-step WebHDFS protocol., Read file bytes from HDFS.

### Community 7 - "Community 7"
Cohesion: 0.25
Nodes (10): compute_sha256(), download_file(), main(), parse_mimic_events(), Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads, Parses transfers.csv.gz to extract structured patient ICU transition events., Compute SHA-256 hash of a local file., Download a file with HTTP streaming. (+2 more)

### Community 8 - "Community 8"
Cohesion: 0.33
Nodes (3): Polls Kafka for messages up to max_messages or until idle_timeout expires,, Parses and validates a Kafka message payload.         Returns (valid_event, None, Processes a bounded batch of Kafka messages:         1. Validates and segregates

### Community 9 - "Community 9"
Cohesion: 0.4
Nodes (4): BaseSettings, Application configuration module using Pydantic Settings. Loads environment vari, System-wide configuration settings with environment override support., Settings

### Community 10 - "Community 10"
Cohesion: 0.67
Nodes (3): main(), parse_args(), Demonstration script for Phase 2: Live Streaming Ingestion and Kafka-to-HDFS Pip

## Knowledge Gaps
- **85 isolated node(s):** `Standalone CLI script to generate reproducible synthetic ICU event datasets and`, `Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2. Downloads`, `Compute SHA-256 hash of a local file.`, `Download a file with HTTP streaming.`, `Verify downloaded files against SHA256SUMS.txt if available.` (+80 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ICUEvent` connect `Community 4` to `Community 0`, `Community 1`, `Community 2`, `Community 3`, `Community 5`, `Community 7`?**
  _High betweenness centrality (0.361) - this node is a cross-community bridge._
- **Why does `HDFSClient` connect `Community 0` to `Community 10`, `Community 6`?**
  _High betweenness centrality (0.265) - this node is a cross-community bridge._
- **Why does `OccupancyCalculator` connect `Community 1` to `Community 0`, `Community 3`, `Community 4`, `Community 5`, `Community 10`?**
  _High betweenness centrality (0.146) - this node is a cross-community bridge._
- **Are the 26 inferred relationships involving `ICUEvent` (e.g. with `OccupancyCalculator` and `ICUKafkaProducer`) actually correct?**
  _`ICUEvent` has 26 INFERRED edges - model-reasoned connections that need verification._
- **Are the 9 inferred relationships involving `HDFSClient` (e.g. with `KafkaHDFSSink` and `main()`) actually correct?**
  _`HDFSClient` has 9 INFERRED edges - model-reasoned connections that need verification._
- **Are the 13 inferred relationships involving `OccupancyCalculator` (e.g. with `DataSource` and `EventType`) actually correct?**
  _`OccupancyCalculator` has 13 INFERRED edges - model-reasoned connections that need verification._
- **Are the 12 inferred relationships involving `get_settings()` (e.g. with `main()` and `main()`) actually correct?**
  _`get_settings()` has 12 INFERRED edges - model-reasoned connections that need verification._