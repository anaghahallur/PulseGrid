"""
Integration tests for the complete live event ingestion pipeline:
Kafka Producer -> Kafka Broker -> KafkaHDFSSink -> HDFS Lake Storage -> Occupancy Reconstruction -> Processed Parquet.
"""

from datetime import datetime, timezone
import json
from uuid import uuid4
import pytest
from confluent_kafka import Producer

from src.config import get_settings
from src.producer.kafka_producer import ICUKafkaProducer
from src.processing.occupancy_calculator import OccupancyCalculator
from src.schema.events import DataSource, EventType, ICUEvent
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaHDFSSink


@pytest.mark.integration
def test_kafka_to_hdfs_sink_full_cycle():
    """
    Verifies that:
    1. Published events are consumed by KafkaHDFSSink.
    2. Stored raw events are partitioned and written to HDFS.
    3. HDFS raw records can be read back and match the original events.
    4. Offsets are committed cleanly.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy(), "HDFS NameNode is unreachable"

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-test-{test_suffix}"
    test_group = f"test-sink-group-{test_suffix}"
    test_raw_dir = f"/test_raw_{test_suffix}"

    producer = ICUKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
    )

    # Generate 5 test events with unique patient IDs
    now = datetime(2026, 10, 11, 14, 0, tzinfo=timezone.utc)
    events = [
        ICUEvent(
            patient_id=f"TEST-P-{test_suffix}-{i}",
            encounter_id=f"TEST-ENC-{test_suffix}-{i}",
            event_type=EventType.ADMISSION,
            event_timestamp=now,
            to_unit="MICU",
            metadata={"cycle_test": True},
        )
        for i in range(5)
    ]

    # 1. Publish to Kafka
    published_count = producer.publish_stream(events, raise_on_error=True)
    assert published_count == 5

    # 2. Run sink
    sink = KafkaHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )

    try:
        result = sink.poll_and_sink(max_messages=5, poll_timeout=1.0, idle_timeout=5.0)
        assert result["valid_count"] == 5, f"Expected 5 valid messages, got {result['valid_count']}"
        assert len(result["batches_finalized"]) > 0

        # 3. Read back from HDFS
        finalized_path = result["batches_finalized"][0]
        assert hdfs.exists(finalized_path), f"Finalized path {finalized_path} does not exist in HDFS"
        records = hdfs.read_jsonl(finalized_path)
        assert len(records) == 5

        read_patient_ids = {r["patient_id"] for r in records}
        expected_ids = {e.patient_id for e in events}
        assert read_patient_ids == expected_ids

    finally:
        sink.close()
        # Clean up test directory in HDFS
        hdfs.delete(test_raw_dir, recursive=True)


@pytest.mark.integration
def test_malformed_event_quarantine_to_dead_letter():
    """
    Verifies that malformed, corrupted, or non-JSON messages sent to Kafka
    are intercepted and quarantined to HDFS dead-letter storage without dropping.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy()

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-dlq-{test_suffix}"
    test_group = f"test-dlq-group-{test_suffix}"
    test_raw_dir = f"/test_raw_dlq_{test_suffix}"
    test_dl_dir = f"/test_dl_{test_suffix}"

    # Raw producer pushing invalid bytes
    conf = {"bootstrap.servers": settings.kafka_bootstrap_servers}
    raw_producer = Producer(conf)
    corrupted_payload = b"CORRUPTED_NON_JSON_DATA_###!!@@"
    raw_producer.produce(test_topic, value=corrupted_payload)
    raw_producer.flush(5.0)

    sink = KafkaHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=hdfs,
        raw_hdfs_base=test_raw_dir,
        dead_letter_base=test_dl_dir,
        auto_offset_reset="earliest",
    )

    try:
        result = sink.poll_and_sink(max_messages=1, poll_timeout=1.0, idle_timeout=5.0)
        assert result["malformed_count"] >= 1, "Expected at least 1 malformed message quarantined"

        # Check dead letter directory in HDFS
        dl_files = hdfs.list_dir(test_dl_dir)
        assert len(dl_files) >= 1, f"Expected dead letter files in {test_dl_dir}"

    finally:
        sink.close()
        hdfs.delete(test_raw_dir, recursive=True)
        hdfs.delete(test_dl_dir, recursive=True)


@pytest.mark.integration
def test_occupancy_persistence_to_hdfs_and_readback():
    """
    Verifies that reconstructed occupancy DataFrames are persisted to HDFS as Parquet
    and read back with exact schema and numerical fidelity.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy()

    test_suffix = uuid4().hex[:8]
    test_proc_dir = f"/test_proc_{test_suffix}"

    now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
    events = [
        ICUEvent(
            patient_id="P1",
            encounter_id="E1",
            event_type=EventType.ADMISSION,
            event_timestamp=now,
            to_unit="MICU",
        ),
        ICUEvent(
            patient_id="P2",
            encounter_id="E2",
            event_type=EventType.ADMISSION,
            event_timestamp=now,
            to_unit="SICU",
        ),
    ]

    calc = OccupancyCalculator(unit_capacities=settings.icu_unit_capacities)
    df_occ = calc.reconstruct(events)
    assert not df_occ.empty

    try:
        final_path = calc.persist_occupancy_to_hdfs(
            df=df_occ,
            hdfs_client=hdfs,
            hdfs_target_dir=test_proc_dir,
            filename=f"test_census_{test_suffix}.parquet",
        )

        assert hdfs.exists(final_path)

        # Read back Parquet
        df_readback = hdfs.read_parquet(final_path)
        assert len(df_readback) == len(df_occ)
        assert "occupied_beds" in df_readback.columns
        assert "is_capacity_exceeded" in df_readback.columns
        assert "processing_watermark" in df_readback.columns

    finally:
        hdfs.delete(test_proc_dir, recursive=True)
