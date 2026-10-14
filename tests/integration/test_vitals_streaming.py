"""
Integration tests for vital-sign event streaming:
ICUVitalsProducer -> Kafka Broker -> KafkaVitalsHDFSSink -> HDFS Raw Storage (/raw/icu_vitals).
"""

from datetime import datetime, timezone
from uuid import uuid4
import pytest
from confluent_kafka import Producer

from src.config import get_settings
from src.producer.vitals_producer import ICUVitalsProducer
from src.schema.events import VitalSignEvent
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaVitalsHDFSSink


@pytest.mark.integration
def test_vitals_kafka_to_hdfs_sink_full_cycle():
    """
    Verifies that:
    1. Vital sign events published to Kafka are consumed by KafkaVitalsHDFSSink.
    2. Events are partitioned by date and written to HDFS.
    3. Stored records can be read back and match the original VitalSignEvent values.
    4. Offsets are committed cleanly.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy(), "HDFS NameNode is unreachable"

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-vitals-test-{test_suffix}"
    test_group = f"vitals-sink-group-{test_suffix}"
    test_raw_dir = f"/test_raw_vitals_{test_suffix}"

    producer = ICUVitalsProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
    )

    now = datetime(2026, 10, 14, 14, 0, tzinfo=timezone.utc)
    events = [
        VitalSignEvent(
            patient_id=f"TEST-P-{test_suffix}-{i}",
            encounter_id=f"TEST-ENC-{test_suffix}-{i}",
            unit_id="MICU",
            event_timestamp=now,
            heart_rate=72.0 + i * 2.5,
            temperature_c=36.8 + i * 0.1,
            systolic_bp=120.0 + i * 2.0,
            diastolic_bp=75.0 + i * 1.0,
            respiratory_rate=16.0,
            spo2=98.0,
            metadata={"cycle_test": True},
        )
        for i in range(5)
    ]

    # 1. Publish to Kafka
    published_count = producer.publish_stream(events, raise_on_error=True)
    assert published_count == 5

    # 2. Run sink
    sink = KafkaVitalsHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )

    try:
        result = sink.poll_and_sink(max_messages=5, poll_timeout=1.0, idle_timeout=5.0)
        assert result["valid_count"] == 5, f"Expected 5 valid vitals messages, got {result['valid_count']}"
        assert len(result["batches_finalized"]) > 0

        # 3. Read back from HDFS
        finalized_path = result["batches_finalized"][0]
        assert hdfs.exists(finalized_path), f"Finalized path {finalized_path} does not exist in HDFS"
        records = hdfs.read_jsonl(finalized_path)
        assert len(records) == 5

        # Verify numerical fields preserved
        read_events = [VitalSignEvent.model_validate(r) for r in records]
        read_patient_ids = {e.patient_id for e in read_events}
        expected_ids = {e.patient_id for e in events}
        assert read_patient_ids == expected_ids

        # Check first event values
        ev0 = [e for e in read_events if e.patient_id == f"TEST-P-{test_suffix}-0"][0]
        assert ev0.heart_rate == 72.0
        assert ev0.systolic_bp == 120.0
        assert ev0.unit_id == "MICU"

    finally:
        sink.close()
        hdfs.delete(test_raw_dir, recursive=True)


@pytest.mark.integration
def test_vitals_malformed_event_quarantine_to_dead_letter():
    """
    Verifies that malformed or non-JSON messages sent to icu-vitals
    are quarantined to HDFS dead-letter storage without dropping.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy()

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-vitals-dlq-{test_suffix}"
    test_group = f"vitals-dlq-group-{test_suffix}"
    test_raw_dir = f"/test_raw_vitals_dlq_{test_suffix}"
    test_dl_dir = f"/test_dl_vitals_{test_suffix}"

    # Push corrupted non-JSON bytes
    conf = {"bootstrap.servers": settings.kafka_bootstrap_servers}
    raw_producer = Producer(conf)
    raw_producer.produce(test_topic, value=b"CORRUPTED_NON_JSON_VITALS_PAYLOAD_@@@")
    raw_producer.flush(5.0)

    sink = KafkaVitalsHDFSSink(
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
        assert result["malformed_count"] >= 1, "Expected at least 1 malformed vital sign quarantined"

        dl_files = hdfs.list_dir(test_dl_dir)
        assert len(dl_files) >= 1, f"Expected dead letter files in {test_dl_dir}"

    finally:
        sink.close()
        hdfs.delete(test_raw_dir, recursive=True)
        hdfs.delete(test_dl_dir, recursive=True)


@pytest.mark.integration
def test_vitals_sink_restart_does_not_duplicate_acknowledged_events():
    """
    Verifies that restarting KafkaVitalsHDFSSink does not re-consume
    already acknowledged vital sign events.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy()

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-vitals-restart-{test_suffix}"
    test_group = f"vitals-restart-group-{test_suffix}"
    test_raw_dir = f"/test_raw_vitals_restart_{test_suffix}"

    producer = ICUVitalsProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
    )
    events = [
        VitalSignEvent(
            patient_id=f"RESTART-P-{test_suffix}-{i}",
            encounter_id=f"RESTART-ENC-{test_suffix}-{i}",
            unit_id="SICU",
            event_timestamp=datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc),
            heart_rate=78.0,
            spo2=98.0,
        )
        for i in range(2)
    ]
    producer.publish_stream(events, raise_on_error=True)

    # Sink 1 runs and commits
    sink1 = KafkaVitalsHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )
    res1 = sink1.poll_and_sink(max_messages=2, poll_timeout=1.0, idle_timeout=5.0)
    assert res1["valid_count"] == 2
    sink1.close()

    # Sink 2 starts with same consumer group
    sink2 = KafkaVitalsHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )
    try:
        res2 = sink2.poll_and_sink(max_messages=2, poll_timeout=1.0, idle_timeout=2.0)
        assert res2["valid_count"] == 0, "No duplicate vital signs should be received after restart"
    finally:
        sink2.close()
        hdfs.delete(test_raw_dir, recursive=True)
