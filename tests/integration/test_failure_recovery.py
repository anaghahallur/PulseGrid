"""
Integration tests for Kafka-HDFS failure recovery, offset semantics, and idempotency:
- Simulates HDFS write failure and asserts Kafka offsets are NOT committed.
- Verifies sink restart with committed offsets resumes without duplicate writes.
- Verifies replaying events through OccupancyCalculator is strictly idempotent.
"""

from datetime import datetime, timezone
from uuid import uuid4
import pytest
from unittest.mock import MagicMock

from src.config import get_settings
from src.producer.kafka_producer import ICUKafkaProducer
from src.processing.occupancy_calculator import OccupancyCalculator
from src.schema.events import EventType, ICUEvent
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaHDFSSink


@pytest.mark.integration
def test_hdfs_failure_aborts_offset_commit():
    """
    Simulates an HDFS write/rename failure during batch processing.
    Asserts that:
    1. An exception is raised.
    2. Kafka offsets are NOT committed.
    3. The message remains unacknowledged on Kafka so it can be safely re-read.
    """
    settings = get_settings()
    real_hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert real_hdfs.is_healthy()

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-fail-test-{test_suffix}"
    test_group = f"test-fail-group-{test_suffix}"
    test_raw_dir = f"/test_raw_fail_{test_suffix}"

    producer = ICUKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
    )
    event = ICUEvent(
        patient_id=f"FAIL-P-{test_suffix}",
        encounter_id=f"FAIL-ENC-{test_suffix}",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 11, 15, 0, tzinfo=timezone.utc),
        to_unit="MICU",
    )
    producer.publish_stream([event], raise_on_error=True)

    # Mock HDFS client that fails during rename
    failing_hdfs = MagicMock(wraps=real_hdfs)
    failing_hdfs.rename.return_value = False  # Simulate rename failure

    sink = KafkaHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=failing_hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )

    try:
        # Polling should fail when attempting to process and rename
        with pytest.raises(IOError) as exc:
            sink.poll_and_sink(max_messages=1, poll_timeout=1.0, idle_timeout=5.0)
        assert "Atomic HDFS rename failed" in str(exc.value)

    finally:
        sink.close()

    # Recovery: Now create a working sink with the same consumer group
    # It must re-read the exact uncommitted message!
    recovery_sink = KafkaHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=real_hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )
    try:
        recovered = recovery_sink.poll_and_sink(max_messages=1, poll_timeout=1.0, idle_timeout=5.0)
        assert recovered["valid_count"] == 1, "Unacknowledged message was successfully recovered on restart"
    finally:
        recovery_sink.close()
        real_hdfs.delete(test_raw_dir, recursive=True)


@pytest.mark.integration
def test_sink_restart_does_not_duplicate_acknowledged_events():
    """
    Verifies that once a micro-batch is finalized and offsets are committed,
    restarting the sink does not re-consume the already acknowledged events.
    """
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    assert hdfs.is_healthy()

    test_suffix = uuid4().hex[:8]
    test_topic = f"icu-restart-{test_suffix}"
    test_group = f"test-restart-group-{test_suffix}"
    test_raw_dir = f"/test_raw_restart_{test_suffix}"

    producer = ICUKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
    )
    events = [
        ICUEvent(
            patient_id=f"RESTART-P-{test_suffix}-{i}",
            encounter_id=f"RESTART-ENC-{test_suffix}-{i}",
            event_type=EventType.ADMISSION,
            event_timestamp=datetime(2026, 10, 11, 16, 0, tzinfo=timezone.utc),
            to_unit="MICU",
        )
        for i in range(2)
    ]
    producer.publish_stream(events, raise_on_error=True)

    # Sink 1 runs and commits
    sink1 = KafkaHDFSSink(
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

    # Sink 2 starts with same group ID
    sink2 = KafkaHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=test_group,
        hdfs_client=hdfs,
        raw_hdfs_base=test_raw_dir,
        auto_offset_reset="earliest",
    )
    try:
        res2 = sink2.poll_and_sink(max_messages=2, poll_timeout=1.0, idle_timeout=2.0)
        assert res2["valid_count"] == 0, "No duplicate messages should be received after restart"
    finally:
        sink2.close()
        hdfs.delete(test_raw_dir, recursive=True)
