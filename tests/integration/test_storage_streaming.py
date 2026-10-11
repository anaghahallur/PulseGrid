"""Integration tests for Kafka streaming and Hadoop HDFS storage."""

from datetime import datetime, timezone
import json
import pytest

from src.config import get_settings
from src.producer.kafka_producer import ICUKafkaProducer
from src.schema.events import EventType, ICUEvent
from src.storage.hdfs_client import HDFSClient


@pytest.mark.integration
def test_hdfs_connectivity_and_rw():
    """Verify WebHDFS read and write cycle."""
    settings = get_settings()
    client = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)

    assert client.is_healthy(), f"HDFS NameNode at {settings.hdfs_namenode_url} is not healthy"

    test_dir = "/test_bda_pipeline"
    test_file = f"{test_dir}/sample_event.json"
    payload = json.dumps({"test": "bda_hdfs_write_ok", "value": 42}).encode("utf-8")

    # 1. Create directory
    client.mkdir(test_dir)

    # 2. Write file
    assert client.write_file(test_file, payload, overwrite=True) is True

    # 3. Read back file
    read_bytes = client.read_file(test_file)
    assert read_bytes == payload

    # 4. Clean up
    assert client.delete(test_dir, recursive=True) is True


@pytest.mark.integration
def test_kafka_producer_publish():
    """Verify Kafka producer connection and message emission."""
    settings = get_settings()
    producer = ICUKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_icu_events,
    )

    sample_event = ICUEvent(
        patient_id="TEST-P-001",
        encounter_id="TEST-ENC-001",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc),
        to_unit="MICU",
        metadata={"integration_test": True},
    )

    producer.send_event(sample_event)
    unflushed = producer.flush(timeout=5.0)
    assert unflushed == 0, f"Kafka failed to flush messages. Unflushed count: {unflushed}"
