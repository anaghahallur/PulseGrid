"""
Integration test suite for MIMIC-IV Demo Kafka streaming and HDFS persistence.
Verifies:
- Publishing extracted MIMIC ADT events to Kafka topic and landing into HDFS.
- Publishing extracted MIMIC vital sign events to Kafka topic and landing into HDFS.
- Reading back persisted files from HDFS and verifying schema and metadata fidelity.
"""

from datetime import datetime, timezone
import uuid
import pytest

from src.config import get_settings
from src.schema.events import DataSource, EventType, ICUEvent, VitalSignEvent
from src.producer.kafka_producer import ICUKafkaProducer
from src.producer.vitals_producer import ICUVitalsProducer
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaHDFSSink, KafkaVitalsHDFSSink


def test_mimic_adt_and_vitals_kafka_to_hdfs_streaming():
    """Verify end-to-end Kafka-to-HDFS streaming of MIMIC-IV demo events."""
    settings = get_settings()
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    if not hdfs.is_healthy():
        pytest.skip("HDFS NameNode is not accessible")

    test_id = str(uuid.uuid4())[:8]
    adt_topic = f"icu-events-mimic-{test_id}"
    vit_topic = f"icu-vitals-mimic-{test_id}"

    sub_id = f"MIMIC-P-{test_id}"
    enc_id = f"MIMIC-ENC-100-STAY-{test_id}"
    ts = datetime(2026, 10, 14, 12, 0, 0, tzinfo=timezone.utc)

    # 1. Prepare sample MIMIC ADT event
    adt_event = ICUEvent(
        patient_id=sub_id,
        encounter_id=enc_id,
        event_type=EventType.ADMISSION,
        event_timestamp=ts,
        to_unit="TSICU",
        data_source=DataSource.MIMIC_DEMO,
        metadata={"test_tag": test_id},
    )

    # 2. Prepare sample MIMIC VitalSign event
    vital_event = VitalSignEvent(
        patient_id=sub_id,
        encounter_id=enc_id,
        unit_id="TSICU",
        event_timestamp=ts,
        heart_rate=78.5,
        temperature_c=37.1,
        systolic_bp=122.0,
        diastolic_bp=79.0,
        respiratory_rate=16.0,
        spo2=98.5,
        data_source=DataSource.MIMIC_DEMO,
        metadata={"test_tag": test_id},
    )

    # 3. Publish to isolated Kafka topics
    adt_prod = ICUKafkaProducer(topic=adt_topic)
    adt_prod.publish_stream([adt_event], events_per_second=100.0)

    vit_prod = ICUVitalsProducer(topic=vit_topic)
    vit_prod.publish_stream([vital_event], events_per_second=100.0)

    # 4. Ingest via Sinks into unique test HDFS directories
    adt_raw_hdfs = f"/test_mimic_adt_{test_id}"
    vit_raw_hdfs = f"/test_mimic_vit_{test_id}"

    adt_sink = KafkaHDFSSink(
        topic=adt_topic,
        group_id=f"test-adt-group-{test_id}",
        raw_hdfs_base=adt_raw_hdfs,
        auto_offset_reset="earliest",
    )
    res_adt = adt_sink.poll_and_sink(max_messages=1, idle_timeout=5.0)
    adt_sink.close()

    vit_sink = KafkaVitalsHDFSSink(
        topic=vit_topic,
        group_id=f"test-vit-group-{test_id}",
        raw_hdfs_base=vit_raw_hdfs,
        auto_offset_reset="earliest",
    )
    res_vit = vit_sink.poll_and_sink(max_messages=1, idle_timeout=5.0)
    vit_sink.close()

    assert res_adt["valid_count"] == 1
    assert res_vit["valid_count"] == 1

    # 5. Read back from HDFS and verify contents
    adt_jsonl = hdfs.find_files(adt_raw_hdfs, suffix=".jsonl")
    assert len(adt_jsonl) >= 1

    read_adt_events = hdfs.read_jsonl(adt_jsonl[0])
    matched_adt = [e for e in read_adt_events if e.get("metadata", {}).get("test_tag") == test_id]
    assert len(matched_adt) == 1
    assert matched_adt[0]["patient_id"] == sub_id
    assert matched_adt[0]["to_unit"] == "TSICU"
    assert matched_adt[0]["data_source"] == "MIMIC_DEMO"

    vit_jsonl = hdfs.find_files(vit_raw_hdfs, suffix=".jsonl")
    assert len(vit_jsonl) >= 1

    read_vit_events = hdfs.read_jsonl(vit_jsonl[0])
    matched_vit = [v for v in read_vit_events if v.get("metadata", {}).get("test_tag") == test_id]
    assert len(matched_vit) == 1
    assert matched_vit[0]["patient_id"] == sub_id
    assert matched_vit[0]["unit_id"] == "TSICU"
    assert matched_vit[0]["heart_rate"] == 78.5
    assert matched_vit[0]["data_source"] == "MIMIC_DEMO"

    # Cleanup test directories
    hdfs.delete(adt_raw_hdfs, recursive=True)
    hdfs.delete(vit_raw_hdfs, recursive=True)
