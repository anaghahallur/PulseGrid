"""
Pipeline Reliability Audit Integration Tests for Phase 2.1.
Verifies failure scenarios, multi-date partition handling, crash replay,
dead-letter segregation, and cold-restart reproducibility against live Docker services.
"""

from datetime import datetime, timezone
import json
from unittest.mock import MagicMock
from uuid import uuid4
import pytest

from confluent_kafka import Message, Producer
from src.config import get_settings
from src.schema.events import EventType, ICUEvent
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaHDFSSink
from src.processing.occupancy_calculator import OccupancyCalculator


@pytest.fixture(scope="module")
def hdfs_client():
    client = HDFSClient()
    if not client.is_healthy():
        pytest.skip("HDFS NameNode is unavailable. Skipping integration audit.")
    return client


@pytest.fixture(scope="module")
def kafka_settings():
    return get_settings()


def _create_mock_message(payload_bytes: bytes, topic: str = "test-topic", partition: int = 0, offset: int = 0) -> MagicMock:
    msg = MagicMock(spec=Message)
    msg.value.return_value = payload_bytes
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.error.return_value = None
    msg.key.return_value = b"test-key"
    return msg


def test_multi_date_partition_hdfs_failure_aborts_offset_commit(hdfs_client):
    """
    Scenario: A micro-batch contains events spanning multiple date partitions.
    Finalization succeeds for partition 1, but fails on partition 2 (e.g. DataNode error).
    Verifies that the sink raises IOError and aborts the Kafka offset commit.
    """
    audit_id = uuid4().hex[:8]
    raw_base = f"/test_audit_{audit_id}/raw"
    dl_base = f"/test_audit_{audit_id}/dead_letter"

    # Event 1: Date 2026-10-11
    ev_day1 = ICUEvent(
        patient_id="P-D1",
        encounter_id="ENC-D1",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc),
        to_unit="MICU",
    )
    # Event 2: Date 2026-10-12
    ev_day2 = ICUEvent(
        patient_id="P-D2",
        encounter_id="ENC-D2",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc),
        to_unit="SICU",
    )

    msg1 = _create_mock_message(json.dumps(ev_day1.to_dict()).encode("utf-8"), offset=1)
    msg2 = _create_mock_message(json.dumps(ev_day2.to_dict()).encode("utf-8"), offset=2)

    sink = KafkaHDFSSink(
        topic="test-dummy-topic",
        group_id=f"audit-group-{audit_id}",
        hdfs_client=hdfs_client,
        raw_hdfs_base=raw_base,
        dead_letter_base=dl_base,
    )

    # Spy and intercept rename to fail specifically on day=12
    real_rename = hdfs_client.rename
    def selective_failing_rename(src: str, dst: str) -> bool:
        if "day=12" in dst:
            return False  # Simulate HDFS DataNode finalization failure on partition 2
        return real_rename(src, dst)

    hdfs_client.rename = selective_failing_rename
    sink._commit_offsets = MagicMock()

    try:
        with pytest.raises(IOError) as exc_info:
            sink.process_batch([msg1, msg2])
        assert "Atomic HDFS rename failed" in str(exc_info.value)

        # CRITICAL AUDIT CHECK: Offsets must NOT have been committed!
        sink._commit_offsets.assert_not_called()
    finally:
        hdfs_client.rename = real_rename
        sink.close()
        hdfs_client.delete(f"/test_audit_{audit_id}", recursive=True)


def test_post_finalization_crash_replay_and_downstream_dedup(kafka_settings, hdfs_client):
    """
    Scenario: HDFS finalization succeeds, but process crashes before the Kafka offset commit.
    On restart, the uncommitted batch is replayed, resulting in duplicate raw records.
    Verifies downstream OccupancyCalculator correctly deduplicates and prevents double-counting.
    """
    test_id = uuid4().hex[:8]
    topic = f"audit-topic-{test_id}"
    group_id = f"audit-group-{test_id}"
    raw_base = f"/test_crash_replay_{test_id}/raw"

    # Step 1: Publish 2 valid events
    p = Producer({"bootstrap.servers": kafka_settings.kafka_bootstrap_servers})
    ts = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
    ev1 = ICUEvent(
        patient_id="P-CRASH-1",
        encounter_id="ENC-CRASH-1",
        event_type=EventType.ADMISSION,
        event_timestamp=ts,
        to_unit="MICU",
    )
    ev2 = ICUEvent(
        patient_id="P-CRASH-2",
        encounter_id="ENC-CRASH-2",
        event_type=EventType.ADMISSION,
        event_timestamp=ts,
        to_unit="MICU",
    )
    for ev in [ev1, ev2]:
        p.produce(topic, key=ev.encounter_id.encode("utf-8"), value=json.dumps(ev.to_dict()).encode("utf-8"))
    p.flush(5.0)

    # Step 2: Sink 1 consumes, writes to HDFS, but crashes before commit
    sink1 = KafkaHDFSSink(
        topic=topic,
        group_id=group_id,
        hdfs_client=hdfs_client,
        raw_hdfs_base=raw_base,
        auto_offset_reset="earliest",
    )
    batch_msgs = []
    for _ in range(10):
        m = sink1.consumer.poll(1.0)
        if m and not m.error():
            batch_msgs.append(m)
        if len(batch_msgs) == 2:
            break

    assert len(batch_msgs) == 2

    # Intercept commit on sink1 to simulate crash
    def crash_before_commit(*args, **kwargs):
        raise RuntimeError("SIMULATED PROCESS CRASH PRIOR TO KAFKA OFFSET COMMIT")

    sink1._commit_offsets = crash_before_commit

    with pytest.raises(RuntimeError):
        sink1.process_batch(batch_msgs)
    sink1.close()

    # Step 3: Sink 2 starts up (restart after crash)
    # Because sink 1 did not commit, sink 2 will re-consume the 2 messages
    sink2 = KafkaHDFSSink(
        topic=topic,
        group_id=group_id,
        hdfs_client=hdfs_client,
        raw_hdfs_base=raw_base,
        auto_offset_reset="earliest",
    )
    res2 = sink2.poll_and_sink(max_messages=10, poll_timeout=1.0, idle_timeout=3.0)
    sink2.close()

    assert res2["valid_count"] == 2

    # Step 4: Verify HDFS contains duplicated raw files
    raw_files = hdfs_client.find_files(raw_base, suffix=".jsonl")
    assert len(raw_files) >= 2, "Expected multiple batch files due to replay"

    # Step 5: Read all raw files comprehensively and reconstruct occupancy
    all_raw_records = hdfs_client.read_all_jsonl(raw_base)
    assert len(all_raw_records) == 4, "Expected 4 total raw records (2 events x 2 files)"

    recovered_events = [ICUEvent.model_validate(r) for r in all_raw_records]
    calculator = OccupancyCalculator(unit_capacities=kafka_settings.icu_unit_capacities)
    df = calculator.reconstruct(recovered_events)

    micu_row = df[(df["unit_id"] == "MICU") & (df["timestamp"] == datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc))].iloc[0]
    # Even though 4 records were ingested into HDFS, exact census must be 2 (zero double-counting)
    assert micu_row["occupied_beds"] == 2, f"Double-counting occurred! Expected 2, got {micu_row['occupied_beds']}"

    # Cleanup
    hdfs_client.delete(f"/test_crash_replay_{test_id}", recursive=True)


def test_quarantine_segregation_in_mixed_microbatch(hdfs_client):
    """
    Scenario: A micro-batch arrives with a mix of valid events, malformed JSON,
    and schema validation failures.
    Verifies that:
    - Invalid messages are quarantined to HDFS dead-letter without data loss.
    - Valid messages in the exact same batch are written to raw HDFS.
    - Offsets are committed cleanly.
    """
    test_id = uuid4().hex[:8]
    raw_base = f"/test_quarantine_{test_id}/raw"
    dl_base = f"/test_quarantine_{test_id}/dead_letter"

    valid_ev1 = ICUEvent(
        patient_id="P-VAL-1",
        encounter_id="ENC-VAL-1",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 11, 8, 0, tzinfo=timezone.utc),
        to_unit="MICU",
    )
    valid_ev2 = ICUEvent(
        patient_id="P-VAL-2",
        encounter_id="ENC-VAL-2",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 11, 9, 0, tzinfo=timezone.utc),
        to_unit="SICU",
    )

    msg_valid1 = _create_mock_message(json.dumps(valid_ev1.to_dict()).encode("utf-8"), offset=0)
    msg_malformed_json = _create_mock_message(b'{"corrupt": invalid_json', offset=1)
    msg_schema_error = _create_mock_message(
        json.dumps({
            "event_id": str(uuid4()),
            "patient_id": "P-BAD",
            "encounter_id": "ENC-BAD",
            "event_type": "ADMISSION",
            "event_timestamp": "2026-10-11T08:00:00Z",
            "to_unit": None,  # ADMISSION missing to_unit violates schema!
        }).encode("utf-8"),
        offset=2,
    )
    msg_valid2 = _create_mock_message(json.dumps(valid_ev2.to_dict()).encode("utf-8"), offset=3)

    sink = KafkaHDFSSink(
        topic="dummy-topic",
        group_id=f"audit-group-{test_id}",
        hdfs_client=hdfs_client,
        raw_hdfs_base=raw_base,
        dead_letter_base=dl_base,
    )
    sink._commit_offsets = MagicMock()

    batch = [msg_valid1, msg_malformed_json, msg_schema_error, msg_valid2]
    res = sink.process_batch(batch)
    sink.close()

    assert res["valid_count"] == 2
    assert res["malformed_count"] == 2

    # Check dead-letter files
    dl_files = hdfs_client.find_files(dl_base, suffix=".jsonl")
    assert len(dl_files) == 1
    dl_records = hdfs_client.read_jsonl(dl_files[0])
    assert len(dl_records) == 2

    # Check raw valid files
    raw_files = hdfs_client.find_files(raw_base, suffix=".jsonl")
    assert len(raw_files) == 1
    raw_records = hdfs_client.read_jsonl(raw_files[0])
    assert len(raw_records) == 2

    # Check offsets committed
    sink._commit_offsets.assert_called_once()

    # Cleanup
    hdfs_client.delete(f"/test_quarantine_{test_id}", recursive=True)


def test_occupancy_calculator_cold_restart_reconstruction(kafka_settings, hdfs_client):
    """
    Scenario: Calculator processes raw HDFS events and persists Parquet.
    Process crashes / restarts completely from scratch.
    The new instance reads raw HDFS and reconstructs identical Parquet output.
    """
    test_id = uuid4().hex[:8]
    raw_base = f"/test_cold_restart_{test_id}/raw"
    proc_base = f"/test_cold_restart_{test_id}/processed"

    # Write raw events into HDFS
    ts = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
    events = [
        ICUEvent(
            patient_id=f"P-COLD-{i}",
            encounter_id=f"ENC-COLD-{i}",
            event_type=EventType.ADMISSION,
            event_timestamp=ts,
            to_unit="MICU" if i % 2 == 0 else "SICU",
        )
        for i in range(6)
    ]
    raw_path = f"{raw_base}/year=2026/month=10/day=11/events_{test_id}.jsonl"
    hdfs_client.write_jsonl(raw_path, [e.to_dict() for e in events])

    # Run Instance 1
    calc_1 = OccupancyCalculator(unit_capacities=kafka_settings.icu_unit_capacities)
    raw_records_1 = hdfs_client.read_all_jsonl(raw_base)
    evs_1 = [ICUEvent.model_validate(r) for r in raw_records_1]
    df_1 = calc_1.reconstruct(evs_1)
    file_1 = calc_1.persist_occupancy_to_hdfs(df_1, hdfs_client, hdfs_target_dir=proc_base, filename=f"run1_{test_id}.parquet")

    # Complete destruction of instance 1
    del calc_1
    del df_1

    # Cold Restart: Run Instance 2
    calc_2 = OccupancyCalculator(unit_capacities=kafka_settings.icu_unit_capacities)
    raw_records_2 = hdfs_client.read_all_jsonl(raw_base)
    evs_2 = [ICUEvent.model_validate(r) for r in raw_records_2]
    df_2 = calc_2.reconstruct(evs_2)
    file_2 = calc_2.persist_occupancy_to_hdfs(df_2, hdfs_client, hdfs_target_dir=proc_base, filename=f"run2_{test_id}.parquet")

    # Compare persisted outputs from HDFS
    df_read_1 = hdfs_client.read_parquet(file_1)
    df_read_2 = hdfs_client.read_parquet(file_2)

    # Compare all columns except processing_watermark
    cols = [c for c in df_read_1.columns if c != "processing_watermark"]
    assert df_read_1[cols].equals(df_read_2[cols])

    # Cleanup
    hdfs_client.delete(f"/test_cold_restart_{test_id}", recursive=True)


def test_dlq_write_failure_aborts_offset_commit(hdfs_client):
    """
    Scenario: A micro-batch contains a malformed message.
    Writing the dead-letter quarantine to HDFS fails (e.g. returns False or throws).
    Verifies that the sink raises IOError and aborts the Kafka offset commit,
    preventing silent loss of unparseable telemetry.
    """
    audit_id = uuid4().hex[:8]
    raw_base = f"/test_audit_dlq_fail_{audit_id}/raw"
    dl_base = f"/test_audit_dlq_fail_{audit_id}/dead_letter"

    msg_corrupt = _create_mock_message(b'{"malformed": true, broken', offset=10)

    sink = KafkaHDFSSink(
        topic="test-dlq-topic",
        group_id=f"audit-dlq-group-{audit_id}",
        hdfs_client=hdfs_client,
        raw_hdfs_base=raw_base,
        dead_letter_base=dl_base,
    )
    sink._commit_offsets = MagicMock()

    real_write_jsonl = hdfs_client.write_jsonl
    def failing_write_jsonl(path: str, records: list, overwrite: bool = True) -> bool:
        if dl_base in path:
            return False  # Simulate DLQ write failure
        return real_write_jsonl(path, records, overwrite=overwrite)

    hdfs_client.write_jsonl = failing_write_jsonl
    try:
        with pytest.raises(IOError) as exc_info:
            sink.process_batch([msg_corrupt])
        assert "Failed to write dead-letter quarantine" in str(exc_info.value)
        # CRITICAL AUDIT CHECK: Offsets must NOT be committed!
        sink._commit_offsets.assert_not_called()
    finally:
        hdfs_client.write_jsonl = real_write_jsonl

