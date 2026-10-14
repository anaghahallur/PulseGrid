"""
Unit tests for the patient vital signs schema, simulator, and sink processing.
"""

from datetime import datetime, timezone
import json
from unittest.mock import MagicMock
from uuid import UUID, uuid4
import pytest
from confluent_kafka import Message

from src.schema.events import DataSource, VitalSignEvent
from src.producer.vitals_simulator import VitalSignSimulator
from src.storage.kafka_sink import KafkaVitalsHDFSSink


# ==============================================================================
# 1. VitalSignEvent Schema Validation Tests
# ==============================================================================

def test_valid_vital_sign_event_all_channels():
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    ev = VitalSignEvent(
        patient_id="SYN-P-000001",
        encounter_id="SYN-ENC-000001",
        unit_id="MICU",
        event_timestamp=now,
        heart_rate=76.5,
        temperature_c=37.1,
        systolic_bp=124.0,
        diastolic_bp=78.0,
        respiratory_rate=16.0,
        spo2=98.5,
        metadata={"device": "TEST-MONITOR-01"},
    )
    assert isinstance(ev.event_id, UUID)
    assert ev.patient_id == "SYN-P-000001"
    assert ev.unit_id == "MICU"
    assert ev.heart_rate == 76.5
    assert ev.temperature_c == 37.1
    assert ev.systolic_bp == 124.0
    assert ev.diastolic_bp == 78.0
    assert ev.respiratory_rate == 16.0
    assert ev.spo2 == 98.5
    assert ev.data_source == DataSource.SYNTHETIC


def test_valid_vital_sign_event_partial_channels():
    """Validates that partial channels (e.g. pulse ox without NIBP cuff) are accepted."""
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    ev = VitalSignEvent(
        patient_id="SYN-P-000002",
        encounter_id="SYN-ENC-000002",
        unit_id="SICU",
        event_timestamp=now,
        heart_rate=82.0,
        spo2=97.0,
    )
    assert ev.heart_rate == 82.0
    assert ev.spo2 == 97.0
    assert ev.temperature_c is None
    assert ev.systolic_bp is None
    assert ev.diastolic_bp is None


def test_vital_sign_event_rejects_all_none_measurements():
    """At least one vital measurement must be present."""
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError) as exc:
        VitalSignEvent(
            patient_id="SYN-P-000003",
            encounter_id="SYN-ENC-000003",
            unit_id="CCU",
            event_timestamp=now,
        )
    assert "must contain at least one valid measurement" in str(exc.value)


def test_vital_sign_event_systolic_below_diastolic_rejected():
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError) as exc:
        VitalSignEvent(
            patient_id="SYN-P-000004",
            encounter_id="SYN-ENC-000004",
            unit_id="CCU",
            event_timestamp=now,
            systolic_bp=70.0,
            diastolic_bp=90.0,
        )
    assert "cannot be lower than diastolic BP" in str(exc.value)


@pytest.mark.parametrize("hr", [10.0, 350.0])
def test_vital_sign_event_out_of_range_heart_rate(hr):
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        VitalSignEvent(
            patient_id="P1",
            encounter_id="E1",
            unit_id="MICU",
            event_timestamp=now,
            heart_rate=hr,
        )


@pytest.mark.parametrize("temp", [22.0, 48.0])
def test_vital_sign_event_out_of_range_temperature(temp):
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        VitalSignEvent(
            patient_id="P1",
            encounter_id="E1",
            unit_id="MICU",
            event_timestamp=now,
            temperature_c=temp,
        )


@pytest.mark.parametrize("spo2", [45.0, 105.0])
def test_vital_sign_event_out_of_range_spo2(spo2):
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        VitalSignEvent(
            patient_id="P1",
            encounter_id="E1",
            unit_id="MICU",
            event_timestamp=now,
            spo2=spo2,
        )


def test_vital_sign_event_timezone_normalization():
    naive_dt = datetime(2026, 10, 14, 10, 0, 0)
    ev = VitalSignEvent(
        patient_id="P1",
        encounter_id="E1",
        unit_id="micu",  # Test lowercase normalization too
        event_timestamp=naive_dt,
        heart_rate=72.0,
    )
    assert ev.event_timestamp.tzinfo == timezone.utc
    assert ev.unit_id == "MICU"


def test_vital_sign_event_serialization_roundtrip():
    now = datetime(2026, 10, 14, 10, 30, tzinfo=timezone.utc)
    ev = VitalSignEvent(
        patient_id="P-100",
        encounter_id="E-200",
        unit_id="CSRU",
        event_timestamp=now,
        heart_rate=80.0,
        temperature_c=36.8,
        systolic_bp=118.0,
        diastolic_bp=74.0,
        respiratory_rate=14.0,
        spo2=99.0,
    )
    d = ev.to_dict()
    assert d["unit_id"] == "CSRU"
    assert d["heart_rate"] == 80.0
    assert d["event_timestamp"] == now.isoformat()

    # Deserialization check
    ev2 = VitalSignEvent.model_validate(d)
    assert ev2.event_id == ev.event_id
    assert ev2.patient_id == ev.patient_id
    assert ev2.heart_rate == ev.heart_rate


# ==============================================================================
# 2. VitalSignSimulator Tests
# ==============================================================================

def test_vital_sign_simulator_determinism():
    sim1 = VitalSignSimulator(random_seed=42)
    evs1 = sim1.generate_events(num_events=10)

    sim2 = VitalSignSimulator(random_seed=42)
    evs2 = sim2.generate_events(num_events=10)

    assert len(evs1) == len(evs2) == 10
    for e1, e2 in zip(evs1, evs2):
        assert e1.patient_id == e2.patient_id
        assert e1.heart_rate == e2.heart_rate
        assert e1.systolic_bp == e2.systolic_bp
        assert e1.event_timestamp == e2.event_timestamp


def test_vital_sign_simulator_seed_variation():
    sim1 = VitalSignSimulator(random_seed=42)
    evs1 = sim1.generate_events(num_events=5)

    sim2 = VitalSignSimulator(random_seed=999)
    evs2 = sim2.generate_events(num_events=5)

    # Different seeds should yield different numerical vitals
    assert evs1[0].heart_rate != evs2[0].heart_rate


def test_vital_sign_simulator_chronological_order():
    sim = VitalSignSimulator(random_seed=42)
    evs = sim.generate_events(num_events=20, interval_seconds=15.0)

    for i in range(len(evs) - 1):
        assert evs[i].event_timestamp <= evs[i + 1].event_timestamp


# ==============================================================================
# 3. KafkaVitalsHDFSSink Unit Tests (Mocked Storage)
# ==============================================================================

def _create_mock_message(payload_bytes: bytes, topic: str = "icu-vitals", partition: int = 0, offset: int = 0) -> MagicMock:
    msg = MagicMock(spec=Message)
    msg.value.return_value = payload_bytes
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.error.return_value = None
    msg.key.return_value = b"test-key"
    return msg


def test_vitals_sink_parse_and_process_batch():
    mock_hdfs = MagicMock()
    mock_hdfs.is_healthy.return_value = True
    mock_hdfs.write_jsonl.return_value = True
    mock_hdfs.rename.return_value = True

    sink = KafkaVitalsHDFSSink(
        topic="test-vitals-topic",
        group_id="test-vitals-group",
        hdfs_client=mock_hdfs,
        raw_hdfs_base="/test_raw_vitals",
        dead_letter_base="/test_dl_vitals",
    )
    sink._commit_offsets = MagicMock()

    # Create 2 valid messages and 1 malformed message
    now = datetime(2026, 10, 14, 10, 0, tzinfo=timezone.utc)
    ev1 = VitalSignEvent(
        patient_id="P1", encounter_id="E1", unit_id="MICU", event_timestamp=now, heart_rate=75.0, spo2=98.0
    )
    ev2 = VitalSignEvent(
        patient_id="P2", encounter_id="E2", unit_id="SICU", event_timestamp=now, heart_rate=80.0, systolic_bp=120.0, diastolic_bp=75.0
    )

    msg1 = _create_mock_message(json.dumps(ev1.to_dict()).encode("utf-8"), offset=1)
    msg2 = _create_mock_message(json.dumps(ev2.to_dict()).encode("utf-8"), offset=2)
    msg_bad = _create_mock_message(b"NOT_VALID_JSON_BYTES", offset=3)

    result = sink.process_batch([msg1, msg2, msg_bad])

    assert result["valid_count"] == 2
    assert result["malformed_count"] == 1
    assert len(result["batches_finalized"]) == 1

    # Staging write, DLQ write, and rename must have been called
    assert mock_hdfs.write_jsonl.call_count >= 2  # 1 for valid batch, 1 for DLQ
    assert mock_hdfs.rename.call_count == 1
    sink._commit_offsets.assert_called_once()
