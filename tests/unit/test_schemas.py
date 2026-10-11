"""Unit tests for ICU ADT event schema validation."""

from datetime import datetime, timezone
from uuid import uuid4
import pytest
from pydantic import ValidationError

from src.schema.events import DataSource, EventType, ICUEvent


def test_valid_admission_event():
    ts = datetime(2026, 10, 1, 10, 30, tzinfo=timezone.utc)
    ev = ICUEvent(
        patient_id="P-001",
        encounter_id="ENC-001",
        event_type=EventType.ADMISSION,
        event_timestamp=ts,
        to_unit="MICU",
    )
    assert ev.event_type == EventType.ADMISSION
    assert ev.to_unit == "MICU"
    assert ev.from_unit is None
    assert ev.data_source == DataSource.SYNTHETIC
    assert ev.version == 1


def test_admission_missing_to_unit_fails():
    ts = datetime(2026, 10, 1, 10, 30, tzinfo=timezone.utc)
    with pytest.raises(ValidationError) as exc:
        ICUEvent(
            patient_id="P-001",
            encounter_id="ENC-001",
            event_type=EventType.ADMISSION,
            event_timestamp=ts,
            to_unit=None,
        )
    assert "ADMISSION event must specify a destination 'to_unit'" in str(exc.value)


def test_admission_with_from_unit_fails():
    ts = datetime(2026, 10, 1, 10, 30, tzinfo=timezone.utc)
    with pytest.raises(ValidationError) as exc:
        ICUEvent(
            patient_id="P-001",
            encounter_id="ENC-001",
            event_type=EventType.ADMISSION,
            event_timestamp=ts,
            to_unit="MICU",
            from_unit="SICU",
        )
    assert "ADMISSION event must not specify 'from_unit'" in str(exc.value)


def test_valid_discharge_event():
    ts = datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)
    ev = ICUEvent(
        patient_id="P-001",
        encounter_id="ENC-001",
        event_type=EventType.DISCHARGE,
        event_timestamp=ts,
        from_unit="MICU",
    )
    assert ev.event_type == EventType.DISCHARGE
    assert ev.from_unit == "MICU"
    assert ev.to_unit is None


def test_discharge_with_to_unit_fails():
    ts = datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError) as exc:
        ICUEvent(
            patient_id="P-001",
            encounter_id="ENC-001",
            event_type=EventType.DISCHARGE,
            event_timestamp=ts,
            from_unit="MICU",
            to_unit="SICU",
        )
    assert "DISCHARGE event must not specify 'to_unit'" in str(exc.value)


def test_valid_transfer_event():
    ts = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    ev = ICUEvent(
        patient_id="P-001",
        encounter_id="ENC-001",
        event_type=EventType.TRANSFER,
        event_timestamp=ts,
        from_unit="MICU",
        to_unit="SICU",
    )
    assert ev.event_type == EventType.TRANSFER
    assert ev.from_unit == "MICU"
    assert ev.to_unit == "SICU"


def test_transfer_with_identical_units_fails():
    ts = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError) as exc:
        ICUEvent(
            patient_id="P-001",
            encounter_id="ENC-001",
            event_type=EventType.TRANSFER,
            event_timestamp=ts,
            from_unit="MICU",
            to_unit="MICU",
        )
    assert "cannot be identical" in str(exc.value)


def test_correction_event_validation():
    orig_id = uuid4()
    ts = datetime(2026, 10, 1, 13, 0, tzinfo=timezone.utc)

    # Missing original_event_id when is_correction=True should fail
    with pytest.raises(ValidationError) as exc:
        ICUEvent(
            patient_id="P-001",
            encounter_id="ENC-001",
            event_type=EventType.ADMISSION,
            event_timestamp=ts,
            to_unit="MICU",
            is_correction=True,
            original_event_id=None,
        )
    assert "'original_event_id' must reference the superseded event" in str(exc.value)

    # Valid correction passes
    corr_ev = ICUEvent(
        patient_id="P-001",
        encounter_id="ENC-001",
        event_type=EventType.ADMISSION,
        event_timestamp=ts,
        to_unit="MICU",
        is_correction=True,
        original_event_id=orig_id,
        version=2,
    )
    assert corr_ev.is_correction is True
    assert corr_ev.original_event_id == orig_id
    assert corr_ev.version == 2


def test_serialization_roundtrip():
    ts = datetime(2026, 10, 1, 10, 30, tzinfo=timezone.utc)
    ev = ICUEvent(
        patient_id="P-999",
        encounter_id="ENC-888",
        event_type=EventType.ADMISSION,
        event_timestamp=ts,
        to_unit="CCU",
        metadata={"triage_score": 3},
    )
    d = ev.to_dict()
    assert d["patient_id"] == "P-999"
    assert d["event_type"] == "ADMISSION"
    assert d["to_unit"] == "CCU"
    assert d["metadata"]["triage_score"] == 3
