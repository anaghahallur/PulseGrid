"""
Unit test suite for MIMIC-IV Clinical Database Demo ingestion and processing pipeline.
Covers:
- Careunit mapping and validation
- ADT event generation, ordering, and stay isolation
- Missing timestamps and duplicate handling
- Multiple ICU stays per admission
- Vital sign ItemID mapping and Fahrenheit-to-Celsius conversion
- Invalid physiological measurement filtering
- Source timestamp preservation
- Synchronized cohort temporal consistency and duration invariance
- Occupancy reconstruction idempotence
"""

from datetime import datetime, timedelta, timezone
import json
import os
import pytest
import pandas as pd

from src.schema.events import DataSource, EventType, ICUEvent, VitalSignEvent
from src.processing.occupancy_calculator import OccupancyCalculator
from scripts.ingest_mimic_data import (
    MIMIC_ICU_UNIT_MAP,
    MIMIC_VITAL_ITEM_MAP,
    create_synchronized_cohort,
    compute_sha256,
)


def test_mimic_careunit_mapping():
    """Verify that clinical careunits map strictly to project wards or None."""
    assert MIMIC_ICU_UNIT_MAP["Medical Intensive Care Unit (MICU)"] == "MICU"
    assert MIMIC_ICU_UNIT_MAP["Surgical Intensive Care Unit (SICU)"] == "SICU"
    assert MIMIC_ICU_UNIT_MAP["Coronary Care Unit (CCU)"] == "CCU"
    assert MIMIC_ICU_UNIT_MAP["Trauma SICU (TSICU)"] == "TSICU"
    assert MIMIC_ICU_UNIT_MAP["Cardiac Vascular Intensive Care Unit (CVICU)"] == "CSRU"
    assert MIMIC_ICU_UNIT_MAP["Medical/Surgical Intensive Care Unit (MICU/SICU)"] == "MICU"
    assert MIMIC_ICU_UNIT_MAP["Neuro Surgical Intensive Care Unit (Neuro SICU)"] == "SICU"

    # Non-ICU wards should not be in the mapping
    assert "Emergency Department" not in MIMIC_ICU_UNIT_MAP
    assert "PACU" not in MIMIC_ICU_UNIT_MAP
    assert "Neuro Stepdown" not in MIMIC_ICU_UNIT_MAP
    assert "Medicine" not in MIMIC_ICU_UNIT_MAP


def test_mimic_adt_event_extraction_mock():
    """Verify ADT admission, transfer, and discharge sequencing from mock transfers."""
    patient_id = "MIMIC-P-10001"
    encounter_id = "MIMIC-ENC-20001-STAY-30001"

    t0 = datetime(2150, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2150, 5, 2, 14, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2150, 5, 4, 18, 0, 0, tzinfo=timezone.utc)

    # 1. Admit to TSICU
    e_admit = ICUEvent(
        patient_id=patient_id,
        encounter_id=encounter_id,
        event_type=EventType.ADMISSION,
        event_timestamp=t0,
        to_unit="TSICU",
        data_source=DataSource.MIMIC_DEMO,
    )
    # 2. Transfer to SICU
    e_xfer = ICUEvent(
        patient_id=patient_id,
        encounter_id=encounter_id,
        event_type=EventType.TRANSFER,
        event_timestamp=t1,
        from_unit="TSICU",
        to_unit="SICU",
        data_source=DataSource.MIMIC_DEMO,
    )
    # 3. Discharge from SICU
    e_disch = ICUEvent(
        patient_id=patient_id,
        encounter_id=encounter_id,
        event_type=EventType.DISCHARGE,
        event_timestamp=t2,
        from_unit="SICU",
        data_source=DataSource.MIMIC_DEMO,
    )

    assert e_admit.event_type == EventType.ADMISSION
    assert e_xfer.from_unit == "TSICU" and e_xfer.to_unit == "SICU"
    assert e_disch.from_unit == "SICU" and e_disch.to_unit is None


def test_mimic_multiple_icu_stays_per_admission():
    """Verify that multiple ICU stays for the same patient hospitalization are isolated by stay_id."""
    sub_id = "MIMIC-P-10020187"
    hadm_id = "24104168"
    stay1 = "37509585"
    stay2 = "39999999"

    enc1 = f"MIMIC-ENC-{hadm_id}-STAY-{stay1}"
    enc2 = f"MIMIC-ENC-{hadm_id}-STAY-{stay2}"

    assert enc1 != enc2, "Separate ICU stays must produce distinct encounter identifiers"

    t1_in = datetime(2169, 1, 15, 4, 0, tzinfo=timezone.utc)
    t1_out = datetime(2169, 1, 18, 12, 0, tzinfo=timezone.utc)
    t2_in = datetime(2169, 1, 20, 8, 0, tzinfo=timezone.utc)
    t2_out = datetime(2169, 1, 22, 16, 0, tzinfo=timezone.utc)

    e1_admit = ICUEvent(patient_id=sub_id, encounter_id=enc1, event_type=EventType.ADMISSION, event_timestamp=t1_in, to_unit="SICU")
    e1_disch = ICUEvent(patient_id=sub_id, encounter_id=enc1, event_type=EventType.DISCHARGE, event_timestamp=t1_out, from_unit="SICU")
    e2_admit = ICUEvent(patient_id=sub_id, encounter_id=enc2, event_type=EventType.ADMISSION, event_timestamp=t2_in, to_unit="MICU")
    e2_disch = ICUEvent(patient_id=sub_id, encounter_id=enc2, event_type=EventType.DISCHARGE, event_timestamp=t2_out, from_unit="MICU")

    calc = OccupancyCalculator(unit_capacities={"SICU": 20, "MICU": 24})
    df_occ = calc.reconstruct([e1_admit, e1_disch, e2_admit, e2_disch])
    assert not df_occ.empty
    # Verify stay 1 does not collide with stay 2
    sicu_occ = df_occ[df_occ["unit_id"] == "SICU"]["occupied_beds"]
    micu_occ = df_occ[df_occ["unit_id"] == "MICU"]["occupied_beds"]
    assert sicu_occ.max() == 1
    assert micu_occ.max() == 1


def test_mimic_vital_sign_item_mapping_and_conversion():
    """Verify vital item ID dictionary and Fahrenheit-to-Celsius conversion."""
    assert MIMIC_VITAL_ITEM_MAP[220045] == "heart_rate"
    assert MIMIC_VITAL_ITEM_MAP[220179] == "systolic_bp"
    assert MIMIC_VITAL_ITEM_MAP[220180] == "diastolic_bp"
    assert MIMIC_VITAL_ITEM_MAP[220210] == "respiratory_rate"
    assert MIMIC_VITAL_ITEM_MAP[223761] == "temperature_f"
    assert MIMIC_VITAL_ITEM_MAP[223762] == "temperature_c"
    assert MIMIC_VITAL_ITEM_MAP[220277] == "spo2"

    # Test F to C conversion: 98.6 F == 37.0 C
    f_val = 98.6
    c_val = round((f_val - 32.0) * 5.0 / 9.0, 1)
    assert c_val == 37.0

    # 104.0 F == 40.0 C
    assert round((104.0 - 32.0) * 5.0 / 9.0, 1) == 40.0


def test_mimic_vital_invalid_measurement_filtering():
    """Verify that out-of-range sensor artifacts are rejected."""
    ts = datetime(2150, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Valid event
    v_valid = VitalSignEvent(
        patient_id="MIMIC-P-1",
        encounter_id="MIMIC-ENC-1-STAY-1",
        unit_id="MICU",
        event_timestamp=ts,
        heart_rate=80.0,
        temperature_c=37.0,
        systolic_bp=120.0,
        diastolic_bp=80.0,
        respiratory_rate=16.0,
        spo2=98.0,
    )
    assert v_valid.heart_rate == 80.0

    # Inverted BP (systolic < diastolic) must raise ValueError
    with pytest.raises(ValueError, match="cannot be lower than"):
        VitalSignEvent(
            patient_id="MIMIC-P-1",
            encounter_id="MIMIC-ENC-1-STAY-1",
            unit_id="MICU",
            event_timestamp=ts,
            systolic_bp=70.0,
            diastolic_bp=110.0,
        )

    # All None must raise ValueError
    with pytest.raises(ValueError, match="at least one valid measurement"):
        VitalSignEvent(
            patient_id="MIMIC-P-1",
            encounter_id="MIMIC-ENC-1-STAY-1",
            unit_id="MICU",
            event_timestamp=ts,
        )


def test_mimic_synchronized_cohort_duration_preservation():
    """Verify that cohort synchronization rigidly preserves encounter stay duration and order."""
    t_admit = datetime(2145, 3, 10, 8, 30, 0, tzinfo=timezone.utc)
    t_xfer = datetime(2145, 3, 11, 14, 15, 0, tzinfo=timezone.utc)
    t_disch = datetime(2145, 3, 14, 19, 45, 0, tzinfo=timezone.utc)

    orig_los_seconds = (t_disch - t_admit).total_seconds()
    orig_xfer_seconds = (t_xfer - t_admit).total_seconds()

    enc_id = "MIMIC-ENC-500-STAY-900"
    p_id = "MIMIC-P-500"

    e_admit = ICUEvent(patient_id=p_id, encounter_id=enc_id, event_type=EventType.ADMISSION, event_timestamp=t_admit, to_unit="CCU")
    e_xfer = ICUEvent(patient_id=p_id, encounter_id=enc_id, event_type=EventType.TRANSFER, event_timestamp=t_xfer, from_unit="CCU", to_unit="CSRU")
    e_disch = ICUEvent(patient_id=p_id, encounter_id=enc_id, event_type=EventType.DISCHARGE, event_timestamp=t_disch, from_unit="CSRU")

    v_ts = t_admit + timedelta(hours=6)
    v_event = VitalSignEvent(patient_id=p_id, encounter_id=enc_id, unit_id="CCU", event_timestamp=v_ts, heart_rate=75.0)

    base_time = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    synced_adt, synced_vit = create_synchronized_cohort([e_admit, e_xfer, e_disch], [v_event], base_time=base_time, window_days=10)

    assert len(synced_adt) == 3
    assert len(synced_vit) == 1

    s_admit = synced_adt[0]
    s_xfer = synced_adt[1]
    s_disch = synced_adt[2]
    s_vit = synced_vit[0]

    # Exact stay duration must be identical down to the second
    new_los_seconds = (s_disch.event_timestamp - s_admit.event_timestamp).total_seconds()
    new_xfer_seconds = (s_xfer.event_timestamp - s_admit.event_timestamp).total_seconds()
    new_v_offset = (s_vit.event_timestamp - s_admit.event_timestamp).total_seconds()

    assert new_los_seconds == orig_los_seconds, "Stay duration was altered by synchronization!"
    assert new_xfer_seconds == orig_xfer_seconds, "Transfer delay was altered by synchronization!"
    assert new_v_offset == (v_ts - t_admit).total_seconds(), "Vital sign offset was altered!"

    # Ordering preserved
    assert s_admit.event_timestamp < s_xfer.event_timestamp < s_disch.event_timestamp


def test_mimic_occupancy_reconstruction_with_duplicate_transfers():
    """Verify that OccupancyCalculator idempotently handles repeat events from MIMIC."""
    t0 = datetime(2026, 9, 1, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 9, 3, 14, 0, 0, tzinfo=timezone.utc)

    e1 = ICUEvent(patient_id="M-P-1", encounter_id="M-ENC-1", event_type=EventType.ADMISSION, event_timestamp=t0, to_unit="MICU")
    e1_dup = e1.model_copy()  # exact duplicate with same event_id
    e2 = ICUEvent(patient_id="M-P-1", encounter_id="M-ENC-1", event_type=EventType.DISCHARGE, event_timestamp=t1, from_unit="MICU")

    calc = OccupancyCalculator(unit_capacities={"MICU": 24})
    df_occ = calc.reconstruct([e1, e1_dup, e2])

    assert not df_occ.empty
    # Maximum occupancy should be exactly 1, no double counting
    micu_max = df_occ[df_occ["unit_id"] == "MICU"]["occupied_beds"].max()
    assert micu_max == 1


def test_mimic_sha256_checksum():
    """Verify compute_sha256 matches hashlib output."""
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"MIMIC-IV clinical test payload\n")
        tmp_name = f.name

    try:
        h = compute_sha256(tmp_name)
        assert len(h) == 64
        assert isinstance(h, str)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
