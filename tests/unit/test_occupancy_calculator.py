"""Unit tests for occupancy calculation and double-counting prevention."""

from datetime import datetime, timezone
import pytest

from src.processing.occupancy_calculator import OccupancyCalculator
from src.schema.events import EventType, ICUEvent


def test_deterministic_census_reconstruction(sample_units):
    """
    Carefully crafted test verifying exact census timeline:
    Patient 1:
      10:15 - Admit MICU
      12:45 - Discharge
    Patient 2:
      11:30 - Admit MICU
      13:15 - Transfer MICU -> SICU
      15:30 - Discharge
    """
    events = [
        ICUEvent(
            patient_id="P1",
            encounter_id="E1",
            event_type=EventType.ADMISSION,
            event_timestamp=datetime(2026, 10, 1, 10, 15, tzinfo=timezone.utc),
            to_unit="MICU",
        ),
        ICUEvent(
            patient_id="P2",
            encounter_id="E2",
            event_type=EventType.ADMISSION,
            event_timestamp=datetime(2026, 10, 1, 11, 30, tzinfo=timezone.utc),
            to_unit="MICU",
        ),
        ICUEvent(
            patient_id="P1",
            encounter_id="E1",
            event_type=EventType.DISCHARGE,
            event_timestamp=datetime(2026, 10, 1, 12, 45, tzinfo=timezone.utc),
            from_unit="MICU",
        ),
        ICUEvent(
            patient_id="P2",
            encounter_id="E2",
            event_type=EventType.TRANSFER,
            event_timestamp=datetime(2026, 10, 1, 13, 15, tzinfo=timezone.utc),
            from_unit="MICU",
            to_unit="SICU",
        ),
        ICUEvent(
            patient_id="P2",
            encounter_id="E2",
            event_type=EventType.DISCHARGE,
            event_timestamp=datetime(2026, 10, 1, 15, 30, tzinfo=timezone.utc),
            from_unit="SICU",
        ),
    ]

    calc = OccupancyCalculator(unit_capacities=sample_units)
    start_hr = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
    end_hr = datetime(2026, 10, 1, 16, 0, tzinfo=timezone.utc)

    df = calc.reconstruct(events, start_hour=start_hr, end_hour=end_hr)

    # Filter for MICU and SICU
    micu = df[df["unit_id"] == "MICU"].set_index("timestamp")["occupied_beds"]
    sicu = df[df["unit_id"] == "SICU"].set_index("timestamp")["occupied_beds"]

    # 10:00: Neither patient has arrived yet
    assert micu.loc[datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)] == 0
    assert sicu.loc[datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)] == 0

    # 11:00: P1 is in MICU
    assert micu.loc[datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)] == 1
    assert sicu.loc[datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)] == 0

    # 12:00: P1 and P2 are in MICU
    assert micu.loc[datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)] == 2
    assert sicu.loc[datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)] == 0

    # 13:00: P1 discharged at 12:45; P2 is in MICU
    assert micu.loc[datetime(2026, 10, 1, 13, 0, tzinfo=timezone.utc)] == 1
    assert sicu.loc[datetime(2026, 10, 1, 13, 0, tzinfo=timezone.utc)] == 0

    # 14:00: P2 transferred to SICU at 13:15
    assert micu.loc[datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)] == 0
    assert sicu.loc[datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)] == 1

    # 15:00: P2 still in SICU
    assert micu.loc[datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)] == 0
    assert sicu.loc[datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)] == 1

    # 16:00: P2 discharged at 15:30; both empty
    assert micu.loc[datetime(2026, 10, 1, 16, 0, tzinfo=timezone.utc)] == 0
    assert sicu.loc[datetime(2026, 10, 1, 16, 0, tzinfo=timezone.utc)] == 0


def test_no_double_counting_with_duplicates(sample_units):
    """
    Verifies that feeding duplicate events (with the same event_id)
    does not inflate bed occupancy.
    """
    ev_admit = ICUEvent(
        patient_id="P1",
        encounter_id="E1",
        event_type=EventType.ADMISSION,
        event_timestamp=datetime(2026, 10, 1, 10, 30, tzinfo=timezone.utc),
        to_unit="MICU",
    )
    ev_disch = ICUEvent(
        patient_id="P1",
        encounter_id="E1",
        event_type=EventType.DISCHARGE,
        event_timestamp=datetime(2026, 10, 1, 14, 30, tzinfo=timezone.utc),
        from_unit="MICU",
    )

    # Duplicate the admission 3 times
    events_with_dupes = [ev_admit, ev_admit, ev_admit, ev_disch]

    calc = OccupancyCalculator(unit_capacities=sample_units)
    df = calc.reconstruct(
        events_with_dupes,
        start_hour=datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc),
        end_hour=datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc),
    )

    micu = df[df["unit_id"] == "MICU"].set_index("timestamp")["occupied_beds"]
    # Peak occupancy must remain exactly 1 bed
    assert micu.max() == 1
