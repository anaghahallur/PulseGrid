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


def test_transfer_preserves_total_count(sample_units):
    """
    Verifies that a cross-unit transfer atomically preserves total active patient count.
    Total census before, during, and after transfer across the two units is invariant.
    """
    events = [
        ICUEvent(
            patient_id="P1",
            encounter_id="E1",
            event_type=EventType.ADMISSION,
            event_timestamp=datetime(2026, 10, 1, 10, 10, tzinfo=timezone.utc),
            to_unit="MICU",
        ),
        ICUEvent(
            patient_id="P1",
            encounter_id="E1",
            event_type=EventType.TRANSFER,
            event_timestamp=datetime(2026, 10, 1, 11, 20, tzinfo=timezone.utc),
            from_unit="MICU",
            to_unit="SICU",
        ),
        ICUEvent(
            patient_id="P1",
            encounter_id="E1",
            event_type=EventType.DISCHARGE,
            event_timestamp=datetime(2026, 10, 1, 13, 0, tzinfo=timezone.utc),
            from_unit="SICU",
        ),
    ]

    calc = OccupancyCalculator(unit_capacities=sample_units)
    df = calc.reconstruct(
        events,
        start_hour=datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc),
        end_hour=datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc),
    )

    # Hourly total across MICU and SICU
    pivot = df.pivot(index="timestamp", columns="unit_id", values="occupied_beds")
    total_micu_sicu = pivot["MICU"] + pivot["SICU"]

    # 10:00: 0
    assert total_micu_sicu.loc[datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)] == 0
    # 11:00: 1 (in MICU)
    assert total_micu_sicu.loc[datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)] == 1
    # 12:00: 1 (transferred to SICU)
    assert total_micu_sicu.loc[datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)] == 1
    # 13:00: 0 (discharged at 13:00 top of hour)
    assert total_micu_sicu.loc[datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)] == 0


def test_capacity_exceedance_representation():
    """
    Verifies that when demand exceeds capacity, physical occupancy is not falsely clipped,
    and capacity-exceedance indicators are flagged accurately.
    """
    small_capacity = {"MICU": 2}
    events = [
        ICUEvent(
            patient_id=f"P{i}",
            encounter_id=f"E{i}",
            event_type=EventType.ADMISSION,
            event_timestamp=datetime(2026, 10, 1, 10, 10, tzinfo=timezone.utc),
            to_unit="MICU",
        )
        for i in range(1, 5)  # 4 admissions for 2 beds
    ]

    calc = OccupancyCalculator(unit_capacities=small_capacity)
    df = calc.reconstruct(
        events,
        start_hour=datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc),
        end_hour=datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc),
    )

    row_11 = df[(df["unit_id"] == "MICU") & (df["timestamp"] == datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc))].iloc[0]
    assert row_11["occupied_beds"] == 4
    assert row_11["capacity"] == 2
    assert row_11["available_beds"] == -2
    assert row_11["is_capacity_exceeded"] is True or row_11["is_capacity_exceeded"] == 1
    assert row_11["exceedance_beds"] == 2
    assert row_11["occupancy_rate"] == 2.0


def test_out_of_order_events(sample_units):
    """
    Verifies that supplying events in shuffled or reverse chronological order
    produces the exact same occupancy reconstruction.
    """
    import random

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
    df_ordered = calc.reconstruct(events)

    # Reverse order
    df_reversed = calc.reconstruct(list(reversed(events)))

    # Shuffled order with fixed seed
    shuffled = list(events)
    random.Random(99).shuffle(shuffled)
    df_shuffled = calc.reconstruct(shuffled)

    assert df_ordered["occupied_beds"].equals(df_reversed["occupied_beds"])
    assert df_ordered["occupied_beds"].equals(df_shuffled["occupied_beds"])


def test_unmatched_discharge_does_not_crash(sample_units):
    """
    Verifies that a discharge event arriving without a matching prior admission
    does not cause negative census or raise an unhandled exception.
    """
    orphan_disch = ICUEvent(
        patient_id="ORPHAN-P",
        encounter_id="ORPHAN-ENC",
        event_type=EventType.DISCHARGE,
        event_timestamp=datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc),
        from_unit="MICU",
    )

    calc = OccupancyCalculator(unit_capacities=sample_units)
    df = calc.reconstruct([orphan_disch])
    assert not df.empty
    micu_census = df[df["unit_id"] == "MICU"]["occupied_beds"]
    assert (micu_census >= 0).all()
