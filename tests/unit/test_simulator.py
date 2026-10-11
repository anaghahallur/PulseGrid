"""Unit tests for synthetic event simulation."""

from datetime import datetime, timezone
import pytest

from src.producer.simulator import ICUSimulator
from src.schema.events import EventType


def test_simulation_determinism():
    start_dt = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
    sim1 = ICUSimulator(start_date=start_dt, days=3, random_seed=123)
    events1 = sim1.generate_events()

    sim2 = ICUSimulator(start_date=start_dt, days=3, random_seed=123)
    events2 = sim2.generate_events()

    assert len(events1) == len(events2)
    assert len(events1) > 0
    for e1, e2 in zip(events1, events2):
        assert e1.patient_id == e2.patient_id
        assert e1.encounter_id == e2.encounter_id
        assert e1.event_type == e2.event_type
        assert e1.event_timestamp == e2.event_timestamp
        assert e1.to_unit == e2.to_unit
        assert e1.from_unit == e2.from_unit


def test_timeline_consistency():
    start_dt = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
    sim = ICUSimulator(start_date=start_dt, days=5, random_seed=42)
    events = sim.generate_events()

    # Group events by encounter
    encounters = {}
    for ev in events:
        encounters.setdefault(ev.encounter_id, []).append(ev)

    for enc_id, enc_events in encounters.items():
        # Each encounter should have at least 1 admission and 1 discharge
        admissions = [e for e in enc_events if e.event_type == EventType.ADMISSION]
        discharges = [e for e in enc_events if e.event_type == EventType.DISCHARGE]
        transfers = [e for e in enc_events if e.event_type == EventType.TRANSFER]

        assert len(admissions) == 1, f"Encounter {enc_id} has {len(admissions)} admissions"
        assert len(discharges) == 1, f"Encounter {enc_id} has {len(discharges)} discharges"

        admit_time = admissions[0].event_timestamp
        disch_time = discharges[0].event_timestamp

        assert admit_time <= disch_time, f"Admit {admit_time} > Disch {disch_time}"

        for tr in transfers:
            assert admit_time <= tr.event_timestamp <= disch_time, (
                f"Transfer {tr.event_timestamp} out of bounds [{admit_time}, {disch_time}]"
            )


def test_surge_scenario_impact():
    start_dt = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
    # Baseline simulation
    sim_base = ICUSimulator(start_date=start_dt, days=10, random_seed=77)
    events_base = sim_base.generate_events()
    tsicu_base_adm = [e for e in events_base if e.event_type == EventType.ADMISSION and e.to_unit == "TSICU"]

    # Surge simulation (3x on TSICU for 48 hours starting on day 2)
    surge = [{"day": 2, "duration_hours": 48, "unit": "TSICU", "factor": 3.0}]
    sim_surge = ICUSimulator(start_date=start_dt, days=10, random_seed=77, surge_scenarios=surge)
    events_surge = sim_surge.generate_events()
    tsicu_surge_adm = [e for e in events_surge if e.event_type == EventType.ADMISSION and e.to_unit == "TSICU"]

    assert len(tsicu_surge_adm) > len(tsicu_base_adm)
