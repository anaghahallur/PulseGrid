"""
Deterministic Synthetic ICU Event Generator.
Produces high-fidelity patient admission, transfer, and discharge events
modeled after empirical ICU arrival rates, diurnal curves, and log-normal length of stay.
"""

from datetime import datetime, timedelta, timezone
import json
import math
from typing import Dict, List, Optional
import numpy as np

from src.schema.events import DataSource, EventType, ICUEvent


class ICUSimulator:
    """
    Simulates patient flow through an ICU network with configurable capacities,
    stochastic arrival processes, diurnal modulation, and controllable surge scenarios.
    """

    def __init__(
        self,
        start_date: datetime,
        days: int = 60,
        random_seed: int = 42,
        units: Optional[Dict[str, int]] = None,
        base_arrival_rates: Optional[Dict[str, float]] = None,
        mean_los_hours: float = 48.0,
        transfer_probability: float = 0.12,
        surge_scenarios: Optional[List[dict]] = None,
    ):
        """
        Initialize the simulator.

        :param start_date: Starting datetime (UTC).
        :param days: Span of simulation in days.
        :param random_seed: Seed for numpy random generator for 100% determinism.
        :param units: Dict mapping unit identifiers to bed capacity.
        :param base_arrival_rates: Average admissions per day per unit.
        :param mean_los_hours: Mean length of stay in hours.
        :param transfer_probability: Likelihood that an encounter involves a cross-ICU transfer.
        :param surge_scenarios: List of surge overrides, e.g.
               [{'day': 14, 'duration_hours': 36, 'unit': 'TSICU', 'factor': 2.5}]
        """
        if start_date.tzinfo is None:
            self.start_date = start_date.replace(tzinfo=timezone.utc)
        else:
            self.start_date = start_date.astimezone(timezone.utc)

        self.days = days
        self.random_seed = random_seed
        self.rng = np.random.default_rng(random_seed)

        self.units = units or {
            "MICU": 24,
            "SICU": 20,
            "CCU": 16,
            "TSICU": 16,
            "CSRU": 18,
        }

        # Average arrivals per day per unit
        self.base_arrival_rates = base_arrival_rates or {
            "MICU": 7.5,
            "SICU": 6.0,
            "CCU": 5.0,
            "TSICU": 4.5,
            "CSRU": 4.0,
        }

        self.mean_los_hours = mean_los_hours
        self.transfer_probability = transfer_probability
        self.surge_scenarios = surge_scenarios or []

    def _get_arrival_multiplier(self, dt: datetime, unit: str) -> float:
        """
        Computes diurnal and day-of-week arrival rate multiplier.
        Hospital admissions peak during 10:00-18:00 and drop overnight.
        Surgical units experience reduced elective intakes on weekends.
        """
        hour = dt.hour + dt.minute / 60.0
        # Diurnal wave peaking around 14:00 (hour 14)
        diurnal = 1.0 + 0.45 * math.sin(math.pi * (hour - 8) / 12) if 8 <= hour <= 20 else 0.55

        # Weekend dampening for surgical units
        weekday = dt.weekday()
        weekend_mult = 1.0
        if weekday in (5, 6) and unit in ("SICU", "CSRU"):
            weekend_mult = 0.5

        # Surge multiplier check
        surge_mult = 1.0
        sim_elapsed_hours = (dt - self.start_date).total_seconds() / 3600.0
        for surge in self.surge_scenarios:
            surge_start_hour = surge.get("day", 0) * 24.0
            surge_end_hour = surge_start_hour + surge.get("duration_hours", 24)
            if surge_start_hour <= sim_elapsed_hours <= surge_end_hour:
                if surge.get("unit") in (unit, "ALL", None):
                    surge_mult = max(surge_mult, surge.get("factor", 2.0))

        return max(0.1, diurnal * weekend_mult * surge_mult)

    def _sample_los_hours(self) -> float:
        """
        Samples ICU length of stay from a log-normal distribution.
        Clinical ICU stays are heavily right-skewed.
        """
        # Parameterize lognormal such that mean matches self.mean_los_hours
        sigma = 0.75
        mu = math.log(self.mean_los_hours) - (sigma**2 / 2.0)
        los = float(self.rng.lognormal(mean=mu, sigma=sigma))
        # Clip to realistic range [4 hours, 720 hours (30 days)]
        return float(np.clip(los, 4.0, 720.0))

    def generate_events(
        self,
        inject_duplicates: bool = False,
        duplicate_rate: float = 0.01,
    ) -> List[ICUEvent]:
        """
        Generates chronologically ordered ICU events across the simulation window.

        :param inject_duplicates: If True, randomly injects duplicate events for resilience testing.
        :param duplicate_rate: Fraction of events to duplicate when inject_duplicates is True.
        :return: List of validated ICUEvent instances.
        """
        events: List[ICUEvent] = []
        patient_counter = 1
        unit_names = list(self.units.keys())

        # Step through hourly slices to simulate non-homogeneous Poisson arrival process
        total_hours = self.days * 24
        current_time = self.start_date

        for h in range(total_hours):
            hour_start = current_time + timedelta(hours=h)

            for unit_id in unit_names:
                base_daily_lambda = self.base_arrival_rates.get(unit_id, 5.0)
                hourly_lambda = (base_daily_lambda / 24.0) * self._get_arrival_multiplier(hour_start, unit_id)
                num_arrivals = self.rng.poisson(lam=hourly_lambda)

                for _ in range(num_arrivals):
                    # Arrival minute within the hour
                    minute_offset = float(self.rng.uniform(0, 59.9))
                    admit_time = hour_start + timedelta(minutes=minute_offset)

                    patient_id = f"SYN-P-{patient_counter:06d}"
                    encounter_id = f"SYN-ENC-{patient_counter:06d}"
                    bed_id = f"{unit_id}-BED-{self.rng.integers(1, self.units[unit_id] + 1):02d}"
                    patient_counter += 1

                    # 1. Emission: Admission
                    admit_event = ICUEvent(
                        patient_id=patient_id,
                        encounter_id=encounter_id,
                        event_type=EventType.ADMISSION,
                        event_timestamp=admit_time,
                        from_unit=None,
                        to_unit=unit_id,
                        bed_id=bed_id,
                        data_source=DataSource.SYNTHETIC,
                        metadata={
                            "simulated": True,
                            "admission_type": "EMERGENCY" if self.rng.random() > 0.35 else "ELECTIVE",
                        },
                    )
                    events.append(admit_event)

                    total_los = self._sample_los_hours()
                    current_unit = unit_id

                    # 2. Check for intermediate transfer
                    if self.rng.random() < self.transfer_probability:
                        transfer_fraction = float(self.rng.uniform(0.25, 0.75))
                        transfer_time = admit_time + timedelta(hours=total_los * transfer_fraction)
                        candidate_units = [u for u in unit_names if u != current_unit]
                        to_unit = str(self.rng.choice(candidate_units))
                        transfer_bed = f"{to_unit}-BED-{self.rng.integers(1, self.units[to_unit] + 1):02d}"

                        transfer_event = ICUEvent(
                            patient_id=patient_id,
                            encounter_id=encounter_id,
                            event_type=EventType.TRANSFER,
                            event_timestamp=transfer_time,
                            from_unit=current_unit,
                            to_unit=to_unit,
                            bed_id=transfer_bed,
                            data_source=DataSource.SYNTHETIC,
                            metadata={"simulated": True, "reason": "SPECIALTY_CARE"},
                        )
                        events.append(transfer_event)
                        current_unit = to_unit

                    # 3. Discharge
                    disch_time = admit_time + timedelta(hours=total_los)
                    disch_event = ICUEvent(
                        patient_id=patient_id,
                        encounter_id=encounter_id,
                        event_type=EventType.DISCHARGE,
                        event_timestamp=disch_time,
                        from_unit=current_unit,
                        to_unit=None,
                        bed_id=None,
                        data_source=DataSource.SYNTHETIC,
                        metadata={"simulated": True, "los_hours": round(total_los, 2)},
                    )
                    events.append(disch_event)

        # Inject duplicates if requested for resilience validation
        if inject_duplicates and events:
            num_dupes = max(1, int(len(events) * duplicate_rate))
            dupe_indices = self.rng.choice(len(events), size=num_dupes, replace=False)
            for idx in dupe_indices:
                events.append(events[idx])

        # Strictly sort chronologically by timestamp, version
        events.sort(key=lambda e: (e.event_timestamp, e.version))
        return events

    def export_jsonl(self, filepath: str, events: Optional[List[ICUEvent]] = None) -> int:
        """
        Saves simulated events to JSON Lines format.

        :param filepath: Target path.
        :param events: Optional pre-generated events. Generates fresh if None.
        :return: Total records written.
        """
        if events is None:
            events = self.generate_events()

        with open(filepath, "w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event.to_dict()) + "\n")

        return len(events)
