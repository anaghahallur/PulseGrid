"""
Synthetic ICU Vital Signs Telemetry Simulator.
Produces deterministic, physiologically bounded multi-parameter vital signs
for simulated ICU patient encounters across monitored units.
"""

from datetime import datetime, timedelta, timezone
import json
from typing import Dict, List, Optional
import numpy as np

from src.schema.events import DataSource, VitalSignEvent


class VitalSignSimulator:
    """
    Simulates multi-parameter bedside physiological telemetry for ICU patients.
    Generates realistic heart rate, temperature, blood pressure, respiratory rate,
    and SpO2 streams with deterministic seeding and clinical plausibility limits.

    Academic / Demonstration Notice:
    All vital signs produced by this simulator are purely synthetic demonstrations
    for streaming and storage architectural validation. They DO NOT reflect real
    patient physiology, clinical diagnostics, or medical device telemetry.
    """

    def __init__(
        self,
        random_seed: int = 42,
        units: Optional[Dict[str, int]] = None,
        base_patient_count: int = 20,
    ):
        """
        :param random_seed: Seed for numpy RNG for deterministic generation.
        :param units: Dict mapping unit identifiers to bed capacities.
        :param base_patient_count: Number of distinct synthetic patients to rotate.
        """
        self.random_seed = random_seed
        self.rng = np.random.default_rng(random_seed)

        self.units = units or {
            "MICU": 24,
            "SICU": 20,
            "CCU": 16,
            "TSICU": 16,
            "CSRU": 18,
        }
        self.unit_names = sorted(list(self.units.keys()))
        self.base_patient_count = max(1, base_patient_count)

        # Pre-generate synthetic patient cohort
        self.cohort: List[dict] = []
        for i in range(1, self.base_patient_count + 1):
            assigned_unit = self.unit_names[(i - 1) % len(self.unit_names)]
            self.cohort.append({
                "patient_id": f"SYN-P-{i:06d}",
                "encounter_id": f"SYN-ENC-{i:06d}",
                "unit_id": assigned_unit,
                # Patient-specific baseline offsets for realistic individual variation
                "hr_baseline": float(self.rng.normal(78.0, 8.0)),
                "temp_baseline": float(self.rng.normal(37.0, 0.3)),
                "sbp_baseline": float(self.rng.normal(122.0, 10.0)),
                "dbp_baseline": float(self.rng.normal(76.0, 7.0)),
                "rr_baseline": float(self.rng.normal(16.0, 2.5)),
                "spo2_baseline": float(self.rng.normal(97.5, 1.2)),
            })

    def generate_events(
        self,
        num_events: int = 50,
        start_time: Optional[datetime] = None,
        interval_seconds: float = 60.0,
        simulate_partial_channels: bool = True,
    ) -> List[VitalSignEvent]:
        """
        Generates chronologically ordered VitalSignEvent instances.

        :param num_events: Total number of vital sign observations to generate.
        :param start_time: Starting timestamp (UTC). Defaults to current top-of-hour.
        :param interval_seconds: Time progression between observations.
        :param simulate_partial_channels: If True, simulates episodic NIBP and periodic temp sampling.
        :return: List of validated VitalSignEvent objects.
        """
        if start_time is None:
            current_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        elif start_time.tzinfo is None:
            current_time = start_time.replace(tzinfo=timezone.utc)
        else:
            current_time = start_time.astimezone(timezone.utc)

        events: List[VitalSignEvent] = []

        for i in range(num_events):
            patient = self.cohort[i % len(self.cohort)]
            event_ts = current_time + timedelta(seconds=i * interval_seconds)

            # Sample physiological values with patient-specific baselines + Gaussian jitter
            hr = float(np.clip(self.rng.normal(patient["hr_baseline"], 4.0), 45.0, 160.0))
            temp = round(float(np.clip(self.rng.normal(patient["temp_baseline"], 0.15), 35.5, 40.0)), 1)
            sbp = float(np.clip(self.rng.normal(patient["sbp_baseline"], 5.0), 85.0, 185.0))
            dbp = float(np.clip(self.rng.normal(patient["dbp_baseline"], 4.0), 50.0, 110.0))
            # Enforce SBP > DBP
            if sbp <= dbp:
                sbp = dbp + 15.0

            rr = float(np.clip(self.rng.normal(patient["rr_baseline"], 1.5), 8.0, 35.0))
            spo2 = round(float(np.clip(self.rng.normal(patient["spo2_baseline"], 0.8), 88.0, 100.0)), 1)

            # Realistic channel modulation (e.g. episodic NIBP or intermittent thermometer)
            if simulate_partial_channels:
                # NIBP cuff measured episodically (~15% of records omit BP)
                if self.rng.random() < 0.15:
                    sbp = None
                    dbp = None
                # Body temp measured periodically (~25% of records omit temp)
                if self.rng.random() < 0.25:
                    temp = None

            ev = VitalSignEvent(
                patient_id=patient["patient_id"],
                encounter_id=patient["encounter_id"],
                unit_id=patient["unit_id"],
                event_timestamp=event_ts,
                heart_rate=round(hr, 1) if hr is not None else None,
                temperature_c=temp,
                systolic_bp=round(sbp, 1) if sbp is not None else None,
                diastolic_bp=round(dbp, 1) if dbp is not None else None,
                respiratory_rate=round(rr, 1) if rr is not None else None,
                spo2=spo2,
                data_source=DataSource.SYNTHETIC,
                metadata={
                    "simulated": True,
                    "disclaimer": "SYNTHETIC DEMONSTRATION DATA ONLY - Not validated clinical physiology",
                    "device_type": "BEDSIDE_MULTI_PARAMETER_MONITOR",
                },
            )
            events.append(ev)

        # Ensure strict chronological sorting
        events.sort(key=lambda e: e.event_timestamp)
        return events

    def export_jsonl(self, filepath: str, events: Optional[List[VitalSignEvent]] = None) -> int:
        """
        Saves simulated vital-sign events to JSON Lines format.

        :param filepath: Target path.
        :param events: Optional pre-generated events. Generates 50 fresh if None.
        :return: Total records written.
        """
        if events is None:
            events = self.generate_events()

        with open(filepath, "w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event.to_dict()) + "\n")

        return len(events)
