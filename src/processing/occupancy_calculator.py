"""
Hourly Occupancy Reconstruction Engine.
Reconstructs exact instantaneous unit census from ADT event streams
with rigorous double-counting prevention, deduplication, and transfer state tracking.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set
import pandas as pd

from src.schema.events import DataSource, EventType, ICUEvent
from src.schema.occupancy import HourlyOccupancy


class OccupancyCalculator:
    """
    State machine that processes patient ADT events to reconstruct hourly census.
    Guarantees that no patient is counted in more than one unit simultaneously.
    """

    def __init__(self, unit_capacities: Dict[str, int]):
        """
        :param unit_capacities: Mapping of unit identifier to total bed capacity.
        """
        self.unit_capacities = unit_capacities
        self.known_units = set(unit_capacities.keys())

    def reconstruct(
        self,
        events: List[ICUEvent],
        start_hour: Optional[datetime] = None,
        end_hour: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Reconstructs hourly occupancy time series across all known units.

        :param events: List of ICUEvent objects.
        :param start_hour: Optional start timestamp (rounded down to top of hour).
        :param end_hour: Optional end timestamp (rounded up to top of hour).
        :return: DataFrame sorted by (timestamp, unit_id) matching HourlyOccupancy schema.
        """
        if not events:
            return pd.DataFrame(columns=[
                "timestamp", "unit_id", "occupied_beds", "capacity",
                "available_beds", "occupancy_rate", "admissions_count",
                "discharges_count", "transfers_in_count", "transfers_out_count",
                "data_source"
            ])

        # Step 1: Deduplication by event_id (idempotency enforcement)
        seen_event_ids: Set[str] = set()
        deduped_events: List[ICUEvent] = []
        for ev in events:
            ev_id_str = str(ev.event_id)
            if ev_id_str not in seen_event_ids:
                seen_event_ids.add(ev_id_str)
                deduped_events.append(ev)

        # Step 2: Chronological ordering
        deduped_events.sort(key=lambda e: (e.event_timestamp, e.version))

        # Determine time horizon boundaries
        first_event_ts = deduped_events[0].event_timestamp
        last_event_ts = deduped_events[-1].event_timestamp

        grid_start = start_hour or first_event_ts.replace(minute=0, second=0, microsecond=0)
        grid_end = end_hour or (last_event_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))

        if grid_start.tzinfo is None:
            grid_start = grid_start.replace(tzinfo=timezone.utc)
        if grid_end.tzinfo is None:
            grid_end = grid_end.replace(tzinfo=timezone.utc)

        # Step 3: Event indexation & interval metrics tracking
        # We index event transitions to efficiently evaluate state at hour boundaries
        # Active patient location tracking: (patient_id, encounter_id) -> current_unit
        active_encounters: Dict[str, str] = {}
        
        # Step through all hour milestones
        total_hours = int((grid_end - grid_start).total_seconds() // 3600) + 1
        records: List[dict] = []

        event_idx = 0
        num_events = len(deduped_events)

        prev_milestone = grid_start - timedelta(hours=1)

        for h_step in range(total_hours):
            current_milestone = grid_start + timedelta(hours=h_step)

            # Initialize interval flow counters for (prev_milestone, current_milestone]
            admissions_interval = {u: 0 for u in self.known_units}
            discharges_interval = {u: 0 for u in self.known_units}
            transfers_in_interval = {u: 0 for u in self.known_units}
            transfers_out_interval = {u: 0 for u in self.known_units}

            # Process all events occurring up to current_milestone
            while event_idx < num_events and deduped_events[event_idx].event_timestamp <= current_milestone:
                ev = deduped_events[event_idx]
                key = f"{ev.patient_id}:{ev.encounter_id}"

                if ev.event_type == EventType.ADMISSION:
                    # Enforce single occupancy location
                    if ev.to_unit in self.known_units:
                        active_encounters[key] = ev.to_unit
                        admissions_interval[ev.to_unit] += 1

                elif ev.event_type == EventType.DISCHARGE:
                    # Remove from active encounters
                    if key in active_encounters:
                        prev_u = active_encounters.pop(key)
                        if prev_u in self.known_units:
                            discharges_interval[prev_u] += 1
                    elif ev.from_unit in self.known_units:
                        discharges_interval[ev.from_unit] += 1

                elif ev.event_type == EventType.TRANSFER:
                    # Atomic transition from from_unit to to_unit
                    if ev.from_unit in self.known_units:
                        transfers_out_interval[ev.from_unit] += 1
                    
                    if ev.to_unit in self.known_units:
                        active_encounters[key] = ev.to_unit
                        transfers_in_interval[ev.to_unit] += 1
                    else:
                        # Transferred out of monitored ICUs into general ward
                        active_encounters.pop(key, None)

                event_idx += 1

            # Tally instantaneous occupancy per unit at exact current_milestone
            unit_census = {u: 0 for u in self.known_units}
            for loc in active_encounters.values():
                if loc in unit_census:
                    unit_census[loc] += 1

            # Compile census records for each unit
            source_label = deduped_events[0].data_source.value if deduped_events else DataSource.SYNTHETIC.value

            for unit_id, capacity in self.unit_capacities.items():
                occupied = unit_census[unit_id]
                available = capacity - occupied
                rate = round(occupied / capacity, 4) if capacity > 0 else 0.0

                records.append({
                    "timestamp": current_milestone,
                    "unit_id": unit_id,
                    "occupied_beds": occupied,
                    "capacity": capacity,
                    "available_beds": available,
                    "occupancy_rate": rate,
                    "admissions_count": admissions_interval[unit_id],
                    "discharges_count": discharges_interval[unit_id],
                    "transfers_in_count": transfers_in_interval[unit_id],
                    "transfers_out_count": transfers_out_interval[unit_id],
                    "data_source": source_label,
                })

            prev_milestone = current_milestone

        df = pd.DataFrame(records)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df.sort_values(by=["timestamp", "unit_id"], inplace=True)
            df.reset_index(drop=True, inplace=True)

        return df
