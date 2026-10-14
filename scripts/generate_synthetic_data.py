"""
Standalone CLI script to generate reproducible synthetic ICU event datasets
and precompute ground-truth hourly occupancy series for training and evaluation.
"""

import argparse
from datetime import datetime, timedelta, timezone
import os
import sys

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import get_settings
from src.producer.simulator import ICUSimulator
from src.processing.occupancy_calculator import OccupancyCalculator


def parse_utc_datetime(dt_str: str) -> datetime:
    """Parses an ISO or datetime string and normalizes to a timezone-aware UTC datetime."""
    dt_str = dt_str.strip()
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate reproducible synthetic ICU ADT events and hourly census."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to simulate (default: 90).",
    )
    parser.add_argument(
        "--reference-time",
        "--end-date",
        dest="reference_time",
        type=str,
        default=None,
        help="Target forecast origin / simulation end time (ISO format, e.g. '2026-10-14T10:00:00Z'). "
             "Defaults to system clock aligned to top of current hour in UTC.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Explicit simulation start time (UTC). If omitted, calculated as reference_time - days.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic generation (default: 42).",
    )
    parser.add_argument(
        "--output-events",
        type=str,
        default="data/synthetic/simulated_icu_events.jsonl",
        help="Target file path for simulated events JSONL.",
    )
    parser.add_argument(
        "--output-occupancy",
        type=str,
        default="data/synthetic/simulated_hourly_occupancy.parquet",
        help="Target file path for hourly occupancy parquet.",
    )
    parser.add_argument(
        "--surge",
        action="store_true",
        help="Inject a controlled surge scenario (day 35, TSICU, 2.5x volume for 48h).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    settings = get_settings()

    os.makedirs(os.path.dirname(args.output_events), exist_ok=True)
    os.makedirs(os.path.dirname(args.output_occupancy), exist_ok=True)

    # 1. Resolve simulation reference time (end date) and start date
    if args.reference_time:
        ref_time = parse_utc_datetime(args.reference_time)
    else:
        ref_time = datetime.now(timezone.utc)

    # Align reference time strictly to the top of the hour (:00:00)
    ref_time = ref_time.replace(minute=0, second=0, microsecond=0)

    if args.start_date:
        start_date = parse_utc_datetime(args.start_date).replace(minute=0, second=0, microsecond=0)
        sim_days = max(1, int((ref_time - start_date).total_seconds() // 86400))
    else:
        sim_days = args.days
        start_date = ref_time - timedelta(days=sim_days)

    surge_scenarios = []
    if args.surge:
        surge_day = min(35, max(1, sim_days // 2))
        surge_scenarios.append({
            "day": surge_day,
            "duration_hours": 48,
            "unit": "TSICU",
            "factor": 2.5,
        })

    print(f"============================================================")
    print(f"ICU Synthetic Event Generator")
    print(f"============================================================")
    print(f"Simulation Start:     {start_date.isoformat()}")
    print(f"Simulation End / Ref: {ref_time.isoformat()}")
    print(f"Duration:             {sim_days} days ({sim_days * 24} hours)")
    print(f"Random Seed:          {args.seed}")
    print(f"Monitored Units:      {list(settings.icu_unit_capacities.keys())}")
    print(f"Surge Injection:      {'Active (Day ' + str(surge_scenarios[0]['day']) + ')' if args.surge else 'Disabled'}")
    print(f"Output Events:        {args.output_events}")
    print(f"Output Occupancy:     {args.output_occupancy}")
    print(f"------------------------------------------------------------")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    simulator = ICUSimulator(
        start_date=start_date,
        days=sim_days,
        random_seed=args.seed,
        units=settings.icu_unit_capacities,
        surge_scenarios=surge_scenarios,
    )

    events = simulator.generate_events()

    # Retain events up to reference_time in the JSONL export to avoid future events beyond current simulation clock
    events_to_export = [e for e in events if e.event_timestamp <= ref_time]
    written_count = simulator.export_jsonl(args.output_events, events_to_export)
    print(f"[OK] Generated and saved {written_count:,} events (up to {ref_time.isoformat()}) to {args.output_events}")

    # Calculate hourly occupancy terminating exactly at reference_time
    calculator = OccupancyCalculator(unit_capacities=settings.icu_unit_capacities)
    df_occupancy = calculator.reconstruct(
        events=events,
        start_hour=start_date,
        end_hour=ref_time,
    )
    df_occupancy.to_parquet(args.output_occupancy, index=False)
    print(f"[OK] Reconstructed {len(df_occupancy):,} hourly census rows ({df_occupancy['timestamp'].min()} -> {df_occupancy['timestamp'].max()}) to {args.output_occupancy}")

    # Summary Statistics
    print(f"\n--- Occupancy Statistics by Unit ---")
    summary = df_occupancy.groupby("unit_id")["occupied_beds"].agg(["mean", "min", "max", "std"])
    for unit_id, row in summary.iterrows():
        cap = settings.icu_unit_capacities.get(unit_id, 0)
        print(f"  Unit: {unit_id:<6} (Cap: {cap:2d}) | Mean: {row['mean']:5.1f} | Min: {int(row['min']):2d} | Max: {int(row['max']):2d} | Std: {row['std']:4.1f}")

    # Latest Census at Forecast Origin
    print(f"\n--- Latest Occupancy at Forecast Origin ({ref_time.isoformat()}) ---")
    latest_df = df_occupancy[df_occupancy["timestamp"] == ref_time].sort_values("unit_id")
    for _, row in latest_df.iterrows():
        cap = row["capacity"]
        occ = row["occupied_beds"]
        rate = row["occupancy_rate"] * 100
        print(f"  Unit: {row['unit_id']:<6} | Occupied: {occ:2d} / {cap:2d} beds ({rate:5.1f}%) | Available: {row['available_beds']:2d} beds")

    print(f"\n[NOTICE - Clinical Limitations]:")
    print("This dataset is purely synthetic and intended strictly for developing and testing")
    print("the data engineering, streaming, and machine learning pipeline architecture.")
    print("It DOES NOT establish clinical validity or real-world medical efficacy.")
    print(f"============================================================\n")


if __name__ == "__main__":
    main()
