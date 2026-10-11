"""
Standalone CLI script to generate reproducible synthetic ICU event datasets
and precompute ground-truth hourly occupancy series for training and evaluation.
"""

import argparse
from datetime import datetime, timezone
import os
import sys

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import get_settings
from src.producer.simulator import ICUSimulator
from src.processing.occupancy_calculator import OccupancyCalculator


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate reproducible synthetic ICU ADT events and hourly census."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Number of days to simulate (default: 60).",
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

    start_date = datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)

    surge_scenarios = []
    if args.surge:
        surge_scenarios.append({
            "day": 35,
            "duration_hours": 48,
            "unit": "TSICU",
            "factor": 2.5,
        })

    print(f"============================================================")
    print(f"ICU Synthetic Event Generator")
    print(f"============================================================")
    print(f"Simulation Start:     {start_date.isoformat()}")
    print(f"Duration:             {args.days} days")
    print(f"Random Seed:          {args.seed}")
    print(f"Monitored Units:      {list(settings.icu_unit_capacities.keys())}")
    print(f"Surge Injection:      {'Active' if args.surge else 'Disabled'}")
    print(f"Output Events:        {args.output_events}")
    print(f"Output Occupancy:     {args.output_occupancy}")
    print(f"------------------------------------------------------------")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    simulator = ICUSimulator(
        start_date=start_date,
        days=args.days,
        random_seed=args.seed,
        units=settings.icu_unit_capacities,
        surge_scenarios=surge_scenarios,
    )

    events = simulator.generate_events()
    written_count = simulator.export_jsonl(args.output_events, events)
    print(f"[OK] Generated and saved {written_count:,} events to {args.output_events}")

    # Calculate hourly occupancy
    calculator = OccupancyCalculator(unit_capacities=settings.icu_unit_capacities)
    df_occupancy = calculator.reconstruct(events)
    df_occupancy.to_parquet(args.output_occupancy, index=False)
    print(f"[OK] Reconstructed {len(df_occupancy):,} hourly census rows to {args.output_occupancy}")

    # Summary Statistics
    print(f"\n--- Occupancy Statistics by Unit ---")
    summary = df_occupancy.groupby("unit_id")["occupied_beds"].agg(["mean", "min", "max", "std"])
    for unit_id, row in summary.iterrows():
        cap = settings.icu_unit_capacities.get(unit_id, 0)
        print(f"  Unit: {unit_id:<6} (Cap: {cap:2d}) | Mean: {row['mean']:5.1f} | Min: {int(row['min']):2d} | Max: {int(row['max']):2d} | Std: {row['std']:4.1f}")

    print(f"\n[NOTICE - Clinical Limitations]:")
    print("This dataset is purely synthetic and intended strictly for developing and testing")
    print("the data engineering, streaming, and machine learning pipeline architecture.")
    print("It DOES NOT establish clinical validity or real-world medical efficacy.")
    print(f"============================================================\n")


if __name__ == "__main__":
    main()
