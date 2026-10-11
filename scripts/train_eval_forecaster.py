"""
Standalone CLI script to execute the Phase 3 ICU Occupancy Forecasting Training & Evaluation Pipeline.
Trains baselines and candidate models, validates chronologically, selects best model,
evaluates on held-out test data, and saves all artifacts.
"""

import argparse
import os
import sys
import pandas as pd

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.ml.pipeline import run_training_and_evaluation, save_pipeline_artifacts


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train and evaluate multi-horizon ICU occupancy forecasting models."
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/synthetic/simulated_hourly_occupancy.parquet",
        help="Path to hourly occupancy Parquet dataset.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="models",
        help="Directory to persist fitted model and evaluation artifacts.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic training.",
    )
    parser.add_argument(
        "--include-rf",
        action="store_true",
        help="Include Random Forest in the candidate model suite.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("======================================================================")
    print("ICU Bed Demand Forecasting — Phase 3 Training and Evaluation Pipeline")
    print("======================================================================")
    print(f"Dataset Path:       {args.data_path}")
    print(f"Output Directory:   {args.output_dir}")
    print(f"Random Seed:        {args.seed}")
    print(f"Include RF:         {args.include_rf}")
    print("----------------------------------------------------------------------")

    if not os.path.exists(args.data_path):
        print(f"[ERROR] Dataset file not found at: {args.data_path}")
        print("Please run `python scripts/generate_synthetic_data.py --days 90 --surge` first.")
        sys.exit(1)

    print("[1/4] Loading historical hourly occupancy dataset...")
    df = pd.read_parquet(args.data_path)
    print(f"      [OK] Loaded {len(df):,} rows spanning {df['timestamp'].min()} -> {df['timestamp'].max()}")
    print(f"      [OK] Units present: {sorted(df['unit_id'].unique().tolist())}")

    print("\n[2/4] Engineering features and splitting chronologically (70% Train, 15% Val, 15% Test)...")
    pipeline_out = run_training_and_evaluation(
        df=df,
        random_seed=args.seed,
        include_rf=args.include_rf,
    )
    bounds = pipeline_out["boundaries"]
    print(f"      [OK] Train samples:      {bounds['sample_counts']['train']:,} ({bounds['train_start']} -> {bounds['train_end']})")
    print(f"      [OK] Validation samples: {bounds['sample_counts']['val']:,} ({bounds['val_start']} -> {bounds['val_end']})")
    print(f"      [OK] Held-Out Test:      {bounds['sample_counts']['test']:,} ({bounds['test_start']} -> {bounds['test_end']})")
    print(f"      [OK] Target buffer gap:  {bounds['target_buffer_hours']} hours between partitions")
    print(f"      [OK] Feature count:      {len(pipeline_out['feature_names'])}")

    val_res = pipeline_out["validation_results"]
    test_res = pipeline_out["test_results"]
    selected_name = pipeline_out["selected_model_name"]

    print("\n[3/4] Model Evaluation Summary on Validation & Held-out Test Sets:")
    print("-------------------------------------------------------------------------------------")
    print(f"{'Model':<38} | {'Val MAE':<9} | {'Test MAE':<9} | {'Test RMSE':<9} | {'Test WAPE':<9}")
    print("-------------------------------------------------------------------------------------")
    for m_name in test_res.keys():
        v_mae = val_res[m_name]["overall"]["mae"]
        t_mae = test_res[m_name]["overall"]["mae"]
        t_rmse = test_res[m_name]["overall"]["rmse"]
        t_wape = test_res[m_name]["overall"]["wape"] * 100
        marker = " *" if m_name == selected_name else ""
        print(f"{m_name + marker:<38} | {v_mae:<9.2f} | {t_mae:<9.2f} | {t_rmse:<9.2f} | {t_wape:<8.1f}%")
    print("-------------------------------------------------------------------------------------")
    print(f"(*) Selected optimal model based on lowest Validation MAE: {selected_name}")

    print("\n[4/4] Persisting model artifacts and evaluation reports...")
    saved_paths = save_pipeline_artifacts(pipeline_out, output_dir=args.output_dir)
    for k, p in saved_paths.items():
        print(f"      • {k}: {p}")

    best_test = test_res[selected_name]
    print("\n----------------------------------------------------------------------")
    print(f"Selected Model ({selected_name}) Horizon Breakdown (Sample Horizons):")
    print("----------------------------------------------------------------------")
    for h in [1, 2, 3, 6, 12, 18, 24]:
        h_m = best_test["by_horizon"][f"h{h}"]
        print(f"  Horizon +{h:2d}h: MAE = {h_m['mae']:4.2f} beds | RMSE = {h_m['rmse']:4.2f} beds | WAPE = {h_m['wape'] * 100:4.1f}%")

    print("\nUnit Breakdown:")
    for u, u_m in sorted(best_test["by_unit"].items()):
        print(f"  Unit {u:<6}: MAE = {u_m['mae']:4.2f} beds | RMSE = {u_m['rmse']:4.2f} beds | WAPE = {u_m['wape'] * 100:4.1f}%")

    surge_test = best_test["error_analysis"]["surge_periods"]
    norm_test = best_test["error_analysis"]["normal_periods"]
    print("\nOperating Regime Breakdown:")
    if surge_test:
        print(f"  Surge / Exceedance Periods: MAE = {surge_test['mae']:4.2f} beds | RMSE = {surge_test['rmse']:4.2f} beds | WAPE = {surge_test['wape'] * 100:4.1f}% (N={surge_test['sample_count']})")
    if norm_test:
        print(f"  Normal Operating Periods:   MAE = {norm_test['mae']:4.2f} beds | RMSE = {norm_test['rmse']:4.2f} beds | WAPE = {norm_test['wape'] * 100:4.1f}% (N={norm_test['sample_count']})")

    print("\n[SUCCESS] Phase 3 Machine Learning pipeline executed and saved cleanly!")
    print("======================================================================\n")


if __name__ == "__main__":
    main()
