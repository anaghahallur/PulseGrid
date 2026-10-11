"""
End-to-End Training, Evaluation, and Serialization Pipeline for ICU Occupancy Forecasting.
Enforces chronological split boundaries with 24-hour target buffer gaps to eliminate leakage,
compares candidate models against baselines, and persists reproducible artifacts.
"""

from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Dict, List, Optional, Tuple
import joblib
import numpy as np
import pandas as pd
import sklearn

from src.ml.features import prepare_forecasting_dataset
from src.ml.metrics import evaluate_forecasts
from src.ml.models import (
    BaseForecaster,
    DirectHistGradientBoostingForecaster,
    DirectRandomForestForecaster,
    DirectRidgeForecaster,
    PersistenceBaseline,
    SeasonalNaiveBaseline,
)


def chronological_split(
    X: pd.DataFrame,
    Y: pd.DataFrame,
    metadata_df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    buffer_hours: int = 24,
) -> Dict[str, Any]:
    """
    Splits features and targets strictly chronologically by forecast origin timestamp.
    Applies a buffer_hours gap prior to validation and test boundaries so that multi-step
    future targets y_{t+h} (h in 1..24) do not overlap with subsequent evaluation periods.

    :param X: Feature DataFrame.
    :param Y: Multi-horizon target DataFrame.
    :param metadata_df: Metadata containing 'timestamp' and 'unit_id'.
    :param train_ratio: Fraction of timeline allocated to training (default: 0.70).
    :param val_ratio: Fraction of timeline allocated to validation (default: 0.15).
    :param buffer_hours: Gap in hours to eliminate boundary target overlap (default: 24).
    :return: Dictionary containing train, val, and test partitions along with split boundaries.
    """
    timestamps = pd.to_datetime(metadata_df["timestamp"], utc=True)
    unique_ts = sorted(timestamps.unique())
    num_ts = len(unique_ts)

    idx_val_start = int(num_ts * train_ratio)
    idx_test_start = int(num_ts * (train_ratio + val_ratio))

    ts_val_start = unique_ts[idx_val_start]
    ts_test_start = unique_ts[idx_test_start]

    # Buffer: training origins must finish buffer_hours prior to validation start
    ts_train_cutoff = ts_val_start - pd.Timedelta(hours=buffer_hours)
    # Buffer: validation origins must finish buffer_hours prior to test start
    ts_val_cutoff = ts_test_start - pd.Timedelta(hours=buffer_hours)

    train_mask = (timestamps <= ts_train_cutoff).to_numpy()
    val_mask = ((timestamps >= ts_val_start) & (timestamps <= ts_val_cutoff)).to_numpy()
    test_mask = (timestamps >= ts_test_start).to_numpy()

    boundaries = {
        "train_start": str(unique_ts[0]),
        "train_end": str(ts_train_cutoff),
        "val_start": str(ts_val_start),
        "val_end": str(ts_val_cutoff),
        "test_start": str(ts_test_start),
        "test_end": str(unique_ts[-1]),
        "target_buffer_hours": buffer_hours,
        "sample_counts": {
            "train": int(np.sum(train_mask)),
            "val": int(np.sum(val_mask)),
            "test": int(np.sum(test_mask)),
        },
    }

    return {
        "train": (X[train_mask].copy(), Y[train_mask].copy(), metadata_df[train_mask].copy()),
        "val": (X[val_mask].copy(), Y[val_mask].copy(), metadata_df[val_mask].copy()),
        "test": (X[test_mask].copy(), Y[test_mask].copy(), metadata_df[test_mask].copy()),
        "boundaries": boundaries,
    }


def run_training_and_evaluation(
    df: pd.DataFrame,
    random_seed: int = 42,
    include_rf: bool = False,
) -> Dict[str, Any]:
    """
    Executes the complete chronological training and evaluation pipeline:
    1. Prepares leakage-safe features and multi-horizon targets.
    2. Chronologically splits data into Train, Validation, and Test sets.
    3. Fits baselines (Persistence, Seasonal-Naive) and candidate models (Ridge, HistGradientBoosting).
    4. Evaluates all models on the Validation set.
    5. Selects the optimal model based strictly on validation MAE.
    6. Evaluates the selected model and baselines on the held-out Test set.

    :param df: Input DataFrame with historical hourly unit occupancy.
    :param random_seed: Seed for reproducible training.
    :param include_rf: If True, also fits and evaluates Random Forest (slower).
    :return: Dictionary containing fitted models, evaluation reports, and metadata.
    """
    # 1. Feature Engineering
    X, Y, metadata_df, drop_stats = prepare_forecasting_dataset(df)

    # 2. Chronological Split
    splits = chronological_split(X, Y, metadata_df)
    X_train, Y_train, meta_train = splits["train"]
    X_val, Y_val, meta_val = splits["val"]
    X_test, Y_test, meta_test = splits["test"]

    # 3. Model Suite Initialization
    models: List[BaseForecaster] = [
        PersistenceBaseline(horizons=24),
        SeasonalNaiveBaseline(horizons=24),
        DirectRidgeForecaster(alpha=10.0, horizons=24, random_state=random_seed),
        DirectHistGradientBoostingForecaster(
            max_iter=100, max_depth=6, min_samples_leaf=20, horizons=24, random_state=random_seed
        ),
    ]

    if include_rf:
        models.append(
            DirectRandomForestForecaster(n_estimators=50, max_depth=10, horizons=24, random_state=random_seed)
        )

    # 4. Training & Validation
    val_results = {}
    fitted_models: Dict[str, BaseForecaster] = {}

    for model in models:
        # Fit on train partition only
        model.fit(X_train, Y_train)
        fitted_models[model.name] = model

        # Predict on validation partition
        y_val_pred = model.predict(X_val)
        val_eval = evaluate_forecasts(Y_val.to_numpy(), y_val_pred, meta_val)
        val_results[model.name] = {
            "overall": val_eval["overall"],
            "by_horizon": val_eval["by_horizon"],
            "by_unit": val_eval["by_unit"],
            "error_analysis": val_eval["error_analysis"],
        }

    # 5. Model Selection (Based strictly on validation MAE)
    # Filter candidates (excluding baselines)
    candidates = [
        m for m in models
        if m.name not in ("Persistence Baseline", "Seasonal-Naive Baseline (24h)")
    ]
    best_candidate = min(candidates, key=lambda m: val_results[m.name]["overall"]["mae"])

    # 6. Final Evaluation on Held-out Test Set
    test_results = {}
    for model_name, model in fitted_models.items():
        y_test_pred = model.predict(X_test)
        test_eval = evaluate_forecasts(Y_test.to_numpy(), y_test_pred, meta_test)
        test_results[model_name] = {
            "overall": test_eval["overall"],
            "by_horizon": test_eval["by_horizon"],
            "by_unit": test_eval["by_unit"],
            "error_analysis": test_eval["error_analysis"],
        }

    return {
        "selected_model": best_candidate,
        "selected_model_name": best_candidate.name,
        "fitted_models": fitted_models,
        "validation_results": val_results,
        "test_results": test_results,
        "boundaries": splits["boundaries"],
        "drop_stats": drop_stats,
        "feature_names": X.columns.tolist(),
        "random_seed": random_seed,
        "environment": {
            "scikit_learn_version": sklearn.__version__,
            "joblib_version": joblib.__version__,
            "numpy_version": np.__version__,
            "pandas_version": pd.__version__,
        },
    }


def save_pipeline_artifacts(
    pipeline_output: Dict[str, Any],
    output_dir: str = "models",
) -> Dict[str, str]:
    """
    Persists selected model and evaluation reports to disk:
    - Model artifact: joblib
    - Metadata and evaluation JSON
    - Formatted Markdown report

    :param pipeline_output: Output from run_training_and_evaluation.
    :param output_dir: Target directory (default: 'models').
    :return: Dictionary of saved file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    selected_model = pipeline_output["selected_model"]
    model_name = pipeline_output["selected_model_name"]

    model_path = os.path.join(output_dir, "icu_occupancy_forecaster.joblib")
    meta_path = os.path.join(output_dir, "feature_metadata.json")
    report_json_path = os.path.join(output_dir, "evaluation_report.json")
    report_md_path = os.path.join(output_dir, "EVALUATION_REPORT.md")

    # 1. Save Selected Fitted Model
    joblib.dump(selected_model, model_path)

    # 2. Save Feature & Model Metadata
    metadata_payload = {
        "model_name": model_name,
        "horizons": selected_model.horizons,
        "feature_count": len(pipeline_output["feature_names"]),
        "feature_names": pipeline_output["feature_names"],
        "split_boundaries": pipeline_output["boundaries"],
        "drop_stats": pipeline_output["drop_stats"],
        "random_seed": pipeline_output["random_seed"],
        "environment": pipeline_output["environment"],
        "security_note": "CAUTION: joblib artifacts must only be loaded from verified internal repositories.",
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata_payload, f, indent=2)

    # 3. Save Machine-Readable Evaluation Report
    report_payload = {
        "selected_model": model_name,
        "validation_results": pipeline_output["validation_results"],
        "test_results": pipeline_output["test_results"],
        "split_boundaries": pipeline_output["boundaries"],
    }
    with open(report_json_path, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2)

    # 4. Generate Human-Readable Markdown Report
    val_res = pipeline_output["validation_results"]
    test_res = pipeline_output["test_results"]
    best_test = test_res[model_name]

    md_lines = [
        "# ICU Occupancy 24-Hour Forecasting — Evaluation Report",
        "",
        f"**Selected Model:** {model_name} (selected based on lowest Validation MAE)",
        f"**Generated:** {datetime.now(timezone.utc).isoformat()}",
        f"**Random Seed:** {pipeline_output['random_seed']}",
        "",
        "## 1. Dataset & Chronological Split Boundaries",
        f"- **Train Period:** {pipeline_output['boundaries']['train_start']} to {pipeline_output['boundaries']['train_end']} ({pipeline_output['boundaries']['sample_counts']['train']:,} samples)",
        f"- **Validation Period:** {pipeline_output['boundaries']['val_start']} to {pipeline_output['boundaries']['val_end']} ({pipeline_output['boundaries']['sample_counts']['val']:,} samples)",
        f"- **Test Period (Untouched):** {pipeline_output['boundaries']['test_start']} to {pipeline_output['boundaries']['test_end']} ({pipeline_output['boundaries']['sample_counts']['test']:,} samples)",
        f"- **Target Buffer Gap:** {pipeline_output['boundaries']['target_buffer_hours']} hours between partitions (guarantees zero target leakage)",
        f"- **Features:** {len(pipeline_output['feature_names'])} past-only features (lags 0..24, 48, 72, 168; rolling 6, 24, 168; cyclical calendar)",
        "",
        "## 2. Model Comparison on Held-Out Test Set",
        "| Model | Test MAE | Test RMSE | Test WAPE | Validation MAE |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]

    for m_name in test_res.keys():
        t_m = test_res[m_name]["overall"]
        v_m = val_res[m_name]["overall"]
        sel_marker = " **(Selected)**" if m_name == model_name else ""
        md_lines.append(
            f"| {m_name}{sel_marker} | {t_m['mae']:.2f} beds | {t_m['rmse']:.2f} beds | {t_m['wape'] * 100:.1f}% | {v_m['mae']:.2f} beds |"
        )

    md_lines.extend([
        "",
        "## 3. Test Performance by Forecast Horizon (Selected Model)",
        "| Horizon | Forecast Lead | Test MAE | Test RMSE | Test WAPE |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ])

    for h in [1, 2, 3, 6, 12, 18, 24]:
        h_key = f"h{h}"
        if h_key in best_test["by_horizon"]:
            h_m = best_test["by_horizon"][h_key]
            md_lines.append(
                f"| {h_key} | +{h} hour(s) | {h_m['mae']:.2f} beds | {h_m['rmse']:.2f} beds | {h_m['wape'] * 100:.1f}% |"
            )

    md_lines.extend([
        "",
        "## 4. Test Performance by ICU Unit (Selected Model)",
        "| Unit ID | Staffed Capacity | Test MAE | Test RMSE | Test WAPE |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ])

    for u_name, u_m in sorted(best_test["by_unit"].items()):
        md_lines.append(
            f"| {u_name} | — | {u_m['mae']:.2f} beds | {u_m['rmse']:.2f} beds | {u_m['wape'] * 100:.1f}% |"
        )

    surge_test = best_test["error_analysis"]["surge_periods"]
    norm_test = best_test["error_analysis"]["normal_periods"]

    md_lines.extend([
        "",
        "## 5. Surge and Capacity Exceedance Error Analysis",
    ])

    if surge_test:
        md_lines.append(
            f"- **Surge / Exceedance Periods:** MAE = {surge_test['mae']:.2f} beds, RMSE = {surge_test['rmse']:.2f} beds, WAPE = {surge_test['wape'] * 100:.1f}% (Count: {surge_test['sample_count']})"
        )
    else:
        md_lines.append("- **Surge / Exceedance Periods:** No capacity exceedances occurred during the test window.")

    if norm_test:
        md_lines.append(
            f"- **Normal Operating Periods:** MAE = {norm_test['mae']:.2f} beds, RMSE = {norm_test['rmse']:.2f} beds, WAPE = {norm_test['wape'] * 100:.1f}% (Count: {norm_test['sample_count']})"
        )

    md_lines.extend([
        "",
        "## 6. Model Artifact Security Note",
        "> [!IMPORTANT]",
        "> Model artifacts serialized with `joblib` must only be loaded from trusted internal sources.",
        "> Deserialization of untrusted files poses arbitrary code execution risks.",
        "",
        "## 7. Limitations & Clinical Validity Disclaimer",
        "- Predictions are developed and validated using synthetic ADT event simulations.",
        "- This model establishes data engineering, feature pipeline, and multi-horizon forecasting validity.",
        "- It does NOT constitute a clinically validated medical device or guarantee real-world patient outcomes.",
    ])

    with open(report_md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    return {
        "model_path": model_path,
        "metadata_path": meta_path,
        "report_json_path": report_json_path,
        "report_md_path": report_md_path,
    }


def load_forecaster(model_path: str = "models/icu_occupancy_forecaster.joblib") -> BaseForecaster:
    """
    Safely loads a previously fitted BaseForecaster model from disk.

    SECURITY WARNING:
    Only load model files from trusted, internal paths.
    Never deserialize arbitrary untrusted pickle/joblib files.
    """
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Fitted model artifact not found at: {model_path}")
    model = joblib.load(model_path)
    if not isinstance(model, BaseForecaster):
        raise TypeError(f"Loaded object is not a BaseForecaster instance: {type(model)}")
    return model
