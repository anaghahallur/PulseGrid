"""
Evaluation Metrics and Error Analysis for ICU Occupancy Forecasting.
Computes MAE, RMSE, and WAPE (with explicit zero-denominator handling)
overall, per-unit, per-horizon, and across surge/capacity-exceedance segments.
"""

from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd


def calculate_mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Error."""
    y_t = np.asarray(y_true, dtype=float)
    y_p = np.asarray(y_pred, dtype=float)
    return float(np.mean(np.abs(y_t - y_p)))


def calculate_rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Root Mean Squared Error."""
    y_t = np.asarray(y_true, dtype=float)
    y_p = np.asarray(y_pred, dtype=float)
    return float(np.sqrt(np.mean((y_t - y_p) ** 2)))


def calculate_wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Weighted Absolute Percentage Error:
    WAPE = sum(|y_true - y_pred|) / sum(y_true)

    Denominator Semantics:
    - If sum(y_true) > 0: returns sum(abs_error) / sum(y_true).
    - If sum(y_true) == 0 and sum(abs_error) == 0: returns 0.0 (perfect agreement on zero volume).
    - If sum(y_true) == 0 and sum(abs_error) > 0: returns np.nan (undefined percentage error).
    """
    y_t = np.asarray(y_true, dtype=float)
    y_p = np.asarray(y_pred, dtype=float)
    abs_errors = np.sum(np.abs(y_t - y_p))
    total_actual = np.sum(y_t)

    if total_actual == 0.0:
        if abs_errors == 0.0:
            return 0.0
        return float(np.nan)

    return float(abs_errors / total_actual)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Computes MAE, RMSE, and WAPE dictionary for matching truth and predictions."""
    return {
        "mae": round(calculate_mae(y_true, y_pred), 4),
        "rmse": round(calculate_rmse(y_true, y_pred), 4),
        "wape": round(calculate_wape(y_true, y_pred), 4),
    }


def evaluate_forecasts(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    metadata_df: pd.DataFrame,
    horizons: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Comprehensive evaluation of 24-horizon forecast matrix:
    - Overall aggregate metrics across all units and horizons.
    - Metrics partitioned by forecast horizon (h=1..24).
    - Metrics partitioned by ICU unit identifier.
    - Sliced metrics for surge / capacity exceedance periods.

    :param y_true: Ground truth 2D array of shape (N, H).
    :param y_pred: Model prediction 2D array of shape (N, H).
    :param metadata_df: Metadata DataFrame of length N containing 'unit_id', 'capacity', 'occupied_beds'.
    :param horizons: List of horizon integers (defaults to 1..H).
    :return: Nested dictionary of evaluation metrics.
    """
    y_t = np.asarray(y_true, dtype=float)
    y_p = np.asarray(y_pred, dtype=float)
    num_samples, num_horizons = y_t.shape
    h_list = horizons or list(range(1, num_horizons + 1))

    # 1. Overall Metrics
    overall = compute_metrics(y_t, y_p)

    # 2. Per-Horizon Breakdown
    by_horizon = {}
    for idx, h in enumerate(h_list):
        h_true = y_t[:, idx]
        h_pred = y_p[:, idx]
        by_horizon[f"h{h}"] = compute_metrics(h_true, h_pred)

    # 3. Per-Unit Breakdown
    by_unit = {}
    units = metadata_df["unit_id"].unique()
    for unit in sorted(units):
        mask = (metadata_df["unit_id"] == unit).to_numpy()
        if np.any(mask):
            unit_true = y_t[mask]
            unit_pred = y_p[mask]
            by_unit[unit] = compute_metrics(unit_true, unit_pred)

    # 4. Surge / Capacity Exceedance Error Analysis
    # A sample is flagged as 'surge' if current occupied beds exceed unit capacity
    surge_mask = np.zeros(num_samples, dtype=bool)
    if "is_capacity_exceeded" in metadata_df.columns:
        surge_mask = metadata_df["is_capacity_exceeded"].astype(bool).to_numpy()
    elif "occupied_beds" in metadata_df.columns and "capacity" in metadata_df.columns:
        surge_mask = (metadata_df["occupied_beds"] > metadata_df["capacity"]).to_numpy()

    surge_metrics = None
    normal_metrics = None

    if np.any(surge_mask):
        surge_metrics = compute_metrics(y_t[surge_mask], y_p[surge_mask])
        surge_metrics["sample_count"] = int(np.sum(surge_mask))

    non_surge_mask = ~surge_mask
    if np.any(non_surge_mask):
        normal_metrics = compute_metrics(y_t[non_surge_mask], y_p[non_surge_mask])
        normal_metrics["sample_count"] = int(np.sum(non_surge_mask))

    return {
        "overall": overall,
        "by_horizon": by_horizon,
        "by_unit": by_unit,
        "error_analysis": {
            "surge_periods": surge_metrics,
            "normal_periods": normal_metrics,
            "total_surge_samples": int(np.sum(surge_mask)),
        },
    }
