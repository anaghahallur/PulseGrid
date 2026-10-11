"""
Feature Engineering and Target Construction for ICU Occupancy Forecasting.
Constructs leakage-safe past-only lag, rolling, calendar, and unit features
for 1-to-24 hour ahead multi-horizon prediction, enforcing strict unit boundary isolation.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd


# Default lag offsets in hours (all hourly lags 0..24 for daily cycle, plus multi-day/weekly)
DEFAULT_LAGS = sorted(list(set(list(range(0, 25)) + [48, 72, 168])))
# Default rolling window lengths in hours
DEFAULT_ROLLING_WINDOWS = [6, 24, 168]
# Multi-horizon forecast range: 1 to 24 hours ahead
DEFAULT_HORIZONS = list(range(1, 25))


def build_unit_features_and_targets(
    unit_df: pd.DataFrame,
    horizons: List[int] = DEFAULT_HORIZONS,
    lags: List[int] = DEFAULT_LAGS,
    rolling_windows: List[int] = DEFAULT_ROLLING_WINDOWS,
) -> pd.DataFrame:
    """
    Constructs leakage-safe features and targets for a single ICU unit.
    All shifts and rolling windows operate strictly within this unit's timeline.

    :param unit_df: DataFrame containing at least ['timestamp', 'unit_id', 'occupied_beds', 'capacity'].
    :param horizons: List of future horizons in hours (e.g., 1..24).
    :param lags: List of past observation lag offsets in hours (0 = current observation at origin t).
    :param rolling_windows: List of past rolling aggregation window sizes in hours.
    :return: DataFrame containing feature columns, target columns (target_h), and index metadata.
    """
    df = unit_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)

    # 1. Past-only Lagged Occupancy
    for lag in lags:
        # lag=0 is current occupancy y_t at forecast origin t
        # lag=1 is y_{t-1}, lag=24 is y_{t-24}, etc.
        col_name = f"lag_{lag}" if lag > 0 else "occ_current"
        df[col_name] = df["occupied_beds"].shift(lag)

    # 2. Past-only Rolling Statistics (closed='right' on past values up to and including t)
    for window in rolling_windows:
        # Rolling over occupied_beds with min_periods=window ensures no NaN filling
        roll = df["occupied_beds"].rolling(window=window, min_periods=window)
        df[f"roll_mean_{window}"] = roll.mean()
        df[f"roll_std_{window}"] = roll.std()

    # 3. Calendar & Diurnal Features at Forecast Origin t
    hours = df["timestamp"].dt.hour
    dows = df["timestamp"].dt.dayofweek
    df["hour_sin"] = np.sin(2 * np.pi * hours / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hours / 24.0)
    df["dow_sin"] = np.sin(2 * np.pi * dows / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * dows / 7.0)
    df["is_weekend"] = dows.isin([5, 6]).astype(float)

    # 4. Flow aggregation over past 24h (if present in historical records)
    for flow_col in ["admissions_count", "discharges_count", "transfers_in_count", "transfers_out_count"]:
        if flow_col in df.columns:
            df[f"{flow_col}_past_24h"] = df[flow_col].rolling(window=24, min_periods=24).sum()

    # 5. Capacity and Utilization Context
    if "capacity" in df.columns:
        df["capacity_val"] = df["capacity"].astype(float)
        # Avoid division by zero
        cap = df["capacity_val"].replace(0, np.nan)
        df["occ_rate_current"] = df["occ_current"] / cap

    # 6. Future Multi-Horizon Targets: target_1 .. target_24
    for h in horizons:
        # shift(-h) aligns observation at t+h with row at forecast origin t
        df[f"target_h{h}"] = df["occupied_beds"].shift(-h)

    return df


def prepare_forecasting_dataset(
    df: pd.DataFrame,
    horizons: List[int] = DEFAULT_HORIZONS,
    lags: List[int] = DEFAULT_LAGS,
    rolling_windows: List[int] = DEFAULT_ROLLING_WINDOWS,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """
    Constructs full forecasting dataset across all ICU units while strictly isolating
    each unit's time series to eliminate cross-unit leakage.

    :param df: Input DataFrame with historical hourly occupancy across units.
    :param horizons: Multi-horizon forecast steps (default: 1..24).
    :param lags: Past observation lags (default: [0, 1, 2, 3, 6, 12, 24, 48, 72, 168]).
    :param rolling_windows: Past rolling window lengths (default: [6, 24, 168]).
    :return: Tuple of (X, Y, metadata_df, drop_stats)
             X: DataFrame of feature columns (with one-hot encoded unit IDs).
             Y: DataFrame of target columns (target_h1 .. target_h24).
             metadata_df: DataFrame with origin timestamps, unit_id, capacity, etc.
             drop_stats: Dictionary tracking dropped warmup and tail rows.
    """
    processed_units: List[pd.DataFrame] = []
    initial_total_rows = len(df)
    max_lookback = max(lags + rolling_windows)
    max_horizon = max(horizons)

    unit_groups = df.groupby("unit_id")
    for unit_id, unit_data in unit_groups:
        unit_features = build_unit_features_and_targets(
            unit_data,
            horizons=horizons,
            lags=lags,
            rolling_windows=rolling_windows,
        )
        processed_units.append(unit_features)

    combined_df = pd.concat(processed_units, ignore_index=True)

    # Identify feature and target column names
    target_cols = [f"target_h{h}" for h in horizons]
    meta_cols = [
        "timestamp", "unit_id", "occupied_beds", "capacity",
        "available_beds", "occupancy_rate", "is_capacity_exceeded",
        "exceedance_beds", "data_source", "processing_watermark"
    ]
    meta_cols = [c for c in meta_cols if c in combined_df.columns]

    feature_cols = [
        c for c in combined_df.columns
        if c not in target_cols and c not in meta_cols and pd.api.types.is_numeric_dtype(combined_df[c])
    ]

    # Drop initial warmup rows (where lookback features are NaN)
    # and tail rows (where future targets are NaN)
    valid_mask = combined_df[feature_cols + target_cols].notna().all(axis=1)
    clean_df = combined_df[valid_mask].copy()

    # Sort strictly by timestamp and unit_id
    clean_df = clean_df.sort_values(by=["timestamp", "unit_id"]).reset_index(drop=True)

    dropped_rows = initial_total_rows - len(clean_df)
    drop_stats = {
        "initial_rows": initial_total_rows,
        "valid_rows_retained": len(clean_df),
        "rows_dropped_total": dropped_rows,
        "lookback_warmup_hours_per_unit": max_lookback,
        "forecast_horizon_tail_hours_per_unit": max_horizon,
    }

    # Extract X, Y, and metadata
    X = clean_df[feature_cols].copy()
    Y = clean_df[target_cols].copy()
    metadata_df = clean_df[meta_cols].copy()

    # Add one-hot encoded unit indicators to X
    unit_dummies = pd.get_dummies(clean_df["unit_id"], prefix="unit", dtype=float)
    X = pd.concat([X, unit_dummies], axis=1)

    return X, Y, metadata_df, drop_stats
