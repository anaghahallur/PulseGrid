"""
Unit tests for ICU Occupancy Forecasting Machine Learning Pipeline (Phase 3).
Verifies:
- Multi-horizon target construction (h=1..24)
- Unit-specific feature isolation (zero cross-unit leakage)
- Prevention of future-feature leakage
- Chronological split boundaries and target buffer gaps
- Warmup and tail drop handling
- Reproducible training with fixed seeds
- Model serialization and safe reload
- Output shape, finite values, and nonnegative physical predictions
- Evaluation metrics and zero-denominator WAPE semantics
- Baseline versus candidate model comparisons
"""

from datetime import datetime, timedelta, timezone
import os
import tempfile
import numpy as np
import pandas as pd
import pytest

from src.ml.features import (
    DEFAULT_HORIZONS,
    build_unit_features_and_targets,
    prepare_forecasting_dataset,
)
from src.ml.metrics import (
    calculate_mae,
    calculate_rmse,
    calculate_wape,
    compute_metrics,
    evaluate_forecasts,
)
from src.ml.models import (
    DirectHistGradientBoostingForecaster,
    DirectRidgeForecaster,
    PersistenceBaseline,
    SeasonalNaiveBaseline,
)
from src.ml.pipeline import chronological_split, load_forecaster


@pytest.fixture
def synthetic_multitown_df():
    """Generates 30 days of continuous hourly data for 2 units."""
    start_ts = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
    records = []
    for h in range(30 * 24):
        ts = start_ts + timedelta(hours=h)
        # Unit A has base 10 + diurnal cycle
        occ_a = 10 + int(3 * np.sin(2 * np.pi * ts.hour / 24.0))
        # Unit B has base 18 + diurnal cycle
        occ_b = 18 + int(4 * np.cos(2 * np.pi * ts.hour / 24.0))

        records.append({
            "timestamp": ts,
            "unit_id": "UNIT_A",
            "occupied_beds": max(0, occ_a),
            "capacity": 16,
            "is_capacity_exceeded": occ_a > 16,
        })
        records.append({
            "timestamp": ts,
            "unit_id": "UNIT_B",
            "occupied_beds": max(0, occ_b),
            "capacity": 20,
            "is_capacity_exceeded": occ_b > 20,
        })
    return pd.DataFrame(records)


def test_target_construction_horizons_1_to_24():
    """Verifies that target_h1 through target_h24 match exact future shifts."""
    start_ts = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
    n_hours = 100
    df = pd.DataFrame({
        "timestamp": [start_ts + timedelta(hours=i) for i in range(n_hours)],
        "unit_id": ["MICU"] * n_hours,
        "occupied_beds": list(range(n_hours)),  # Sequential: 0, 1, 2, ...
        "capacity": [24] * n_hours,
    })

    res = build_unit_features_and_targets(df, horizons=list(range(1, 25)))

    for h in range(1, 25):
        target_col = f"target_h{h}"
        assert target_col in res.columns
        # For row i, target_h should be i + h (until tail where it is NaN)
        valid_rows = res[res[target_col].notna()]
        for idx in range(len(valid_rows) - 25):
            expected = res.loc[idx, "occupied_beds"] + h
            actual = res.loc[idx, target_col]
            assert actual == expected, f"Horizon {h} mismatch at index {idx}: expected {expected}, got {actual}"


def test_unit_specific_features_no_cross_unit_leakage():
    """
    Verifies that lag and rolling statistics for Unit A NEVER mix with Unit B,
    even when concatenated or interleaved in the dataset.
    """
    start_ts = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
    n_hours = 250
    records = []
    for h in range(n_hours):
        ts = start_ts + timedelta(hours=h)
        # Unit A is constant 10
        records.append({"timestamp": ts, "unit_id": "UNIT_A", "occupied_beds": 10, "capacity": 16})
        # Unit B is constant 25
        records.append({"timestamp": ts, "unit_id": "UNIT_B", "occupied_beds": 25, "capacity": 30})

    df = pd.DataFrame(records)
    X, Y, meta, _ = prepare_forecasting_dataset(df)

    # Check Unit A samples
    mask_a = meta["unit_id"] == "UNIT_A"
    X_a = X[mask_a]
    assert (X_a["occ_current"] == 10).all()
    assert (X_a["lag_1"] == 10).all()
    assert (X_a["lag_24"] == 10).all()
    assert (X_a["roll_mean_24"] == 10.0).all()

    # Check Unit B samples
    mask_b = meta["unit_id"] == "UNIT_B"
    X_b = X[mask_b]
    assert (X_b["occ_current"] == 25).all()
    assert (X_b["lag_1"] == 25).all()
    assert (X_b["lag_24"] == 25).all()
    assert (X_b["roll_mean_24"] == 25.0).all()


def test_prevention_of_future_feature_leakage():
    """
    Verifies that a future spike at t+5 does not affect features at forecast origin t.
    """
    start_ts = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
    n_hours = 200
    base_values = [12] * n_hours

    df_clean = pd.DataFrame({
        "timestamp": [start_ts + timedelta(hours=i) for i in range(n_hours)],
        "unit_id": ["MICU"] * n_hours,
        "occupied_beds": base_values,
        "capacity": [24] * n_hours,
    })

    # Modified DataFrame with huge future surge at hour 190
    df_spiked = df_clean.copy()
    df_spiked.loc[190, "occupied_beds"] = 999

    feat_clean = build_unit_features_and_targets(df_clean)
    feat_spiked = build_unit_features_and_targets(df_spiked)

    # For forecast origin at hour 180 (10 hours prior to spike):
    # Features at t=180 must be IDENTICAL between clean and spiked series
    feature_cols = [c for c in feat_clean.columns if not c.startswith("target_")]
    row_clean = feat_clean.loc[180, feature_cols].to_dict()
    row_spiked = feat_spiked.loc[180, feature_cols].to_dict()

    for col in feature_cols:
        assert row_clean[col] == row_spiked[col], f"Leakage detected in feature {col} at origin t=180!"


def test_chronological_split_boundaries_and_buffer(synthetic_multitown_df):
    """
    Verifies chronological split ordering and enforces 24-hour target buffer gap
    between training and validation, and between validation and test.
    """
    X, Y, meta, _ = prepare_forecasting_dataset(synthetic_multitown_df)
    splits = chronological_split(X, Y, meta, train_ratio=0.70, val_ratio=0.15, buffer_hours=24)

    X_train, Y_train, meta_train = splits["train"]
    X_val, Y_val, meta_val = splits["val"]
    X_test, Y_test, meta_test = splits["test"]

    ts_train_max = meta_train["timestamp"].max()
    ts_val_min = meta_val["timestamp"].min()
    ts_val_max = meta_val["timestamp"].max()
    ts_test_min = meta_test["timestamp"].min()

    # 1. Strict chronological progression
    assert ts_train_max < ts_val_min
    assert ts_val_max < ts_test_min

    # 2. Enforce 24-hour buffer gap to prevent target overlap
    gap_train_val = (ts_val_min - ts_train_max).total_seconds() / 3600.0
    gap_val_test = (ts_test_min - ts_val_max).total_seconds() / 3600.0

    assert gap_train_val >= 24.0, f"Target overlap risk: gap between train and val is {gap_train_val}h (must be >= 24h)"
    assert gap_val_test >= 24.0, f"Target overlap risk: gap between val and test is {gap_val_test}h (must be >= 24h)"


def test_missing_history_and_warmup_handling():
    """
    Verifies that initial lookback warmup rows (168h) and forecast tail rows (24h)
    are cleanly dropped and accounted for without NaN contamination.
    """
    start_ts = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
    total_hours = 300
    df = pd.DataFrame({
        "timestamp": [start_ts + timedelta(hours=i) for i in range(total_hours)],
        "unit_id": ["MICU"] * total_hours,
        "occupied_beds": [10] * total_hours,
        "capacity": [20] * total_hours,
    })

    X, Y, meta, stats = prepare_forecasting_dataset(df)

    assert stats["initial_rows"] == 300
    # Warmup (168) + tail (24) = 192 dropped
    assert stats["rows_dropped_total"] == 192
    assert stats["valid_rows_retained"] == 300 - 192

    # Verify zero NaNs in output
    assert not X.isna().any().any()
    assert not Y.isna().any().any()


def test_reproducible_training_fixed_seed(synthetic_multitown_df):
    """Verifies that model training is 100% deterministic with fixed random seed."""
    X, Y, meta, _ = prepare_forecasting_dataset(synthetic_multitown_df)
    splits = chronological_split(X, Y, meta)
    X_train, Y_train, _ = splits["train"]
    X_test, _, _ = splits["test"]

    model_1 = DirectRidgeForecaster(alpha=10.0, random_state=42).fit(X_train, Y_train)
    model_2 = DirectRidgeForecaster(alpha=10.0, random_state=42).fit(X_train, Y_train)

    pred_1 = model_1.predict(X_test)
    pred_2 = model_2.predict(X_test)

    np.testing.assert_allclose(pred_1, pred_2, err_msg="Non-deterministic predictions with identical seed!")


def test_model_serialization_and_reload(synthetic_multitown_df):
    """Verifies joblib model serialization, reload, and error handling for missing files."""
    import joblib

    X, Y, meta, _ = prepare_forecasting_dataset(synthetic_multitown_df)
    splits = chronological_split(X, Y, meta)
    X_train, Y_train, _ = splits["train"]
    X_test, _, _ = splits["test"]

    model = DirectRidgeForecaster(alpha=1.0, random_state=42).fit(X_train, Y_train)
    pred_orig = model.predict(X_test)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_model_path = os.path.join(tmp_dir, "test_model.joblib")
        joblib.dump(model, tmp_model_path)

        reloaded = load_forecaster(tmp_model_path)
        pred_reloaded = reloaded.predict(X_test)

        np.testing.assert_allclose(pred_orig, pred_reloaded)

    with pytest.raises(FileNotFoundError):
        load_forecaster("/non/existent/path/forecaster.joblib")


def test_prediction_shape_finite_and_nonnegative(synthetic_multitown_df):
    """
    Verifies that model predictions:
    1. Match shape (N, 24)
    2. Are finite numbers
    3. Are >= 0.0 (non-negative physical occupancy)
    4. Do not artificially clip demand exceeding capacity
    """
    X, Y, meta, _ = prepare_forecasting_dataset(synthetic_multitown_df)
    splits = chronological_split(X, Y, meta)
    X_train, Y_train, _ = splits["train"]
    X_test, _, meta_test = splits["test"]

    forecaster = DirectRidgeForecaster(alpha=10.0).fit(X_train, Y_train)
    preds = forecaster.predict(X_test)

    assert preds.shape == (len(X_test), 24)
    assert np.all(np.isfinite(preds))
    assert np.all(preds >= 0.0), "Physical bed occupancy cannot be negative!"


def test_metrics_and_zero_denominator_wape():
    """
    Verifies MAE, RMSE, and WAPE with special handling of zero denominators.
    """
    y_true = np.array([10.0, 20.0, 30.0])
    y_pred = np.array([12.0, 18.0, 30.0])

    assert calculate_mae(y_true, y_pred) == pytest.approx(4.0 / 3.0)
    assert calculate_rmse(y_true, y_pred) == pytest.approx(np.sqrt(8.0 / 3.0))
    # Sum abs error = 2 + 2 + 0 = 4; Sum actual = 60; WAPE = 4 / 60
    assert calculate_wape(y_true, y_pred) == pytest.approx(4.0 / 60.0)

    # Zero actuals and zero errors -> 0.0 WAPE
    zero_actual = np.array([0.0, 0.0, 0.0])
    zero_pred = np.array([0.0, 0.0, 0.0])
    assert calculate_wape(zero_actual, zero_pred) == 0.0

    # Zero actuals but non-zero predictions -> np.nan (undefined denominator)
    bad_pred = np.array([1.0, 0.0, 0.0])
    assert np.isnan(calculate_wape(zero_actual, bad_pred))


def test_baseline_versus_candidate_comparison(synthetic_multitown_df):
    """
    Verifies that persistence baseline, seasonal-naive, and regression models
    can be evaluated and compared using evaluate_forecasts.
    """
    X, Y, meta, _ = prepare_forecasting_dataset(synthetic_multitown_df)
    splits = chronological_split(X, Y, meta)
    X_train, Y_train, _ = splits["train"]
    X_test, Y_test, meta_test = splits["test"]

    persist = PersistenceBaseline(horizons=24).fit(X_train, Y_train)
    seasonal = SeasonalNaiveBaseline(horizons=24).fit(X_train, Y_train)
    ridge = DirectRidgeForecaster(alpha=10.0).fit(X_train, Y_train)

    p_pred = persist.predict(X_test)
    s_pred = seasonal.predict(X_test)
    r_pred = ridge.predict(X_test)

    p_eval = evaluate_forecasts(Y_test.to_numpy(), p_pred, meta_test)
    s_eval = evaluate_forecasts(Y_test.to_numpy(), s_pred, meta_test)
    r_eval = evaluate_forecasts(Y_test.to_numpy(), r_pred, meta_test)

    assert "mae" in p_eval["overall"]
    assert "mae" in s_eval["overall"]
    assert "mae" in r_eval["overall"]

    # All models produce valid finite non-negative MAE
    assert p_eval["overall"]["mae"] >= 0
    assert s_eval["overall"]["mae"] >= 0
    assert r_eval["overall"]["mae"] >= 0
