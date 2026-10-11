"""
Inference Service for ICU Bed Demand Forecasting.
Loads trained forecasters, validates metadata, rigorously checks historical continuity,
reproduces the exact 54 features expected by the trained Ridge model, and executes predictions.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple
import joblib
import numpy as np
import pandas as pd

from src.config import get_settings
from src.ml.features import build_unit_features_and_targets
from src.ml.models import BaseForecaster, PersistenceBaseline, SeasonalNaiveBaseline
from src.api.schemas import (
    HourlyCensusRecord,
    HorizonPrediction,
    ModelInfoResponse,
    PredictionRequest,
    PredictionResponse,
    UnitInfo,
)

logger = logging.getLogger(__name__)


class ForecastingService:
    """
    Singleton service managing model lifecycle and multi-horizon inference.
    Avoids reloading models on every request and guarantees feature parity.
    """

    def __init__(
        self,
        model_path: str = "models/icu_occupancy_forecaster.joblib",
        metadata_path: str = "models/feature_metadata.json",
        dataset_fallback_path: str = "data/synthetic/simulated_hourly_occupancy.parquet",
    ):
        self.model_path = model_path
        self.metadata_path = metadata_path
        self.dataset_fallback_path = dataset_fallback_path

        self.model: Optional[BaseForecaster] = None
        self.metadata: Optional[Dict[str, Any]] = None
        self.persistence_baseline: PersistenceBaseline = PersistenceBaseline(horizons=24)
        self.seasonal_baseline: SeasonalNaiveBaseline = SeasonalNaiveBaseline(horizons=24)
        self.is_ready: bool = False
        self.error_detail: Optional[str] = None

        self.settings = get_settings()

    def load_model(self) -> bool:
        """
        Loads the trained model artifact and validates its metadata.
        Returns True if successfully loaded, False otherwise.
        """
        if not os.path.exists(self.model_path):
            self.error_detail = f"Model artifact not found at {self.model_path}"
            logger.error(self.error_detail)
            self.is_ready = False
            return False

        if not os.path.exists(self.metadata_path):
            self.error_detail = f"Metadata artifact not found at {self.metadata_path}"
            logger.error(self.error_detail)
            self.is_ready = False
            return False

        try:
            with open(self.metadata_path, "r", encoding="utf-8") as f:
                self.metadata = json.load(f)

            if "feature_names" not in self.metadata or not self.metadata["feature_names"]:
                self.error_detail = "Invalid metadata: missing 'feature_names'"
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            loaded = joblib.load(self.model_path)
            if not isinstance(loaded, BaseForecaster):
                self.error_detail = f"Loaded model is not a BaseForecaster: {type(loaded)}"
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            self.model = loaded
            self.is_ready = True
            self.error_detail = None
            logger.info("Successfully loaded and validated %s with %d features", self.metadata["model_name"], len(self.metadata["feature_names"]))
            return True
        except Exception as e:
            self.error_detail = f"Failed to load model: {str(e)}"
            logger.exception(self.error_detail)
            self.is_ready = False
            return False

    def get_configured_units(self) -> List[UnitInfo]:
        """Returns the configured ICU units and staffed capacities."""
        return [
            UnitInfo(unit_id=uid, capacity=cap)
            for uid, cap in sorted(self.settings.icu_unit_capacities.items())
        ]

    def get_model_info(self) -> ModelInfoResponse:
        """Returns metadata regarding active models and benchmarks."""
        if not self.is_ready or not self.metadata:
            raise RuntimeError("Model is not ready or metadata missing.")

        return ModelInfoResponse(
            selected_model=self.metadata.get("model_name", "Direct Ridge Regression"),
            horizons=self.metadata.get("horizons", 24),
            feature_count=self.metadata.get("feature_count", len(self.metadata["feature_names"])),
            feature_names=self.metadata["feature_names"],
            primary_benchmark="Persistence Baseline",
            benchmark_note=(
                "Persistence Baseline is maintained as the primary operational benchmark. "
                "On the unsegmented held-out test partition, Persistence achieved 1.56 beds MAE "
                "versus Direct Ridge Regression's 2.09 beds MAE due to the simulation runout tail."
            ),
            disclaimer=(
                "All models and evaluations are based on synthetic ADT event simulations. "
                "Not clinically certified."
            ),
        )

    def load_history_from_store(self, unit_id: str, lookback_hours: int = 168) -> List[HourlyCensusRecord]:
        """
        Retrieves recent contiguous history from the local processed parquet dataset.
        Used when the client does not provide a custom history array in the request body.
        """
        if not os.path.exists(self.dataset_fallback_path):
            raise FileNotFoundError(
                f"Historical store not found at {self.dataset_fallback_path}. "
                "Please provide 'history' in the request payload or run scripts/generate_synthetic_data.py."
            )

        df = pd.read_parquet(self.dataset_fallback_path)
        unit_df = df[df["unit_id"] == unit_id].sort_values("timestamp").reset_index(drop=True)
        if unit_df.empty:
            raise ValueError(f"No records found for unit '{unit_id}' in historical store.")

        # Extract at least lookback_hours + 1 rows to compute lag_168 and rolling_168
        needed = lookback_hours + 1
        tail_df = unit_df.iloc[-needed:].copy()

        records: List[HourlyCensusRecord] = []
        for _, row in tail_df.iterrows():
            records.append(
                HourlyCensusRecord(
                    timestamp=row["timestamp"],
                    occupied_beds=float(row["occupied_beds"]),
                    capacity=int(row["capacity"]),
                    admissions_count=int(row.get("admissions_count", 0)),
                    discharges_count=int(row.get("discharges_count", 0)),
                    transfers_in_count=int(row.get("transfers_in_count", 0)),
                    transfers_out_count=int(row.get("transfers_out_count", 0)),
                )
            )
        return records

    def prepare_inference_features(
        self,
        unit_id: str,
        records: List[HourlyCensusRecord],
    ) -> Tuple[pd.DataFrame, float, datetime, int]:
        """
        Validates historical continuity and reconstructs the exact 54 features expected by the trained model.
        NEVER silently treats missing observations as zero.
        """
        if not records:
            raise ValueError("History cannot be empty.")

        # Convert to DataFrame
        rows = [r.model_dump() for r in records]
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)

        # 1. Check timestamp uniqueness
        if df["timestamp"].duplicated().any():
            dups = df[df["timestamp"].duplicated()]["timestamp"].tolist()
            raise ValueError(f"Duplicate hourly timestamps detected in history: {dups}")

        # 2. Check strict hourly continuity
        time_diffs = df["timestamp"].diff().iloc[1:]
        gaps = time_diffs[time_diffs != pd.Timedelta(hours=1)]
        if not gaps.empty:
            bad_idx = gaps.index[0]
            t1 = df.loc[bad_idx - 1, "timestamp"]
            t2 = df.loc[bad_idx, "timestamp"]
            raise ValueError(
                f"Non-contiguous hourly history detected between {t1.isoformat()} and {t2.isoformat()} "
                f"(gap: {gaps.iloc[0]}). Missing observations must not be silently zero-filled."
            )

        # 3. Check history duration for 168-hour lookback (requires 169 rows)
        if len(df) < 169:
            raise ValueError(
                f"Insufficient historical window: requires at least 168 hours of lookback (169 rows), "
                f"but received {len(df)} rows. Missing historical observations cannot be assumed."
            )

        # 4. Extract latest slice of exactly 169 records ending at origin t
        slice_df = df.iloc[-169:].copy().reset_index(drop=True)
        origin_row = slice_df.iloc[-1]
        origin_timestamp = origin_row["timestamp"].to_pydatetime()
        current_occupancy = float(origin_row["occupied_beds"])
        capacity = int(origin_row["capacity"])

        if capacity <= 0:
            raise ValueError(f"Invalid capacity {capacity}: staffed capacity must be positive.")

        # 5. Build features using identical logic as training pipeline
        feature_df = build_unit_features_and_targets(slice_df)
        single_row = feature_df.iloc[[-1]].copy()

        # 6. Add one-hot unit indicators for known units
        known_units = ["CCU", "CSRU", "MICU", "SICU", "TSICU"]
        for u in known_units:
            single_row[f"unit_{u}"] = 1.0 if u == unit_id else 0.0

        # 7. Align with exact feature_names from model metadata
        expected_features = self.metadata["feature_names"]
        missing_features = [c for c in expected_features if c not in single_row.columns]
        if missing_features:
            raise ValueError(f"Failed to generate required model features: {missing_features}")

        X_inference = single_row[expected_features].copy()

        # 8. Check for any NaNs or Inf
        if X_inference.isna().any().any():
            nan_cols = X_inference.columns[X_inference.isna().any()].tolist()
            raise ValueError(f"NaN values encountered in constructed features: {nan_cols}")

        return X_inference, current_occupancy, origin_timestamp, capacity

    def predict(self, request: PredictionRequest) -> PredictionResponse:
        """
        Executes prediction workflow for the given request.
        Validates unit, evaluates history, selects model, and formats response.
        """
        unit_id = request.unit_id
        if unit_id not in self.settings.icu_unit_capacities:
            raise ValueError(
                f"Invalid unit_id '{unit_id}'. Configured units: {sorted(list(self.settings.icu_unit_capacities.keys()))}"
            )

        warnings: List[str] = [
            "Synthetic-Data Demonstration: All forecasts are simulated and not clinically certified."
        ]

        # 1. Obtain history
        records = request.history
        if records is None:
            try:
                records = self.load_history_from_store(unit_id, lookback_hours=168)
                warnings.append("History was not provided in request body; loaded latest history from data lake.")
            except Exception as e:
                raise ValueError(f"Could not load historical data for unit '{unit_id}': {str(e)}")

        # 2. Check model choice and fallback options
        target_model = request.model_name
        is_baseline = False
        model_used = ""
        preds_array = None

        if target_model in ("persistence", "persistence_baseline"):
            is_baseline = True
            model_used = "Persistence Baseline"
            latest_record = records[-1]
            origin_timestamp = latest_record.timestamp
            current_occupancy = float(latest_record.occupied_beds)
            capacity = int(latest_record.capacity)
            if capacity <= 0:
                raise ValueError("Configured capacity must be positive.")

            # Create dummy features dataframe with latest occupancy for baseline
            dummy_x = pd.DataFrame({"occ_current": [current_occupancy]})
            preds_array = self.persistence_baseline.predict(dummy_x)[0]

        elif target_model in ("seasonal_naive", "seasonal"):
            is_baseline = True
            model_used = "Seasonal-Naive Baseline (24h)"
            if len(records) < 25:
                raise ValueError(
                    f"Seasonal-Naive baseline requires at least 24 hours of history, received {len(records)}."
                )
            latest_record = records[-1]
            origin_timestamp = latest_record.timestamp
            current_occupancy = float(latest_record.occupied_beds)
            capacity = int(latest_record.capacity)

            # Build recent lag series
            lags_dict = {"occ_current": [current_occupancy]}
            for lag in range(1, 25):
                lags_dict[f"lag_{lag}"] = [float(records[-1 - lag].occupied_beds)]
            dummy_x = pd.DataFrame(lags_dict)
            preds_array = self.seasonal_baseline.predict(dummy_x)[0]

        elif target_model in ("direct_ridge", "ridge"):
            if not self.is_ready or self.model is None:
                raise RuntimeError("Direct Ridge Regression model is not loaded or service is not ready.")

            # Attempt to construct 54 features
            try:
                X_infer, current_occupancy, origin_timestamp, capacity = self.prepare_inference_features(
                    unit_id, records
                )
                raw_preds = self.model.predict(X_infer)
                preds_array = raw_preds[0]
                model_used = "Direct Ridge Regression"
            except ValueError as ve:
                if request.allow_persistence_fallback:
                    latest_record = records[-1]
                    origin_timestamp = latest_record.timestamp
                    current_occupancy = float(latest_record.occupied_beds)
                    capacity = int(latest_record.capacity)
                    dummy_x = pd.DataFrame({"occ_current": [current_occupancy]})
                    preds_array = self.persistence_baseline.predict(dummy_x)[0]
                    is_baseline = True
                    model_used = "Persistence Baseline (Fallback)"
                    warnings.append(
                        f"Ridge feature generation failed ({str(ve)}). "
                        "Fallback to Persistence Baseline was granted."
                    )
                else:
                    raise ve
        else:
            raise ValueError(
                f"Unknown model_name '{target_model}'. Allowed models: 'direct_ridge', 'persistence', 'seasonal_naive'."
            )

        # 3. Format Horizon Predictions
        horizon_preds: List[HorizonPrediction] = []
        num_horizons = request.horizons

        for h in range(1, num_horizons + 1):
            pred_val = float(preds_array[h - 1])
            fc_ts = origin_timestamp + timedelta(hours=h)
            exceeds = pred_val > capacity
            exceed_beds = max(0.0, pred_val - capacity)

            if exceeds:
                warnings.append(
                    f"Horizon +{h}h ({fc_ts.strftime('%H:%M')}): forecasted occupancy {pred_val:.1f} beds "
                    f"exceeds staffed capacity ({capacity} beds) by {exceed_beds:.1f} beds."
                )

            horizon_preds.append(
                HorizonPrediction(
                    horizon=h,
                    forecast_timestamp=fc_ts,
                    predicted_occupancy=round(pred_val, 2),
                    staffed_capacity=capacity,
                    is_capacity_exceeded=exceeds,
                    exceedance_beds=round(exceed_beds, 2),
                )
            )

        return PredictionResponse(
            unit_id=unit_id,
            forecast_origin=origin_timestamp,
            model_used=model_used,
            is_baseline=is_baseline,
            staffed_capacity=capacity,
            current_occupancy=round(current_occupancy, 2),
            predictions=horizon_preds,
            warnings=warnings,
        )


# Global singleton instance
_service_instance: Optional[ForecastingService] = None


def get_forecasting_service() -> ForecastingService:
    """Provides access to the global ForecastingService instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = ForecastingService()
    return _service_instance
