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
from src.storage.hdfs_client import HDFSClient
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
        Validates model/metadata compatibility, feature count, horizon count,
        sub-estimator dimensions, and artifact provenance at startup.
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

            # 1. Feature names validation
            if "feature_names" not in self.metadata or not self.metadata["feature_names"]:
                self.error_detail = "Invalid metadata: missing 'feature_names'"
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            expected_features = self.metadata["feature_names"]
            num_features = len(expected_features)

            # 2. Feature count consistency validation
            meta_feature_count = self.metadata.get("feature_count")
            if meta_feature_count is not None and meta_feature_count != num_features:
                self.error_detail = (
                    f"Metadata feature_count mismatch: 'feature_count' is {meta_feature_count} "
                    f"but {num_features} features are listed in 'feature_names'."
                )
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            # 3. Provenance and split validation
            if "split_boundaries" not in self.metadata and "random_seed" not in self.metadata:
                self.error_detail = "Metadata provenance verification failed: missing training split boundaries."
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            # 4. Deserialization of model artifact
            loaded = joblib.load(self.model_path)
            if not isinstance(loaded, BaseForecaster):
                self.error_detail = f"Loaded model is not a BaseForecaster: {type(loaded)}"
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            # 5. Horizon count compatibility validation
            meta_horizons = self.metadata.get("horizons", 24)
            if hasattr(loaded, "horizons") and loaded.horizons != meta_horizons:
                self.error_detail = (
                    f"Horizon count mismatch: model has {loaded.horizons} horizons, "
                    f"metadata specifies {meta_horizons}."
                )
                logger.error(self.error_detail)
                self.is_ready = False
                return False

            # 6. Sub-estimator feature dimension compatibility validation
            if hasattr(loaded, "models") and isinstance(loaded.models, dict):
                for h, sub_model in loaded.models.items():
                    if hasattr(sub_model, "n_features_in_") and sub_model.n_features_in_ != num_features:
                        self.error_detail = (
                            f"Model feature dimension mismatch: estimator for horizon {h} expects "
                            f"{sub_model.n_features_in_} features, but metadata has {num_features}."
                        )
                        logger.error(self.error_detail)
                        self.is_ready = False
                        return False

            self.model = loaded
            self.is_ready = True
            self.error_detail = None
            logger.info(
                "Successfully loaded and validated %s with %d features and %d horizons",
                self.metadata.get("model_name", "Forecaster"),
                num_features,
                meta_horizons,
            )
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

    def load_history_from_store(
        self, unit_id: str, lookback_hours: int = 168
    ) -> Tuple[List[HourlyCensusRecord], str, bool]:
        """
        Retrieves recent contiguous history.
        Prioritizes the live processed census lake in HDFS (/processed/icu_occupancy_hourly).
        Cleanly falls back to local canonical synthetic store when HDFS is unreachable or unpopulated.
        Returns: (records, data_source_label, is_live_stream)
        """
        hdfs_records: Optional[List[HourlyCensusRecord]] = None
        data_source = "LOCAL_SYNTHETIC_STORE"
        is_live = False

        # Attempt HDFS read first
        try:
            hdfs = HDFSClient(
                base_url=self.settings.hdfs_namenode_url,
                user=self.settings.hdfs_user,
                timeout=5,
            )
            if hdfs.is_healthy():
                processed_files = hdfs.find_files(self.settings.hdfs_processed_path, suffix=".parquet")
                valid_files = [f for f in processed_files if not os.path.basename(f).startswith(".")]
                if valid_files:
                    valid_files.sort()
                    latest_file = valid_files[-1]
                    logger.info("Attempting to load processed census from HDFS: %s", latest_file)
                    df_hdfs = hdfs.read_parquet(latest_file)
                    if isinstance(df_hdfs, pd.DataFrame) and not df_hdfs.empty and "unit_id" in df_hdfs.columns:
                        unit_df = df_hdfs[df_hdfs["unit_id"] == unit_id].sort_values("timestamp").reset_index(drop=True)
                        if len(unit_df) >= min(25, lookback_hours + 1):
                            needed = lookback_hours + 1
                            tail_df = unit_df.iloc[-needed:].copy()
                            records = []
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
                            hdfs_records = records
                            data_source = "HDFS_PROCESSED_LAKE"
                            is_live = True
                            logger.info("Successfully loaded %d records from HDFS for unit %s", len(records), unit_id)
        except Exception as e:
            logger.warning("Could not read from HDFS processed store (%s); falling back to local store.", str(e))

        if hdfs_records is not None:
            return hdfs_records, data_source, is_live

        # Fallback to local Parquet store
        if not os.path.exists(self.dataset_fallback_path):
            raise FileNotFoundError(
                f"Historical store not found at {self.dataset_fallback_path} and HDFS processed lake is unavailable. "
                "Please provide 'history' in request payload or run scripts/generate_synthetic_data.py."
            )

        df = pd.read_parquet(self.dataset_fallback_path)
        unit_df = df[df["unit_id"] == unit_id].sort_values("timestamp").reset_index(drop=True)
        if unit_df.empty:
            raise ValueError(f"No records found for unit '{unit_id}' in historical store.")

        needed = lookback_hours + 1
        tail_df = unit_df.iloc[-needed:].copy()

        records = []
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
        return records, "LOCAL_SYNTHETIC_STORE", False

    def validate_and_normalize_history(
        self,
        records: List[HourlyCensusRecord],
        min_required_hours: int,
        unit_id: str,
        scenario_capacity: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, datetime, float, int]:
        """
        Rigorously validates and normalizes historical observations across all forecasting paths.
        Enforces:
        1. Non-empty record list.
        2. Exact top-of-the-hour alignment (:00:00).
        3. Chronological sorting (ensuring origin is the newest record).
        4. Strict timestamp uniqueness (no duplicate hours).
        5. Exact hourly continuity (gap == 1 hour; never silently zero-filled).
        6. Minimum historical observation count for the chosen model.
        7. Physical capacity validation (scenario or server default, must be > 0).
        """
        if not records:
            raise ValueError("History cannot be empty.")

        # Convert to DataFrame
        rows = [r.model_dump() for r in records]
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # 1. Check exact top-of-hour alignment
        for ts in df["timestamp"]:
            if ts.minute != 0 or ts.second != 0 or ts.microsecond != 0:
                raise ValueError(
                    f"Off-hour timestamp detected: {ts.isoformat()}. "
                    "Historical census records must be aligned to the exact top of the hour (:00:00)."
                )

        # 2. Chronological sorting to ensure origin is always the newest record
        df = df.sort_values("timestamp").reset_index(drop=True)

        # 3. Check timestamp uniqueness
        if df["timestamp"].duplicated().any():
            dups = df[df["timestamp"].duplicated()]["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
            raise ValueError(f"Duplicate hourly timestamps detected in history: {dups}")

        # 4. Check strict hourly continuity
        if len(df) > 1:
            time_diffs = df["timestamp"].diff().iloc[1:]
            gaps = time_diffs[time_diffs != pd.Timedelta(hours=1)]
            if not gaps.empty:
                bad_idx = gaps.index[0]
                t1 = df.loc[bad_idx - 1, "timestamp"].isoformat()
                t2 = df.loc[bad_idx, "timestamp"].isoformat()
                raise ValueError(
                    f"Non-contiguous hourly history detected between {t1} and {t2} "
                    f"(gap: {gaps.iloc[0]}). Missing observations must not be silently zero-filled."
                )

        # 5. Check minimum history count
        if len(df) < min_required_hours:
            raise ValueError(
                f"Insufficient historical window: requires at least {min_required_hours} hourly observations, "
                f"but received {len(df)} rows. Missing historical observations cannot be assumed."
            )

        # 6. Extract newest valid record as origin t
        origin_row = df.iloc[-1]
        origin_timestamp = origin_row["timestamp"].to_pydatetime()
        current_occupancy = float(origin_row["occupied_beds"])

        # Determine authoritative capacity
        if scenario_capacity is not None:
            if scenario_capacity <= 0:
                raise ValueError(f"Invalid scenario_capacity {scenario_capacity}: must be positive.")
            capacity = int(scenario_capacity)
        elif "capacity" in origin_row and pd.notna(origin_row["capacity"]) and origin_row["capacity"] > 0:
            capacity = int(origin_row["capacity"])
        else:
            capacity = self.settings.icu_unit_capacities.get(unit_id, 20)

        if capacity <= 0:
            raise ValueError(f"Invalid capacity {capacity}: staffed capacity must be positive.")

        return df, origin_timestamp, current_occupancy, capacity

    def prepare_inference_features(
        self,
        unit_id: str,
        records: List[HourlyCensusRecord],
        scenario_capacity: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, float, datetime, int]:
        """
        Validates historical continuity and reconstructs the exact 54 features expected by the trained Ridge model.
        NEVER silently treats missing observations as zero.
        """
        df, origin_timestamp, current_occupancy, capacity = self.validate_and_normalize_history(
            records=records,
            min_required_hours=169,
            unit_id=unit_id,
            scenario_capacity=scenario_capacity,
        )

        # Extract latest slice of exactly 169 records ending at origin t
        slice_df = df.iloc[-169:].copy().reset_index(drop=True)
        slice_df["capacity"] = capacity

        # Build features using identical logic as training pipeline
        feature_df = build_unit_features_and_targets(slice_df)
        single_row = feature_df.iloc[[-1]].copy()

        # Add one-hot unit indicators for known units
        known_units = ["CCU", "CSRU", "MICU", "SICU", "TSICU"]
        for u in known_units:
            single_row[f"unit_{u}"] = 1.0 if u == unit_id else 0.0

        # Align with exact feature_names from model metadata
        expected_features = self.metadata["feature_names"]
        missing_features = [c for c in expected_features if c not in single_row.columns]
        if missing_features:
            raise ValueError(f"Failed to generate required model features: {missing_features}")

        X_inference = single_row[expected_features].copy()

        # Check for any NaNs or Inf
        if X_inference.isna().any().any():
            nan_cols = X_inference.columns[X_inference.isna().any()].tolist()
            raise ValueError(f"NaN values encountered in constructed features: {nan_cols}")

        return X_inference, current_occupancy, origin_timestamp, capacity

    def predict(self, request: PredictionRequest) -> PredictionResponse:
        """
        Executes prediction workflow for the given request.
        Validates unit, evaluates history, selects model, surfaces provenance, and formats response.
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
        data_source = "CLIENT_PAYLOAD"
        is_live_stream = False
        records = request.history

        if records is None:
            try:
                records, data_source, is_live_stream = self.load_history_from_store(unit_id, lookback_hours=168)
                if is_live_stream:
                    warnings.append("History refreshed live from HDFS processed census lake.")
                else:
                    warnings.append("History was loaded from local synthetic store (offline benchmark replay).")
            except Exception as e:
                raise ValueError(f"Could not load historical data for unit '{unit_id}': {str(e)}")

        target_model = request.model_name
        is_baseline = False
        model_used = ""
        preds_array = None

        if target_model in ("persistence", "persistence_baseline"):
            is_baseline = True
            model_used = "Persistence Baseline"
            _, origin_timestamp, current_occupancy, capacity = self.validate_and_normalize_history(
                records=records,
                min_required_hours=1,
                unit_id=unit_id,
                scenario_capacity=request.scenario_capacity,
            )
            dummy_x = pd.DataFrame({"occ_current": [current_occupancy]})
            preds_array = self.persistence_baseline.predict(dummy_x)[0]

        elif target_model in ("seasonal_naive", "seasonal"):
            is_baseline = True
            model_used = "Seasonal-Naive Baseline (24h)"
            norm_df, origin_timestamp, current_occupancy, capacity = self.validate_and_normalize_history(
                records=records,
                min_required_hours=25,
                unit_id=unit_id,
                scenario_capacity=request.scenario_capacity,
            )
            lags_dict = {"occ_current": [current_occupancy]}
            for lag in range(1, 25):
                lags_dict[f"lag_{lag}"] = [float(norm_df.iloc[-1 - lag]["occupied_beds"])]
            dummy_x = pd.DataFrame(lags_dict)
            preds_array = self.seasonal_baseline.predict(dummy_x)[0]

        elif target_model in ("direct_ridge", "ridge"):
            if not self.is_ready or self.model is None:
                raise RuntimeError("Direct Ridge Regression model is not loaded or service is not ready.")

            try:
                X_infer, current_occupancy, origin_timestamp, capacity = self.prepare_inference_features(
                    unit_id=unit_id,
                    records=records,
                    scenario_capacity=request.scenario_capacity,
                )
                raw_preds = self.model.predict(X_infer)
                preds_array = raw_preds[0]
                model_used = "Direct Ridge Regression"
            except ValueError as ve:
                if request.allow_persistence_fallback:
                    _, origin_timestamp, current_occupancy, capacity = self.validate_and_normalize_history(
                        records=records,
                        min_required_hours=1,
                        unit_id=unit_id,
                        scenario_capacity=request.scenario_capacity,
                    )
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

        # 3. Source Age and Simulation-Time Labeling
        now_utc = datetime.now(timezone.utc)
        source_age_hours = round(max(0.0, (now_utc - origin_timestamp).total_seconds() / 3600.0), 2)

        if is_live_stream:
            simulation_label = "STREAMING_LIVE_LAKE"
            if source_age_hours > 2.0:
                warnings.append(
                    f"STALE_STREAM_WARNING: Newest census event in HDFS is {source_age_hours:.1f} hours old."
                )
        else:
            simulation_label = "SYNTHETIC_SIMULATION_BENCHMARK"
            warnings.append(
                "HISTORICAL_REPLAY: Forecast origin is from synthetic benchmark simulation, not active streaming telemetry."
            )

        # 4. Format Horizon Predictions
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
            data_source=data_source,
            is_live_stream=is_live_stream,
            source_timestamp=origin_timestamp,
            source_age_hours=source_age_hours,
            simulation_time_label=simulation_label,
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
