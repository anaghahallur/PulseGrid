"""
Multi-Horizon Forecasting Models for ICU Bed Demand.
Implements:
1. Persistence Baseline (future = latest observation)
2. Seasonal-Naive Baseline (same hour on previous day)
3. Direct Ridge Regression (L2 regularized with scaling)
4. Direct HistGradientBoosting (fast non-linear ensemble)
5. Direct Random Forest (bagging tree ensemble)
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class BaseForecaster(ABC):
    """Abstract base class for 24-horizon ICU occupancy forecasters."""

    def __init__(self, name: str, horizons: int = 24):
        self.name = name
        self.horizons = horizons
        self.is_fitted = False

    @abstractmethod
    def fit(self, X: pd.DataFrame, Y: pd.DataFrame) -> "BaseForecaster":
        """Fits the multi-horizon forecaster on training features and targets."""
        pass

    @abstractmethod
    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        """Internal predict returning raw 2D array of shape (N, horizons)."""
        pass

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Generates predictions for all 24 horizons.
        Guarantees non-negative physical bed counts without clipping to capacity.
        """
        raw = self._raw_predict(X)
        # Verify finite numbers
        if not np.all(np.isfinite(raw)):
            raw = np.nan_to_num(raw, nan=0.0, posinf=100.0, neginf=0.0)
        # Occupancy cannot be negative beds (clip at 0.0, do not clip at top capacity)
        return np.clip(raw, 0.0, None)


class PersistenceBaseline(BaseForecaster):
    """
    Persistence Baseline:
    Forecasts all future horizons h in {1..24} using the latest observed occupancy at origin t.
    y_{t+h} = y_t
    """

    def __init__(self, horizons: int = 24):
        super().__init__(name="Persistence Baseline", horizons=horizons)
        self.is_fitted = True

    def fit(self, X: pd.DataFrame, Y: pd.DataFrame) -> "PersistenceBaseline":
        self.is_fitted = True
        return self

    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        # Retrieve current occupancy column (occ_current or lag_0)
        if "occ_current" in X.columns:
            latest = X["occ_current"].to_numpy(dtype=float)
        elif "lag_0" in X.columns:
            latest = X["lag_0"].to_numpy(dtype=float)
        else:
            # Fall back to first column
            latest = X.iloc[:, 0].to_numpy(dtype=float)

        # Tile across all 24 horizons
        return np.tile(latest[:, np.newaxis], (1, self.horizons))


class SeasonalNaiveBaseline(BaseForecaster):
    """
    Daily Seasonal-Naive Baseline:
    Uses the observation from the corresponding hour on the previous day.
    For horizon h in {1..24}: target hour is t + h.
    Same hour yesterday is (t + h) - 24 = t - (24 - h).
    Available at origin t as lag_{24 - h}.
    """

    def __init__(self, horizons: int = 24):
        super().__init__(name="Seasonal-Naive Baseline (24h)", horizons=horizons)
        self.is_fitted = True

    def fit(self, X: pd.DataFrame, Y: pd.DataFrame) -> "SeasonalNaiveBaseline":
        self.is_fitted = True
        return self

    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        N = len(X)
        preds = np.zeros((N, self.horizons), dtype=float)

        for h in range(1, self.horizons + 1):
            col_idx = h - 1
            lag_needed = 24 - h
            col_name = "occ_current" if lag_needed == 0 else f"lag_{lag_needed}"

            if col_name in X.columns:
                preds[:, col_idx] = X[col_name].to_numpy(dtype=float)
            elif "lag_24" in X.columns:
                # Fallback to lag_24
                preds[:, col_idx] = X["lag_24"].to_numpy(dtype=float)
            elif "occ_current" in X.columns:
                preds[:, col_idx] = X["occ_current"].to_numpy(dtype=float)
            else:
                preds[:, col_idx] = X.iloc[:, 0].to_numpy(dtype=float)

        return preds


class DirectRidgeForecaster(BaseForecaster):
    """
    Direct Multi-Horizon Ridge Regression.
    Trains independent regularized linear estimators for each horizon h in {1..24}
    with standardized numeric features.
    """

    def __init__(self, alpha: float = 10.0, horizons: int = 24, random_state: int = 42):
        super().__init__(name="Direct Ridge Regression", horizons=horizons)
        self.alpha = alpha
        self.random_state = random_state

        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=self.alpha, random_state=self.random_state)),
        ])
        self.model = MultiOutputRegressor(pipeline, n_jobs=-1)

    def fit(self, X: pd.DataFrame, Y: pd.DataFrame) -> "DirectRidgeForecaster":
        self.model.fit(X, Y)
        self.is_fitted = True
        return self

    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)


class DirectHistGradientBoostingForecaster(BaseForecaster):
    """
    Direct Multi-Horizon Histogram Gradient Boosting Regressor.
    Fast non-linear tree ensemble capturing non-linear interactions
    between diurnal cycles, day of week, capacity, and rolling dynamics.
    """

    def __init__(
        self,
        max_iter: int = 100,
        max_depth: int = 6,
        min_samples_leaf: int = 20,
        l2_regularization: float = 1.0,
        horizons: int = 24,
        random_state: int = 42,
    ):
        super().__init__(name="Direct HistGradientBoosting", horizons=horizons)
        self.max_iter = max_iter
        self.max_depth = max_depth
        self.min_samples_leaf = min_samples_leaf
        self.l2_regularization = l2_regularization
        self.random_state = random_state

        base_estimator = HistGradientBoostingRegressor(
            max_iter=self.max_iter,
            max_depth=self.max_depth,
            min_samples_leaf=self.min_samples_leaf,
            l2_regularization=self.l2_regularization,
            random_state=self.random_state,
        )
        self.model = MultiOutputRegressor(base_estimator, n_jobs=-1)

    def fit(self, X: pd.DataFrame, Y: pd.DataFrame) -> "DirectHistGradientBoostingForecaster":
        self.model.fit(X, Y)
        self.is_fitted = True
        return self

    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)


class DirectRandomForestForecaster(BaseForecaster):
    """
    Direct Multi-Horizon Random Forest Regressor.
    Ensemble of bagging decision trees.
    """

    def __init__(
        self,
        n_estimators: int = 100,
        max_depth: Optional[int] = 10,
        horizons: int = 24,
        random_state: int = 42,
    ):
        super().__init__(name="Direct Random Forest", horizons=horizons)
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.random_state = random_state

        base_estimator = RandomForestRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=self.random_state,
            n_jobs=-1,
        )
        self.model = MultiOutputRegressor(base_estimator, n_jobs=-1)

    def fit(self, X: pd.DataFrame, Y: pd.DataFrame) -> "DirectRandomForestForecaster":
        self.model.fit(X, Y)
        self.is_fitted = True
        return self

    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)
