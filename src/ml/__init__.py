"""
Machine Learning Subpackage for ICU Occupancy Forecasting.
Exposes feature engineering, multi-horizon models, metrics, and training pipeline.
"""

from src.ml.features import (
    DEFAULT_HORIZONS,
    DEFAULT_LAGS,
    DEFAULT_ROLLING_WINDOWS,
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
    BaseForecaster,
    DirectHistGradientBoostingForecaster,
    DirectRandomForestForecaster,
    DirectRidgeForecaster,
    PersistenceBaseline,
    SeasonalNaiveBaseline,
)
from src.ml.pipeline import (
    chronological_split,
    load_forecaster,
    run_training_and_evaluation,
    save_pipeline_artifacts,
)

__all__ = [
    "DEFAULT_HORIZONS",
    "DEFAULT_LAGS",
    "DEFAULT_ROLLING_WINDOWS",
    "build_unit_features_and_targets",
    "prepare_forecasting_dataset",
    "calculate_mae",
    "calculate_rmse",
    "calculate_wape",
    "compute_metrics",
    "evaluate_forecasts",
    "BaseForecaster",
    "PersistenceBaseline",
    "SeasonalNaiveBaseline",
    "DirectRidgeForecaster",
    "DirectHistGradientBoostingForecaster",
    "DirectRandomForestForecaster",
    "chronological_split",
    "run_training_and_evaluation",
    "save_pipeline_artifacts",
    "load_forecaster",
]
