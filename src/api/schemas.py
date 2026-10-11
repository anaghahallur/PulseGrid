"""
Pydantic Request and Response Schemas for the ICU Occupancy Forecasting API.
Enforces strict input validation on unit IDs, timestamps, history continuity,
and physical capacity bounds.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class HourlyCensusRecord(BaseModel):
    """Represents a single hourly census observation for an ICU unit."""

    timestamp: datetime = Field(..., description="Hourly observation timestamp (UTC preferred)")
    occupied_beds: float = Field(..., ge=0.0, description="Current number of occupied beds (>= 0)")
    capacity: int = Field(..., gt=0, description="Configured staffed bed capacity for the unit (> 0)")
    admissions_count: int = Field(default=0, ge=0, description="Admissions during this hourly interval")
    discharges_count: int = Field(default=0, ge=0, description="Discharges during this hourly interval")
    transfers_in_count: int = Field(default=0, ge=0, description="Transfers into unit during this interval")
    transfers_out_count: int = Field(default=0, ge=0, description="Transfers out of unit during this interval")

    @field_validator("timestamp")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class PredictionRequest(BaseModel):
    """Request payload for 1-to-24 hour ICU occupancy forecasts."""

    unit_id: str = Field(..., description="Target ICU unit identifier (e.g. MICU, SICU, CCU, CSRU, TSICU)")
    model_name: str = Field(
        default="direct_ridge",
        description="Forecasting model to invoke: 'direct_ridge', 'persistence', or 'seasonal_naive'",
    )
    horizons: int = Field(
        default=24,
        ge=1,
        le=24,
        description="Number of hourly forecast horizons requested (1 to 24)",
    )
    history: Optional[List[HourlyCensusRecord]] = Field(
        default=None,
        description=(
            "Historical contiguous hourly census observations up to forecast origin. "
            "Requires at least 168 hours (169 records) for direct_ridge. "
            "If omitted, the server will retrieve latest recorded history from the data lake."
        ),
    )
    allow_persistence_fallback: bool = Field(
        default=False,
        description="If True and history is insufficient for ridge regression, falls back to persistence baseline.",
    )

    @field_validator("unit_id")
    @classmethod
    def normalize_unit_id(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("model_name")
    @classmethod
    def normalize_model_name(cls, v: str) -> str:
        return v.strip().lower()


class HorizonPrediction(BaseModel):
    """Forecast result for a specific future hour."""

    horizon: int = Field(..., ge=1, le=24, description="Forecast step ahead in hours (1..24)")
    forecast_timestamp: datetime = Field(..., description="Predicted timestamp in UTC")
    predicted_occupancy: float = Field(..., ge=0.0, description="Forecasted bed occupancy")
    staffed_capacity: int = Field(..., gt=0, description="Unit staffed capacity at forecast time")
    is_capacity_exceeded: bool = Field(..., description="Whether predicted occupancy exceeds staffed capacity")
    exceedance_beds: float = Field(default=0.0, ge=0.0, description="Number of beds above capacity (if surge)")


class PredictionResponse(BaseModel):
    """Complete prediction response from the forecasting service."""

    unit_id: str = Field(..., description="ICU unit identifier")
    forecast_origin: datetime = Field(..., description="Origin timestamp from which predictions project")
    model_used: str = Field(..., description="Name of the model that generated the forecasts")
    is_baseline: bool = Field(..., description="True if a baseline model produced this forecast")
    staffed_capacity: int = Field(..., gt=0, description="Configured capacity of the unit")
    current_occupancy: float = Field(..., ge=0.0, description="Occupancy at forecast origin")
    predictions: List[HorizonPrediction] = Field(..., description="List of hourly horizon forecasts")
    warnings: List[str] = Field(default_factory=list, description="Operational notices and warnings")
    disclaimer: str = Field(
        default=(
            "All forecasts are generated from synthetic simulation data. "
            "This service demonstrates data pipeline and multi-horizon forecasting architecture "
            "and does not constitute a certified medical device."
        ),
        description="Clinical validity and academic usage disclaimer",
    )


class HealthResponse(BaseModel):
    """Service liveness probe response."""

    status: str = Field(default="ok")
    service: str = Field(default="icu-bed-demand-forecasting")
    version: str = Field(default="0.1.0")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ReadinessResponse(BaseModel):
    """Service readiness probe response."""

    ready: bool
    model_loaded: bool
    model_name: Optional[str] = None
    horizons: Optional[int] = None
    features_count: Optional[int] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    details: Optional[str] = None


class UnitInfo(BaseModel):
    """ICU unit capacity metadata."""

    unit_id: str
    capacity: int


class ModelInfoResponse(BaseModel):
    """Information regarding the active forecasting model and baselines."""

    selected_model: str
    horizons: int
    feature_count: int
    feature_names: List[str]
    primary_benchmark: str = "Persistence Baseline"
    benchmark_note: str
    disclaimer: str
