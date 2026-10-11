"""
Unit tests for FastAPI schemas, input validators, and ForecastingService feature reconstruction.
Validates:
1. Pydantic schema constraints (bounds, uppercase normalization).
2. Physical constraints (non-negative beds, positive capacity).
3. Strict history continuity (rejection of timestamp gaps and non-silent handling).
4. Insufficient history lookback rejection for Direct Ridge.
5. Graceful fallback to Persistence Baseline when permitted.
6. Model-loading failure handling and readiness signaling.
"""

from datetime import datetime, timedelta, timezone
import pytest
from pydantic import ValidationError

from src.api.schemas import (
    HourlyCensusRecord,
    PredictionRequest,
    PredictionResponse,
    HealthResponse,
    ReadinessResponse,
)
from src.api.service import ForecastingService


def test_hourly_census_record_validation():
    """Verifies valid hourly record parsing and timezone normalization."""
    now = datetime(2026, 10, 1, 12, 0, 0)
    record = HourlyCensusRecord(
        timestamp=now,
        occupied_beds=8.0,
        capacity=12,
        admissions_count=2,
        discharges_count=1,
    )
    assert record.occupied_beds == 8.0
    assert record.capacity == 12
    assert record.timestamp.tzinfo == timezone.utc


def test_hourly_census_negative_beds_rejected():
    """Ensures occupied beds cannot be negative."""
    with pytest.raises(ValidationError):
        HourlyCensusRecord(
            timestamp=datetime.now(timezone.utc),
            occupied_beds=-1.0,
            capacity=12,
        )


def test_hourly_census_zero_or_negative_capacity_rejected():
    """Ensures staffed capacity must be strictly positive."""
    with pytest.raises(ValidationError):
        HourlyCensusRecord(
            timestamp=datetime.now(timezone.utc),
            occupied_beds=5.0,
            capacity=0,
        )


def test_prediction_request_normalizes_case():
    """Verifies unit_id is uppercase and model_name is lowercase."""
    req = PredictionRequest(unit_id="micu", model_name="DIRECT_RIDGE", horizons=12)
    assert req.unit_id == "MICU"
    assert req.model_name == "direct_ridge"
    assert req.horizons == 12


def test_prediction_request_invalid_horizons_rejected():
    """Verifies horizons must be within [1, 24]."""
    with pytest.raises(ValidationError):
        PredictionRequest(unit_id="MICU", horizons=0)

    with pytest.raises(ValidationError):
        PredictionRequest(unit_id="MICU", horizons=25)


def test_service_detects_non_contiguous_history():
    """
    Verifies that timestamp gaps are detected and rejected.
    Missing observations MUST NOT be silently treated as zero.
    """
    service = ForecastingService()
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    records = []
    for h in range(170):
        # Inject a 2-hour gap at hour 50
        offset = h if h < 50 else h + 2
        ts = start + timedelta(hours=offset)
        records.append(
            HourlyCensusRecord(timestamp=ts, occupied_beds=5.0, capacity=10)
        )

    with pytest.raises(ValueError, match="Non-contiguous hourly history detected"):
        service.prepare_inference_features("MICU", records)


def test_service_detects_insufficient_lookback_history():
    """Verifies that fewer than 169 records raises ValueError for Ridge."""
    service = ForecastingService()
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    # Only 50 hours provided (requires 168h lookback = 169 rows)
    records = [
        HourlyCensusRecord(timestamp=start + timedelta(hours=h), occupied_beds=5.0, capacity=10)
        for h in range(50)
    ]

    with pytest.raises(ValueError, match="Insufficient historical window"):
        service.prepare_inference_features("MICU", records)


def test_service_detects_duplicate_timestamps():
    """Verifies duplicate timestamps in history are rejected."""
    service = ForecastingService()
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    records = [
        HourlyCensusRecord(timestamp=start + timedelta(hours=h), occupied_beds=5.0, capacity=10)
        for h in range(168)
    ]
    # Add duplicate of the last timestamp
    records.append(HourlyCensusRecord(timestamp=records[-1].timestamp, occupied_beds=5.0, capacity=10))

    with pytest.raises(ValueError, match="Duplicate hourly timestamps detected"):
        service.prepare_inference_features("MICU", records)


def test_persistence_fallback_when_history_is_insufficient():
    """Verifies that allow_persistence_fallback=True successfully falls back to persistence."""
    service = ForecastingService()
    service.load_model()
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    # Only 10 hours provided
    short_records = [
        HourlyCensusRecord(timestamp=start + timedelta(hours=h), occupied_beds=7.0, capacity=12)
        for h in range(10)
    ]

    req = PredictionRequest(
        unit_id="MICU",
        model_name="direct_ridge",
        horizons=12,
        history=short_records,
        allow_persistence_fallback=True,
    )
    res = service.predict(req)
    assert res.is_baseline is True
    assert "Persistence Baseline (Fallback)" in res.model_used
    assert len(res.predictions) == 12
    # Persistence carries forward current occupancy
    for p in res.predictions:
        assert p.predicted_occupancy == 7.0
    assert any("Fallback to Persistence" in w for w in res.warnings)


def test_model_loading_failure_handling():
    """Verifies that non-existent model files gracefully mark service as not ready."""
    broken_service = ForecastingService(model_path="non_existent_model.joblib")
    loaded = broken_service.load_model()
    assert loaded is False
    assert broken_service.is_ready is False
    assert "not found" in broken_service.error_detail
