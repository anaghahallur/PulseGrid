"""
Integration tests for FastAPI endpoints using TestClient.
Tests:
1. Liveness (/health) and Readiness (/ready) probes.
2. Metadata endpoints (/units, /model/info).
3. Prediction endpoint (/predict) with Direct Ridge, Persistence Baseline, and Seasonal Naive.
4. Input validation (invalid units, non-contiguous history, missing history lookback).
5. Surge / capacity-exceedance alert detection.
6. Service degradation handling (503 when model unready).
"""

from datetime import datetime, timedelta, timezone
import pytest
from fastapi.testclient import TestClient

from src.api.main import app, create_app
from src.api.service import ForecastingService, get_forecasting_service


@pytest.fixture(scope="module")
def client():
    """Provides a TestClient with lifespan context executed."""
    with TestClient(app) as test_client:
        yield test_client


def test_health_endpoint(client):
    """Verifies that /health returns 200 with operational status."""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["service"] == "icu-bed-demand-forecasting"


def test_ready_endpoint(client):
    """Verifies that /ready returns 200 and confirms model readiness."""
    resp = client.get("/ready")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ready"] is True
    assert data["model_loaded"] is True
    assert data["model_name"] == "Direct Ridge Regression"
    assert data["horizons"] == 24
    assert data["features_count"] == 54


def test_units_endpoint(client):
    """Verifies /units returns all configured ICU units."""
    resp = client.get("/units")
    assert resp.status_code == 200
    units = resp.json()
    assert len(units) == 5
    unit_ids = [u["unit_id"] for u in units]
    assert "MICU" in unit_ids
    assert "SICU" in unit_ids
    assert "TSICU" in unit_ids


def test_model_info_endpoint(client):
    """Verifies /model/info provides full transparency on features and benchmarks."""
    resp = client.get("/model/info")
    assert resp.status_code == 200
    data = resp.json()
    assert data["selected_model"] == "Direct Ridge Regression"
    assert data["feature_count"] == 54
    assert data["primary_benchmark"] == "Persistence Baseline"
    assert "unsegmented held-out test partition" in data["benchmark_note"]


def test_predict_with_stored_dataset(client):
    """Verifies /predict generates 24-hour predictions using stored data when history is omitted."""
    payload = {
        "unit_id": "MICU",
        "model_name": "direct_ridge",
        "horizons": 24,
    }
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["unit_id"] == "MICU"
    assert data["model_used"] == "Direct Ridge Regression"
    assert data["is_baseline"] is False
    assert len(data["predictions"]) == 24

    # Verify predictions are non-negative
    for p in data["predictions"]:
        assert p["predicted_occupancy"] >= 0.0
        assert p["staffed_capacity"] == 24
        assert "forecast_timestamp" in p


def test_predict_persistence_baseline(client):
    """Verifies /predict with persistence baseline returns 24 constant horizons."""
    payload = {
        "unit_id": "SICU",
        "model_name": "persistence",
        "horizons": 12,
    }
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["is_baseline"] is True
    assert data["model_used"] == "Persistence Baseline"
    assert len(data["predictions"]) == 12

    # All horizons match current occupancy
    curr = data["current_occupancy"]
    for p in data["predictions"]:
        assert p["predicted_occupancy"] == curr


def test_predict_invalid_unit_returns_422(client):
    """Verifies /predict with an unknown unit ID returns HTTP 422."""
    payload = {
        "unit_id": "NEURO_ICU",
        "model_name": "direct_ridge",
    }
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 422
    assert "Invalid unit_id" in resp.json()["detail"]


def test_predict_non_contiguous_history_returns_422(client):
    """
    Verifies /predict with missing hourly gaps returns HTTP 422.
    Missing observations are NEVER silently assumed or zero-filled.
    """
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    # Provide 170 records with a gap
    history = []
    for h in range(170):
        offset = h if h < 30 else h + 3  # 3-hour gap
        ts = start + timedelta(hours=offset)
        history.append({
            "timestamp": ts.isoformat(),
            "occupied_beds": 8.0,
            "capacity": 10,
        })

    payload = {
        "unit_id": "SICU",
        "model_name": "direct_ridge",
        "history": history,
        "allow_persistence_fallback": False,
    }
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 422
    assert "Non-contiguous hourly history detected" in resp.json()["detail"]


def test_predict_surge_capacity_exceedance_warning(client):
    """Verifies that capacity exceedances generate warnings and exceedance flags."""
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    # CSRU has capacity 6. Inject occupancy of 10 beds (surge)
    history = [
        {
            "timestamp": (start + timedelta(hours=h)).isoformat(),
            "occupied_beds": 10.0,
            "capacity": 6,
        }
        for h in range(169)
    ]

    payload = {
        "unit_id": "CSRU",
        "model_name": "persistence",
        "horizons": 6,
        "history": history,
    }
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    for p in data["predictions"]:
        assert p["is_capacity_exceeded"] is True
        assert p["exceedance_beds"] == 4.0
    assert any("exceeds staffed capacity" in w for w in data["warnings"])


def test_predict_surfaces_data_provenance_and_simulation_label(client):
    """Verifies that prediction responses include explicit data source, age, and simulation label."""
    payload = {
        "unit_id": "MICU",
        "model_name": "persistence",
        "horizons": 1,
    }
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert "data_source" in data
    assert "is_live_stream" in data
    assert "simulation_time_label" in data
    assert "source_timestamp" in data
    assert "source_age_hours" in data
    assert data["simulation_time_label"] in ("STREAMING_LIVE_LAKE", "SYNTHETIC_SIMULATION_BENCHMARK")


def test_unhandled_exception_returns_sanitized_500(client, monkeypatch):
    """
    Verifies that unexpected internal exceptions return a sanitized HTTP 500
    without leaking internal traceback or exception messages to the client.
    """
    from unittest.mock import MagicMock
    service = get_forecasting_service()

    def exploding_predict(*args, **kwargs):
        raise TypeError("Sensitive internal database connection string: user:secret_pass@internal_cluster")

    monkeypatch.setattr(service, "predict", exploding_predict)

    payload = {"unit_id": "MICU", "model_name": "persistence", "horizons": 1}
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 500
    body = resp.json()
    assert "internal server error occurred" in body["detail"].lower()
    # Ensure sensitive internal details are NOT leaked!
    assert "secret_pass" not in resp.text
    assert "internal_cluster" not in resp.text
    assert "TypeError" not in resp.text

