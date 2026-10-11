"""
Unit tests for Streamlit Dashboard components and offline fallback resilience.
Tests:
1. API reachability check and graceful offline status.
2. Local fallback forecast generation when API is unreachable.
3. Recent history loading from local data store.
"""

from unittest.mock import patch
import pytest
import requests

from src.dashboard.app import (
    check_api_health,
    fetch_prediction_local_fallback,
    load_recent_history,
)


def test_dashboard_api_health_check_handles_downtime():
    """Verifies that API downtime is caught gracefully without throwing exceptions."""
    with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection refused")):
        ok, msg = check_api_health()
        assert ok is False
        assert "API Offline" in msg


def test_dashboard_api_health_check_success():
    """Verifies successful API health response parsing."""
    class MockResp:
        status_code = 200
        def json(self):
            return {"status": "ok"}

    with patch("requests.get", return_value=MockResp()):
        ok, msg = check_api_health()
        assert ok is True
        assert "API Connected" in msg


def test_dashboard_local_fallback_forecast_generation():
    """Verifies dashboard can generate forecasts internally when API is offline."""
    result = fetch_prediction_local_fallback(
        unit_id="MICU",
        model_name="persistence",
        horizons=12,
    )
    assert result is not None
    assert result["unit_id"] == "MICU"
    assert result["is_baseline"] is True
    assert len(result["predictions"]) == 12


def test_dashboard_load_recent_history():
    """Verifies loading recent historical census slices for dashboard visual context."""
    df_hist = load_recent_history("SICU", hours=48)
    assert not df_hist.empty
    assert len(df_hist) <= 48
    assert "occupied_beds" in df_hist.columns
    assert "timestamp" in df_hist.columns
