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


def test_service_rejects_off_hour_timestamps():
    """Verifies that records with minute/second offsets (off-hour) are rejected across models."""
    service = ForecastingService()
    start = datetime(2026, 10, 1, 12, 17, 30, tzinfo=timezone.utc)
    records = [
        HourlyCensusRecord(timestamp=start, occupied_beds=5.0, capacity=10)
    ]
    req = PredictionRequest(unit_id="MICU", model_name="persistence", history=records)
    with pytest.raises(ValueError, match="Off-hour timestamp detected"):
        service.predict(req)


def test_service_sorts_unsorted_records_ensuring_newest_origin():
    """Verifies that unsorted records are sorted chronologically and origin is newest record."""
    service = ForecastingService()
    t1 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 10, 1, 11, 0, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Provide records in scrambled order: t2, t3, t1
    scrambled = [
        HourlyCensusRecord(timestamp=t2, occupied_beds=6.0, capacity=10),
        HourlyCensusRecord(timestamp=t3, occupied_beds=9.0, capacity=10),
        HourlyCensusRecord(timestamp=t1, occupied_beds=4.0, capacity=10),
    ]

    req = PredictionRequest(unit_id="MICU", model_name="persistence", history=scrambled)
    res = service.predict(req)
    # Origin must be newest timestamp t3 with occupancy 9.0
    assert res.forecast_origin == t3
    assert res.current_occupancy == 9.0
    for p in res.predictions:
        assert p.predicted_occupancy == 9.0


def test_service_rejects_gap_and_duplicates_on_seasonal_naive():
    """Verifies that duplicate and gap validation applies to seasonal-naive baseline."""
    service = ForecastingService()
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    records = [
        HourlyCensusRecord(timestamp=start + timedelta(hours=h), occupied_beds=5.0, capacity=10)
        for h in range(25)
    ]
    # Inject duplicate
    records[10].timestamp = records[9].timestamp
    req_dup = PredictionRequest(unit_id="MICU", model_name="seasonal_naive", history=records)
    with pytest.raises(ValueError, match="Duplicate hourly timestamps detected"):
        service.predict(req_dup)


def test_service_rejects_short_history_on_seasonal_naive():
    """Verifies that fewer than 25 records is rejected for seasonal-naive."""
    service = ForecastingService()
    start = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    records = [
        HourlyCensusRecord(timestamp=start + timedelta(hours=h), occupied_beds=5.0, capacity=10)
        for h in range(20)
    ]
    req = PredictionRequest(unit_id="MICU", model_name="seasonal_naive", history=records)
    with pytest.raises(ValueError, match="Insufficient historical window"):
        service.predict(req)


def test_service_validates_scenario_capacity_override():
    """Verifies that explicit scenario_capacity overrides unit default and affects exceedance."""
    service = ForecastingService()
    start = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    records = [
        HourlyCensusRecord(timestamp=start, occupied_beds=15.0, capacity=20)
    ]
    # Unit MICU default capacity is 24, but scenario sets capacity to 10 (surge scenario)
    req = PredictionRequest(
        unit_id="MICU",
        model_name="persistence",
        history=records,
        scenario_capacity=10,
    )
    res = service.predict(req)
    assert res.staffed_capacity == 10
    assert res.predictions[0].is_capacity_exceeded is True
    assert res.predictions[0].exceedance_beds == 5.0


def test_service_rejects_invalid_scenario_capacity():
    """Verifies that non-positive scenario_capacity is rejected."""
    with pytest.raises(ValidationError):
        PredictionRequest(unit_id="MICU", scenario_capacity=0)


def test_service_rejects_metadata_mismatched_feature_count(tmp_path):
    """Verifies that load_model fails if feature_count does not match feature_names length."""
    import json
    import joblib
    from src.ml.models import PersistenceBaseline

    fake_model_path = tmp_path / "model.joblib"
    fake_meta_path = tmp_path / "metadata.json"

    dummy = PersistenceBaseline(horizons=24)
    joblib.dump(dummy, fake_model_path)

    bad_meta = {
        "model_name": "Direct Ridge Regression",
        "horizons": 24,
        "feature_count": 99,  # Mismatch with actual length of 2
        "feature_names": ["f1", "f2"],
        "random_seed": 42,
    }
    with open(fake_meta_path, "w") as f:
        json.dump(bad_meta, f)

    test_service = ForecastingService(model_path=str(fake_model_path), metadata_path=str(fake_meta_path))
    assert test_service.load_model() is False
    assert "feature_count mismatch" in test_service.error_detail


def test_service_rejects_metadata_mismatched_horizons(tmp_path):
    """Verifies that load_model fails if model horizons do not match metadata horizons."""
    import json
    import joblib
    from src.ml.models import PersistenceBaseline

    fake_model_path = tmp_path / "model.joblib"
    fake_meta_path = tmp_path / "metadata.json"

    dummy = PersistenceBaseline(horizons=12)  # Model has 12
    joblib.dump(dummy, fake_model_path)

    bad_meta = {
        "model_name": "Persistence",
        "horizons": 24,  # Metadata claims 24
        "feature_count": 1,
        "feature_names": ["occ_current"],
        "random_seed": 42,
    }
    with open(fake_meta_path, "w") as f:
        json.dump(bad_meta, f)

    test_service = ForecastingService(model_path=str(fake_model_path), metadata_path=str(fake_meta_path))
    assert test_service.load_model() is False
    assert "Horizon count mismatch" in test_service.error_detail


def test_clock_skew_tolerance_accepts_slight_future_origin(monkeypatch):
    """Verifies that an origin up to 5 minutes in the future is accepted within clock-skew tolerance."""
    service = ForecastingService()
    # Mock server clock to 19:58:00 UTC
    mock_now = datetime(2026, 10, 11, 19, 58, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(service, "get_current_time", lambda: mock_now)

    # Origin is at 20:00:00 UTC (2 minutes in future, within 5-min tolerance)
    origin_aligned = datetime(2026, 10, 11, 20, 0, 0, tzinfo=timezone.utc)
    records = [
        HourlyCensusRecord(timestamp=origin_aligned, occupied_beds=8.0, capacity=12)
    ]
    req = PredictionRequest(
        unit_id="MICU",
        model_name="persistence",
        history=records,
        reject_future_data=True,
    )
    res = service.predict(req)
    assert res.source_age_hours == 0.0
    assert not any("FUTURE_DATED" in w for w in res.warnings)


def test_future_dated_forecast_origin_rejected_when_configured():
    """Verifies that an origin beyond clock skew tolerance raises ValueError when reject_future_data is True."""
    service = ForecastingService()
    future_time = (datetime.now(timezone.utc) + timedelta(hours=5)).replace(minute=0, second=0, microsecond=0)
    records = [
        HourlyCensusRecord(timestamp=future_time, occupied_beds=8.0, capacity=12)
    ]
    req = PredictionRequest(
        unit_id="MICU",
        model_name="persistence",
        history=records,
        reject_future_data=True,
    )
    with pytest.raises(ValueError, match="Future-dated forecast origin"):
        service.predict(req)


def test_future_dated_forecast_origin_negative_age_unclamped_and_warns():
    """
    Verifies that when reject_future_data is False, future origin preserves negative source age
    (never silently clamped to 0.0) and emits a prominent FUTURE_DATED_DATA_WARNING.
    """
    service = ForecastingService()
    now = datetime.now(timezone.utc)
    future_time = (now + timedelta(hours=4)).replace(minute=0, second=0, microsecond=0)
    records = [
        HourlyCensusRecord(timestamp=future_time, occupied_beds=8.0, capacity=12)
    ]
    req = PredictionRequest(
        unit_id="MICU",
        model_name="persistence",
        history=records,
        reject_future_data=False,
    )
    res = service.predict(req)
    # source_age_hours must NOT be clamped to 0.0!
    assert res.source_age_hours is not None
    assert res.source_age_hours < 0.0
    assert any("FUTURE_DATED_DATA_WARNING" in w for w in res.warnings)
    # Future data cannot be marked as active live stream
    assert res.is_live_stream is False


def test_load_history_from_store_picks_newest_timestamp_over_alphabetical_filename(monkeypatch):
    """
    Verifies that load_history_from_store chooses the dataset with the newest data timestamps,
    even when alphabetical filename sorting disagrees (e.g., 'z_old.parquet' vs 'a_new.parquet').
    """
    import pandas as pd
    from unittest.mock import MagicMock

    # Create two DataFrames:
    # Older data with 'z' prefix (Oct 1..2)
    start_old = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    old_ts = [start_old + timedelta(hours=h) for h in range(30)]
    df_old = pd.DataFrame({
        "timestamp": old_ts,
        "unit_id": "MICU",
        "occupied_beds": 10.0,
        "capacity": 24,
    })

    # Newer data with 'a' prefix (Oct 10..11)
    start_new = datetime(2026, 10, 10, 0, 0, 0, tzinfo=timezone.utc)
    new_ts = [start_new + timedelta(hours=h) for h in range(30)]
    df_new = pd.DataFrame({
        "timestamp": new_ts,
        "unit_id": "MICU",
        "occupied_beds": 14.0,
        "capacity": 24,
    })

    mock_hdfs = MagicMock()
    mock_hdfs.is_healthy.return_value = True
    mock_hdfs.exists.return_value = False  # No manifest, force timestamp inspection
    mock_hdfs.find_files.return_value = ["/processed/z_old_run.parquet", "/processed/a_new_run.parquet"]

    def fake_read_parquet(path):
        if "z_old" in path:
            return df_old
        if "a_new" in path:
            return df_new
        raise FileNotFoundError(path)

    mock_hdfs.read_parquet.side_effect = fake_read_parquet

    service = ForecastingService()
    monkeypatch.setattr("src.api.service.HDFSClient", lambda **kwargs: mock_hdfs)

    records, data_source, is_live = service.load_history_from_store(unit_id="MICU", lookback_hours=24)
    assert data_source == "HDFS_PROCESSED_LAKE"
    # The origin record must be from df_new (Oct 11), NOT df_old (Oct 2)
    newest_ts = max(r.timestamp for r in records)
    assert newest_ts.day >= 10
    assert newest_ts.month == 10


def test_load_history_from_store_resolves_latest_manifest(monkeypatch):
    """Verifies that load_history_from_store respects latest_manifest.json when present."""
    import json
    import pandas as pd
    from unittest.mock import MagicMock

    start_ts = datetime(2026, 10, 5, 0, 0, 0, tzinfo=timezone.utc)
    ts_list = [start_ts + timedelta(hours=h) for h in range(30)]
    df_manifest_target = pd.DataFrame({
        "timestamp": ts_list,
        "unit_id": "SICU",
        "occupied_beds": 7.0,
        "capacity": 10,
    })

    manifest_payload = json.dumps({
        "latest_file": "/processed/icu_occupancy_hourly/manifest_selected.parquet",
        "max_timestamp": "2026-10-06T05:00:00Z",
    }).encode("utf-8")

    mock_hdfs = MagicMock()
    mock_hdfs.is_healthy.return_value = True

    def fake_exists(path):
        return True

    def fake_read_file(path):
        return manifest_payload

    def fake_read_parquet(path):
        return df_manifest_target

    mock_hdfs.exists.side_effect = fake_exists
    mock_hdfs.read_file.side_effect = fake_read_file
    mock_hdfs.read_parquet.side_effect = fake_read_parquet

    service = ForecastingService()
    monkeypatch.setattr("src.api.service.HDFSClient", lambda **kwargs: mock_hdfs)

    records, data_source, is_live = service.load_history_from_store(unit_id="SICU", lookback_hours=24)
    assert data_source == "HDFS_PROCESSED_LAKE"
    assert len(records) == 25
    assert records[-1].timestamp.day >= 5


def test_is_live_stream_decoupled_from_hdfs_storage(monkeypatch):
    """
    Verifies that a Parquet file existing in HDFS does NOT imply is_live_stream is True
    if the data is stale (> 2 hours old).
    """
    service = ForecastingService()
    now = datetime.now(timezone.utc)
    # Stale data: 10 hours old
    stale_ts = (now - timedelta(hours=10)).replace(minute=0, second=0, microsecond=0)
    stale_records = [
        HourlyCensusRecord(timestamp=stale_ts, occupied_beds=5.0, capacity=10)
    ]

    monkeypatch.setattr(
        service,
        "load_history_from_store",
        lambda unit_id, lookback_hours: (stale_records, "HDFS_PROCESSED_LAKE", True),
    )

    req = PredictionRequest(unit_id="MICU", model_name="persistence", history=None)
    res = service.predict(req)
    assert res.data_source == "HDFS_PROCESSED_LAKE"
    # Must NOT be marked live stream since 10h > 2h!
    assert res.is_live_stream is False
    assert res.source_age_hours is not None and res.source_age_hours >= 10.0
    assert any("STALE_STREAM_WARNING" in w for w in res.warnings)
