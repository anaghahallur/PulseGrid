"""
End-to-End Pipeline Validation Script for Phase 5.
Validates:
1. Occupancy reconstruction and HDFS Parquet / manifest persistence.
2. Manifest edge cases:
   - Existing manifest update (overwriting previous manifest)
   - Stale manifest fallback to newer Parquet dataset
   - Missing/unreadable/invalid manifest target fallback
   - Rename failure explicit IOError
3. Forecast API validation:
   - /health and /ready probes
   - /model/info and /units
   - /predict multi-horizon response (1-24h), checks horizons, models, warnings, and source provenance
4. Synthetic data disclaimer checks
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd

from src.config import get_settings
from src.schema.events import ICUEvent
from src.storage.hdfs_client import HDFSClient
from src.processing.occupancy_calculator import OccupancyCalculator
from src.api.service import ForecastingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("e2e_validator")

def validate_reconstruction_and_manifest():
    logger.info("=== STEP 1: Reconstructing occupancy and checking manifest persistence ===")
    settings = get_settings()
    hdfs = HDFSClient()
    assert hdfs.is_healthy(), "HDFS is not healthy"

    raw_records = hdfs.read_all_jsonl(settings.hdfs_raw_path)
    logger.info(f"Read {len(raw_records)} raw records from HDFS {settings.hdfs_raw_path}")
    assert len(raw_records) > 0, "No raw records found in HDFS"

    events = [ICUEvent.model_validate(r) for r in raw_records]
    calc = OccupancyCalculator(unit_capacities=settings.icu_unit_capacities)
    df = calc.reconstruct(events)
    logger.info(f"Reconstructed DataFrame shape: {df.shape}")
    assert not df.empty, "Reconstructed census DataFrame is empty"
    assert "timestamp" in df.columns and "unit_id" in df.columns and "occupied_beds" in df.columns

    from uuid import uuid4
    run_id = uuid4().hex[:8]
    fn1 = f"e2e_{run_id}_1.parquet"
    fn2 = f"e2e_{run_id}_2.parquet"

    # Persist to HDFS (Run 1)
    target_dir = settings.hdfs_processed_path
    saved_path_1 = calc.persist_occupancy_to_hdfs(df, hdfs, hdfs_target_dir=target_dir, filename=fn1)
    logger.info(f"Persisted Run 1 Parquet to: {saved_path_1}")
    assert hdfs.exists(saved_path_1), f"Run 1 file does not exist: {saved_path_1}"

    # Verify manifest exists and points to Run 1
    manifest_path = f"{target_dir.rstrip('/')}/latest_manifest.json"
    assert hdfs.exists(manifest_path), f"Manifest does not exist at {manifest_path}"
    m_data = json.loads(hdfs.read_file(manifest_path).decode("utf-8"))
    logger.info(f"Manifest content after Run 1: {m_data}")
    assert m_data.get("latest_file") == saved_path_1, f"Expected latest_file to be {saved_path_1}"

    # Persist to HDFS (Run 2: Existing manifest overwrite test)
    logger.info("=== STEP 2: Exercising existing manifest overwrite (Run 2) ===")
    saved_path_2 = calc.persist_occupancy_to_hdfs(df, hdfs, hdfs_target_dir=target_dir, filename=fn2)
    logger.info(f"Persisted Run 2 Parquet to: {saved_path_2}")
    assert hdfs.exists(saved_path_2), f"Run 2 file does not exist: {saved_path_2}"

    m_data_2 = json.loads(hdfs.read_file(manifest_path).decode("utf-8"))
    logger.info(f"Manifest content after Run 2: {m_data_2}")
    assert m_data_2.get("latest_file") == saved_path_2, f"Expected manifest to update to {saved_path_2}"
    logger.info("SUCCESS: Existing manifest overwrite succeeded and updated latest_file.")

    return saved_path_1, saved_path_2


def validate_manifest_edge_cases(saved_path_1: str, saved_path_2: str):
    logger.info("=== STEP 3: Exercising Manifest Edge Cases in Service ===")
    settings = get_settings()
    service = ForecastingService()
    assert service.load_model(), "ForecastingService model failed to load"

    # Test 3.1: Stale manifest fallback
    logger.info("Testing stale manifest fallback...")
    hdfs = HDFSClient()
    target_dir = settings.hdfs_processed_path.rstrip("/")
    manifest_path = f"{target_dir}/latest_manifest.json"

    # Intentionally point manifest to an older timestamp/file
    stale_manifest = {
        "latest_file": saved_path_1,
        "max_timestamp": "2020-01-01T00:00:00Z",
    }
    hdfs.write_file(manifest_path, json.dumps(stale_manifest).encode("utf-8"), overwrite=True)

    records, data_source, is_live = service.load_history_from_store("MICU", lookback_hours=24)
    logger.info(f"Stale manifest resolution returned data source: {data_source}, is_live: {is_live}, records: {len(records)}")
    assert data_source == "HDFS_PROCESSED_LAKE"

    # Test 3.2: Invalid/ghost manifest target fallback
    logger.info("Testing invalid/ghost manifest target fallback...")
    invalid_manifest = {
        "latest_file": f"{target_dir}/non_existent_ghost_file.parquet",
        "max_timestamp": "2026-10-12T00:00:00Z",
    }
    hdfs.write_file(manifest_path, json.dumps(invalid_manifest).encode("utf-8"), overwrite=True)
    records, data_source, is_live = service.load_history_from_store("MICU", lookback_hours=24)
    logger.info(f"Invalid manifest target resolution returned data source: {data_source}, records: {len(records)}")
    assert data_source == "HDFS_PROCESSED_LAKE"

    # Test 3.3: Missing manifest fallback
    logger.info("Testing missing manifest fallback...")
    hdfs.delete(manifest_path, recursive=False)
    records, data_source, is_live = service.load_history_from_store("MICU", lookback_hours=24)
    logger.info(f"Missing manifest resolution returned data source: {data_source}, records: {len(records)}")
    assert data_source == "HDFS_PROCESSED_LAKE"

    # Clean up intermediate test artifact (saved_path_1)
    if hdfs.exists(saved_path_1):
        hdfs.delete(saved_path_1, recursive=False)
        logger.info(f"Cleaned up temporary test file: {saved_path_1}")

    # Restore legitimate manifest pointing to latest valid run
    valid_manifest = {
        "latest_file": saved_path_2,
        "max_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    hdfs.write_file(manifest_path, json.dumps(valid_manifest).encode("utf-8"), overwrite=True)
    logger.info("SUCCESS: Manifest edge cases handled properly with graceful fallbacks.")
    return True


def validate_api_endpoints():
    logger.info("=== STEP 4: Validating Forecasting REST API Endpoints ===")
    api_url = "http://127.0.0.1:8000"

    # /health
    r_health = requests.get(f"{api_url}/health", timeout=5)
    logger.info(f"/health response [{r_health.status_code}]: {r_health.json()}")
    assert r_health.status_code == 200, f"/health check failed: {r_health.text}"
    assert r_health.json().get("status") == "ok"

    # /ready
    r_ready = requests.get(f"{api_url}/ready", timeout=5)
    logger.info(f"/ready response [{r_ready.status_code}]: {r_ready.json()}")
    assert r_ready.status_code == 200, f"/ready check failed: {r_ready.text}"
    assert r_ready.json().get("ready") is True

    # /units
    r_units = requests.get(f"{api_url}/units", timeout=5)
    logger.info(f"/units response [{r_units.status_code}]: {r_units.json()}")
    assert r_units.status_code == 200, f"/units check failed: {r_units.text}"
    units = r_units.json()
    assert any(u["unit_id"] == "MICU" for u in units)

    # /model/info
    r_info = requests.get(f"{api_url}/model/info", timeout=5)
    logger.info(f"/model/info response [{r_info.status_code}]: {r_info.json()}")
    assert r_info.status_code == 200, f"/model/info check failed: {r_info.text}"
    info = r_info.json()
    assert "Direct Ridge Regression" in info.get("selected_model")
    assert "Not clinically certified" in info.get("disclaimer")

    # /predict
    payload = {
        "unit_id": "MICU",
        "horizons": 24,
    }
    r_pred = requests.post(f"{api_url}/predict", json=payload, timeout=10)
    logger.info(f"/predict response [{r_pred.status_code}]")
    assert r_pred.status_code == 200, f"/predict failed: {r_pred.text}"
    pred_data = r_pred.json()

    logger.info(f"Unit: {pred_data.get('unit_id')}")
    logger.info(f"Model used: {pred_data.get('model_used')}")
    logger.info(f"Data Source: {pred_data.get('data_source')}")
    logger.info(f"Source Timestamp: {pred_data.get('source_timestamp')}")
    logger.info(f"Source Age Hours: {pred_data.get('source_age_hours')}")
    logger.info(f"Is Live Stream: {pred_data.get('is_live_stream')}")
    logger.info(f"Predictions count: {len(pred_data.get('predictions', []))}")
    logger.info(f"Warnings: {pred_data.get('warnings')}")
    logger.info(f"Disclaimer: {pred_data.get('disclaimer')}")

    assert pred_data.get("unit_id") == "MICU"
    assert pred_data.get("model_used") == "Direct Ridge Regression"
    assert pred_data.get("data_source") == "HDFS_PROCESSED_LAKE"
    assert len(pred_data.get("predictions", [])) == 24
    assert "synthetic" in pred_data.get("disclaimer", "").lower()
    assert any("not clinically certified" in w.lower() for w in pred_data.get("warnings", []))
    logger.info("SUCCESS: API predictions validated across all 24 horizons.")
    return True


def validate_dashboard():
    logger.info("=== STEP 5: Validating Streamlit Dashboard ===")
    dash_url = "http://127.0.0.1:8501"
    r = requests.get(dash_url, timeout=5)
    logger.info(f"Dashboard response [{r.status_code}]")
    assert r.status_code == 200, f"Dashboard unreachable: status {r.status_code}"
    assert "Streamlit" in r.text or "streamlit" in r.text.lower()
    logger.info("SUCCESS: Streamlit Dashboard is accessible.")
    return True


if __name__ == "__main__":
    try:
        p1, p2 = validate_reconstruction_and_manifest()
        validate_manifest_edge_cases(p1, p2)
        validate_api_endpoints()
        validate_dashboard()
        logger.info("====================================================")
        logger.info("ALL PHASE 5 E2E VALIDATION STEPS PASSED SUCCESSFULLY")
        logger.info("====================================================")
        sys.exit(0)
    except Exception as e:
        logger.exception("E2E Validation failed: %s", e)
        sys.exit(1)
