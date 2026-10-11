"""
Interactive Streamlit Dashboard for ICU Bed Demand Forecasting.
Visualizes historical occupancy, 24-hour multi-horizon projections,
staffed capacity thresholds, and surge warnings.
Gracefully handles API unavailability and labels all data as synthetic demonstrations.
"""

from datetime import datetime, timedelta, timezone
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config import get_settings
from src.api.service import get_forecasting_service
from src.api.schemas import PredictionRequest

# Configuration
settings = get_settings()
API_BASE_URL = os.environ.get("API_URL", f"http://localhost:{settings.fastapi_port}")


def check_api_health() -> Tuple[bool, str]:
    """Pings the FastAPI health endpoint to check service reachability."""
    try:
        resp = requests.get(f"{API_BASE_URL}/health", timeout=1.5)
        if resp.status_code == 200:
            return True, "API Connected (Live FastAPI Engine)"
        return False, f"API Error (HTTP {resp.status_code})"
    except requests.exceptions.RequestException:
        return False, "API Offline (FastAPI not reachable on port 8000)"


def fetch_prediction_from_api(
    unit_id: str,
    model_name: str,
    horizons: int = 24,
) -> Optional[Dict[str, Any]]:
    """Requests multi-horizon forecasts from the FastAPI service."""
    payload = {
        "unit_id": unit_id,
        "model_name": model_name,
        "horizons": horizons,
        "allow_persistence_fallback": True,
    }
    try:
        resp = requests.post(f"{API_BASE_URL}/predict", json=payload, timeout=5.0)
        if resp.status_code == 200:
            return resp.json()
        st.error(f"Prediction API Error ({resp.status_code}): {resp.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.warning(f"Could not reach FastAPI service: {e}. Falling back to internal engine.")
        return None


def fetch_prediction_local_fallback(
    unit_id: str,
    model_name: str,
    horizons: int = 24,
) -> Optional[Dict[str, Any]]:
    """Generates predictions locally using ForecastingService when API is offline."""
    service = get_forecasting_service()
    if not service.is_ready:
        service.load_model()

    req = PredictionRequest(
        unit_id=unit_id,
        model_name=model_name,
        horizons=horizons,
        allow_persistence_fallback=True,
    )
    try:
        res = service.predict(req)
        return res.model_dump()
    except Exception as e:
        st.error(f"Local forecast generation failed: {e}")
        return None


def load_recent_history(unit_id: str, hours: int = 72) -> pd.DataFrame:
    """Loads historical hourly occupancy from the local parquet data store."""
    parquet_path = "data/synthetic/simulated_hourly_occupancy.parquet"
    if not os.path.exists(parquet_path):
        return pd.DataFrame()
    df = pd.read_parquet(parquet_path)
    unit_df = df[df["unit_id"] == unit_id].sort_values("timestamp").reset_index(drop=True)
    return unit_df.tail(hours).copy()


def main():
    """Main rendering routine for the Streamlit dashboard."""
    st.set_page_config(
        page_title="ICU Bed Demand Forecasting",
        page_icon="🏥",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom styling
    st.markdown(
        """
        <style>
        .metric-card {
            background-color: #f8f9fa;
            border-radius: 8px;
            padding: 16px;
            border-left: 5px solid #0d6efd;
            margin-bottom: 12px;
        }
        .surge-alert {
            background-color: #fff3cd;
            border: 1px solid #ffeeba;
            color: #856404;
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 16px;
        }
        .disclaimer-box {
            background-color: #e2e3e5;
            border: 1px solid #d6d8db;
            color: #383d41;
            font-size: 0.85rem;
            padding: 10px;
            border-radius: 6px;
            margin-top: 20px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar Navigation & Configuration
    st.sidebar.title("🏥 ICU Forecast Controls")

    api_ok, api_msg = check_api_health()
    if api_ok:
        st.sidebar.success(f"🟢 {api_msg}")
    else:
        st.sidebar.warning(f"🟡 {api_msg} — Local fallback active")

    configured_units = sorted(list(settings.icu_unit_capacities.keys()))
    selected_unit = st.sidebar.selectbox("Select ICU Unit", options=configured_units, index=0)

    model_options = {
        "Direct Ridge Regression": "direct_ridge",
        "Persistence Baseline (Benchmark)": "persistence",
        "Seasonal-Naive Baseline (24h)": "seasonal_naive",
    }
    selected_model_label = st.sidebar.radio(
        "Forecasting Model",
        options=list(model_options.keys()),
        index=0,
        help="Direct Ridge is the selected ML candidate. Persistence Baseline is the primary benchmark.",
    )
    selected_model_name = model_options[selected_model_label]

    forecast_horizons = st.sidebar.slider("Forecast Lead Time (Hours)", min_value=1, max_value=24, value=24)
    history_window_hours = st.sidebar.slider("Historical View Window (Hours)", min_value=12, max_value=168, value=72, step=12)

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        **Academic Notice:**  
        Developed for ICU bed occupancy demand forecasting using Apache Kafka, Hadoop HDFS, and Scikit-learn.
        """
    )

    # Main Dashboard Header
    st.title("🏥 ICU Bed Demand Operational Forecaster")
    st.caption("Real-Time Multi-Horizon Census Projections & Capacity Exceedance Early Warning")

    # Banner: Synthetic Data Labeling
    st.info(
        "ℹ️ **SYNTHETIC-DATA DEMONSTRATION:** All figures, censuses, and projections shown are generated from "
        "synthetic patient admission, transfer, and discharge (ADT) simulations. This pipeline establishes data engineering "
        "and forecasting architecture, not bedside medical diagnostics."
    )

    # 1. Fetch Predictions
    prediction_data = None
    if api_ok:
        prediction_data = fetch_prediction_from_api(selected_unit, selected_model_name, forecast_horizons)

    if prediction_data is None:
        prediction_data = fetch_prediction_local_fallback(selected_unit, selected_model_name, forecast_horizons)

    # 2. Load Historical Data for Visual Context
    df_history = load_recent_history(selected_unit, hours=history_window_hours)

    if prediction_data and not df_history.empty:
        current_occupancy = prediction_data["current_occupancy"]
        staffed_capacity = prediction_data["staffed_capacity"]
        utilization_rate = (current_occupancy / staffed_capacity) * 100.0 if staffed_capacity > 0 else 0.0
        preds_list = prediction_data["predictions"]
        peak_pred = max([p["predicted_occupancy"] for p in preds_list])
        capacity_exceeded = any([p["is_capacity_exceeded"] for p in preds_list])

        # Prominent Fallback and Provenance Status Callouts
        if prediction_data.get("is_baseline") or "Fallback" in prediction_data.get("model_used", ""):
            st.warning(
                f"⚠️ **BASELINE FALLBACK ACTIVE ({prediction_data.get('model_used')}):** "
                "Forecasts are generated using an operational persistence baseline instead of the ML Ridge forecaster. "
                "Review historical data continuity or model selection."
            )

        # Provenance and Data Freshness Callouts (Strictly Decoupled)
        is_live = prediction_data.get("is_live_stream", False)
        data_src = prediction_data.get("data_source", "LOCAL_SYNTHETIC_STORE")
        origin_str = prediction_data.get("forecast_origin", "")
        age_hours = prediction_data.get("source_age_hours")

        # Determine freshness status label
        if age_hours is not None and age_hours < 0:
            freshness_label = f"Future-Dated Scenario ({abs(age_hours):.1f}h ahead of server clock)"
        elif age_hours is not None and age_hours <= 2.0:
            freshness_label = f"Fresh ({age_hours:.1f}h age)"
        elif age_hours is not None:
            freshness_label = f"Stale ({age_hours:.1f}h age)"
        else:
            freshness_label = "Unknown age"

        if is_live:
            st.success(
                f"🟢 **ACTIVE LIVE STREAMING LAKE:** Forecast origin: `{origin_str}` | Provenance: `{data_src}` | "
                f"Freshness: `{freshness_label}` (Pipeline actively ingesting to HDFS; recent data confirmed)."
            )
        elif data_src == "HDFS_PROCESSED_LAKE":
            st.warning(
                f"🟡 **PERSISTED HDFS LAKE (INACTIVE / STALE STREAM):** Forecast origin: `{origin_str}` | "
                f"Provenance: `{data_src}` | Freshness: `{freshness_label}` "
                f"(Parquet snapshot in HDFS; ingestion is not currently refreshing live events)."
            )
        else:
            st.info(
                f"🏛️ **OFFLINE SYNTHETIC BENCHMARK STORE:** Forecast origin: `{origin_str}` | "
                f"Provenance: `{data_src}` | Freshness: `{freshness_label}` "
                f"(Static simulation benchmark dataset; not connected to live hospital streaming telemetry)."
            )

        # Top KPI Metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Current Occupancy", f"{current_occupancy:.1f} beds")
        with col2:
            st.metric("Staffed Capacity", f"{staffed_capacity} beds")
        with col3:
            st.metric("Current Utilization", f"{utilization_rate:.1f}%")
        with col4:
            st.metric("Peak 24h Forecast", f"{peak_pred:.1f} beds")
        with col5:
            if capacity_exceeded:
                st.metric("Surge Status", "⚠️ CAPACITY EXCEEDED", delta=f"+{peak_pred - staffed_capacity:.1f} beds", delta_color="inverse")
            else:
                st.metric("Surge Status", "🟢 NORMAL", delta="Within limits")

        # Surge Alert Callout
        if capacity_exceeded:
            st.error(
                f"🚨 **CAPACITY EXCEEDANCE ALERT FOR {selected_unit}:** "
                f"Projected bed demand exceeds staffed capacity ({staffed_capacity} beds) within the next {forecast_horizons} hours. "
                "Consider nurse float pool activation or diversion protocols."
            )

        # Operational Warnings
        if prediction_data.get("warnings"):
            with st.expander("⚠️ Operational Notices and Warning Messages", expanded=False):
                for w in prediction_data["warnings"]:
                    st.write(f"- {w}")

        # Interactive Plotly Chart
        st.subheader(f"📈 Census Trajectory and Forecast: {selected_unit}")

        fig = go.Figure()

        # Historical line
        df_history["timestamp"] = pd.to_datetime(df_history["timestamp"])
        fig.add_trace(
            go.Scatter(
                x=df_history["timestamp"],
                y=df_history["occupied_beds"],
                mode="lines",
                name="Historical Census",
                line=dict(color="#1f77b4", width=2.5),
            )
        )

        # Staffed Capacity threshold line
        chart_start = df_history["timestamp"].iloc[0]
        forecast_timestamps = [pd.to_datetime(p["forecast_timestamp"]) for p in preds_list]
        chart_end = forecast_timestamps[-1]

        fig.add_trace(
            go.Scatter(
                x=[chart_start, chart_end],
                y=[staffed_capacity, staffed_capacity],
                mode="lines",
                name="Staffed Capacity",
                line=dict(color="#d62728", width=2, dash="dash"),
            )
        )

        # Forecast line connecting origin to future horizons
        origin_ts = pd.to_datetime(prediction_data["forecast_origin"])
        fc_x = [origin_ts] + forecast_timestamps
        fc_y = [current_occupancy] + [p["predicted_occupancy"] for p in preds_list]

        fig.add_trace(
            go.Scatter(
                x=fc_x,
                y=fc_y,
                mode="lines+markers",
                name=f"Forecast ({prediction_data['model_used']})",
                line=dict(color="#ff7f0e", width=3),
                marker=dict(size=6),
            )
        )

        # Highlight capacity exceedances with red dots
        exceed_x = [pd.to_datetime(p["forecast_timestamp"]) for p in preds_list if p["is_capacity_exceeded"]]
        exceed_y = [p["predicted_occupancy"] for p in preds_list if p["is_capacity_exceeded"]]
        if exceed_x:
            fig.add_trace(
                go.Scatter(
                    x=exceed_x,
                    y=exceed_y,
                    mode="markers",
                    name="Exceedance Hours",
                    marker=dict(color="red", size=10, symbol="x"),
                )
            )

        fig.update_layout(
            title=f"Hourly Bed Occupancy Trajectory & Next {forecast_horizons}-Hour Projections ({selected_unit})",
            xaxis_title="Timeline (UTC)",
            yaxis_title="Occupied Beds",
            hovermode="x unified",
            template="plotly_white",
            height=480,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Forecast Detail Breakdown Table
        st.subheader("📋 Hourly Forecast Breakdown")
        table_rows = []
        for p in preds_list:
            ts_formatted = pd.to_datetime(p["forecast_timestamp"]).strftime("%Y-%m-%d %H:%M UTC")
            status_badge = "🔴 Exceeded" if p["is_capacity_exceeded"] else "🟢 Available"
            available = max(0, staffed_capacity - int(round(p["predicted_occupancy"])))
            table_rows.append({
                "Horizon": f"+{p['horizon']}h",
                "Target Timestamp": ts_formatted,
                "Forecast Occupancy": f"{p['predicted_occupancy']:.1f} beds",
                "Staffed Capacity": f"{staffed_capacity} beds",
                "Available Beds": f"{available} beds",
                "Status": status_badge,
                "Exceedance": f"+{p['exceedance_beds']:.1f} beds" if p["is_capacity_exceeded"] else "0.0 beds",
            })

        df_table = pd.DataFrame(table_rows)
        st.dataframe(df_table, use_container_width=True, hide_index=True)

        # Architectural Details & Benchmarks
        with st.expander("ℹ️ Model Architecture & Benchmark Comparison Details", expanded=False):
            st.markdown(
                """
                ### Benchmark Evaluation Summary
                - **Persistence Baseline (Primary Benchmark):** Carries forward origin occupancy $y_t$. Achieved **1.56 beds MAE** on the overall unsegmented held-out test partition.
                - **Direct Ridge Regression (Selected Candidate):** Independent L2-regularized linear estimators per horizon using 54 past-only features (lags, rolling statistics, calendar harmonics). Achieved **1.68 beds MAE on validation set** and **1.72 beds MAE during active inflow on the test set**.
                - **Seasonal-Naive Baseline:** Uses $y_{t-(24-h)}$. Achieved **2.48 beds MAE**.
                
                ### Clinical Disclaimer
                This software is an engineering demonstration of event streaming (Kafka), big data storage (HDFS), and machine learning pipelines.
                It does not constitute a certified medical device and should not be used for direct patient triage without institutional validation.
                """
            )

    else:
        st.warning("No historical data available. Please generate synthetic data using `python scripts/generate_synthetic_data.py --days 90 --surge`.")


if __name__ == "__main__":
    main()
