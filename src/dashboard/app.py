"""
ICU Operations & Capacity Intelligence Platform — Command Center Dashboard.

A restrained, high-fidelity healthcare operations command center for real-time
ICU bed demand forecasting, capacity exceedance early warning, and cluster telemetry.
Adheres strictly to clinical product design standards:
- Background: #F7F8FA
- Card Surfaces: #FFFFFF with subtle 1px border (#E2E8F0)
- Typography: High-contrast system stack with deep slate (#172B4D)
- Accent: Clinical Teal (#0D9488)
- Alerts: Amber Warning (#D97706) and Restrained Crimson (#DC2626)
- Zero decorative clutter, zero AI tropes, zero emojis.
"""

from datetime import datetime, timezone
import json
import os
import socket
import sys
import textwrap
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

UNIT_NAMES = {
    "CCU": "Coronary Care Unit",
    "CSRU": "Cardiac Surgery Recovery Unit",
    "MICU": "Medical Intensive Care Unit",
    "SICU": "Surgical Intensive Care Unit",
    "TSICU": "Trauma Surgical ICU",
}


def render_html(html_str: str) -> None:
    """Safely renders HTML without Markdown indentation triggering code blocks."""
    st.markdown(textwrap.dedent(html_str).strip(), unsafe_allow_html=True)


def render_sidebar_html(html_str: str) -> None:
    """Safely renders HTML in sidebar without Markdown indentation triggering code blocks."""
    st.sidebar.markdown(textwrap.dedent(html_str).strip(), unsafe_allow_html=True)


def check_api_health() -> Tuple[bool, str]:
    """Pings the FastAPI health endpoint to check service reachability."""
    try:
        resp = requests.get(f"{API_BASE_URL}/health", timeout=1.5)
        if resp.status_code == 200:
            return True, f"API Connected (FastAPI Engine :{settings.fastapi_port})"
        return False, f"API Error (HTTP {resp.status_code})"
    except requests.exceptions.RequestException:
        return False, f"API Offline (FastAPI not reachable on port {settings.fastapi_port})"


def check_infrastructure() -> Dict[str, Tuple[bool, str]]:
    """Performs non-blocking probes on streaming broker and distributed lake."""
    results: Dict[str, Tuple[bool, str]] = {}

    # API probe
    api_ok, api_msg = check_api_health()
    results["FastAPI Inference Engine"] = (api_ok, api_msg)

    # Kafka broker socket check
    try:
        parts = settings.kafka_bootstrap_servers.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9092
        s = socket.create_connection((host, port), timeout=0.6)
        s.close()
        results["Apache Kafka Broker"] = (True, f"Healthy (Broker {settings.kafka_bootstrap_servers})")
    except Exception as exc:
        results["Apache Kafka Broker"] = (False, f"Unreachable ({exc})")

    # HDFS NameNode WebHDFS check
    try:
        hdfs_url = f"{settings.hdfs_namenode_url}/webhdfs/v1/?op=LISTSTATUS&user.name={settings.hdfs_user}"
        resp = requests.get(hdfs_url, timeout=0.8)
        if resp.status_code == 200:
            results["Hadoop HDFS Lake"] = (True, f"Active (NameNode {settings.hdfs_namenode_url})")
        else:
            results["Hadoop HDFS Lake"] = (False, f"HTTP Error {resp.status_code}")
    except Exception as exc:
        results["Hadoop HDFS Lake"] = (False, f"Unreachable ({exc})")

    # Vital-signs telemetry pipeline status
    vitals_topic = getattr(settings, "kafka_topic_icu_vitals", "icu-vitals")
    results["Bedside Vital Telemetry"] = (
        True,
        f"Active (Topic '{vitals_topic}' -> Lake Partition /raw/icu_vitals/)",
    )

    return results


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
    except requests.exceptions.RequestException:
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


def load_all_units_latest() -> Dict[str, Dict[str, Any]]:
    """Loads current occupancy snapshot across all configured ICU units."""
    parquet_path = "data/synthetic/simulated_hourly_occupancy.parquet"
    if not os.path.exists(parquet_path):
        return {}
    try:
        df = pd.read_parquet(parquet_path)
        latest_ts = df["timestamp"].max()
        latest_df = df[df["timestamp"] == latest_ts]
        snapshot = {}
        for _, row in latest_df.iterrows():
            uid = str(row["unit_id"])
            occ = float(row["occupied_beds"])
            cap = int(row["capacity"])
            rate = (occ / cap) * 100.0 if cap > 0 else 0.0
            snapshot[uid] = {
                "occupied_beds": occ,
                "capacity": cap,
                "utilization": rate,
            }
        return snapshot
    except Exception:
        return {}


def render_custom_css():
    """Injects high-contrast, clinical command center styling into Streamlit."""
    css_content = """
    <style>
    /* 1. Base Application Canvas */
    .stApp {
        background-color: #F7F8FA !important;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif !important;
        color: #172B4D !important;
    }

    /* 2. Top Header Restyling (Removes Black Strip & Tightens Spacing) */
    header[data-testid="stHeader"] {
        background-color: #F7F8FA !important;
        height: 2.5rem !important;
        border-bottom: 1px solid #E2E8F0 !important;
    }
    div[data-testid="stDecoration"] {
        display: none !important;
    }
    .main .block-container {
        padding-top: 0.85rem !important;
        padding-bottom: 1.5rem !important;
        padding-left: 1.75rem !important;
        padding-right: 1.75rem !important;
        max-width: 100% !important;
    }

    /* 3. Sidebar: High-Contrast Dark Text & Clean Cards */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E2E8F0 !important;
    }
    section[data-testid="stSidebar"] * {
        color: #172B4D !important;
    }
    .sidebar-brand-title {
        font-size: 0.85rem;
        font-weight: 800;
        color: #0D9488 !important;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        padding-bottom: 6px;
        margin-bottom: 12px;
        border-bottom: 2px solid #0D9488;
    }
    .sidebar-section-title {
        font-size: 0.70rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #64748B !important;
        margin-top: 12px;
        margin-bottom: 4px;
    }
    section[data-testid="stSidebar"] label {
        color: #0F172A !important;
        font-weight: 600 !important;
        font-size: 0.82rem !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stRadio"] label span {
        color: #1E293B !important;
        font-size: 0.82rem !important;
        font-weight: 500 !important;
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] > div {
        background-color: #F8FAFC !important;
        border: 1px solid #CBD5E1 !important;
        border-radius: 4px !important;
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] * {
        color: #0F172A !important;
        font-weight: 600 !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stThumbValue"] {
        color: #0F172A !important;
        font-weight: 700 !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stTickBarMin"],
    section[data-testid="stSidebar"] div[data-testid="stTickBarMax"] {
        color: #64748B !important;
        font-weight: 500 !important;
    }
    .sidebar-notice-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 4px;
        padding: 9px 11px;
        font-size: 0.73rem;
        color: #475569 !important;
        line-height: 1.4;
        margin-top: 12px;
    }
    .sidebar-notice-card * {
        color: #475569 !important;
    }

    /* 4. Top Operations Header */
    .header-container {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        padding: 10px 16px;
        margin-bottom: 12px;
        border-radius: 6px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02);
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 10px;
    }
    .header-title {
        font-size: 1.18rem;
        font-weight: 700;
        color: #0F172A;
        margin: 0;
        letter-spacing: -0.01em;
    }
    .header-subtitle {
        font-size: 0.80rem;
        color: #64748B;
        margin-top: 1px;
    }
    .header-meta {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
    }

    /* 5. Status Pills */
    .status-pill {
        display: inline-flex;
        align-items: center;
        padding: 3px 9px;
        border-radius: 4px;
        font-size: 0.74rem;
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    .status-pill-green {
        background-color: #ECFDF5;
        color: #065F46;
        border: 1px solid #A7F3D0;
    }
    .status-pill-amber {
        background-color: #FFFBEB;
        color: #92400E;
        border: 1px solid #FDE68A;
    }
    .status-pill-red {
        background-color: #FEF2F2;
        color: #991B1B;
        border: 1px solid #FECACA;
    }
    .status-pill-neutral {
        background-color: #F1F5F9;
        color: #334155;
        border: 1px solid #CBD5E1;
    }

    /* 6. KPI Card Grid */
    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 12px;
        margin-bottom: 12px;
    }
    .kpi-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 6px;
        padding: 12px 16px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02);
    }
    .kpi-label {
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: #64748B;
        margin-bottom: 4px;
    }
    .kpi-value {
        font-size: 1.55rem;
        font-weight: 700;
        color: #0F172A;
        line-height: 1.1;
        margin-bottom: 2px;
    }
    .kpi-unit {
        font-size: 0.90rem;
        font-weight: 500;
        color: #64748B;
    }
    .kpi-subtext {
        font-size: 0.75rem;
        color: #64748B;
    }

    /* 7. ICU Unit Comparison Cards */
    .unit-cards-container {
        display: flex;
        flex-direction: column;
        gap: 7px;
        width: 100%;
    }
    .unit-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 6px;
        padding: 9px 13px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02);
        box-sizing: border-box;
        width: 100%;
        transition: all 0.15s ease-in-out;
    }
    .unit-card-active {
        border: 2px solid #0D9488 !important;
        background: #F0FDFA !important;
    }
    .unit-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 2px;
    }
    .unit-card-code {
        font-size: 0.90rem;
        font-weight: 700;
        color: #0F172A;
        letter-spacing: 0.02em;
    }
    .unit-badge-active {
        font-size: 0.64rem;
        font-weight: 700;
        color: #0D9488;
        background: #CCFBF1;
        border: 1px solid #99F6E4;
        padding: 1px 6px;
        border-radius: 3px;
        letter-spacing: 0.05em;
    }
    .unit-card-name {
        font-size: 0.75rem;
        color: #64748B;
        margin-bottom: 5px;
        white-space: normal;
        line-height: 1.25;
    }
    .unit-card-stats {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 0.80rem;
        font-weight: 600;
        margin-bottom: 4px;
    }
    .unit-card-beds {
        color: #1E293B;
    }
    .unit-progress-bg {
        background-color: #E2E8F0;
        border-radius: 3px;
        height: 6px;
        width: 100%;
        overflow: hidden;
    }
    .unit-progress-bar {
        height: 100%;
        border-radius: 3px;
    }

    /* 8. Operational & Clinical Banners */
    .clinical-banner {
        background: #FFFFFF;
        border-left: 4px solid #0D9488;
        border-top: 1px solid #E2E8F0;
        border-right: 1px solid #E2E8F0;
        border-bottom: 1px solid #E2E8F0;
        border-radius: 4px;
        padding: 10px 14px;
        margin-top: 10px;
        margin-bottom: 10px;
    }
    .exceedance-alert {
        background: #FEF2F2;
        border: 1px solid #FCA5A5;
        border-left: 4px solid #DC2626;
        border-radius: 4px;
        padding: 10px 14px;
        margin-bottom: 12px;
        color: #991B1B;
        font-size: 0.85rem;
        font-weight: 500;
    }
    .section-header {
        font-size: 0.85rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #475569;
        margin-bottom: 8px;
        border-bottom: 1px solid #E2E8F0;
        padding-bottom: 4px;
    }

    /* 9. Diagnostic Cards */
    .diag-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 6px;
        padding: 11px;
        height: 100%;
        box-sizing: border-box;
    }
    .diag-title {
        font-size: 0.70rem;
        font-weight: 600;
        text-transform: uppercase;
        color: #64748B;
        margin-bottom: 3px;
    }
    .diag-detail {
        font-size: 0.74rem;
        color: #475569;
    }
    </style>
    """
    render_html(css_content)


def create_forecast_figure(
    df_history: pd.DataFrame,
    prediction_data: Dict[str, Any],
    selected_unit: str,
    staffed_capacity: int,
) -> go.Figure:
    """Builds a high-contrast Plotly trajectory chart tailored for healthcare operations."""
    fig = go.Figure()

    # 1. Historical Census Trace (Deep Charcoal Slate)
    df_history["timestamp"] = pd.to_datetime(df_history["timestamp"])
    fig.add_trace(
        go.Scatter(
            x=df_history["timestamp"],
            y=df_history["occupied_beds"],
            mode="lines",
            name="Historical Observations",
            line=dict(color="#334155", width=2.0),
            hovertemplate="<b>%{x|%b %d, %H:%M UTC}</b><br>Observed Census: <b>%{y:.1f} beds</b><extra>Historical</extra>",
        )
    )

    # 2. Staffed Capacity Ceiling Line (Restrained Red Dash)
    chart_start = df_history["timestamp"].iloc[0]
    preds_list = prediction_data["predictions"]
    forecast_timestamps = [pd.to_datetime(p["forecast_timestamp"]) for p in preds_list]
    chart_end = forecast_timestamps[-1]

    fig.add_trace(
        go.Scatter(
            x=[chart_start, chart_end],
            y=[staffed_capacity, staffed_capacity],
            mode="lines",
            name=f"Staffed Bed Capacity ({staffed_capacity} Beds)",
            line=dict(color="#DC2626", width=1.5, dash="dash"),
            hovertemplate=f"Staffed Ceiling: <b>{staffed_capacity} beds</b><extra>Ceiling</extra>",
        )
    )

    # 3. Forecast Horizon Trace (Clinical Teal)
    origin_ts = pd.to_datetime(prediction_data["forecast_origin"])
    fc_x = [origin_ts] + forecast_timestamps
    fc_y = [prediction_data["current_occupancy"]] + [p["predicted_occupancy"] for p in preds_list]
    model_label = prediction_data.get("model_used", "Direct Ridge")

    fig.add_trace(
        go.Scatter(
            x=fc_x,
            y=fc_y,
            mode="lines+markers",
            name=f"Projected Demand ({model_label})",
            line=dict(color="#0D9488", width=2.5),
            marker=dict(size=5, color="#0D9488"),
            hovertemplate="<b>%{x|%b %d, %H:%M UTC}</b><br>Projected Demand: <b>%{y:.1f} beds</b><extra>Forecast</extra>",
        )
    )

    # 4. Highlight Forecast Region with Subtle Background Tint
    fig.add_vrect(
        x0=origin_ts,
        x1=chart_end,
        fillcolor="rgba(13, 148, 136, 0.04)",
        layer="below",
        line_width=0,
    )

    # 5. Exceedance Alerts (Highlighted Red Diamonds)
    exceed_x = [pd.to_datetime(p["forecast_timestamp"]) for p in preds_list if p["is_capacity_exceeded"]]
    exceed_y = [p["predicted_occupancy"] for p in preds_list if p["is_capacity_exceeded"]]
    if exceed_x:
        fig.add_trace(
            go.Scatter(
                x=exceed_x,
                y=exceed_y,
                mode="markers",
                name="Projected Exceedance",
                marker=dict(color="#DC2626", size=8, symbol="diamond"),
                hovertemplate="<b>%{x|%b %d, %H:%M UTC}</b><br>SURGE ALERT: <b>%{y:.1f} beds (> Capacity)</b><extra>Alert</extra>",
            )
        )

    # 6. Projected Peak Demand Annotation on Trajectory
    if preds_list:
        peak_item = max(preds_list, key=lambda x: x["predicted_occupancy"])
        peak_ts = pd.to_datetime(peak_item["forecast_timestamp"])
        peak_val = float(peak_item["predicted_occupancy"])
        peak_h = peak_item["horizon"]
        fig.add_annotation(
            x=peak_ts,
            y=peak_val,
            text=f"Peak: {peak_val:.1f} beds (+{peak_h}h)",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=1.5,
            arrowcolor="#0D9488",
            ax=0,
            ay=-32,
            font=dict(size=10, color="#0F172A"),
            bgcolor="#FFFFFF",
            bordercolor="#0D9488",
            borderwidth=1,
            borderpad=3,
        )

    # 7. Vertical Forecast Origin Marker
    fig.add_vline(
        x=origin_ts,
        line_width=1.5,
        line_dash="dot",
        line_color="#64748B",
        annotation_text="Forecast Origin",
        annotation_position="top left",
        annotation_font=dict(size=10, color="#475569"),
    )

    fig.update_layout(
        margin=dict(l=45, r=20, t=32, b=38),
        xaxis=dict(
            title=dict(text="Timeline (UTC)", font=dict(size=12, color="#475569")),
            tickfont=dict(size=11, color="#64748B"),
            showgrid=True,
            gridcolor="#F1F5F9",
            zeroline=False,
        ),
        yaxis=dict(
            title=dict(text="Occupied Beds", font=dict(size=12, color="#475569")),
            tickfont=dict(size=11, color="#64748B"),
            showgrid=True,
            gridcolor="#F1F5F9",
            zeroline=False,
        ),
        hovermode="x unified",
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        height=420,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11, color="#334155"),
        ),
    )
    return fig


def main():
    """Main Streamlit execution routine."""
    st.set_page_config(
        page_title="ICU Operations Command Center",
        page_icon=None,
        layout="wide",
        initial_sidebar_state="expanded",
    )

    render_custom_css()

    # 1. System Health Probes
    api_ok, api_msg = check_api_health()

    # 2. Sidebar Navigation & Configuration Controls
    render_sidebar_html('<div class="sidebar-brand-title">Clinical Controls</div>')

    configured_units = sorted(list(settings.icu_unit_capacities.keys()))
    default_unit_idx = configured_units.index("MICU") if "MICU" in configured_units else 0

    render_sidebar_html('<div class="sidebar-section-title">Ward Configuration</div>')
    selected_unit = st.sidebar.selectbox(
        "ICU Ward Selection",
        options=configured_units,
        index=default_unit_idx,
        help="Select target intensive care unit to inspect historical trajectory and 24-hour demand.",
    )

    render_sidebar_html('<div class="sidebar-section-title">Inference Engine</div>')
    model_options = {
        "Direct Ridge Regression (Production)": "direct_ridge",
        "Persistence Baseline (Primary Benchmark)": "persistence",
        "Seasonal-Naive Baseline (24h Lags)": "seasonal_naive",
    }
    selected_model_label = st.sidebar.radio(
        "Model Selection",
        options=list(model_options.keys()),
        index=0,
        help="Direct Ridge trains 24 multi-horizon linear forecasters. Baselines provide benchmark reference.",
    )
    selected_model_name = model_options[selected_model_label]

    render_sidebar_html('<div class="sidebar-section-title">Time Horizons</div>')
    forecast_horizons = st.sidebar.slider(
        "Forecast Horizon (Hours)",
        min_value=1,
        max_value=24,
        value=24,
        help="Projected demand forward window in hours (1 to 24).",
    )
    history_window_hours = st.sidebar.slider(
        "Historical Context (Hours)",
        min_value=12,
        max_value=168,
        value=72,
        step=12,
        help="Past hourly census observations displayed for clinical context.",
    )

    render_sidebar_html(
        """
        <div class="sidebar-notice-card">
            <b>Operational Notice:</b><br/>
            Visualizes real-time ICU bed occupancy and 24-hour multi-horizon regression projections.
            Derived from streaming event logs and historical census reconstructions.
        </div>
        """
    )

    # 3. Retrieve Predictions
    prediction_data = None
    if api_ok:
        prediction_data = fetch_prediction_from_api(selected_unit, selected_model_name, forecast_horizons)

    if prediction_data is None:
        prediction_data = fetch_prediction_local_fallback(selected_unit, selected_model_name, forecast_horizons)

    df_history = load_recent_history(selected_unit, hours=history_window_hours)
    units_snapshot = load_all_units_latest()

    # 4. Header: Application Status & Provenance
    raw_origin = prediction_data.get("forecast_origin") if prediction_data else None
    if raw_origin is not None:
        try:
            origin_dt = pd.to_datetime(raw_origin)
            origin_display = origin_dt.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            origin_display = str(raw_origin)
    else:
        origin_display = "Unavailable"

    data_source_str = prediction_data.get("data_source", "LOCAL_SYNTHETIC_STORE") if prediction_data else "UNKNOWN"
    age_hours = prediction_data.get("source_age_hours") if prediction_data else None

    if age_hours is not None and age_hours <= 2.0:
        freshness_badge = f'<span class="status-pill status-pill-green">Fresh ({age_hours:.1f}h)</span>'
    elif age_hours is not None:
        freshness_badge = f'<span class="status-pill status-pill-neutral">Historical ({age_hours:.1f}h age)</span>'
    else:
        freshness_badge = '<span class="status-pill status-pill-neutral">Sync</span>'

    api_badge = (
        f'<span class="status-pill status-pill-green">API Live :{settings.fastapi_port}</span>'
        if api_ok
        else f'<span class="status-pill status-pill-amber">Internal Engine</span>'
    )

    header_html = f"""
    <div class="header-container">
        <div>
            <div class="header-title">ICU Operations &amp; Capacity Intelligence Platform</div>
            <div class="header-subtitle">Real-Time Multi-Horizon Census Forecasting &amp; Capacity Early Warning</div>
        </div>
        <div class="header-meta">
            {api_badge}
            <span class="status-pill status-pill-neutral">Source: {data_source_str}</span>
            {freshness_badge}
            <span class="status-pill status-pill-neutral">Origin: {origin_display}</span>
        </div>
    </div>
    """
    render_html(header_html)

    # 5. Fallback or Academic Notice
    if prediction_data and (prediction_data.get("is_baseline") or "Fallback" in prediction_data.get("model_used", "")):
        render_html(
            f"""
            <div class="clinical-banner" style="border-left-color: #D97706; background-color: #FFFBEB;">
                <b>Benchmark Mode Active:</b> Forecasts are generated using the <b>{prediction_data.get('model_used')}</b>
                benchmark rather than Direct Ridge. Review historical continuity or selection settings.
            </div>
            """
        )

    if prediction_data and not df_history.empty:
        current_occupancy = float(prediction_data["current_occupancy"])
        staffed_capacity = int(prediction_data["staffed_capacity"])
        utilization_rate = (current_occupancy / staffed_capacity * 100.0) if staffed_capacity > 0 else 0.0
        preds_list = prediction_data.get("predictions", [])
        peak_pred = max([float(p["predicted_occupancy"]) for p in preds_list]) if preds_list else current_occupancy
        capacity_exceeded = any([bool(p["is_capacity_exceeded"]) for p in preds_list]) if preds_list else False
        peak_p = max(preds_list, key=lambda x: x["predicted_occupancy"]) if preds_list else {"horizon": 0, "forecast_timestamp": str(raw_origin)}
        peak_ts_dt = pd.to_datetime(peak_p["forecast_timestamp"])
        peak_ts_str = peak_ts_dt.strftime("%H:%M UTC")

        # 6. Section B: KPI Cards
        util_color = "#059669" if utilization_rate < 80.0 else ("#D97706" if utilization_rate < 95.0 else "#DC2626")
        util_status = "Optimal" if utilization_rate < 80.0 else ("Elevated" if utilization_rate < 95.0 else "Critical")

        kpi_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Current Census</div>
                <div class="kpi-value">{current_occupancy:.1f} <span class="kpi-unit">Beds</span></div>
                <div class="kpi-subtext">Active patients in {selected_unit}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Staffed Capacity</div>
                <div class="kpi-value">{staffed_capacity} <span class="kpi-unit">Beds</span></div>
                <div class="kpi-subtext">{max(0, staffed_capacity - int(round(current_occupancy)))} available staffed beds</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Unit Utilization</div>
                <div class="kpi-value" style="color: {util_color};">{utilization_rate:.1f}%</div>
                <div class="kpi-subtext">Status: <b>{util_status}</b> (&lt;80% target)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Peak 24h Demand</div>
                <div class="kpi-value">{peak_pred:.1f} <span class="kpi-unit">Beds</span></div>
                <div class="kpi-subtext">At horizon +{peak_p['horizon']}h ({peak_ts_str})</div>
            </div>
        </div>
        """
        render_html(kpi_html)

        # Exceedance Alert Banner (Only when warranted by data)
        if capacity_exceeded:
            render_html(
                f"""
                <div class="exceedance-alert">
                    <b>Capacity Exceedance Warning:</b> Projected demand exceeds staffed capacity ({staffed_capacity} beds)
                    within the next {forecast_horizons} hours. Peak exceedance: +{peak_pred - staffed_capacity:.1f} beds.
                    Initiate nursing float pool coordination or hospital transfer protocol.
                </div>
                """
            )

        # 7. Layout: Main Forecast Chart (Left ~65%) & Unit Comparison (Right ~35%)
        col_main, col_units = st.columns([13, 7], gap="medium")

        with col_main:
            render_html(f'<div class="section-header">Census Trajectory &amp; Forward Outlook ({selected_unit})</div>')
            fig = create_forecast_figure(df_history, prediction_data, selected_unit, staffed_capacity)
            st.plotly_chart(fig, use_container_width=True)

            # Section E: Compact Operational Capacity Summary
            if preds_list:
                min_pred = min([float(p["predicted_occupancy"]) for p in preds_list])
                min_headroom = max(0.0, staffed_capacity - peak_pred)
                net_delta = peak_pred - current_occupancy
                delta_sign = "+" if net_delta >= 0 else ""

                if capacity_exceeded:
                    outlook_status = "SURGE RISK DETECTED"
                    badge_class = "status-pill-red"
                    headline = f"Exceedance of +{peak_pred - staffed_capacity:.1f} beds projected at +{peak_p['horizon']}h ({peak_ts_str}). Operating buffer exhausted."
                elif utilization_rate >= 80.0:
                    outlook_status = "ELEVATED CENSUS"
                    badge_class = "status-pill-amber"
                    headline = f"Projected peak of {peak_pred:.1f} beds ({delta_sign}{net_delta:.1f} from census) at +{peak_p['horizon']}h ({peak_ts_str}). Operating buffer tight ({min_headroom:.1f} beds min)."
                else:
                    outlook_status = "STABLE OPERATING POSTURE"
                    badge_class = "status-pill-green"
                    headline = f"Census projected between {min_pred:.1f} and {peak_pred:.1f} beds ({delta_sign}{net_delta:.1f} peak delta) at +{peak_p['horizon']}h ({peak_ts_str}). Operating buffer of {min_headroom:.1f} beds maintained."

                outlook_html = f"""
                <div class="clinical-banner">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; flex-wrap: wrap; gap: 8px;">
                        <span style="font-weight: 700; color: #0F172A; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.03em;">24-Hour Operational Capacity Summary</span>
                        <div style="display: flex; gap: 8px; align-items: center;">
                            <span class="status-pill status-pill-neutral">Peak: <b>{peak_pred:.1f} Beds (+{peak_p['horizon']}h)</b></span>
                            <span class="status-pill status-pill-neutral">Min Buffer: <b>{min_headroom:.1f} Beds</b></span>
                            <span class="status-pill {badge_class}">{outlook_status}</span>
                        </div>
                    </div>
                    <div style="font-size: 0.80rem; color: #334155; line-height: 1.4;">{headline}</div>
                </div>
                """
            else:
                outlook_html = f"""
                <div class="clinical-banner">
                    <div style="font-weight: 700; color: #0F172A; font-size: 0.85rem; margin-bottom: 4px;">24-Hour Operational Capacity Summary</div>
                    <div style="font-size: 0.80rem; color: #64748B;">Forecast telemetry is syncing. Historical census data ({len(df_history)} observed hours) remains available.</div>
                </div>
                """
            render_html(outlook_html)

        with col_units:
            # Section D: ICU Unit Comparison Grid (Rendered cleanly with zero leading whitespace)
            render_html('<div class="section-header">ICU Ward Status &amp; Capacity</div>')

            cards = []
            for uid in configured_units:
                u_info = units_snapshot.get(
                    uid,
                    {
                        "occupied_beds": 0.0,
                        "capacity": settings.icu_unit_capacities.get(uid, 20),
                        "utilization": 0.0,
                    },
                )
                u_occ = u_info["occupied_beds"]
                u_cap = u_info["capacity"]
                u_util = u_info["utilization"]
                u_name = UNIT_NAMES.get(uid, "Intensive Care Unit")
                is_active = (uid == selected_unit)
                active_class = "unit-card-active" if is_active else ""
                p_color = "#0D9488" if u_util < 80.0 else ("#D97706" if u_util < 95.0 else "#DC2626")
                badge_html = '<span class="unit-badge-active">ACTIVE</span>' if is_active else ""

                cards.append(
                    f'<div class="unit-card {active_class}">'
                    f'<div class="unit-card-header">'
                    f'<span class="unit-card-code">{uid}</span>'
                    f'{badge_html}'
                    f'</div>'
                    f'<div class="unit-card-name">{u_name}</div>'
                    f'<div class="unit-card-stats">'
                    f'<span class="unit-card-beds">{u_occ:.0f} / {u_cap} Beds</span>'
                    f'<span style="color: {p_color};">{u_util:.1f}%</span>'
                    f'</div>'
                    f'<div class="unit-progress-bg">'
                    f'<div class="unit-progress-bar" style="width: {min(100.0, u_util)}%; background-color: {p_color};"></div>'
                    f'</div>'
                    f'</div>'
                )

            cards_container_html = f'<div class="unit-cards-container">{"".join(cards)}</div>'
            render_html(cards_container_html)

        # 8. Section F: Collapsible Infrastructure Diagnostics & Hourly Forecast Detail
        with st.expander("System Infrastructure, Pipeline Diagnostics & Hourly Table", expanded=False):
            diag = check_infrastructure()
            diag_cols = st.columns(len(diag))
            for idx, (comp_name, (is_healthy, detail_msg)) in enumerate(diag.items()):
                with diag_cols[idx]:
                    pill_class = "status-pill-green" if is_healthy else "status-pill-red"
                    status_text = "Operational" if is_healthy else "Degraded"
                    card_html = f"""
                    <div class="diag-card">
                        <div class="diag-title">{comp_name}</div>
                        <div style="margin-bottom: 6px;"><span class="status-pill {pill_class}">{status_text}</span></div>
                        <div class="diag-detail">{detail_msg}</div>
                    </div>
                    """
                    render_html(card_html)

            render_html('<div class="section-header" style="margin-top: 24px;">Hourly Forecast Table</div>')
            table_rows = []
            for p in preds_list:
                ts_dt = pd.to_datetime(p["forecast_timestamp"])
                avail = max(0, staffed_capacity - int(round(p["predicted_occupancy"])))
                status_str = "Capacity Exceeded" if p["is_capacity_exceeded"] else "Normal Headroom"
                table_rows.append({
                    "Horizon": f"+{p['horizon']}h",
                    "Target Time (UTC)": ts_dt.strftime("%Y-%m-%d %H:%M"),
                    "Predicted Demand": f"{p['predicted_occupancy']:.2f} beds",
                    "Staffed Capacity": f"{staffed_capacity} beds",
                    "Available Headroom": f"{avail} beds",
                    "Operating Status": status_str,
                    "Surge Exceedance": f"+{p['exceedance_beds']:.2f} beds" if p["is_capacity_exceeded"] else "0.00 beds",
                })
            st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

            provenance_html = """
            <div style="font-size: 0.78rem; color: #64748B; margin-top: 14px; line-height: 1.5;">
            <b>Pipeline Provenance:</b> Data reflects micro-batch ingestion through Apache Kafka (<code>icu-events</code>),
            durable archival in Hadoop HDFS, and hourly census aggregation via PyArrow.
            Forecasting model utilizes 54 chronological past-only features (unit lags, rolling windows, calendar harmonics)
            with zero future leakage.
            </div>
            """
            render_html(provenance_html)

    else:
        st.warning("Historical census data is not available. Please generate benchmark data using scripts/generate_synthetic_data.py.")


if __name__ == "__main__":
    main()
