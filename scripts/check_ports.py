"""
CLI utility to audit, verify, and report port assignments for ICU Bed Demand Forecasting.
Scans Docker infrastructure, API service, and Streamlit dashboard ports.
"""

import argparse
import socket
import sys
import os

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import get_settings


def check_port(host: str, port: int, timeout: float = 1.0) -> bool:
    """Returns True if the port is open/listening on the given host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False


def main():
    parser = argparse.ArgumentParser(description="Audit and report port availability for ICU project.")
    parser.add_argument("--host", default="127.0.0.1", help="Host IP to scan (default: 127.0.0.1)")
    args = parser.parse_args()

    settings = get_settings()

    port_definitions = [
        {
            "name": "ZooKeeper",
            "role": "Cluster coordination",
            "port": 2181,
            "type": "infrastructure",
            "expected_state": "listening",
        },
        {
            "name": "Apache Kafka",
            "role": "ADT event streaming broker",
            "port": int(settings.kafka_bootstrap_servers.split(":")[-1]),
            "type": "infrastructure",
            "expected_state": "listening",
        },
        {
            "name": "HDFS NameNode RPC",
            "role": "Hadoop IPC RPC filesystem protocol",
            "port": int(settings.hdfs_namenode_rpc.split(":")[-1]),
            "type": "infrastructure",
            "expected_state": "listening",
        },
        {
            "name": "HDFS NameNode WebHDFS",
            "role": "WebHDFS REST API & NameNode UI",
            "port": int(settings.hdfs_namenode_url.split(":")[-1]),
            "type": "infrastructure",
            "expected_state": "listening",
        },
        {
            "name": "HDFS DataNode",
            "role": "Hadoop DataNode block transfer & HTTP",
            "port": 9864,
            "type": "infrastructure",
            "expected_state": "listening",
        },
        {
            "name": "FastAPI Service",
            "role": "1-24h multi-horizon prediction REST API",
            "port": settings.fastapi_port,
            "type": "application",
            "expected_state": "free_or_running",
        },
        {
            "name": "Streamlit Dashboard",
            "role": "Clinical capacity operational UI",
            "port": settings.streamlit_port,
            "type": "application",
            "expected_state": "free_or_running",
        },
    ]

    print("==========================================================================")
    print("ICU Bed Demand Forecasting — Port Management & Audit Utility")
    print("==========================================================================")
    print(f"{'Service / Component':<25} {'Port':<8} {'State':<12} {'Status / Recommendation'}")
    print("--------------------------------------------------------------------------")

    all_good = True

    for p in port_definitions:
        port = p["port"]
        name = p["name"]
        role = p["role"]
        is_listening = check_port(args.host, port)

        if p["type"] == "infrastructure":
            if is_listening:
                state_str = "[ACTIVE]"
                desc = f"OK — Container is listening ({role})"
            else:
                state_str = "[STOPPED]"
                desc = f"WARN — Not listening. Start via `docker compose up -d`"
                all_good = False
        else:
            if is_listening:
                state_str = "[OCCUPIED]"
                desc = f"Service is currently running on this port ({role})"
            else:
                state_str = "[FREE]"
                desc = f"Available to bind. (Configured in .env as {p['port']})"

        print(f"{name:<25} {port:<8} {state_str:<12} {desc}")

    print("==========================================================================")
    print("Assigned Configuration Summary:")
    print(f"  * Backend Prediction API:  http://{args.host}:{settings.fastapi_port}")
    print(f"  * Interactive Dashboard:   http://{args.host}:{settings.streamlit_port}")
    print(f"  * WebHDFS Data Lake:       {settings.hdfs_namenode_url}")
    print(f"  * Kafka Bootstrap Broker:  {settings.kafka_bootstrap_servers}")
    print("--------------------------------------------------------------------------")
    print("To reassign application ports, edit `.env` or set environment variables:")
    print("  FASTAPI_PORT=8000")
    print("  STREAMLIT_PORT=8501")
    print("==========================================================================\n")

    return 0 if all_good else 1


if __name__ == "__main__":
    sys.exit(main())
