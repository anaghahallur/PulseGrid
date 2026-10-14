"""
Application configuration module using Pydantic Settings.
Loads environment variables safely without hardcoding credentials or secrets.
"""

from functools import lru_cache
from typing import Dict
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """System-wide configuration settings with environment override support."""

    # Kafka Broker & Topics
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers comma-separated",
    )
    kafka_topic_icu_events: str = Field(
        default="icu-events",
        description="Primary topic for ICU ADT events",
    )
    kafka_topic_icu_vitals: str = Field(
        default="icu-vitals",
        description="Topic for ICU patient vital signs telemetry",
    )
    kafka_consumer_group_id: str = Field(
        default="icu-hdfs-sink-group",
        description="Consumer group ID for HDFS sink writer",
    )
    kafka_vitals_consumer_group_id: str = Field(
        default="icu-vitals-hdfs-sink-group",
        description="Consumer group ID for vitals HDFS sink writer",
    )
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        description="Kafka consumer offset reset policy",
    )

    # Hadoop HDFS
    hdfs_namenode_url: str = Field(
        default="http://localhost:9870",
        description="WebHDFS REST API endpoint for NameNode",
    )
    hdfs_namenode_rpc: str = Field(
        default="hdfs://localhost:9000",
        description="HDFS IPC connection URI",
    )
    hdfs_user: str = Field(
        default="root",
        description="HDFS user for WebHDFS operations",
    )
    hdfs_raw_path: str = Field(
        default="/raw/icu_events",
        description="HDFS path for raw ingested event batches",
    )
    hdfs_vitals_raw_path: str = Field(
        default="/raw/icu_vitals",
        description="HDFS path for raw ingested vital sign event batches",
    )
    hdfs_processed_path: str = Field(
        default="/processed/icu_occupancy_hourly",
        description="HDFS path for processed hourly census time series",
    )

    # API & Dashboard
    fastapi_host: str = Field(default="0.0.0.0", description="FastAPI host binding")
    fastapi_port: int = Field(default=8000, description="FastAPI port")
    streamlit_port: int = Field(default=8501, description="Streamlit dashboard port")

    # Simulation & Ingestion
    simulation_random_seed: int = Field(
        default=42, description="Random seed for reproducible generation"
    )
    simulation_default_days: int = Field(
        default=30, description="Default span of simulated history in days"
    )
    data_spool_dir: str = Field(
        default="data/spool",
        description="Local disk spool directory for temporary network retries",
    )

    # ICU Units & Default Capacities (Beds)
    icu_unit_capacities: Dict[str, int] = Field(
        default_factory=lambda: {
            "MICU": 24,   # Medical Intensive Care Unit
            "SICU": 20,   # Surgical Intensive Care Unit
            "CCU": 16,    # Coronary Care Unit
            "TSICU": 16,  # Trauma-Surgical Intensive Care Unit
            "CSRU": 18,   # Cardiac Surgery Recovery Unit
        },
        description="Mapping of supported ICU unit identifiers to bed capacities",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Retrieve cached application settings instance."""
    return Settings()
