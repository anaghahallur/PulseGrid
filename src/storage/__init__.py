"""Storage module exports."""
from src.storage.hdfs_client import HDFSClient

try:
    from src.storage.kafka_sink import KafkaHDFSSink, KafkaVitalsHDFSSink
except ImportError:  # pragma: no cover
    KafkaHDFSSink = None  # type: ignore
    KafkaVitalsHDFSSink = None  # type: ignore

__all__ = ["HDFSClient", "KafkaHDFSSink", "KafkaVitalsHDFSSink"]
