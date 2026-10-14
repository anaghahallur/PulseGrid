"""Producer module exports."""
from src.producer.simulator import ICUSimulator
from src.producer.vitals_simulator import VitalSignSimulator

try:
    from src.producer.kafka_producer import ICUKafkaProducer
    from src.producer.vitals_producer import ICUVitalsProducer
except ImportError:  # pragma: no cover
    ICUKafkaProducer = None  # type: ignore
    ICUVitalsProducer = None  # type: ignore

__all__ = ["ICUSimulator", "ICUKafkaProducer", "VitalSignSimulator", "ICUVitalsProducer"]
