"""
Resilient Kafka Event Producer.
Publishes ICU ADT events to Kafka topic with partition key routing by unit.
"""

import json
from typing import Callable, Optional
from confluent_kafka import KafkaException, Producer

from src.config import get_settings
from src.schema.events import ICUEvent


class ICUKafkaProducer:
    """
    Publishes serialized ICUEvent records to Apache Kafka.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        on_delivery: Optional[Callable] = None,
    ):
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_icu_events
        self.on_delivery = on_delivery or self._default_delivery_report

        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "icu-event-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 250,
        }
        self.producer = Producer(self.conf)

    @staticmethod
    def _default_delivery_report(err, msg):
        """Default delivery report callback."""
        if err is not None:
            print(f"[!] Kafka delivery failed: {err}")

    def send_event(self, event: ICUEvent, flush_immediately: bool = False) -> None:
        """
        Publishes a single ICUEvent to Kafka.
        Uses primary unit (to_unit or from_unit) as partition key to maintain unit ordering.
        """
        key = (event.to_unit or event.from_unit or "GLOBAL").encode("utf-8")
        payload = json.dumps(event.to_dict()).encode("utf-8")

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            callback=self.on_delivery,
        )
        self.producer.poll(0)

        if flush_immediately:
            self.flush()

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for outstanding messages to be delivered."""
        return self.producer.flush(timeout=timeout)
