"""
Resilient Kafka Vital Signs Event Producer.
Publishes patient VitalSignEvent telemetry records to Kafka topic 'icu-vitals'
with encounter-level partition key routing, strict idempotence, and delivery tracking.
"""

import json
import logging
import time
from typing import Callable, List, Optional
from confluent_kafka import KafkaError, KafkaException, Producer

from src.config import get_settings
from src.schema.events import VitalSignEvent

logger = logging.getLogger(__name__)


class ICUVitalsProducer:
    """
    Publishes serialized VitalSignEvent records to Apache Kafka.
    Maintains encounter-level partition ordering and delivery confirmations.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        key_strategy: str = "encounter",  # "encounter", "patient", or "unit"
        on_delivery: Optional[Callable] = None,
    ):
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_icu_vitals
        self.key_strategy = key_strategy
        self.custom_callback = on_delivery

        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "icu-vitals-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 250,
            "enable.idempotence": True,
        }
        self.producer = Producer(self.conf)
        self.failed_deliveries: List[dict] = []
        self.delivered_count = 0

    def _delivery_report(self, err, msg):
        """Internal callback tracking delivery success and failures."""
        if err is not None:
            err_info = {
                "error": str(err),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "topic": msg.topic(),
                "partition": msg.partition(),
            }
            self.failed_deliveries.append(err_info)
            logger.error("Kafka vitals delivery failed: %s", err)
            print(f"[!] Kafka vitals delivery failed: {err}")
        else:
            self.delivered_count += 1

        if self.custom_callback:
            self.custom_callback(err, msg)

    def _get_partition_key(self, event: VitalSignEvent) -> bytes:
        """
        Determines the partition routing key.
        - 'encounter': Routes vitals for the same encounter to the exact same partition in FIFO order.
        - 'patient': Routes by patient ID.
        - 'unit': Routes by ICU unit.
        """
        if self.key_strategy == "encounter":
            return event.encounter_id.encode("utf-8")
        elif self.key_strategy == "patient":
            return event.patient_id.encode("utf-8")
        elif self.key_strategy == "unit":
            return event.unit_id.encode("utf-8")
        return event.encounter_id.encode("utf-8")

    def send_event(self, event: VitalSignEvent, flush_immediately: bool = False) -> None:
        """
        Publishes a single validated VitalSignEvent to Kafka.
        """
        key = self._get_partition_key(event)
        payload = json.dumps(event.to_dict()).encode("utf-8")

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            callback=self._delivery_report,
        )
        self.producer.poll(0)

        if flush_immediately:
            self.flush()

    def publish_stream(
        self,
        events: List[VitalSignEvent],
        events_per_second: Optional[float] = None,
        max_events: Optional[int] = None,
        raise_on_error: bool = True,
    ) -> int:
        """
        Publishes a stream of vital sign events with optional rate throttling and event limiting.

        :param events: List of VitalSignEvent objects to publish.
        :param events_per_second: If set, sleeps between messages to simulate streaming arrival.
        :param max_events: If set, publishes at most this many events.
        :param raise_on_error: If True, raises RuntimeError if any message fails delivery.
        :return: Count of successfully published and flushed events.
        """
        published = 0
        limit = max_events if max_events is not None else len(events)
        delay = (1.0 / events_per_second) if (events_per_second and events_per_second > 0) else 0.0

        for idx, event in enumerate(events):
            if idx >= limit:
                break

            self.send_event(event)
            published += 1

            if delay > 0:
                time.sleep(delay)

        unflushed = self.flush(timeout=10.0)
        if unflushed > 0:
            msg = f"Kafka vitals producer failed to flush {unflushed} messages within timeout."
            if raise_on_error:
                raise KafkaException(KafkaError(KafkaError._TIMED_OUT, msg))
            logger.warning(msg)

        if raise_on_error and self.failed_deliveries:
            raise RuntimeError(f"{len(self.failed_deliveries)} vital sign message(s) failed delivery to Kafka.")

        return published

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all outstanding messages to be delivered."""
        return self.producer.flush(timeout=timeout)

    def close(self, timeout: float = 10.0) -> None:
        """Flush remaining messages and close producer."""
        self.flush(timeout=timeout)
