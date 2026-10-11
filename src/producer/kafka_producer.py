"""
Resilient Kafka Event Producer.
Publishes ICU ADT events to Kafka topic with partition key routing by encounter/patient,
guaranteeing strict FIFO event ordering per encounter.
"""

import json
import time
from typing import Callable, List, Optional
from confluent_kafka import KafkaError, KafkaException, Producer

from src.config import get_settings
from src.schema.events import ICUEvent


class ICUKafkaProducer:
    """
    Publishes serialized ICUEvent records to Apache Kafka with delivery confirmations,
    rate throttling, and encounter-level partition key routing.
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
        self.topic = topic or settings.kafka_topic_icu_events
        self.key_strategy = key_strategy
        self.custom_callback = on_delivery

        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "icu-event-producer",
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
            print(f"[!] Kafka delivery failed: {err}")
        else:
            self.delivered_count += 1

        if self.custom_callback:
            self.custom_callback(err, msg)

    def _get_partition_key(self, event: ICUEvent) -> bytes:
        """
        Determines the partition routing key.
        - 'encounter': Guarantees all transitions for an encounter (admit, transfer, discharge)
          arrive at the exact same partition in strict FIFO order.
        - 'patient': Guarantees all encounters for a given patient are ordered.
        - 'unit': Routes by destination or origin unit.
        """
        if self.key_strategy == "encounter":
            return event.encounter_id.encode("utf-8")
        elif self.key_strategy == "patient":
            return event.patient_id.encode("utf-8")
        elif self.key_strategy == "unit":
            return (event.to_unit or event.from_unit or "GLOBAL").encode("utf-8")
        return event.encounter_id.encode("utf-8")

    def send_event(self, event: ICUEvent, flush_immediately: bool = False) -> None:
        """
        Publishes a single validated ICUEvent to Kafka.
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
        events: List[ICUEvent],
        events_per_second: Optional[float] = None,
        max_events: Optional[int] = None,
        raise_on_error: bool = True,
    ) -> int:
        """
        Publishes an iterable of events with optional rate throttling and event limiting.

        :param events: List of ICUEvent objects to publish.
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
            msg = f"Kafka producer failed to flush {unflushed} messages within timeout."
            if raise_on_error:
                raise KafkaException(KafkaError(KafkaError._TIMED_OUT, msg))
            print(f"[!] {msg}")

        if raise_on_error and self.failed_deliveries:
            raise RuntimeError(f"{len(self.failed_deliveries)} message(s) failed delivery to Kafka.")

        return published

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all outstanding messages to be delivered."""
        return self.producer.flush(timeout=timeout)

    def close(self, timeout: float = 10.0) -> None:
        """Flush remaining messages and close producer."""
        self.flush(timeout=timeout)
