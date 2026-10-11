"""
Kafka-to-HDFS Sink Pipeline.
Consumes events in bounded micro-batches from Apache Kafka, validates schema integrity,
stages batches in HDFS, finalizes with atomic renames, and commits Kafka offsets
strictly post-finalization to ensure durable at-least-once delivery with effectively-once downstream deduplication.
"""

from datetime import datetime, timezone
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from src.config import get_settings
from src.schema.events import ICUEvent
from src.storage.hdfs_client import HDFSClient


class KafkaHDFSSink:
    """
    Micro-batch consumer that lands Kafka events into partitioned HDFS lake storage
    with atomic staging and synchronous offset confirmation.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        hdfs_client: Optional[HDFSClient] = None,
        raw_hdfs_base: Optional[str] = None,
        dead_letter_base: str = "/dead_letter/malformed_events",
        auto_offset_reset: str = "earliest",
    ):
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_icu_events
        self.group_id = group_id or settings.kafka_consumer_group_id
        self.raw_hdfs_base = (raw_hdfs_base or settings.hdfs_raw_path).rstrip("/")
        self.dead_letter_base = dead_letter_base.rstrip("/")
        self.hdfs_client = hdfs_client or HDFSClient()

        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,  # Strict manual commit after HDFS durability
            "session.timeout.ms": 15000,
            "max.poll.interval.ms": 300000,
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])

        # Ensure staging and dead-letter directories exist in HDFS
        self.staging_dir = f"{self.raw_hdfs_base}/.staging"
        if self.hdfs_client.is_healthy():
            self.hdfs_client.mkdir(self.staging_dir)
            self.hdfs_client.mkdir(self.dead_letter_base)

    def _parse_message(self, msg: Message) -> Tuple[Optional[ICUEvent], Optional[dict]]:
        """
        Parses and validates a Kafka message payload.
        Returns (valid_event, None) or (None, malformed_info).
        """
        raw_bytes = msg.value()
        if not raw_bytes:
            return None, {"error": "Empty message payload", "raw": None}

        try:
            payload_str = raw_bytes.decode("utf-8")
            data = json.loads(payload_str)
            event = ICUEvent.model_validate(data)
            return event, None
        except Exception as e:
            return None, {
                "error": str(e),
                "raw_payload": raw_bytes.decode("utf-8", errors="replace"),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def process_batch(self, messages: List[Message]) -> Dict[str, Any]:
        """
        Processes a bounded batch of Kafka messages:
        1. Validates and segregates valid events from malformed payloads.
        2. Routes malformed messages to HDFS dead-letter quarantine.
        3. Writes valid events to temporary HDFS staging path.
        4. Atomically renames staging files to date-partitioned raw paths.
        5. Synchronously commits Kafka offsets only after verified HDFS finalization.
        """
        if not messages:
            return {"valid_count": 0, "malformed_count": 0, "batches_finalized": []}

        valid_events: List[ICUEvent] = []
        malformed_records: List[dict] = []

        for msg in messages:
            event, err = self._parse_message(msg)
            if event:
                valid_events.append(event)
            else:
                malformed_records.append(err)

        finalized_paths: List[str] = []

        # 1. Handle Dead-Letter Quarantine if malformed records present
        if malformed_records:
            dl_uuid = uuid4().hex
            dl_path = f"{self.dead_letter_base}/dead_letter_{dl_uuid}.jsonl"
            self.hdfs_client.write_jsonl(dl_path, malformed_records)
            print(f"[!] Quarantined {len(malformed_records)} malformed message(s) to HDFS: {dl_path}")

        # 2. Write and finalize valid events partitioned by date
        if valid_events:
            # Group events by year, month, day partition
            partition_map: Dict[str, List[dict]] = {}
            for ev in valid_events:
                dt = ev.event_timestamp
                part_key = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
                partition_map.setdefault(part_key, []).append(ev.to_dict())

            for part_key, records in partition_map.items():
                batch_uuid = uuid4().hex
                batch_filename = f"events_{batch_uuid}.jsonl"

                # Temporary staging path
                stage_file = f"{self.staging_dir}/stage_{batch_filename}"
                # Final target path
                target_dir = f"{self.raw_hdfs_base}/{part_key}"
                final_file = f"{target_dir}/{batch_filename}"

                # Ensure date directory exists
                self.hdfs_client.mkdir(target_dir)

                # Write to staging
                write_ok = self.hdfs_client.write_jsonl(stage_file, records, overwrite=True)
                if not write_ok:
                    raise IOError(f"Failed to write staging file to HDFS: {stage_file}")

                # Atomic Rename: stage_file -> final_file
                rename_ok = self.hdfs_client.rename(stage_file, final_file)
                if not rename_ok:
                    raise IOError(f"Atomic HDFS rename failed from {stage_file} to {final_file}")

                finalized_paths.append(final_file)

        # 3. Synchronous Kafka Offset Commit (only after durable HDFS finalization)
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            raise RuntimeError(f"Critical error committing Kafka offsets post-HDFS write: {e}")

        return {
            "valid_count": len(valid_events),
            "malformed_count": len(malformed_records),
            "batches_finalized": finalized_paths,
        }

    def poll_and_sink(
        self,
        max_messages: int = 500,
        poll_timeout: float = 1.0,
        idle_timeout: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Polls Kafka for messages up to max_messages or until idle_timeout expires,
        then processes the batch.

        :param max_messages: Maximum messages to consume in one micro-batch.
        :param poll_timeout: Timeout in seconds for individual poll calls.
        :param idle_timeout: Max seconds to wait with no messages before returning empty.
        :return: Processing summary dictionary.
        """
        messages: List[Message] = []
        start_time = time.time()
        last_msg_time = time.time()

        while len(messages) < max_messages:
            msg = self.consumer.poll(timeout=poll_timeout)

            if msg is None:
                if (time.time() - last_msg_time) >= idle_timeout:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached
                    continue
                else:
                    raise KafkaException(msg.error())

            messages.append(msg)
            last_msg_time = time.time()

        if not messages:
            return {"valid_count": 0, "malformed_count": 0, "batches_finalized": []}

        return self.process_batch(messages)

    def close(self) -> None:
        """Closes the Kafka consumer safely."""
        self.consumer.close()
