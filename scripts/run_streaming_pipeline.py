"""
Demonstration script for Phase 2: Live Streaming Ingestion and Kafka-to-HDFS Pipeline.
Executes the complete event pipeline:
Synthetic Generator -> Kafka Topic -> KafkaHDFSSink -> Raw HDFS Lake -> Occupancy Reconstruction -> Processed HDFS Parquet.
"""

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import time
from uuid import uuid4

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import get_settings
from src.schema.events import ICUEvent
from src.producer.simulator import ICUSimulator
from src.producer.kafka_producer import ICUKafkaProducer
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaHDFSSink
from src.processing.occupancy_calculator import OccupancyCalculator


def parse_args():
    parser = argparse.ArgumentParser(description="Demonstrate live Kafka-to-HDFS streaming and occupancy pipeline.")
    parser.add_argument("--num-events", type=int, default=50, help="Number of synthetic events to stream (default: 50).")
    parser.add_argument("--rate", type=float, default=25.0, help="Publish rate in events per second (default: 25.0).")
    parser.add_argument("--namespace", type=str, default=None, help="Custom isolated HDFS namespace.")
    return parser.parse_args()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = parse_args()
    settings = get_settings()

    run_id = args.namespace or f"demo_{uuid4().hex[:8]}"
    raw_hdfs_dir = f"/demo_{run_id}/raw"
    proc_hdfs_dir = f"/demo_{run_id}/processed"
    test_topic = f"icu-stream-{run_id}"
    consumer_group = f"sink-group-{run_id}"

    print("======================================================================")
    print("ICU Bed Demand Forecasting - Phase 2 Streaming Pipeline Demonstration")
    print("======================================================================")
    print(f"Run ID / Namespace:   {run_id}")
    print(f"Target Events:        {args.num_events}")
    print(f"Streaming Topic:      {test_topic}")
    print(f"Consumer Group:       {consumer_group}")
    print(f"Raw HDFS Base:        {raw_hdfs_dir}")
    print(f"Processed HDFS Base:  {proc_hdfs_dir}")
    print("----------------------------------------------------------------------")

    # Step 1: Healthcheck
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    if not hdfs.is_healthy():
        print(f"[FAIL] HDFS NameNode at {settings.hdfs_namenode_url} is unreachable.")
        sys.exit(1)
    print("[OK] Hadoop HDFS NameNode is reachable and healthy.")

    # Step 2: Generate Bounded Synthetic Event Stream
    print(f"\n[1/5] Generating {args.num_events} realistic synthetic ADT events...")
    start_time = datetime(2026, 10, 11, 8, 0, tzinfo=timezone.utc)
    sim = ICUSimulator(
        start_date=start_time,
        days=5,
        random_seed=101,
        units=settings.icu_unit_capacities,
    )
    all_events = sim.generate_events()
    stream_events = all_events[:args.num_events]
    print(f"      [OK] Sliced {len(stream_events)} chronologically sorted events.")

    # Step 3: Stream to Kafka Topic
    print(f"\n[2/5] Streaming events to Kafka topic '{test_topic}' at ~{args.rate} events/sec...")
    producer = ICUKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        key_strategy="encounter",
    )
    t0 = time.time()
    published = producer.publish_stream(
        events=stream_events,
        events_per_second=args.rate,
        raise_on_error=True,
    )
    elapsed_pub = time.time() - t0
    producer.close()
    print(f"      [OK] Published and flushed {published} events in {elapsed_pub:.2f}s.")

    # Step 4: Run Kafka-to-HDFS Sink
    print(f"\n[3/5] Starting Kafka-to-HDFS Sink (Consuming and writing to HDFS)...")
    sink = KafkaHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=consumer_group,
        hdfs_client=hdfs,
        raw_hdfs_base=raw_hdfs_dir,
        auto_offset_reset="earliest",
    )
    sink_result = sink.poll_and_sink(
        max_messages=args.num_events,
        poll_timeout=1.0,
        idle_timeout=5.0,
    )
    sink.close()

    valid_count = sink_result["valid_count"]
    batches = sink_result["batches_finalized"]
    print(f"      [OK] Consumed {valid_count} messages from Kafka.")
    print(f"      [OK] Finalized {len(batches)} batch file(s) in HDFS:")
    for b in batches:
        print(f"           • {b}")

    # Step 5: Read back and Verify Raw HDFS Records
    print(f"\n[4/5] Reading back raw JSONL records from HDFS for audit...")
    raw_events_recovered = []
    for b in batches:
        file_records = hdfs.read_jsonl(b)
        for r in file_records:
            raw_events_recovered.append(ICUEvent.model_validate(r))
    print(f"      [OK] Successfully deserialized {len(raw_events_recovered)} events from HDFS.")

    # Step 6: Reconstruct Hourly Occupancy & Persist to HDFS
    print(f"\n[5/5] Reconstructing hourly occupancy census and persisting to HDFS...")
    calculator = OccupancyCalculator(unit_capacities=settings.icu_unit_capacities)
    df_occupancy = calculator.reconstruct(raw_events_recovered)

    proc_file = f"hourly_census_{run_id}.parquet"
    final_proc_path = calculator.persist_occupancy_to_hdfs(
        df=df_occupancy,
        hdfs_client=hdfs,
        hdfs_target_dir=proc_hdfs_dir,
        filename=proc_file,
    )
    print(f"      [OK] Processed census persisted atomically to: {final_proc_path}")

    # Step 7: Verify Processed HDFS Parquet Readback
    df_readback = hdfs.read_parquet(final_proc_path)
    print(f"\n----------------------------------------------------------------------")
    print(f"PIPELINE VERIFICATION SUMMARY:")
    print(f"----------------------------------------------------------------------")
    print(f"Raw Events in Kafka/HDFS:        {len(raw_events_recovered)}")
    print(f"Reconstructed Census Rows:       {len(df_readback)}")
    print(f"Time Horizon:                    {df_readback['timestamp'].min()} -> {df_readback['timestamp'].max()}")
    print(f"Capacity Exceedance Hours:       {int(df_readback['is_capacity_exceeded'].sum())}")
    print(f"\nSample Reconstructed Hourly Census (First 5 Rows):")
    sample_cols = ["timestamp", "unit_id", "occupied_beds", "capacity", "available_beds", "occupancy_rate", "is_capacity_exceeded"]
    print(df_readback[sample_cols].head(5).to_string(index=False))
    print(f"----------------------------------------------------------------------")
    print(f"[SUCCESS] Complete streaming ingestion and HDFS persistence pipeline verified!")
    print("======================================================================\n")


if __name__ == "__main__":
    main()
