"""
Demonstration script for Vital Signs Streaming Ingestion and Kafka-to-HDFS Pipeline.
Executes the complete multi-parameter vital signs pipeline:
Synthetic Vitals Generator -> Kafka Topic (icu-vitals) -> KafkaVitalsHDFSSink -> Raw HDFS Lake (/raw/icu_vitals) -> Verification Audit.
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
from src.schema.events import VitalSignEvent
from src.producer.vitals_simulator import VitalSignSimulator
from src.producer.vitals_producer import ICUVitalsProducer
from src.storage.hdfs_client import HDFSClient
from src.storage.kafka_sink import KafkaVitalsHDFSSink


def parse_args():
    parser = argparse.ArgumentParser(
        description="Demonstrate live Kafka-to-HDFS vital signs streaming pipeline."
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=50,
        help="Number of synthetic vital sign events to stream (default: 50).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=25.0,
        help="Publish rate in events per second (default: 25.0).",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default=None,
        help="Custom isolated run namespace/identifier.",
    )
    return parser.parse_args()


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = parse_args()
    settings = get_settings()

    run_id = args.namespace or f"demo_vitals_{uuid4().hex[:8]}"
    raw_hdfs_dir = f"/demo_{run_id}/raw_vitals"
    dead_letter_dir = f"/demo_{run_id}/dead_letter_vitals"
    test_topic = f"icu-vitals-{run_id}"
    consumer_group = f"vitals-sink-group-{run_id}"

    print("======================================================================")
    print("ICU Patient Vital Signs - Streaming Ingestion & HDFS Persistence Demo")
    print("======================================================================")
    print(f"Run ID / Namespace:   {run_id}")
    print(f"Target Events:        {args.num_events}")
    print(f"Streaming Topic:      {test_topic}")
    print(f"Consumer Group:       {consumer_group}")
    print(f"Raw HDFS Base:        {raw_hdfs_dir}")
    print(f"Dead Letter Base:     {dead_letter_dir}")
    print("----------------------------------------------------------------------")

    # Step 1: Healthcheck
    hdfs = HDFSClient(base_url=settings.hdfs_namenode_url, user=settings.hdfs_user)
    if not hdfs.is_healthy():
        print(f"[FAIL] HDFS NameNode at {settings.hdfs_namenode_url} is unreachable.")
        sys.exit(1)
    print("[OK] Hadoop HDFS NameNode is reachable and healthy.")

    # Step 2: Generate Synthetic Vital Signs
    print(f"\n[1/4] Generating {args.num_events} realistic synthetic VitalSignEvent records...")
    sim = VitalSignSimulator(
        random_seed=101,
        units=settings.icu_unit_capacities,
        base_patient_count=15,
    )
    start_time = datetime(2026, 10, 14, 10, 0, tzinfo=timezone.utc)
    stream_events = sim.generate_events(
        num_events=args.num_events,
        start_time=start_time,
        interval_seconds=30.0,
        simulate_partial_channels=True,
    )
    print(f"      [OK] Generated {len(stream_events)} chronologically sorted vital sign observations.")

    # Step 3: Stream to Kafka Topic
    print(f"\n[2/4] Streaming vital sign events to Kafka topic '{test_topic}' at ~{args.rate} events/sec...")
    producer = ICUVitalsProducer(
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
    print(f"\n[3/4] Starting KafkaVitalsHDFSSink (Consuming and landing to HDFS)...")
    sink = KafkaVitalsHDFSSink(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        group_id=consumer_group,
        hdfs_client=hdfs,
        raw_hdfs_base=raw_hdfs_dir,
        dead_letter_base=dead_letter_dir,
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
    print(f"      [OK] Consumed {valid_count} valid vital sign records from Kafka.")
    print(f"      [OK] Finalized {len(batches)} date-partitioned batch file(s) in HDFS:")
    for b in batches:
        print(f"           • {b}")

    # Step 5: Read back and Verify Raw HDFS Records
    print(f"\n[4/4] Reading back raw JSONL records from HDFS for schema & count audit...")
    raw_events_recovered: list[VitalSignEvent] = []
    for b in batches:
        file_records = hdfs.read_jsonl(b)
        for r in file_records:
            raw_events_recovered.append(VitalSignEvent.model_validate(r))

    print(f"      [OK] Successfully deserialized {len(raw_events_recovered)} VitalSignEvent records from HDFS.")

    # Compute descriptive vital statistics
    hr_vals = [e.heart_rate for e in raw_events_recovered if e.heart_rate is not None]
    temp_vals = [e.temperature_c for e in raw_events_recovered if e.temperature_c is not None]
    sbp_vals = [e.systolic_bp for e in raw_events_recovered if e.systolic_bp is not None]
    dbp_vals = [e.diastolic_bp for e in raw_events_recovered if e.diastolic_bp is not None]
    spo2_vals = [e.spo2 for e in raw_events_recovered if e.spo2 is not None]

    print(f"\n----------------------------------------------------------------------")
    print(f"VITAL SIGNS STREAMING PIPELINE VERIFICATION SUMMARY:")
    print(f"----------------------------------------------------------------------")
    print(f"Events Emitted to Kafka:         {published}")
    print(f"Events Ingested by Sink:         {valid_count}")
    print(f"Events Deserialized from HDFS:   {len(raw_events_recovered)}")
    print(f"HDFS Batch Files Created:        {len(batches)}")
    print(f"Unique Encounters Monitored:     {len(set(e.encounter_id for e in raw_events_recovered))}")
    print(f"Unique Units Monitored:          {sorted(list(set(e.unit_id for e in raw_events_recovered)))}")
    print(f"\nPhysiological Telemetry Summary (Synthetic Demonstration):")
    if hr_vals:
        print(f"  • Heart Rate:       Mean {sum(hr_vals)/len(hr_vals):.1f} bpm (Min: {min(hr_vals):.1f}, Max: {max(hr_vals):.1f}) [N={len(hr_vals)}]")
    if temp_vals:
        print(f"  • Temperature:      Mean {sum(temp_vals)/len(temp_vals):.2f} °C (Min: {min(temp_vals):.1f}, Max: {max(temp_vals):.1f}) [N={len(temp_vals)}]")
    if sbp_vals and dbp_vals:
        print(f"  • Blood Pressure:   Mean {sum(sbp_vals)/len(sbp_vals):.0f}/{sum(dbp_vals)/len(dbp_vals):.0f} mmHg [N={len(sbp_vals)}]")
    if spo2_vals:
        print(f"  • SpO2 Saturation:  Mean {sum(spo2_vals)/len(spo2_vals):.1f}% (Min: {min(spo2_vals):.1f}%, Max: {max(spo2_vals):.1f}%) [N={len(spo2_vals)}]")

    print(f"\nSample Vital Sign Observations (First 3 Records):")
    for idx, ev in enumerate(raw_events_recovered[:3], 1):
        bp_str = f"{ev.systolic_bp:.0f}/{ev.diastolic_bp:.0f}" if (ev.systolic_bp and ev.diastolic_bp) else "N/A"
        temp_str = f"{ev.temperature_c:.1f}°C" if ev.temperature_c else "N/A"
        print(f"  [{idx}] {ev.event_timestamp.strftime('%H:%M:%S UTC')} | Unit: {ev.unit_id:<6} | Patient: {ev.patient_id} | HR: {ev.heart_rate} bpm | BP: {bp_str} | SpO2: {ev.spo2}% | Temp: {temp_str}")

    print(f"\n[NOTICE - Clinical Limitations]:")
    print("Vital sign telemetry streams are synthetic and developed strictly for Big Data")
    print("Analytics streaming ingestion and HDFS lake persistence demonstration.")
    print("They ARE NOT currently joined into the ICU bed occupancy forecaster models.")
    print(f"----------------------------------------------------------------------")
    print(f"[SUCCESS] Vital signs streaming pipeline executed and verified cleanly!")
    print("======================================================================\n")


if __name__ == "__main__":
    main()
