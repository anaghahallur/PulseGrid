"""
Reproducible ingestion script for MIMIC-IV Clinical Database Demo 2.2.
Downloads only the required publicly accessible ADT and ICU stay tables from PhysioNet,
verifies cryptographic checksums, documents schemas, and converts stays to canonical events.

Does not bypass credential restrictions for full MIMIC-IV 3.1.
Keeps all downloaded and parsed data outside Git.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import os
import sys
from typing import Dict, List, Optional
import pandas as pd
import requests

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.schema.events import DataSource, EventType, ICUEvent

# Official PhysioNet Demo Base URL
PHYSIONET_DEMO_BASE = "https://physionet.org/files/mimic-iv-demo/2.2"

# Only the core tables required for ICU occupancy tracking
TARGET_FILES = {
    "transfers.csv.gz": f"{PHYSIONET_DEMO_BASE}/hosp/transfers.csv.gz",
    "icustays.csv.gz": f"{PHYSIONET_DEMO_BASE}/icu/icustays.csv.gz",
    "admissions.csv.gz": f"{PHYSIONET_DEMO_BASE}/hosp/admissions.csv.gz",
    "patients.csv.gz": f"{PHYSIONET_DEMO_BASE}/hosp/patients.csv.gz",
    "SHA256SUMS.txt": f"{PHYSIONET_DEMO_BASE}/SHA256SUMS.txt",
}

# Standard mapping from MIMIC-IV careunit names to standardized identifiers
MIMIC_ICU_UNIT_MAP = {
    "Medical Intensive Care Unit (MICU)": "MICU",
    "Surgical Intensive Care Unit (SICU)": "SICU",
    "Coronary Care Unit (CCU)": "CCU",
    "Trauma SICU (TSICU)": "TSICU",
    "Cardiac Vascular Intensive Care Unit (CVICU)": "CSRU",
    "Medical/Surgical Intensive Care Unit (MICU/SICU)": "MICU",
    "Neuro Surgical Intensive Care Unit (Neuro SICU)": "SICU",
}


def compute_sha256(filepath: str) -> str:
    """Compute SHA-256 hash of a local file."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def download_file(url: str, dest_path: str) -> bool:
    """Download a file with HTTP streaming."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"[*] Downloading {os.path.basename(dest_path)} from PhysioNet...")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=32768):
                if chunk:
                    f.write(chunk)
        print(f"    [OK] Saved: {dest_path} ({os.path.getsize(dest_path):,} bytes)")
        return True
    except Exception as e:
        print(f"    [FAIL] Download failed for {url}: {e}")
        return False


def verify_checksums(download_dir: str) -> None:
    """Verify downloaded files against SHA256SUMS.txt if available."""
    sums_file = os.path.join(download_dir, "SHA256SUMS.txt")
    if not os.path.exists(sums_file):
        print("    [!] SHA256SUMS.txt not available to verify.")
        return

    # Parse checksums file
    expected_sums: Dict[str, str] = {}
    with open(sums_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                # Format: hash filename (sometimes path like hosp/transfers.csv.gz)
                hash_val, file_path = parts
                fname = os.path.basename(file_path)
                expected_sums[fname] = hash_val

    print("\n[*] Validating Cryptographic Integrity (SHA-256):")
    for fname, target_url in TARGET_FILES.items():
        if fname == "SHA256SUMS.txt":
            continue
        local_path = os.path.join(download_dir, fname)
        if os.path.exists(local_path):
            computed = compute_sha256(local_path)
            expected = expected_sums.get(fname)
            if expected and computed == expected:
                print(f"    [OK] {fname}: SHA-256 Match ({computed[:12]}...)")
            elif expected:
                print(f"    [!] {fname}: Hash mismatch! Expected {expected[:12]}..., Got {computed[:12]}...")
            else:
                print(f"    [OK] {fname}: Hash computed ({computed[:12]}...)")


def parse_mimic_events(download_dir: str) -> List[ICUEvent]:
    """
    Parses transfers.csv.gz to extract structured patient ICU transition events.
    Documents the date-shift deidentification constraint and standardizes unit labels.
    """
    transfers_path = os.path.join(download_dir, "transfers.csv.gz")
    if not os.path.exists(transfers_path):
        raise FileNotFoundError(f"Required table {transfers_path} missing.")

    df_tr = pd.read_csv(transfers_path)
    print(f"\n[*] Parsing transfers table ({len(df_tr):,} total hospital/ED moves)...")

    # Sort strictly by patient, encounter, and intime
    df_tr["intime"] = pd.to_datetime(df_tr["intime"])
    df_tr["outtime"] = pd.to_datetime(df_tr["outtime"])
    df_tr.sort_values(by=["subject_id", "hadm_id", "intime"], inplace=True)

    events: List[ICUEvent] = []

    # Map each hospitalization's sequence of moves
    grouped = df_tr.groupby(["subject_id", "hadm_id"])

    for (subject_id, hadm_id), group in grouped:
        patient_id = f"MIMIC-P-{subject_id}"
        encounter_id = f"MIMIC-ENC-{hadm_id}"

        prev_icu_unit: Optional[str] = None

        for _, row in group.iterrows():
            careunit = row["careunit"]
            mapped_unit = MIMIC_ICU_UNIT_MAP.get(str(careunit))
            intime = row["intime"]
            outtime = row["outtime"]

            # Transition 1: Entering an ICU from outside
            if mapped_unit and prev_icu_unit is None:
                events.append(
                    ICUEvent(
                        patient_id=patient_id,
                        encounter_id=encounter_id,
                        event_type=EventType.ADMISSION,
                        event_timestamp=intime.to_pydatetime(),
                        from_unit=None,
                        to_unit=mapped_unit,
                        data_source=DataSource.MIMIC_DEMO,
                        metadata={"raw_careunit": careunit, "transfer_id": int(row["transfer_id"])},
                    )
                )
                prev_icu_unit = mapped_unit

            # Transition 2: Direct transfer from one ICU to another ICU
            elif mapped_unit and prev_icu_unit and mapped_unit != prev_icu_unit:
                events.append(
                    ICUEvent(
                        patient_id=patient_id,
                        encounter_id=encounter_id,
                        event_type=EventType.TRANSFER,
                        event_timestamp=intime.to_pydatetime(),
                        from_unit=prev_icu_unit,
                        to_unit=mapped_unit,
                        data_source=DataSource.MIMIC_DEMO,
                        metadata={"raw_careunit": careunit, "transfer_id": int(row["transfer_id"])},
                    )
                )
                prev_icu_unit = mapped_unit

            # Transition 3: Exiting ICU to general floor or discharge
            elif not mapped_unit and prev_icu_unit:
                exit_time = intime if pd.notna(intime) else outtime
                if pd.notna(exit_time):
                    events.append(
                        ICUEvent(
                            patient_id=patient_id,
                            encounter_id=encounter_id,
                            event_type=EventType.DISCHARGE,
                            event_timestamp=exit_time.to_pydatetime(),
                            from_unit=prev_icu_unit,
                            to_unit=None,
                            data_source=DataSource.MIMIC_DEMO,
                            metadata={"to_non_icu": str(careunit)},
                        )
                    )
                    prev_icu_unit = None

        # If patient remained in ICU at end of record, close with final outtime
        if prev_icu_unit:
            last_out = group["outtime"].dropna().max()
            if pd.notna(last_out):
                events.append(
                    ICUEvent(
                        patient_id=patient_id,
                        encounter_id=encounter_id,
                        event_type=EventType.DISCHARGE,
                        event_timestamp=last_out.to_pydatetime(),
                        from_unit=prev_icu_unit,
                        to_unit=None,
                        data_source=DataSource.MIMIC_DEMO,
                        metadata={"auto_closed": True},
                    )
                )

    print(f"    [OK] Converted into {len(events):,} canonical ICU events.")
    return events


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Ingest MIMIC-IV Clinical Database Demo.")
    parser.add_argument(
        "--source",
        choices=["demo", "full"],
        default="demo",
        help="Data source mode. 'demo' uses the open subset (ODbL). 'full' requires PhysioNet credentials.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/mimic_demo",
        help="Local directory for downloaded demo data (excluded from Git).",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading if files are already present.",
    )
    args = parser.parse_args()

    if args.source == "full":
        print("[!] Error: Full MIMIC-IV 3.1 requires credentialed authorization.")
        print("    Please complete the CITI training and PhysioNet DUA as described in Phase 0.")
        sys.exit(1)

    print("============================================================")
    print("MIMIC-IV Demo Data Ingestion Pipeline")
    print("============================================================")
    print(f"Source:       MIMIC-IV Clinical Database Demo v2.2 (Open Access)")
    print(f"Target Dir:   {args.output_dir}")
    print(f"License:      Open Data Commons Open Database License v1.0 (ODbL)")
    print("------------------------------------------------------------")

    os.makedirs(args.output_dir, exist_ok=True)

    if not args.skip_download:
        for fname, url in TARGET_FILES.items():
            dest = os.path.join(args.output_dir, fname)
            download_file(url, dest)

    verify_checksums(args.output_dir)

    # Document Schemas
    print("\n[*] Documenting Table Schemas:")
    for fname in ["transfers.csv.gz", "icustays.csv.gz", "admissions.csv.gz", "patients.csv.gz"]:
        fpath = os.path.join(args.output_dir, fname)
        if os.path.exists(fpath):
            df_sample = pd.read_csv(fpath, nrows=3)
            print(f"    - {fname:<20} | Columns: {list(df_sample.columns)}")

    # Parse and export canonical events
    events = parse_mimic_events(args.output_dir)
    out_events_path = os.path.join(args.output_dir, "mimic_demo_events.jsonl")

    import json
    with open(out_events_path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev.to_dict()) + "\n")
    print(f"\n[OK] Ingestion complete. Parsed events saved to {out_events_path}")

    print("\n[IMPORTANT SCIENTIFIC NOTE ON TEMPORAL VALIDITY]:")
    print("MIMIC-IV patient records carry independent random date-shifts (between 2100-2200).")
    print("The 100 demo patients do not constitute a synchronous simultaneous hospital census.")
    print("Use this data strictly to validate CSV parsing, schema transformation, and transfer logic.")
    print("Use the synthetic dataset for time-series forecasting model training.")
    print("============================================================\n")


if __name__ == "__main__":
    main()
