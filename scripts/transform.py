"""
Transform stage: raw JSON (Stage 1) -> validated, deduped, columnar Parquet (Stage 2/6).

This is the piece an Airflow task calls. Kept as plain functions (not classes)
so it's easy to unit-test and easy to explain line-by-line in an interview.
"""
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine"


def load_raw_events(for_date: datetime) -> pd.DataFrame:
    """Read all raw JSON-lines files for a given partition date into a DataFrame."""
    partition_dir = RAW_DIR / f"year={for_date.year}" / f"month={for_date.month:02d}" / f"day={for_date.day:02d}"
    if not partition_dir.exists():
        raise FileNotFoundError(f"No raw partition for {for_date.date()}: {partition_dir}")

    rows = []
    for file in partition_dir.glob("*.json"):
        with open(file) as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
    return pd.DataFrame(rows)


def validate_and_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split into (clean, quarantined) based on data-quality rules.
    This is the kind of check a real pipeline runs before trusting data downstream.
    """
    bad_mask = (
        df["shipment_id"].isna()
        | (df["weight_kg"] <= 0)
        | df["hub_code"].isna()
    )
    quarantined = df[bad_mask].copy()
    quarantined["quarantine_reason"] = quarantined.apply(_reason, axis=1)

    clean = df[~bad_mask].copy()
    return clean, quarantined


def _reason(row) -> str:
    reasons = []
    if pd.isna(row.get("shipment_id")):
        reasons.append("missing_shipment_id")
    if row.get("weight_kg", 0) <= 0:
        reasons.append("invalid_weight")
    if pd.isna(row.get("hub_code")):
        reasons.append("missing_hub_code")
    return ",".join(reasons)


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Drop exact duplicate events - same shipment_id + event_time + status."""
    before = len(df)
    df = df.drop_duplicates(subset=["shipment_id", "event_time", "status"])
    print(f"Deduped {before - len(df)} rows")
    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns useful for analytics downstream."""
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["event_date"] = df["event_time"].dt.date
    df["event_hour"] = df["event_time"].dt.hour
    df["is_delayed"] = df["status"] == "DELAYED"
    return df


def run(for_date: datetime = None):
    for_date = for_date or datetime.utcnow()

    print(f"Loading raw events for {for_date.date()}...")
    raw_df = load_raw_events(for_date)
    print(f"Loaded {len(raw_df)} raw rows")

    clean_df, quarantined_df = validate_and_split(raw_df)
    print(f"Clean: {len(clean_df)}, Quarantined: {len(quarantined_df)}")

    clean_df = dedupe(clean_df)
    clean_df = enrich(clean_df)

    # Write processed data as Parquet, partitioned by date - columnar format
    # means downstream analytical queries only scan the columns they need.
    processed_partition = (
        PROCESSED_DIR / f"year={for_date.year}" / f"month={for_date.month:02d}" / f"day={for_date.day:02d}"
    )
    processed_partition.mkdir(parents=True, exist_ok=True)
    out_file = processed_partition / "shipments.parquet"
    clean_df.to_parquet(out_file, index=False)
    print(f"Wrote {len(clean_df)} clean rows -> {out_file}")

    if len(quarantined_df) > 0:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        q_file = QUARANTINE_DIR / f"quarantine_{for_date.strftime('%Y%m%d')}.parquet"
        quarantined_df.to_parquet(q_file, index=False)
        print(f"Wrote {len(quarantined_df)} quarantined rows -> {q_file}")

    return out_file


if __name__ == "__main__":
    run()
