"""
Shipment event generator.

Simulates a logistics company's shipment tracking events - the kind of raw
data a pipeline would ingest from hub scanners / IoT devices / partner APIs.

Run modes:
  python generate_shipments.py --mode batch   -> writes one partitioned batch (a "day" of data)
  python generate_shipments.py --mode stream  -> prints events one at a time with a delay (simulates streaming)
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    fake = None

HUBS = [
    ("HUB_HYD", "Hyderabad"), ("HUB_BLR", "Bangalore"), ("HUB_DEL", "Delhi"),
    ("HUB_MUM", "Mumbai"), ("HUB_CHN", "Chennai"), ("HUB_PUN", "Pune"),
]
PARTNERS = ["BlueDart", "Delhivery", "XPO_Direct", "Ekart", "DTDC"]
STATUSES = ["PICKED_UP", "IN_TRANSIT", "AT_HUB", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"]

# Weighted so DELIVERED/IN_TRANSIT dominate but DELAYED shows up enough to analyze
STATUS_WEIGHTS = [0.15, 0.25, 0.20, 0.15, 0.20, 0.05]


def random_shipment_event(base_time: datetime) -> dict:
    hub_code, hub_city = random.choice(HUBS)
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    # Inject occasional bad data on purpose - this is what your data-quality
    # stage (Stage 6) will need to catch. Real pipelines are never clean.
    shipment_id = str(uuid.uuid4()) if random.random() > 0.02 else None  # 2% missing ids
    weight_kg = round(random.uniform(0.2, 30.0), 2) if random.random() > 0.03 else -1  # bad weight

    event = {
        "shipment_id": shipment_id,
        "event_time": (base_time + timedelta(minutes=random.randint(0, 1439))).isoformat(),
        "hub_code": hub_code,
        "hub_city": hub_city,
        "delivery_partner": random.choice(PARTNERS),
        "status": status,
        "weight_kg": weight_kg,
        "destination_pincode": fake.postcode() if fake else str(random.randint(100000, 999999)),
    }
    return event


def generate_batch(n: int, out_dir: Path, for_date: datetime):
    """Write one day's worth of shipment events, partitioned by date (Hive-style)."""
    partition_dir = out_dir / f"year={for_date.year}" / f"month={for_date.month:02d}" / f"day={for_date.day:02d}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    events = [random_shipment_event(for_date) for _ in range(n)]

    # Duplicate a few events on purpose - dedup is a real pipeline task
    events += random.sample(events, k=max(1, n // 50))

    out_file = partition_dir / f"shipments_{for_date.strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(f"Wrote {len(events)} events -> {out_file}")
    return out_file


def stream_events(delay_seconds: float):
    """Simulate a streaming source - prints one JSON event per line, like a Kafka consumer would."""
    print("Streaming shipment events (Ctrl+C to stop)...")
    try:
        while True:
            event = random_shipment_event(datetime.utcnow())
            print(json.dumps(event))
            time.sleep(delay_seconds)
    except KeyboardInterrupt:
        print("\nStream stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
    parser.add_argument("--n", type=int, default=1000, help="number of events for batch mode")
    parser.add_argument("--days-back", type=int, default=0, help="generate for N days ago (0 = today)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between events in stream mode")
    args = parser.parse_args()

    out_dir = Path(__file__).resolve().parent.parent / "data" / "raw"

    if args.mode == "batch":
        for_date = datetime.utcnow() - timedelta(days=args.days_back)
        generate_batch(args.n, out_dir, for_date)
    else:
        stream_events(args.delay)
