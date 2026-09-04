"""
Load stage: processed Parquet (Stage 3) -> star schema in Postgres (Stage 4).

Loads into dim_hub, dim_partner, dim_date, and fact_shipment_events.
Uses simple upserts so re-running the DAG for the same day is idempotent -
a real pipeline requirement (Airflow tasks WILL be retried).
"""
from datetime import datetime
from pathlib import Path
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"


DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "localhost"),
    port=5432,
    dbname="shipments",
    user="postgres",
    password="postgres",
)

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def upsert_dim_hub(conn, df: pd.DataFrame):
    hubs = df[["hub_code", "hub_city"]].drop_duplicates()
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_hub (hub_code, hub_city)
            VALUES %s
            ON CONFLICT (hub_code) DO UPDATE SET hub_city = EXCLUDED.hub_city
            """,
            list(hubs.itertuples(index=False, name=None)),
        )
    conn.commit()


def upsert_dim_partner(conn, df: pd.DataFrame):
    partners = df[["delivery_partner"]].drop_duplicates()
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_partner (partner_name)
            VALUES %s
            ON CONFLICT (partner_name) DO NOTHING
            """,
            list(partners.itertuples(index=False, name=None)),
        )
    conn.commit()


def upsert_dim_date(conn, df: pd.DataFrame):
    dates = df["event_date"].drop_duplicates()
    rows = [
        (d, d.isocalendar()[1], d.month, d.year, d.weekday() >= 5)
        for d in dates
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_date (date_key, week_of_year, month, year, is_weekend)
            VALUES %s
            ON CONFLICT (date_key) DO NOTHING
            """,
            rows,
        )
    conn.commit()


def load_fact(conn, df: pd.DataFrame):
    with conn.cursor() as cur:
        cur.execute("SELECT hub_id, hub_code FROM dim_hub")
        hub_map = {code: hid for hid, code in cur.fetchall()}
        cur.execute("SELECT partner_id, partner_name FROM dim_partner")
        partner_map = {name: pid for pid, name in cur.fetchall()}

    rows = [
        (
            row.shipment_id,
            hub_map[row.hub_code],
            partner_map[row.delivery_partner],
            row.event_date,
            row.event_time,
            row.status,
            row.weight_kg,
            row.destination_pincode,
            bool(row.is_delayed),
        )
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fact_shipment_events
                (shipment_id, hub_id, partner_id, date_key, event_time,
                 status, weight_kg, destination_pincode, is_delayed)
            VALUES %s
            ON CONFLICT (shipment_id, event_time, status) DO NOTHING
            """,
            rows,
        )
    conn.commit()


def run(for_date: datetime = None):
    for_date = for_date or datetime.utcnow()
    partition = PROCESSED_DIR / f"year={for_date.year}" / f"month={for_date.month:02d}" / f"day={for_date.day:02d}"
    parquet_file = partition / "shipments.parquet"

    print(f"Loading {parquet_file} into Postgres...")
    df = pd.read_parquet(parquet_file)

    conn = get_conn()
    try:
        upsert_dim_hub(conn, df)
        upsert_dim_partner(conn, df)
        upsert_dim_date(conn, df)
        load_fact(conn, df)
        print(f"Loaded {len(df)} rows into fact_shipment_events")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
