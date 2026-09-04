"""
Airflow DAG: shipment_pipeline

extract_raw -> transform_and_validate -> load_to_warehouse

Run daily. Idempotent (safe to retry): transform overwrites the day's
Parquet partition, and the fact table load uses ON CONFLICT DO NOTHING
keyed on (shipment_id, event_time, status).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent / "scripts"))

from generate_shipments import generate_batch
from transform import run as run_transform
from load_to_postgres import run as run_load

default_args = {
    "owner": "karthik",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 9, 1),
}

BASE_DIR = Path(__file__).resolve().parent.parent


def _extract(**context):
    execution_date = context["execution_date"]
    out_dir = BASE_DIR / "data" / "raw"
    generate_batch(n=1000, out_dir=out_dir, for_date=execution_date)


def _transform(**context):
    run_transform(for_date=context["execution_date"])


def _load(**context):
    run_load(for_date=context["execution_date"])


with DAG(
    dag_id="shipment_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["data-engineering", "shipment-tracking"],
) as dag:

    extract_raw = PythonOperator(
        task_id="extract_raw",
        python_callable=_extract,
    )

    transform_and_validate = PythonOperator(
        task_id="transform_and_validate",
        python_callable=_transform,
    )

    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=_load,
    )

    extract_raw >> transform_and_validate >> load_to_warehouse
