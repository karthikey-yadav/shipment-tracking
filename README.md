# Shipment Tracking Data Pipeline

A logistics data pipeline that simulates shipment tracking events, cleans and
validates them, models them into a star schema, and loads them into Postgres
for analytics — orchestrated end-to-end with Apache Airflow.

Built to learn (and demonstrate) core data engineering concepts hands-on:
batch ingestion, data quality validation, partitioning, columnar storage,
dimensional modeling, idempotent loads, and pipeline orchestration.

## Architecture

```
generate_shipments.py  -->  raw/ (partitioned JSON)         [Ingestion]
        |
transform.py            -->  processed/ (Parquet, cleaned)  [ETL + Data Quality]
        |                     quarantine/ (bad records)
load_to_postgres.py     -->  Postgres star schema            [Warehouse]
        |
analytics_queries.sql   -->  SQL analysis                    [Serving]

Orchestrated daily by Airflow DAG: extract_raw >> transform_and_validate >> load_to_warehouse
```

## Pipeline running in Airflow

The DAG triggered manually, running all three tasks in sequence:

![DAG triggered](screenshots/dag-triggered.png)

Run history showing all three tasks (`extract_raw`, `transform_and_validate`,
`load_to_warehouse`) completing successfully end-to-end:

![DAG run history](screenshots/dag-run-history.png)

*(The two earlier failed runs visible in the history were caused by a
Docker networking issue — see "A real bug I hit and fixed" below.)*

## Quick start

```bash
pip install -r requirements.txt

# 1. Generate a day of raw data
python scripts/generate_shipments.py --mode batch --n 2000

# 2. Transform + validate
python scripts/transform.py

# 3. Spin up Postgres and Airflow
docker-compose up -d

# 4. Load into the warehouse (run manually, or let Airflow do it)
python scripts/load_to_postgres.py

# 5. Run analytics queries
docker exec -it shipment_postgres psql -U postgres -d shipments -f sql/analytics_queries.sql
```

**Airflow UI:** http://localhost:8080 — unpause and trigger the `shipment_pipeline` DAG to run the whole thing end-to-end on a schedule.

## What this project demonstrates

- **Batch vs streaming**: `generate_shipments.py --mode stream` shows the streaming-source
  mental model vs the batch mode used by the DAG.
- **Partitioning**: raw and processed data both partitioned by `year/month/day` (Hive-style) —
  speeds up queries that filter by date.
- **File formats**: raw = JSON (row-based, easy to append), processed = Parquet
  (columnar, compressed) — trade-off between write simplicity and read performance.
- **Data quality**: `transform.py` deliberately catches missing IDs, invalid
  weights, and duplicates before they reach the warehouse — quarantined instead
  of silently dropped or crashing the pipeline.
- **Idempotency**: fact table has a UNIQUE constraint + `ON CONFLICT DO NOTHING`,
  so re-running the DAG for the same day never double-counts.
- **Star schema**: one fact table (`fact_shipment_events`), three dimensions
  (`dim_hub`, `dim_partner`, `dim_date`). `dim_hub` has SCD-Type-2-ready columns
  (`valid_from`, `valid_to`, `is_current`).
- **Orchestration**: Airflow DAG with retries and clear task boundaries
  (extract / transform / load).
- **SQL depth**: `analytics_queries.sql` covers joins, GROUP BY, window functions
  (RANK, ROW_NUMBER, LAG, running totals), and CTEs.

## A real bug I hit and fixed

`load_to_postgres.py` originally connected to Postgres via `host="localhost"`.
That works fine when running the script directly on the host machine, since
Docker maps the Postgres port out to `localhost` there. But it fails **inside
the Airflow container** — `localhost` there refers to the Airflow container
itself, not the separate Postgres container next to it.

**Fix:** made the DB host configurable via an environment variable (`DB_HOST`),
defaulting to `localhost` for local/manual runs, and set to `postgres` (the
Docker Compose service name, resolvable via Docker's internal DNS) for the
Airflow container specifically. This is a common environment-parity issue in
real pipelines — code that works on a laptop can fail once it runs inside an
orchestrator with a different network context.

## Mapping to XPO's domain

This is deliberately shaped like a freight/logistics tracking system (hubs,
delivery partners, delayed shipments) — the same category of data a logistics
company like XPO deals with day to day.
