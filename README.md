# Shipment Tracking Data Pipeline

A logistics data pipeline that simulates shipment tracking events, cleans and
validates them, models them into a star schema, and loads them into Postgres
for analytics — built to learn (and demonstrate) core data engineering
concepts end to end.

## Architecture

```
generate_shipments.py  -->  raw/ (partitioned JSON)         [Stage 1: Ingestion]
        |
transform.py            -->  processed/ (Parquet, cleaned)  [Stage 2/3/6: Storage + ETL + DQ]
        |                     quarantine/ (bad records)
load_to_postgres.py     -->  Postgres star schema            [Stage 4: Warehouse]
        |
analytics_queries.sql   -->  SQL analysis                    [Stage 5: Serving]

Orchestrated daily by Airflow DAG: extract_raw >> transform_and_validate >> load_to_warehouse
```

## Quick start

```bash
pip install -r requirements.txt

# 1. Generate a day of raw data
python scripts/generate_shipments.py --mode batch --n 2000

# 2. Transform + validate
python scripts/transform.py

# 3. Spin up Postgres (and Airflow if you want the orchestrated version)
docker-compose up -d

# 4. Load into the warehouse
python scripts/load_to_postgres.py

# 5. Run analytics queries
psql -h localhost -U postgres -d shipments -f sql/analytics_queries.sql
```

Airflow UI: http://localhost:8080 (admin/admin) — trigger `shipment_pipeline` DAG manually or let it run on schedule.

## What this project demonstrates (interview talking points)

- **Batch vs streaming**: `generate_shipments.py --mode stream` shows the streaming-source
  mental model vs the batch mode used by the DAG.
- **Partitioning**: raw and processed data both partitioned by `year/month/day` (Hive-style) —
  explain why this speeds up queries that filter by date.
- **File formats**: raw = JSON (row-based, easy to append), processed = Parquet
  (columnar, compressed) — be ready to explain the trade-off.
- **Data quality**: `transform.py` deliberately catches missing IDs, invalid
  weights, and duplicates before they reach the warehouse — quarantine table
  instead of silently dropping or crashing.
- **Idempotency**: fact table has a UNIQUE constraint + `ON CONFLICT DO NOTHING`,
  so re-running the DAG for the same day never double-counts. This is a question
  interviewers love to probe on ("what happens if your pipeline runs twice?").
- **Star schema**: one fact table (`fact_shipment_events`), three dimensions
  (`dim_hub`, `dim_partner`, `dim_date`). `dim_hub` has SCD-Type-2-ready columns
  (`valid_from`, `valid_to`, `is_current`) even if not fully wired up — good to
  mention you know the pattern.
- **Orchestration**: Airflow DAG with retries and clear task boundaries
  (extract / transform / load) — talk through what happens if `transform_and_validate` fails.
- **SQL depth**: `analytics_queries.sql` covers joins, GROUP BY, window functions
  (RANK, ROW_NUMBER, LAG, running totals), and CTEs — the actual content of most
  DE SQL interview rounds.

## Mapping to XPO's domain

This is deliberately shaped like a freight/logistics tracking system (hubs,
delivery partners, delayed shipments) — the same category of data XPO deals
with. Good answer to "why did you build this": ties your projects directly to
their business.
