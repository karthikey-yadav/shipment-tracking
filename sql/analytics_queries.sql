-- Practice queries against the star schema. Run these, understand every clause,
-- and be ready to write them from scratch on a whiteboard/shared doc.

-- 1. Average events per hub per day (basic JOIN + GROUP BY)
SELECT
    h.hub_city,
    d.date_key,
    COUNT(*) AS event_count
FROM fact_shipment_events f
JOIN dim_hub h ON f.hub_id = h.hub_id
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY h.hub_city, d.date_key
ORDER BY d.date_key, h.hub_city;

-- 2. Top 5 hubs by delayed shipment count (aggregation + ORDER BY + LIMIT)
SELECT
    h.hub_city,
    COUNT(*) FILTER (WHERE f.is_delayed) AS delayed_count,
    COUNT(*) AS total_events,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_delayed) / COUNT(*), 2) AS delay_pct
FROM fact_shipment_events f
JOIN dim_hub h ON f.hub_id = h.hub_id
GROUP BY h.hub_city
ORDER BY delay_pct DESC
LIMIT 5;

-- 3. Rank shipments by weight within each hub (WINDOW FUNCTION: RANK)
SELECT
    shipment_id,
    hub_id,
    weight_kg,
    RANK() OVER (PARTITION BY hub_id ORDER BY weight_kg DESC) AS weight_rank
FROM fact_shipment_events;

-- 4. Running daily delivered count per hub (WINDOW FUNCTION: running total)
SELECT
    hub_id,
    date_key,
    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered_today,
    SUM(COUNT(*) FILTER (WHERE status = 'DELIVERED'))
        OVER (PARTITION BY hub_id ORDER BY date_key) AS running_delivered
FROM fact_shipment_events
GROUP BY hub_id, date_key
ORDER BY hub_id, date_key;

-- 5. Day-over-day change in delay rate per hub (LAG)
WITH daily_delay AS (
    SELECT
        hub_id,
        date_key,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_delayed) / COUNT(*), 2) AS delay_pct
    FROM fact_shipment_events
    GROUP BY hub_id, date_key
)
SELECT
    hub_id,
    date_key,
    delay_pct,
    delay_pct - LAG(delay_pct) OVER (PARTITION BY hub_id ORDER BY date_key) AS delay_pct_change
FROM daily_delay
ORDER BY hub_id, date_key;

-- 6. Top-N shipments per partner by weight (window function + filter pattern)
WITH ranked AS (
    SELECT
        shipment_id,
        partner_id,
        weight_kg,
        ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY weight_kg DESC) AS rn
    FROM fact_shipment_events
)
SELECT p.partner_name, r.shipment_id, r.weight_kg
FROM ranked r
JOIN dim_partner p ON r.partner_id = p.partner_id
WHERE rn <= 3
ORDER BY p.partner_name, r.weight_kg DESC;

-- 7. Find duplicate shipment_id + event_time combos (classic "find duplicates" question)
SELECT shipment_id, event_time, status, COUNT(*)
FROM fact_shipment_events
GROUP BY shipment_id, event_time, status
HAVING COUNT(*) > 1;

-- 8. Hourly event volume distribution (EXTRACT / date functions)
SELECT
    EXTRACT(HOUR FROM event_time) AS hour_of_day,
    COUNT(*) AS event_count
FROM fact_shipment_events
GROUP BY hour_of_day
ORDER BY hour_of_day;
