-- Star schema for shipment tracking analytics.
-- fact_shipment_events is the fact table; dim_* are the dimensions.
-- This is the kind of design question interviewers ask you to sketch on a whiteboard.

CREATE TABLE IF NOT EXISTS dim_hub (
    hub_id      SERIAL PRIMARY KEY,
    hub_code    VARCHAR(20) UNIQUE NOT NULL,
    hub_city    VARCHAR(100) NOT NULL,
    -- region added later to demonstrate SCD Type 2 handling if a hub's region changes
    region      VARCHAR(50),
    valid_from  DATE DEFAULT CURRENT_DATE,
    valid_to    DATE DEFAULT NULL,
    is_current  BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_partner (
    partner_id    SERIAL PRIMARY KEY,
    partner_name  VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      DATE PRIMARY KEY,
    week_of_year  INT NOT NULL,
    month         INT NOT NULL,
    year          INT NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_shipment_events (
    event_id             BIGSERIAL PRIMARY KEY,
    shipment_id          UUID NOT NULL,
    hub_id               INT REFERENCES dim_hub(hub_id),
    partner_id           INT REFERENCES dim_partner(partner_id),
    date_key             DATE REFERENCES dim_date(date_key),
    event_time           TIMESTAMP NOT NULL,
    status               VARCHAR(30) NOT NULL,
    weight_kg            NUMERIC(6,2),
    destination_pincode  VARCHAR(10),
    is_delayed           BOOLEAN DEFAULT FALSE,
    UNIQUE (shipment_id, event_time, status)   -- idempotency: prevents dup loads on retry
);

CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_shipment_events(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_hub ON fact_shipment_events(hub_id);
CREATE INDEX IF NOT EXISTS idx_fact_status ON fact_shipment_events(status);
