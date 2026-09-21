-- Serving database (SQLite) DDL: Live lookup table
-- Created once; reused across all runs. Matches
-- warehouse/lookup/ecommerce_orders_lookup.sql column-for-column.

CREATE TABLE IF NOT EXISTS live_order_lookup (
    order_id           VARCHAR(255) PRIMARY KEY,
    customer_id        VARCHAR(255),
    order_date         DATE,
    order_status       VARCHAR(50),
    total_amount       NUMERIC,
    customer_name      VARCHAR(500),
    customer_email     VARCHAR(500),
    loyalty_tier       VARCHAR(50),
    signup_date        DATE,
    item_count         INTEGER,
    total_quantity     INTEGER,
    distinct_products  INTEGER,
    carrier            VARCHAR(100),
    tracking_number    VARCHAR(100),
    ship_date          DATE,
    delivery_status    VARCHAR(50),
    open_ticket_count  INTEGER,
    last_ticket_date   TIMESTAMP,
    _loaded_at         TIMESTAMP
);

-- Run once by pipeline/serving_db.py on first use (CREATE TABLE IF NOT
-- EXISTS is idempotent, so re-running the pipeline never fails on this).
