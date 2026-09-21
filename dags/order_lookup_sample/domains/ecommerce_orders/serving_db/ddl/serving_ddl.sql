-- Serving database DDL: Live lookup table
-- Created once; reused across all runs

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
    delivery_status    VARCHAR(50),
    open_ticket_count  INTEGER,
    last_ticket_date   TIMESTAMP,
    _loaded_at         TIMESTAMP
);

-- TODO: replace with real schema matching final lookup query
