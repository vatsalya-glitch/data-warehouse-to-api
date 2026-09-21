-- Phase 3: Create staging table (fresh, empty schema)
-- Dropped and recreated each run, never touches live table.
-- (SQLite has no CASCADE on DROP TABLE — unlike Postgres, there's no
-- dependent-object cleanup needed since we don't use foreign keys here.)

DROP TABLE IF EXISTS staging_order_lookup;

CREATE TABLE staging_order_lookup (
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
