-- Schema CONTRACT for the orders domain table (the driving domain).
-- Preflight compares this declared column set against what the domain
-- query in warehouse/domain/orders.sql actually produces, and fails
-- loudly on any mismatch -- this is what "fail before you write" checks.
-- This file is never executed against the real domain table; it's read
-- and diffed. See pipeline/preflight.py.

CREATE OR REPLACE TABLE sample_orders (
  order_id        VARCHAR,
  customer_id     VARCHAR,
  order_date      DATE,
  order_status    VARCHAR,
  total_amount    DOUBLE,
  _loaded_at      TIMESTAMP
);
