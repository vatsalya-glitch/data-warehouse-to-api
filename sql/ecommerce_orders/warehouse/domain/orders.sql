-- Phase 2: Build domain table — Orders (the driving domain)
-- Full refresh strategy: CREATE OR REPLACE TABLE ... AS SELECT, DuckDB's
-- single-statement equivalent of TRUNCATE + INSERT (see docs/design-pattern.md)
-- Dedup: one row per order_id, latest by order_date
-- This domain defines which entities are "in scope" for the final lookup —
-- every other domain LEFT JOINs onto this one.

CREATE OR REPLACE TABLE sample_orders AS
SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw_orders
WHERE order_date >= CURRENT_DATE - INTERVAL {rolling_window_days} DAY
  AND order_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) = 1;
