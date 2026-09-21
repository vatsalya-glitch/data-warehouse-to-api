-- Phase 2: Build domain table — Orders (the driving domain)
-- Full refresh strategy: TRUNCATE + INSERT
-- Dedup: one row per order_id, latest by order_date
-- This domain defines which entities are "in scope" for the final lookup —
-- every other domain LEFT JOINs onto this one.

TRUNCATE TABLE staging.sample_orders;

INSERT INTO staging.sample_orders
SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.ecommerce_orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
  AND order_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) = 1;

-- TODO: replace with real source tables, real columns, real dedup key
-- The query shape: TRUNCATE + INSERT + QUALIFY window function
-- The {rolling_window_days} parameter injected by DAG (from dag_config.yaml)
