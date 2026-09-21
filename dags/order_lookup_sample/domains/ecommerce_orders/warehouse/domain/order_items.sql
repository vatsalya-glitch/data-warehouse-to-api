-- Phase 2: Build domain table — Order Items
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via order_id
--
-- Unlike the other domains (one row per key via QUALIFY), order_items is
-- naturally MANY rows per order — one per line item. Collapsing it to the
-- order grain needs aggregation, not just a tie-break: this is the second
-- flattening technique the pattern relies on (see docs/design-pattern.md,
-- "Deduplicate with a window function, not by trusting the source").

TRUNCATE TABLE staging.sample_order_items;

INSERT INTO staging.sample_order_items
SELECT
    order_id,
    COUNT(*) AS item_count,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_id) AS distinct_products,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.ecommerce_order_items
WHERE order_id IS NOT NULL
GROUP BY order_id;

-- TODO: replace with real order_items source and real columns
-- TODO: decide what "one row per order" should mean for your use case —
-- here it's an aggregate summary; another valid choice is a single QUALIFY
-- pick of the "primary" line item, or a nested/repeated field if your
-- warehouse and serving DB both support it
