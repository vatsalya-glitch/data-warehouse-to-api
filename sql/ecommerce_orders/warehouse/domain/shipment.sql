-- Phase 2: Build domain table — Shipment
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via order_id
-- Dedup: one row per order_id, latest shipment event (an order can have
-- multiple shipment scan events; we want only the most recent status)

CREATE OR REPLACE TABLE sample_shipment AS
SELECT
    order_id,
    carrier,
    tracking_number,
    ship_date,
    delivery_status,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw_shipment_events
WHERE order_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp DESC) = 1;
