-- Phase 2: Build domain table — Customer Support
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via customer_id
-- (this one enriches at the CUSTOMER grain, not the order grain — a support
-- agent looking up an order also wants to know "does this customer have
-- open tickets elsewhere," not just tickets tied to this specific order)

CREATE OR REPLACE TABLE sample_customer_support AS
SELECT
    customer_id,
    COUNT(*) AS open_ticket_count,
    MAX(created_at) AS last_ticket_date,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw_support_tickets
WHERE customer_id IS NOT NULL
  AND ticket_status != 'closed'
GROUP BY customer_id;
