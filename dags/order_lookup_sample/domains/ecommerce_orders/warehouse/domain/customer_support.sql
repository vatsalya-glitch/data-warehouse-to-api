-- Phase 2: Build domain table — Customer Support
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via customer_id
-- (this one enriches at the CUSTOMER grain, not the order grain — a support
-- agent looking up an order also wants to know "does this customer have
-- open tickets elsewhere," not just tickets tied to this specific order)

TRUNCATE TABLE staging.sample_customer_support;

INSERT INTO staging.sample_customer_support
SELECT
    customer_id,
    COUNT(*) AS open_ticket_count,
    MAX(created_at) AS last_ticket_date,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.ecommerce_support_tickets
WHERE customer_id IS NOT NULL
  AND ticket_status != 'closed'
GROUP BY customer_id;

-- TODO: replace with real support-ticket source and real columns
-- TODO: decide whether "open tickets" or "all tickets in window" is the
-- right shape for your support tool's needs
