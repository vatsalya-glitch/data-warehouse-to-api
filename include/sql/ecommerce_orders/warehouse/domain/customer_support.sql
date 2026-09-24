-- Phase 2: Build domain table — Customer Support
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via order_id
-- (order-scoped: a support agent looking up an order wants to know if
-- there are open tickets filed about THIS order, not the customer's
-- ticket history across every order they've ever placed)

CREATE OR REPLACE TABLE sample_customer_support AS
SELECT
    order_id,
    COUNT(*) AS open_ticket_count,
    MAX(created_at) AS last_ticket_date,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw_support_tickets
WHERE order_id IS NOT NULL
  AND ticket_status != 'closed'
GROUP BY order_id;
