-- Phase 2: Final lookup assembly
-- Join driving domain (orders) with enrichment domains (customer_attributes,
-- order_items, shipment, customer_support). Uses LEFT JOINs to prevent
-- silent row loss.
--
-- Note the two different join keys: order_items, shipment, and
-- customer_support all join on order_id (they describe the order itself),
-- while customer_attributes joins on customer_id (it describes the
-- customer, and is the same for every order that customer has placed).
--
-- This file is a plain SELECT, not a CREATE TABLE: pipeline/build.py wraps
-- it as `CREATE OR REPLACE TABLE sample_lookup_current AS <this select>`,
-- so the same file can also be DESCRIBE'd directly for the coverage check
-- (see pipeline/preflight.py) without stripping a CREATE TABLE prefix.

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.total_amount,
    c.customer_name,
    c.customer_email,
    c.loyalty_tier,
    c.signup_date,
    i.item_count,
    i.total_quantity,
    i.distinct_products,
    s.carrier,
    s.tracking_number,
    s.ship_date,
    s.delivery_status,
    t.open_ticket_count,
    t.last_ticket_date,
    CURRENT_TIMESTAMP AS _loaded_at
FROM sample_orders o
LEFT JOIN sample_customer_attributes c
    ON o.customer_id = c.customer_id
LEFT JOIN sample_order_items i
    ON o.order_id = i.order_id
LEFT JOIN sample_shipment s
    ON o.order_id = s.order_id
LEFT JOIN sample_customer_support t
    ON o.order_id = t.order_id
WHERE o.order_id IS NOT NULL
