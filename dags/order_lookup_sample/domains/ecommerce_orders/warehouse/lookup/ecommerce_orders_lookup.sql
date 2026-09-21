-- Phase 2: Final lookup assembly
-- Join driving domain (orders) with enrichment domains (customer_attributes,
-- order_items, shipment, customer_support). Uses LEFT JOINs to prevent
-- silent row loss.
--
-- Note the two different join keys: order_items and shipment join on
-- order_id (they describe the order itself), while customer_attributes and
-- customer_support join on customer_id (they describe the customer, and
-- are the same for every order that customer has placed).

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
    s.delivery_status,
    t.open_ticket_count,
    t.last_ticket_date,
    CURRENT_TIMESTAMP() as _loaded_at
FROM staging.sample_orders o
LEFT JOIN staging.sample_customer_attributes c
    ON o.customer_id = c.customer_id
LEFT JOIN staging.sample_order_items i
    ON o.order_id = i.order_id
LEFT JOIN staging.sample_shipment s
    ON o.order_id = s.order_id
LEFT JOIN staging.sample_customer_support t
    ON o.customer_id = t.customer_id
WHERE o.order_id IS NOT NULL;

-- TODO: replace with real join logic
-- Key pattern: LEFT JOIN from driving domain (orders) onto every enrichment
-- This prevents silent row loss if a customer/shipment/ticket record is
-- missing from an enrichment table — a missing shipment nulls three
-- columns, it doesn't delete the order from the lookup
