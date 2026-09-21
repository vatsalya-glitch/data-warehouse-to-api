-- Phase 2: Build domain table — Customer Attributes
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via customer_id

TRUNCATE TABLE staging.sample_customer_attributes;

INSERT INTO staging.sample_customer_attributes
SELECT
    customer_id,
    customer_name,
    customer_email,
    loyalty_tier,
    signup_date,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.ecommerce_customers
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;

-- TODO: replace with real customer source, columns, and dedup tie-breaker
