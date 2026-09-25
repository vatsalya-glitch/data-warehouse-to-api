-- Phase 2: Build domain table — Customer Attributes
-- Enrichment domain: LEFT JOINed onto orders (driving domain) via customer_id

CREATE OR REPLACE TABLE sample_customer_attributes AS
SELECT
    customer_id,
    customer_name,
    customer_email,
    loyalty_tier,
    signup_date,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw_customers
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
