-- Phase 2: Data-quality assertion query
-- Runs after domain tables are built, before the final lookup is rebuilt.
-- A non-zero row count here means "stop" — the DAG task fails and the
-- previous cycle's lookup table is left untouched.

SELECT
    CASE
        WHEN row_count = 0 THEN 'FAIL: driving domain is empty'
        WHEN row_count != distinct_keys THEN 'FAIL: duplicate order_id detected'
        ELSE 'PASS'
    END AS dq_status
FROM (
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_id) AS distinct_keys
    FROM staging.sample_orders
);

-- TODO: replace with real thresholds (min_rows from dag_config.yaml) and
-- real uniqueness/null checks for your driving domain's primary key
