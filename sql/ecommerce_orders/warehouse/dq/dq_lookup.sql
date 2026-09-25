-- Phase 2: Data-quality assertion query
-- Runs after domain tables are built, before the final lookup is rebuilt.
-- A non-PASS status here means "stop" — the pipeline raises and the
-- previous cycle's lookup table is left untouched.
--
-- pipeline/build.py runs the equivalent of this check via
-- warehouse.row_count()/distinct_count(), parameterized by primary_key
-- and min_rows from dag_config.yaml (rather than templating this file) —
-- this version is what you'd run by hand in a SQL client to see the same
-- thing the pipeline checks.

SELECT
    CASE
        WHEN row_count < 10 THEN 'FAIL: driving domain has fewer than min_rows'
        WHEN row_count != distinct_keys THEN 'FAIL: duplicate order_id detected'
        ELSE 'PASS'
    END AS dq_status,
    row_count,
    distinct_keys
FROM (
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_id) AS distinct_keys
    FROM sample_orders
);
