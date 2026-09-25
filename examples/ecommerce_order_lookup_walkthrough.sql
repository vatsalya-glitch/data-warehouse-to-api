-- ============================================================================
-- END-TO-END WALKTHROUGH: Ecommerce Order Lookup Pipeline
-- ============================================================================
--
-- This is a single-file, read-top-to-bottom version of the pattern
-- described in docs/design-pattern.md, in the same production-flavored
-- SQL (BigQuery warehouse, Postgres serving DB) as that document.
--
-- This file is illustrative, not runnable -- for a REAL, runnable
-- implementation of this exact pattern (DuckDB + SQLite, no external
-- services, actually tested), see pipeline/ and sql/ instead.
-- See docs/implementation.md for why DuckDB/SQLite stand in for BigQuery/Postgres.
--
-- Domain (fictional, for illustration only):
--   orders              — the "driving" table — defines which entities are in scope
--   customer_attributes — customer profile (enrichment, joins on customer_id)
--   order_items         — line items, aggregated to one row per order (enrichment)
--   shipment            — latest shipment status per order (enrichment)
--   customer_support    — open ticket summary per order (enrichment)
--
-- Warehouse: BigQuery (illustrative). Serving DB: Postgres (illustrative).
-- Every SQL block below is a minimal stub — replace with real tables/columns.
-- ============================================================================


-- ============================================================================
-- PHASE 1: PREFLIGHT — validate schema before writing anything
-- ============================================================================

-- 1.1 Fresh schema for every domain table (CREATE OR REPLACE, not written by hand
--     each run — see sql/.../warehouse/ddl/)
CREATE OR REPLACE TABLE staging.sample_orders (
    order_id     STRING,
    customer_id  STRING,
    order_date   DATE,
    order_status STRING,
    total_amount NUMERIC
);

-- 1.2 Dry-run the domain INSERT query against that fresh schema.
--     BigQuery: jobConfig.dryRun = true — validates types/columns, scans nothing.
--     (See pipeline/warehouse.py: Warehouse.dry_run)
SELECT * FROM (
    -- the real INSERT's SELECT body goes here
    SELECT order_id, customer_id, order_date, order_status, total_amount
    FROM raw.ecommerce_orders
) LIMIT 0;

-- 1.3 Coverage check (done in Python, not SQL): compare columns produced by
--     each domain query against columns consumed by the lookup query below.
--     Fails the whole run if a domain column was added but never joined in.
--     TODO: replace with real INFORMATION_SCHEMA comparison


-- ============================================================================
-- PHASE 2: BUILD — assemble domain tables, then the final lookup
-- ============================================================================

-- 2.1 Domain table: orders (the driving domain — defines which entities
--     are "in scope" for the final lookup)
TRUNCATE TABLE staging.sample_orders;

INSERT INTO staging.sample_orders
SELECT
    order_id, customer_id, order_date, order_status, total_amount
FROM raw.ecommerce_orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) = 1;
-- Dedup via window function, not by trusting the source grain.
-- TODO: replace with real source table and dedup key

-- 2.2 Domain table: customer_attributes (enrichment, joins on customer_id)
TRUNCATE TABLE staging.sample_customer_attributes;

INSERT INTO staging.sample_customer_attributes
SELECT customer_id, customer_name, loyalty_tier
FROM raw.ecommerce_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
-- TODO: replace with real source table

-- 2.3 Domain table: order_items (enrichment, joins on order_id)
--     Unlike the domains above, order_items is naturally MANY rows per
--     order — one per line item — so it collapses with GROUP BY
--     (aggregate the children), not QUALIFY (pick one version of one row).
TRUNCATE TABLE staging.sample_order_items;

INSERT INTO staging.sample_order_items
SELECT order_id, COUNT(*) AS item_count, SUM(quantity) AS total_quantity
FROM raw.ecommerce_order_items
GROUP BY order_id;
-- TODO: replace with real source table

-- 2.4 Data-quality gate — runs BEFORE the final lookup is rebuilt.
--     If this fails, stop: yesterday's lookup table stays in place.
--     TODO: replace with real checks
--   SELECT COUNT(*) FROM staging.sample_orders;                  -- non-empty?
--   SELECT COUNT(*), COUNT(DISTINCT order_id)                    -- unique keys?
--   FROM staging.sample_orders;

-- 2.5 Final lookup: LEFT JOIN every enrichment domain onto the driving domain.
--     LEFT JOIN (not INNER) so a missing customer/shipment/ticket record nulls
--     a field instead of silently dropping the order row.
--     (shipment and customer_support omitted here for brevity — see the full
--     five-domain join in sql/.../lookup/)
CREATE OR REPLACE TABLE staging.sample_lookup_20240101 AS  -- date-sharded, isolates each run
SELECT
    o.order_id, o.customer_id, o.order_date, o.order_status, o.total_amount,
    c.customer_name, c.loyalty_tier,
    i.item_count, i.total_quantity
FROM staging.sample_orders o
LEFT JOIN staging.sample_customer_attributes c ON o.customer_id = c.customer_id
LEFT JOIN staging.sample_order_items i ON o.order_id = i.order_id;


-- ============================================================================
-- PHASE 3: SERVE — export, import, atomic swap into Postgres
-- ============================================================================

-- 3.1 Duplicate check on the primary key, cheap and early — before the
--     expensive import step. (See pipeline/build.py: run_data_quality_gate)
--     TODO: replace with real query on the export/import staging data

-- 3.2 Export the lookup table to object storage in shards (not streamed
--     through the orchestrator). Illustrative call, not real SQL:
--     bq extract --destination_format=PARQUET
--       project:dataset.sample_lookup_20240101
--       gs://ecommerce-order-lookup-sample-exports/shard_*.parquet

-- 3.3 Import shards in parallel into a *staging* table in Postgres —
--     never the live table directly.
DROP TABLE IF EXISTS staging_order_lookup CASCADE;
CREATE TABLE staging_order_lookup (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    customer_name VARCHAR(500),
    item_count INTEGER
);
-- TODO: COPY each shard into staging_order_lookup, up to import_max_concurrency in parallel

-- 3.4 Build indexes on staging AFTER the bulk load, not before.
CREATE INDEX idx_staging_order_lookup_customer ON staging_order_lookup(customer_id);

-- 3.5 Atomic swap — the load-bearing trick of the whole pattern.
--     Catalog-only rename: zero downtime, live table never empty or partial.
BEGIN;
ALTER TABLE live_order_lookup RENAME TO live_order_lookup_old;
ALTER TABLE staging_order_lookup RENAME TO live_order_lookup;
COMMIT;

-- 3.6 Reconcile row counts: warehouse count vs. serving-DB count.
--     TODO: replace with real comparison; log/alert if they diverge

-- 3.7 Cleanup: drop the table from TWO cycles ago (keep one cycle for
--     manual rollback safety).
DROP TABLE IF EXISTS live_order_lookup_old_old CASCADE;


-- ============================================================================
-- RESULT: application reads live_order_lookup for millisecond point lookups
-- ============================================================================
-- SELECT * FROM live_order_lookup WHERE order_id = 'ORDER-12345';
