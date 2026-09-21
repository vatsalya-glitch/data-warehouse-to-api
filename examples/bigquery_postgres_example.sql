-- BigQuery + Postgres Example Implementation
-- Shows how to implement the three-phase pattern with BigQuery as warehouse
-- and Postgres as serving database

-- ============================================================================
-- PHASE 1: PREFLIGHT
-- ============================================================================
-- These queries run with dryRun=true in BigQuery to validate without scanning

-- 1.1: Create fresh staging schemas for each domain
CREATE OR REPLACE TABLE `project.dataset.domain_customer_core_staging` AS
SELECT
  customer_id,
  customer_name,
  email,
  phone,
  address,
  signup_date
FROM `project.dataset.raw_customers`
WHERE 1=0;  -- Empty result, just validates schema

-- 1.2: Dry-run the actual insert (BigQuery jobConfig.dryRun=true)
-- This would be called from orchestration code, not directly
-- Example Python:
-- job_config = bigquery.LoadJobConfig()
-- job_config.dryRun = True
-- SELECT * FROM `project.dataset.raw_customers` LIMIT 0

-- 1.3: Validate coverage - check all expected columns exist
-- In orchestration code, after loading metadata:
/*
expected_columns = [
  'customer_id', 'customer_name', 'email', 'phone', 'address',
  'signup_date', 'tier', 'total_spent', 'last_order_date'
]

# Query each staging table and verify columns
for domain in domains:
  actual_columns = get_columns(f"dataset.{domain.intermediate_name}")
  missing = set(expected_columns) - set(actual_columns)
  if missing:
    raise Exception(f"Missing columns: {missing}")
*/

-- ============================================================================
-- PHASE 2: BUILD
-- ============================================================================

-- 2.1: Build Domain 1 - Customer Core (the driving table)
TRUNCATE TABLE `project.dataset.domain_customer_core`;

INSERT INTO `project.dataset.domain_customer_core`
SELECT
  customer_id,
  customer_name,
  email,
  phone,
  address,
  signup_date,
  CURRENT_DATE() as `_run_date`
FROM `project.dataset.raw_customers`
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY updated_at DESC
) = 1;

-- 2.2: Build Domain 2 - Customer Engagement
TRUNCATE TABLE `project.dataset.domain_customer_engagement`;

INSERT INTO `project.dataset.domain_customer_engagement`
SELECT
  customer_id,
  tier,
  CAST(total_spent AS FLOAT64) as total_spent,
  updated_at,
  CURRENT_DATE() as `_run_date`
FROM `project.dataset.raw_customer_engagement`
WHERE customer_id IS NOT NULL
  AND DATE(updated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY updated_at DESC
) = 1;

-- 2.3: Build Domain 3 - Customer Activity
TRUNCATE TABLE `project.dataset.domain_customer_activity`;

INSERT INTO `project.dataset.domain_customer_activity`
SELECT
  customer_id,
  DATE(MAX(order_date)) as last_order_date,
  CURRENT_DATE() as `_run_date`
FROM `project.dataset.raw_orders`
WHERE customer_id IS NOT NULL
  AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
GROUP BY customer_id;

-- 2.4: Data Quality Gate - Check driving domain
-- Run before final assembly; if this fails, stop the pipeline
WITH quality_checks AS (
  SELECT
    (SELECT COUNT(*) FROM `project.dataset.domain_customer_core`) as row_count,
    (SELECT COUNT(*) FROM `project.dataset.domain_customer_core`
     WHERE customer_id IS NOT NULL) as non_null_key_count,
    (SELECT COUNT(DISTINCT customer_id) FROM `project.dataset.domain_customer_core`) as distinct_keys
)
SELECT
  CASE
    WHEN row_count = 0 THEN 'FAIL: Driving domain is empty'
    WHEN distinct_keys < non_null_key_count THEN 'FAIL: Duplicate primary keys detected'
    WHEN row_count > 0 THEN 'PASS'
    ELSE 'UNKNOWN'
  END as quality_status
FROM quality_checks;

-- 2.5: Final Assembly - Denormalized Lookup Table
-- Uses date-sharded intermediate table for isolation
TRUNCATE TABLE `project.dataset.customers_lookup_{YYYYMMDD}`;

INSERT INTO `project.dataset.customers_lookup_{YYYYMMDD}`
SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  c.phone,
  c.address,
  c.signup_date,
  e.tier,
  e.total_spent,
  a.last_order_date,
  CURRENT_TIMESTAMP() as `_loaded_at`
FROM `project.dataset.domain_customer_core` c
LEFT JOIN `project.dataset.domain_customer_engagement` e
  ON c.customer_id = e.customer_id
LEFT JOIN `project.dataset.domain_customer_activity` a
  ON c.customer_id = a.customer_id
WHERE c.customer_id IS NOT NULL;

-- ============================================================================
-- PHASE 3: SERVE
-- ============================================================================

-- 3.1: Export to Parquet shards for parallel import
-- In orchestration code:
/*
# Export to shards (e.g., 100k rows per shard)
query_job = client.query("""
  SELECT * FROM `project.dataset.customers_lookup_{YYYYMMDD}`
""")

table_ref = query_job.result()
extract_job = client.extract_table(
  table_ref,
  ["gs://export-bucket/customers_lookup_*.parquet"],
  job_config=bigquery.ExtractJobConfig(
    destination_format=bigquery.DestinationFormat.PARQUET
  )
)
*/

-- 3.2: Postgres import (run in PostgreSQL)
-- Executed from orchestration code after export completes

-- 3.2a: Check for duplicate primary keys before import
/*
WITH import_data AS (
  -- In real orchestration, this reads from exported Parquet files
  SELECT customer_id FROM staging_import
)
SELECT
  CASE
    WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'PASS: No duplicates'
    ELSE 'FAIL: Duplicate keys found'
  END as duplicate_check
FROM import_data;
*/

-- 3.2b: Create staging table (fresh each run)
DROP TABLE IF EXISTS customers_lookup_staging CASCADE;

CREATE TABLE customers_lookup_staging (
  customer_id VARCHAR(255) PRIMARY KEY,
  customer_name VARCHAR(500),
  email VARCHAR(500),
  phone VARCHAR(20),
  address TEXT,
  signup_date DATE,
  tier VARCHAR(50),
  total_spent FLOAT,
  last_order_date DATE,
  _loaded_at TIMESTAMP,
  _import_date DATE DEFAULT CURRENT_DATE
);

-- 3.2c: Bulk import from Parquet shards
-- In orchestration code, parallel import:
/*
import_concurrency = 4  # Capped to avoid starving other queries

for shard_file in ["*.parquet"]:
  # Import shard to staging table in parallel
  COPY customers_lookup_staging FROM 'shard_file.parquet'
  # Orchestration handles parallel execution
*/

-- 3.2d: Build indexes on staging table (after bulk load)
CREATE INDEX idx_customers_lookup_staging_customer_id
  ON customers_lookup_staging(customer_id);

CREATE INDEX idx_customers_lookup_staging_tier
  ON customers_lookup_staging(tier);

-- 3.3: Atomic table swap (zero-downtime)
-- Run as a single transaction
BEGIN;
  ALTER TABLE customers_lookup RENAME TO customers_lookup_old;
  ALTER TABLE customers_lookup_staging RENAME TO customers_lookup;
COMMIT;

-- 3.4: Row count reconciliation
-- Check that warehouse count matches serving DB count
-- Run in orchestration after swap:
/*
warehouse_count = client.query(
  "SELECT COUNT(*) FROM `project.dataset.customers_lookup_{YYYYMMDD}`"
).result()

serving_count = postgres_conn.execute(
  "SELECT COUNT(*) FROM customers_lookup"
).fetchone()

if abs(warehouse_count - serving_count) > tolerance:
  raise Exception(f"Row count mismatch: {warehouse_count} vs {serving_count}")
*/

-- 3.5: Cleanup (run at start of NEXT successful run)
-- Keep previous table for one cycle
DROP TABLE IF EXISTS customers_lookup_old_old CASCADE;
ALTER TABLE customers_lookup_old RENAME TO customers_lookup_old_old;

-- ============================================================================
-- APPLICATION USAGE
-- ============================================================================
-- Applications now query Postgres for sub-millisecond lookups:

-- SELECT * FROM customers_lookup WHERE customer_id = 'CUST-12345';
-- Result: One complete row, cached in Postgres, no warehouse query needed
