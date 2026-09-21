-- ============================================================================
-- END-TO-END WALKTHROUGH: Library Lending Lookup Pipeline
-- ============================================================================
--
-- This is a single-file, read-top-to-bottom version of the SAME pattern
-- implemented as separate files under dags/library_lending_sample/.
--
-- Read this file to understand the full flow in one sitting.
-- Read dags/library_lending_sample/ to see how it's organized as a real,
-- config-driven Airflow project (one query per file, wired by main.py).
--
-- Domain (fictional, for illustration only):
--   loans   — borrowing records (the "driving" table — defines who's in scope)
--   patrons — borrower profiles (enrichment)
--   books   — inventory (enrichment)
--
-- Warehouse: BigQuery (illustrative). Serving DB: Postgres (illustrative).
-- Every SQL block below is a minimal stub — replace with real tables/columns.
-- ============================================================================


-- ============================================================================
-- PHASE 1: PREFLIGHT — validate schema before writing anything
-- ============================================================================

-- 1.1 Fresh schema for every domain table (CREATE OR REPLACE, not written by hand
--     each run — see dags/library_lending_sample/domains/.../warehouse/ddl/)
CREATE OR REPLACE TABLE staging.sample_loan_records (
    loan_id     STRING,
    patron_id   STRING,
    book_id     STRING,
    loan_date   DATE,
    due_date    DATE,
    return_date DATE
);

-- 1.2 Dry-run the domain INSERT query against that fresh schema.
--     BigQuery: jobConfig.dryRun = true — validates types/columns, scans nothing.
--     (See dags/library_lending_sample/utils/dq_utils.py: validate_insert_schema)
SELECT * FROM (
    -- the real INSERT's SELECT body goes here
    SELECT loan_id, patron_id, book_id, loan_date, due_date, return_date
    FROM raw.library_loans
) LIMIT 0;

-- 1.3 Coverage check (done in Python, not SQL): compare columns produced by
--     each domain query against columns consumed by the lookup query below.
--     Fails the whole run if a domain column was added but never joined in.
--     TODO: replace with real INFORMATION_SCHEMA comparison


-- ============================================================================
-- PHASE 2: BUILD — assemble domain tables, then the final lookup
-- ============================================================================

-- 2.1 Domain table: loans (the driving domain — defines which entities
--     are "in scope" for the final lookup)
TRUNCATE TABLE staging.sample_loan_records;

INSERT INTO staging.sample_loan_records
SELECT
    loan_id, patron_id, book_id, loan_date, due_date, return_date
FROM raw.library_loans
WHERE loan_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY loan_date DESC) = 1;
-- Dedup via window function, not by trusting the source grain.
-- TODO: replace with real source table and dedup key

-- 2.2 Domain table: patrons (enrichment)
TRUNCATE TABLE staging.sample_patron_profile;

INSERT INTO staging.sample_patron_profile
SELECT patron_id, patron_name, membership_tier
FROM raw.library_patrons
QUALIFY ROW_NUMBER() OVER (PARTITION BY patron_id ORDER BY updated_at DESC) = 1;
-- TODO: replace with real source table

-- 2.3 Data-quality gate — runs BEFORE the final lookup is rebuilt.
--     If this fails, stop: yesterday's lookup table stays in place.
--     TODO: replace with real checks
--   SELECT COUNT(*) FROM staging.sample_loan_records;                 -- non-empty?
--   SELECT COUNT(*), COUNT(DISTINCT loan_id)                          -- unique keys?
--   FROM staging.sample_loan_records;

-- 2.4 Final lookup: LEFT JOIN every enrichment domain onto the driving domain.
--     LEFT JOIN (not INNER) so a missing patron/book nulls a field instead
--     of silently dropping the loan row.
CREATE OR REPLACE TABLE staging.sample_lookup_20240101 AS  -- date-sharded, isolates each run
SELECT
    l.loan_id, l.patron_id, l.book_id, l.loan_date, l.due_date, l.return_date,
    p.patron_name, p.membership_tier,
    b.book_title, b.author
FROM staging.sample_loan_records l
LEFT JOIN staging.sample_patron_profile p ON l.patron_id = p.patron_id
LEFT JOIN staging.sample_book_inventory b ON l.book_id = b.book_id;


-- ============================================================================
-- PHASE 3: SERVE — export, import, atomic swap into Postgres
-- ============================================================================

-- 3.1 Duplicate check on the primary key, cheap and early — before the
--     expensive import step. (See utils/dq_utils.py: run_domain_dq_checks)
--     TODO: replace with real query on the export/import staging data

-- 3.2 Export the lookup table to object storage in shards (not streamed
--     through the orchestrator). Illustrative call, not real SQL:
--     bq extract --destination_format=PARQUET
--       project:dataset.sample_lookup_20240101
--       gs://library-lending-sample-exports/shard_*.parquet

-- 3.3 Import shards in parallel into a *staging* table in Postgres —
--     never the live table directly.
DROP TABLE IF EXISTS staging_lookup CASCADE;
CREATE TABLE staging_lookup (
    loan_id VARCHAR(255) PRIMARY KEY,
    patron_id VARCHAR(255),
    book_id VARCHAR(255),
    patron_name VARCHAR(500),
    book_title VARCHAR(500)
);
-- TODO: COPY each shard into staging_lookup, up to import_max_concurrency in parallel

-- 3.4 Build indexes on staging AFTER the bulk load, not before.
CREATE INDEX idx_staging_lookup_patron ON staging_lookup(patron_id);

-- 3.5 Atomic swap — the load-bearing trick of the whole pattern.
--     Catalog-only rename: zero downtime, live table never empty or partial.
BEGIN;
ALTER TABLE live_lookup RENAME TO live_lookup_old;
ALTER TABLE staging_lookup RENAME TO live_lookup;
COMMIT;

-- 3.6 Reconcile row counts: warehouse count vs. serving-DB count.
--     TODO: replace with real comparison; log/alert if they diverge

-- 3.7 Cleanup: drop the table from TWO cycles ago (keep one cycle for
--     manual rollback safety).
DROP TABLE IF EXISTS live_lookup_old_old CASCADE;


-- ============================================================================
-- RESULT: application reads live_lookup for millisecond point lookups
-- ============================================================================
-- SELECT * FROM live_lookup WHERE loan_id = 'LOAN-12345';
