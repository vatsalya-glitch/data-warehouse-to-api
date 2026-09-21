-- Phase 2: Build domain table — Loan Records
-- Full refresh strategy: TRUNCATE + INSERT
-- Dedup: one row per loan_id, latest by loan_date

TRUNCATE TABLE staging.sample_360_loan_records;

INSERT INTO staging.sample_360_loan_records
SELECT
    loan_id,
    patron_id,
    book_id,
    loan_date,
    due_date,
    return_date,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.library_loans
WHERE loan_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
  AND loan_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY loan_date DESC) = 1;

-- TODO: replace with real source tables, real columns, real dedup key
-- The query shape: TRUNCATE + INSERT + QUALIFY window function
-- The {rolling_window_days} parameter injected by DAG (from dag_config.yaml)
