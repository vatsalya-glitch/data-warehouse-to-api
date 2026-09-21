-- Phase 3: Create staging table (fresh, empty schema)
-- Dropped and recreated each run, never touches live table

DROP TABLE IF EXISTS staging_lookup CASCADE;

CREATE TABLE staging_lookup (
    loan_id         VARCHAR(255) PRIMARY KEY,
    patron_id       VARCHAR(255),
    book_id         VARCHAR(255),
    loan_date       DATE,
    due_date        DATE,
    return_date     DATE,
    patron_name     VARCHAR(500),
    patron_email    VARCHAR(500),
    membership_tier VARCHAR(50),
    signup_date     DATE,
    book_title      VARCHAR(500),
    author          VARCHAR(500),
    isbn            VARCHAR(20),
    _loaded_at      TIMESTAMP
);

-- TODO: replace with real schema (should match live_lookup exactly)
