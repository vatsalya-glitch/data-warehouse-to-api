-- DDL: Fresh schema for loan_records domain table
-- CREATE OR REPLACE before every run to guarantee fresh schema

CREATE OR REPLACE TABLE staging.sample_360_loan_records (
  loan_id         STRING,
  patron_id       STRING,
  book_id         STRING,
  loan_date       DATE,
  due_date        DATE,
  return_date     DATE,
  _loaded_at      TIMESTAMP
);

-- TODO: replace with real source table and column definitions
