-- DDL: Fresh schema for orders domain table (the driving domain)
-- CREATE OR REPLACE before every run to guarantee fresh schema

CREATE OR REPLACE TABLE staging.sample_orders (
  order_id        STRING,
  customer_id     STRING,
  order_date      DATE,
  order_status    STRING,
  total_amount    NUMERIC,
  _loaded_at      TIMESTAMP
);

-- TODO: replace with real source table and column definitions
