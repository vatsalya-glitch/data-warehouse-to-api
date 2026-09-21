-- Schema CONTRACT for the customer_support domain table.
-- See ddl/orders.sql for how this file is used (read and diffed, never run).

CREATE OR REPLACE TABLE sample_customer_support (
  customer_id        VARCHAR,
  open_ticket_count  BIGINT,
  last_ticket_date   TIMESTAMP,
  _loaded_at         TIMESTAMP
);
