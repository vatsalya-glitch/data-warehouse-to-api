-- Schema CONTRACT for the customer_attributes domain table.
-- See ddl/orders.sql for how this file is used (read and diffed, never run).

CREATE OR REPLACE TABLE sample_customer_attributes (
  customer_id     VARCHAR,
  customer_name   VARCHAR,
  customer_email  VARCHAR,
  loyalty_tier    VARCHAR,
  signup_date     DATE,
  _loaded_at      TIMESTAMP
);
