-- Schema CONTRACT for the order_items domain table.
-- See ddl/orders.sql for how this file is used (read and diffed, never run).

CREATE OR REPLACE TABLE sample_order_items (
  order_id           VARCHAR,
  item_count         BIGINT,
  total_quantity     BIGINT,
  distinct_products  BIGINT,
  _loaded_at         TIMESTAMP
);
