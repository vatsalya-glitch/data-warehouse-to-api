-- Schema CONTRACT for the shipment domain table.
-- See ddl/orders.sql for how this file is used (read and diffed, never run).

CREATE OR REPLACE TABLE sample_shipment (
  order_id          VARCHAR,
  carrier           VARCHAR,
  tracking_number   VARCHAR,
  ship_date         DATE,
  delivery_status   VARCHAR,
  _loaded_at        TIMESTAMP
);
