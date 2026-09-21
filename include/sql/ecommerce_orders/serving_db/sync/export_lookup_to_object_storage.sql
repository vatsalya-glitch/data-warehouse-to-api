-- Phase 3: Export the finished lookup table to object storage, sharded.
-- Illustrative BigQuery EXPORT DATA statement — decouples export speed
-- from import speed, and lets import run in parallel shards.

EXPORT DATA OPTIONS (
    uri = 'gs://ecommerce-order-lookup-sample-exports/ecommerce_orders/shard_*.parquet',
    format = 'PARQUET',
    overwrite = true
) AS
SELECT *
FROM staging.sample_lookup_current;

-- TODO: replace with real lookup table name and real bucket
-- TODO: tune shard count / max file size for your row volume
