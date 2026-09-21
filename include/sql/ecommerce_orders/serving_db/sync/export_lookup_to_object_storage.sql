-- Phase 3: Export the finished lookup table to object storage, sharded.
-- Runs against the WAREHOUSE connection (DuckDB), even though it's grouped
-- here under serving_db/sync/ since it's conceptually part of the
-- serving-sync process. Decouples export speed from import speed.
--
-- Locally, "object storage" is a folder (include/config/infra_config.yaml
-- -> object_storage.export_dir) instead of GCS/S3 — see include/README.md.
-- {export_path} is substituted by pipeline/serve.py.

COPY sample_lookup_current TO '{export_path}' (FORMAT PARQUET);

-- In production, at real row volumes, this would write multiple shard
-- files (e.g. one per N rows) for parallel import. This reference writes
-- a single Parquet file since the local sample data is small enough that
-- sharding wouldn't demonstrate anything — the import step is still
-- written to loop over "however many shard files exist."
