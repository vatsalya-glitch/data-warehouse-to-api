-- Phase 3: Atomic swap — zero downtime table rotation
-- Catalog-only operation: live table is NEVER empty or partially updated

BEGIN;

ALTER TABLE live_order_lookup RENAME TO live_order_lookup_old;
ALTER TABLE staging_order_lookup RENAME TO live_order_lookup;

COMMIT;

-- The swap completes in milliseconds regardless of table size (no data moves).
-- Readers mid-query see either the old complete table or the new complete table,
-- never a partial one.
--
-- Run via sqlite3.Connection.executescript() with isolation_level=None
-- (see pipeline/serving_db.py) so the explicit BEGIN/COMMIT above controls
-- the transaction, rather than Python's own implicit transaction handling.
