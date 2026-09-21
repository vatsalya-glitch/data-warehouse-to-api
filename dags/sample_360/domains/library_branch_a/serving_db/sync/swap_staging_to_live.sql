-- Phase 3: Atomic swap — zero downtime table rotation
-- Catalog-only operation: live table is NEVER empty or partially updated

BEGIN;

ALTER TABLE live_lookup RENAME TO live_lookup_old;
ALTER TABLE staging_lookup RENAME TO live_lookup;

COMMIT;

-- The swap completes in milliseconds regardless of table size (no data moves).
-- Readers mid-query see either the old complete table or the new complete table,
-- never a partial one.
--
-- TODO: ensure this runs in one transaction on your database type
