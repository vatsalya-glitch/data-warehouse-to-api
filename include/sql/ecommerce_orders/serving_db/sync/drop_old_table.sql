-- Phase 3: Cleanup — drop the table from TWO cycles ago.
-- Runs at the END of a successful run, so there is always one prior
-- cycle (`live_order_lookup_old`) available as a manual rollback safety net.

DROP TABLE IF EXISTS live_order_lookup_old_old CASCADE;

-- Called only after swap_staging_to_live.sql has succeeded this run.
-- TODO: rename to match your live table's naming convention
