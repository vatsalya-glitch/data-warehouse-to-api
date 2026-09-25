-- Phase 3: Cleanup — drop the table from TWO cycles ago.
-- Runs at the END of a successful run, so there is always one prior
-- cycle (`live_order_lookup_old`) available as a manual rollback safety net.
-- (SQLite has no CASCADE on DROP TABLE — see create_staging_table.sql.)

DROP TABLE IF EXISTS live_order_lookup_old_old;

-- Called only after swap_staging_to_live.sql has succeeded this run.
