-- Roll back migration 20261001190405 (issue #1667).
--
-- This migration restores nothing. An ordinary CREATE INDEX holds a table lock
-- and blocks audit writes for the whole build. A concurrent build cannot run
-- inside a migration transaction. An older binary works without the index,
-- only slower. Build it by hand, outside a transaction, if you need it:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_audit_log_unexported_idx
--       ON harvest_audit_log (occurred_at, id) WHERE export_seq IS NULL;
SELECT 1;
