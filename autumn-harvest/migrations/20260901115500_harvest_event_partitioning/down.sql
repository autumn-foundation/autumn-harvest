-- Revert issue #958's partitioning machinery.
--
-- Only safe on an UNPARTITIONED deployment. Run `harvest partition disable`
-- first if the layout was converted: dropping the cohort column out from under
-- a partitioned `harvest_events` would drop its partition key.
--
-- `idx_harvest_we_created_at` is deliberately NOT dropped here. up.sql never
-- creates it (see the note there) -- only `harvest partition enable` and
-- `harvest partition plan` do, at conversion time -- so this migration does
-- not own it. An unconditional DROP would delete an unrelated pre-upgrade
-- index of the same name on a deployment that had created its own before
-- upgrading. Rollback of an inert migration must stay inert (issue #1270 item
-- 13). Its removal belongs to `harvest partition disable`, the path that
-- created it.
DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_events_require_execution();
ALTER TABLE harvest_events DROP COLUMN IF EXISTS cohort;
DROP FUNCTION IF EXISTS harvest_event_cohort(TIMESTAMPTZ);
