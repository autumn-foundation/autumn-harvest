-- Revert issue #958's partitioning machinery.
--
-- Only safe on an UNPARTITIONED deployment. Run `harvest partition disable`
-- first if the layout was converted: dropping the cohort column out from under
-- a partitioned `harvest_events` would drop its partition key.
DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_events_require_execution();
DROP INDEX IF EXISTS idx_harvest_we_created_at;
ALTER TABLE harvest_events DROP COLUMN IF EXISTS cohort;
DROP FUNCTION IF EXISTS harvest_event_cohort(TIMESTAMPTZ);
