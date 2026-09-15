-- Revert issue #958's partitioning machinery.
--
-- Only safe on an UNPARTITIONED deployment. Run `harvest partition disable`
-- first if the layout was converted: dropping the cohort column out from under
-- a partitioned `harvest_events` would drop its partition key.
--
-- `idx_harvest_we_created_at` is deliberately NOT dropped here. This
-- migration never creates it. `harvest partition enable` (and `plan`)
-- creates it, as part of opting in. That keeps this inert `up.sql` from
-- building an index that would hold `SHARE` on
-- `harvest_workflow_executions`. An unconditional `DROP INDEX` here would
-- remove an unrelated, pre-existing index of the same name an operator
-- had created themselves before ever opting in. `harvest partition
-- disable` -- the path that created it -- drops it instead.
DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_events_require_execution();
ALTER TABLE harvest_events DROP COLUMN IF EXISTS cohort;
DROP FUNCTION IF EXISTS harvest_event_cohort(TIMESTAMPTZ);
