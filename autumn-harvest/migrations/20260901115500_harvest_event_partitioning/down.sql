-- Revert issue #958's partitioning machinery.
--
-- Only safe on an UNPARTITIONED deployment. Run `harvest partition disable`
-- first if the layout was converted: dropping the cohort column out from under
-- a partitioned `harvest_events` would drop its partition key.
--
-- `idx_harvest_we_created_at` is dropped here ONLY when it carries the
-- marker comment `partition::enable_sql`/`migration_plan_steps` stamp when
-- THEY create it fresh (`DROP_GATE_INDEX_MARKER` in partition.rs). up.sql
-- never creates this index itself (see the note it carries) -- only
-- `harvest partition enable`/`plan` do, at conversion time -- so an
-- unconditional DROP here would delete an unrelated pre-upgrade index of
-- the same name on a deployment that had created its own before upgrading.
-- Checking the marker keeps rollback of an inert migration inert (issue
-- #1270 item 13) while still cleaning up after a conversion plan that
-- created and marked this index, then was abandoned before `harvest
-- partition disable` ever ran to remove it (a later Codex finding on the
-- same item).
DO $$
DECLARE marker text;
BEGIN
    SELECT description INTO marker
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
     WHERE c.relname = 'idx_harvest_we_created_at' AND n.nspname = current_schema();
    IF marker = 'harvest#958 drop-gate index; created by partition enable, safe for partition disable to remove' THEN
        EXECUTE 'DROP INDEX idx_harvest_we_created_at';
    END IF;
END $$;
DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_events_require_execution();
ALTER TABLE harvest_events DROP COLUMN IF EXISTS cohort;
DROP FUNCTION IF EXISTS harvest_event_cohort(TIMESTAMPTZ);
