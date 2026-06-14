-- issue #488: last-completion-result carryover for incremental scheduled jobs
--
-- schedule_id: links each workflow execution back to the harvest_schedules row
-- that fired it. NULL for manually-started (non-scheduled) executions.
-- Used by start_or_load_workflow_execution to resolve the most-recent COMPLETED
-- output for the same schedule (shard-local) and freeze it into the WorkflowStarted
-- event at fire time, so replay is deterministic and never re-queries.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS schedule_id UUID NULL;

-- scheduled_for: the logical schedule slot this run fires for. Carryover is ordered
-- by this (the previous logical fire), NOT completed_at, so overlapping / catch-up /
-- backfilled runs that finish out of slot order can't roll an incremental cursor
-- backward. NULL for manually-started (non-scheduled) executions.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ NULL;

-- Backfill schedule_id (and scheduled_for) for already-fired scheduled runs so the
-- first post-upgrade fire sees prior COMPLETED output instead of resetting to None.
-- Scheduled runs use workflow_id 'sched:{schedule_uuid}:{workflow_name}:{slot}' (see
-- scheduled_workflow_id), where slot is a Unix epoch (optionally with .micros). The
-- regex guards skip any malformed id so the casts can never error.
-- Note: only the UUID-bearing scheduled-id format `sched:{uuid}:{name}:{slot}` is
-- backfilled. Legacy pre-UUID ids (`sched:{name}:{slot}`) carry no schedule UUID, so
-- their rows stay at schedule_id = NULL and are simply excluded from carryover; the
-- first post-upgrade fire of such a schedule starts cursorless until one new-format
-- run completes. This is acceptable: the legacy id format predates scheduled carryover.
-- Reset forks reuse the source run's `sched:` workflow_id (see reset.rs), but they
-- are operator interventions deliberately excluded from scheduled carryover. Skip any
-- execution that has a WorkflowResetFork event so the backfill doesn't re-attach the
-- schedule lineage that the live reset path now withholds (schedule_id = NULL).
UPDATE harvest_workflow_executions hwe
SET schedule_id = split_part(workflow_id, ':', 2)::uuid
WHERE schedule_id IS NULL
  AND workflow_id LIKE 'sched:%'
  AND split_part(workflow_id, ':', 2) ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events e
      WHERE e.workflow_exec_id = hwe.id AND e.event_type = 'WorkflowResetFork'
  );

UPDATE harvest_workflow_executions
SET scheduled_for = to_timestamp(split_part(workflow_id, ':', 4)::double precision)
WHERE scheduled_for IS NULL
  AND workflow_id LIKE 'sched:%'
  AND split_part(workflow_id, ':', 4) ~ '^[0-9]+(\.[0-9]+)?$';

-- Partial index for the carryover lookup, ordered by the schedule slot:
--   SELECT output FROM harvest_workflow_executions
--   WHERE schedule_id = $1 AND state = 'COMPLETED' AND completed_at IS NOT NULL
--     AND scheduled_for < $2
--   ORDER BY scheduled_for DESC LIMIT 1;
--
-- Also serves the last_error query (adds state IN filter, same sort key).
-- Per-schedule cardinality is small; one index covers both queries. Excluding
-- still-running rows (completed_at NULL) keeps the index compact without losing
-- coverage (both queries filter completed_at IS NOT NULL).
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_schedule_carryover
    ON harvest_workflow_executions (schedule_id, scheduled_for DESC)
    WHERE schedule_id IS NOT NULL AND completed_at IS NOT NULL;
