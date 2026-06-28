-- issue #534: per-schedule run history with terminal outcomes
--
-- origin: the dispatch source of a schedule-attributed execution, used to keep a
-- backfill storm or ad-hoc operator trigger from masquerading as normal cadence in
-- the GET /admin/schedules/{id}/runs view. One of 'scheduled', 'backfill', or
-- 'manual_trigger' for schedule-attributed runs; NULL for every non-scheduled run
-- (manual start, signal-with-start, child workflows, etc.). Metadata only — it never
-- affects replay, carryover, or shard routing.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS origin TEXT NULL;

-- Backfill historical schedule-attributed rows to 'scheduled'. Pre-migration runs
-- cannot distinguish a scheduled fire from a backfill (both carried only schedule_id),
-- so they are reported as 'scheduled'. This is documented as expected behaviour:
-- the origin distinction is authoritative only for runs started after this migration.
UPDATE harvest_workflow_executions
SET origin = 'scheduled'
WHERE origin IS NULL AND schedule_id IS NOT NULL;

-- Partial index for the per-schedule run-history listing, ordered newest-first:
--   SELECT id, scheduled_for, started_at, completed_at, state, origin
--   FROM harvest_workflow_executions
--   WHERE schedule_id = $1 [AND state = ANY($states)]
--     [AND (started_at, id) < ($cursor_ts, $cursor_id)]
--   ORDER BY started_at DESC, id DESC LIMIT $n;
--
-- The same index serves the scheduled-cadence summary aggregate
-- (WHERE schedule_id = $1 AND origin = 'scheduled' GROUP BY state). Per-schedule
-- cardinality is bounded by the execution retention window, so one index covers both.
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_schedule_runs
    ON harvest_workflow_executions (schedule_id, started_at DESC, id DESC)
    WHERE schedule_id IS NOT NULL;
