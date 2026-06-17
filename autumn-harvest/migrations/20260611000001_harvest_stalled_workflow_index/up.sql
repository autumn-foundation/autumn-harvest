-- Index to support efficient per-execution last-event-time lookups used by
-- the no_progress_minutes stall filter (issue #486).
-- The composite (workflow_exec_id, timestamp DESC) key lets Postgres stop at
-- the first row per execution, turning an EXISTS check into an O(1) index
-- probe rather than a sequential scan of the entire events table.
CREATE INDEX IF NOT EXISTS idx_harvest_events_exec_last
    ON harvest_events (workflow_exec_id, timestamp DESC);
