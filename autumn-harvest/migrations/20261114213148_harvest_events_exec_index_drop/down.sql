-- Revert #2684 for `harvest_events`. Restore the index as it was built.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2684 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS idx_harvest_events_exec
    ON harvest_events (workflow_exec_id, event_id);
