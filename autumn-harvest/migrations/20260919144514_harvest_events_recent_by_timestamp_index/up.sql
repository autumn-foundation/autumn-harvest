-- Index to support the recent_event_execs CTE in
-- status_summary::count_stalled_candidates and any future timestamp-only
-- scan of harvest_events (issue #1643).
-- Neither existing harvest_events index leads with timestamp, so a query
-- filtering on timestamp alone would fall back to a full table scan. The
-- covering (timestamp, workflow_exec_id) key turns that into an index range
-- scan plus an index-only projection.
CREATE INDEX IF NOT EXISTS idx_harvest_events_recent_by_timestamp
    ON harvest_events (timestamp, workflow_exec_id);
