-- Indexes to bring GET /admin/usage (issue #596) inside its stated
-- <2s/250k-execution SLA. Without these, execution_counts and the merged
-- activity_events CTE in usage.rs's usage_sql() fall back to full/near-full
-- table scans of harvest_workflow_executions / harvest_events regardless of
-- the requested [from, to] window size.
--
-- NOTE: for zero-downtime rollout against a live, already-large database,
-- these can be built with CREATE INDEX CONCURRENTLY outside a transaction
-- before running this migration; IF NOT EXISTS makes this migration's own
-- CREATE INDEX a safe no-op either way.

-- execution_counts CTE: `w.shard_id = $1 AND (w.started_at BETWEEN $3 AND $4
-- OR w.completed_at BETWEEN $3 AND $4)`. Two compound indexes let Postgres
-- BitmapOr the two range predicates instead of scanning every row for the shard.
CREATE INDEX IF NOT EXISTS idx_harvest_we_shard_started
    ON harvest_workflow_executions (shard_id, started_at);

CREATE INDEX IF NOT EXISTS idx_harvest_we_shard_completed
    ON harvest_workflow_executions (shard_id, completed_at)
    WHERE completed_at IS NOT NULL;

-- activity_events CTE (merged by issue #596's F10 fix): `event_type IN
-- ('ActivityStarted','ActivityCompleted','ActivityFailed','ActivityTimedOut')
-- AND timestamp BETWEEN $3 AND $4`. Keep the event_type list in sync with
-- usage.rs's usage_sql().
CREATE INDEX IF NOT EXISTS idx_harvest_events_activity_type_ts
    ON harvest_events (event_type, timestamp)
    WHERE event_type IN (
        'ActivityStarted',
        'ActivityCompleted',
        'ActivityFailed',
        'ActivityTimedOut'
    );
