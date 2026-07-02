-- Indexes to bring GET /admin/usage (issue #596) inside its stated
-- <2s/250k-execution SLA. Without these, execution_starts, terminal_counts,
-- and the merged activity_events CTE in usage.rs's usage_sql() fall back to
-- full/near-full table scans of harvest_workflow_executions / harvest_events
-- regardless of the requested [from, to] window size.
--
-- NOTE: for zero-downtime rollout against a live, already-large database,
-- these can be built with CREATE INDEX CONCURRENTLY outside a transaction
-- before running this migration; IF NOT EXISTS makes this migration's own
-- CREATE INDEX a safe no-op either way.

-- execution_starts CTE: `w.shard_id = $1 AND w.started_at BETWEEN $3 AND $4`.
CREATE INDEX IF NOT EXISTS idx_harvest_we_shard_started
    ON harvest_workflow_executions (shard_id, started_at);

-- terminal_counts CTE (issue #596 redrive-immutability fix, PR #895 review):
-- `event_type IN ('WorkflowCompleted','WorkflowFailed','WorkflowCancelled',
-- 'WorkflowExecutionTimedOut') AND timestamp BETWEEN $3 AND $4`. Terminal
-- outcomes are derived from these durable events rather than the mutable
-- harvest_workflow_executions.completed_at column, so this index replaces
-- the shard_id+completed_at index this migration originally shipped with.
CREATE INDEX IF NOT EXISTS idx_harvest_events_workflow_terminal_type_ts
    ON harvest_events (event_type, timestamp)
    WHERE event_type IN (
        'WorkflowCompleted',
        'WorkflowFailed',
        'WorkflowCancelled',
        'WorkflowExecutionTimedOut'
    );

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
