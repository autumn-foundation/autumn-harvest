-- Partial index to support the workflow-type reachability query (issue #520).
--
-- `GET /admin/workflow-types/reachability` fans out one query per shard.
-- Each call supplies `shard_id = $2`, then groups by `workflow_name` and
-- computes `MIN(started_at)`.  The leading `shard_id` column lets PostgreSQL
-- narrow to one shard's rows first; `workflow_name` covers the GROUP BY; and
-- `started_at` satisfies the MIN aggregate — enabling an index-only scan that
-- avoids any heap lookups.  Without `shard_id` in the index, deployments where
-- multiple logical shards share one Postgres database would scan all non-terminal
-- rows (O(shards × live executions)) on every reachability check.
--
-- The WHERE clause must be kept in sync with the terminal-state list in
-- erase::is_terminal_state and NON_TERMINAL_COUNTS_SQL in execution.rs.

CREATE INDEX IF NOT EXISTS idx_harvest_we_non_terminal_wf_name
    ON harvest_workflow_executions (shard_id, workflow_name, started_at)
    WHERE state NOT IN (
        'COMPLETED',
        'FAILED',
        'CANCELLED',
        'TIMED_OUT',
        'CONTINUED_AS_NEW',
        'TERMINATED'
    );
