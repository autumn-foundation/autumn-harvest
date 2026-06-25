-- Partial index to support the workflow-type reachability query (issue #520).
--
-- `GET /admin/workflow-types/reachability` runs a GROUP BY workflow_name scan
-- over non-terminal executions. Without this index the engine must scan every
-- row in harvest_workflow_executions and filter by state, which is expensive
-- once the table grows.
--
-- `started_at` is included as a second column so PostgreSQL can satisfy both
-- the GROUP BY workflow_name and the MIN(started_at) aggregate entirely from
-- the index (index-only scan), avoiding heap lookups for each non-terminal row.
--
-- The WHERE clause must be kept in sync with the terminal-state list in
-- erase::is_terminal_state and NON_TERMINAL_COUNTS_SQL in execution.rs.

CREATE INDEX IF NOT EXISTS idx_harvest_we_non_terminal_wf_name
    ON harvest_workflow_executions (workflow_name, started_at)
    WHERE state NOT IN (
        'COMPLETED',
        'FAILED',
        'CANCELLED',
        'TIMED_OUT',
        'CONTINUED_AS_NEW',
        'TERMINATED'
    );
