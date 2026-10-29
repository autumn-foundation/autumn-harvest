-- Operator overrides for per-type run defaults (issue #2387, epic #2384).
--
-- `WorkflowInfo::sla` and `WorkflowInfo::execution_timeout` are declared in
-- code. A DAG lowers to a shadow workflow type of the same name, so its
-- `DagInfo` values arrive here too. Each start path resolves the run's
-- `sla_deadline_at` and `deadline_at` from those values, so a change needed a
-- restart.
--
-- This table holds the values that an operator sets at runtime, keyed by the
-- workflow type name. Precedence: override, then code. A NULL column inherits
-- the code value. A row with both columns NULL is never stored.
--
-- The table is fleet-wide and lives on the default shard, as
-- `harvest_admission_gates` does. Each process loads it into a cache with a
-- bounded TTL, so a start pays no query for it.
--
-- The table is new, so no other session holds a lock on it.
CREATE TABLE harvest_run_default_overrides (
    workflow_name          TEXT        PRIMARY KEY,
    sla_secs               BIGINT      CHECK (sla_secs > 0),
    execution_timeout_secs BIGINT      CHECK (execution_timeout_secs > 0),
    reason                 TEXT,
    updated_by             TEXT        NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (sla_secs IS NOT NULL OR execution_timeout_secs IS NOT NULL)
);

COMMENT ON TABLE harvest_run_default_overrides IS
    'Issue #2387: operator overrides for per-type sla and execution_timeout. A NULL column inherits the code value.';
