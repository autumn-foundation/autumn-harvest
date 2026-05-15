-- Recreate harvest_dag_runs for rollback purposes.
-- Note: existing data cannot be restored from this migration alone;
-- restore from a backup if you need the historical rows.

CREATE TABLE harvest_dag_runs (
    id UUID PRIMARY KEY,
    dag_name TEXT NOT NULL,
    workflow_exec_id UUID REFERENCES harvest_workflow_executions(id) ON DELETE SET NULL,
    state TEXT NOT NULL DEFAULT 'QUEUED',
    logical_date TIMESTAMPTZ NOT NULL,
    data_interval_start TIMESTAMPTZ NOT NULL,
    data_interval_end TIMESTAMPTZ NOT NULL,
    conf JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_name, logical_date)
);
