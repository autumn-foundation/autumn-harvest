ALTER TABLE harvest_external_tasks
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX idx_harvest_ext_state_updated
    ON harvest_external_tasks (state, updated_at DESC);

CREATE INDEX idx_harvest_ext_workflow_exec
    ON harvest_external_tasks (workflow_exec_id);

CREATE INDEX idx_harvest_ext_activity_name
    ON harvest_external_tasks (name);
