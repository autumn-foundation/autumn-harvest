-- Add the uniqueness key required by idempotent workflow starts.
--
-- Existing databases may have applied the initial Harvest migration when the
-- only workflow execution uniqueness key was `(workflow_id, run_id)`. The
-- runtime now uses `ON CONFLICT (workflow_name, workflow_id)`, so upgrade those
-- databases while no-oping cleanly on fresh installs that already have it.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'harvest_workflow_executions'::regclass
          AND conname = 'harvest_workflow_executions_workflow_id_run_id_key'
    ) THEN
        ALTER TABLE harvest_workflow_executions
            DROP CONSTRAINT harvest_workflow_executions_workflow_id_run_id_key;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'harvest_workflow_executions'::regclass
          AND conname = 'harvest_workflow_executions_workflow_name_workflow_id_key'
    ) THEN
        ALTER TABLE harvest_workflow_executions
            ADD CONSTRAINT harvest_workflow_executions_workflow_name_workflow_id_key
            UNIQUE (workflow_name, workflow_id);
    END IF;
END $$;
