-- Revert to the legacy workflow execution uniqueness key.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'harvest_workflow_executions'::regclass
          AND conname = 'harvest_workflow_executions_workflow_name_workflow_id_key'
    ) THEN
        ALTER TABLE harvest_workflow_executions
            DROP CONSTRAINT harvest_workflow_executions_workflow_name_workflow_id_key;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'harvest_workflow_executions'::regclass
          AND conname = 'harvest_workflow_executions_workflow_id_run_id_key'
    ) THEN
        ALTER TABLE harvest_workflow_executions
            ADD CONSTRAINT harvest_workflow_executions_workflow_id_run_id_key
            UNIQUE (workflow_id, run_id);
    END IF;
END $$;
