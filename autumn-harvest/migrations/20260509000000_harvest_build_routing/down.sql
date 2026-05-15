-- Rollback: worker build-id routing (issue #171)

DROP TABLE IF EXISTS harvest_build_compat;
DROP TABLE IF EXISTS harvest_build_policies;

DROP INDEX IF EXISTS harvest_wfe_assigned_build_id;
DROP INDEX IF EXISTS harvest_task_queue_required_build_id_pending;

ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS required_build_id;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS assigned_build_id;

ALTER TABLE harvest_workers
    DROP COLUMN IF EXISTS deployment_name,
    DROP COLUMN IF EXISTS build_id;
