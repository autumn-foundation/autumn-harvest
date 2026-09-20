ALTER TABLE harvest_completion_trigger_fires
    DROP COLUMN target_shard,
    DROP COLUMN target_workflow_name;
