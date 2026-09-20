DROP INDEX idx_harvest_completion_trigger_outbox_source_trigger;

ALTER TABLE harvest_completion_trigger_fires
    DROP COLUMN target_shard,
    DROP COLUMN target_workflow_name;
