DROP INDEX idx_harvest_completion_trigger_outbox_shard_next_attempt;

ALTER TABLE harvest_completion_trigger_outbox
    DROP COLUMN next_attempt_at;
