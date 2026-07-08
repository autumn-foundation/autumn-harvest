-- Issue #810: gate completion triggers on source workflow output.
-- Additive only. NULL condition = unconditional trigger (byte-identical
-- legacy behavior for every pre-#810 registered trigger).
ALTER TABLE harvest_completion_triggers
    ADD COLUMN condition JSONB NULL;

-- Resolved-skip recording (exactly-once contract, AC5): a condition-skipped
-- fire inserts its (source_exec_id, trigger_id) row through the same
-- ON CONFLICT DO NOTHING PK path a real fire uses, with the skip reason
-- recorded here. NULL = fired (every pre-#810 row was a real fire);
-- 'condition_unmet' / 'condition_invalid' = resolved-skipped.
ALTER TABLE harvest_completion_trigger_fires
    ADD COLUMN outcome TEXT NULL;
