-- Issue #1401 (Codex review, PR #1673).
--
-- `backup verify`'s completion-trigger fire probe re-derives a delivered
-- fire's target shard via `ShardRouter::pick_for_new_workflow` and its
-- target workflow name via a join to `harvest_completion_triggers`. Both
-- reconstruct a historical fact from CURRENT, mutable state:
--
-- * The router the probe builds treats every supplied shard as writable, so
--   a fire relayed while some shard was DRAINED (readable, not writable)
--   resolves to a different pick than the live relay made at the time.
-- * `sync_completion_triggers` updates an existing trigger's
--   `target_workflow_name` in place (`ON CONFLICT (id) DO UPDATE`), so a
--   trigger whose target changed after a historical fire is joined against
--   the WRONG name.
--
-- Both misreadings can turn a healthy restore into a false
-- `completion_trigger_fire_lost`/`_unproven`, or hide a genuine one.
--
-- Persisting the resolved shard and name on the fires row at relay time
-- removes the need to reconstruct either: the row itself becomes the
-- historical record. Both columns are NULLABLE and NULL on every
-- pre-migration row -- `backup_verify` falls back to the old
-- reconstruction for those, so this is purely additive.
ALTER TABLE harvest_completion_trigger_fires
    ADD COLUMN target_shard INTEGER,
    ADD COLUMN target_workflow_name VARCHAR(255);

-- `backup verify`'s scan excludes a fire still awaiting relay with
-- `NOT EXISTS (... WHERE o.source_exec_id = f.source_exec_id AND
-- o.trigger_id = f.trigger_id)` against this table (Codex review,
-- PR #1673). The table's only other index starts with `target_shard`, so
-- that predicate cannot use it. The scan can run up to 1,000 pages; each
-- page ran this anti-join against the whole outbox with no supporting
-- index.
CREATE INDEX idx_harvest_completion_trigger_outbox_source_trigger
    ON harvest_completion_trigger_outbox (source_exec_id, trigger_id);
