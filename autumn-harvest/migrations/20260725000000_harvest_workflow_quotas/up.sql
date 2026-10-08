-- Per-tenant resource quotas (issue #946): aggregate active-execution,
-- history-byte, and dead-letter caps enforced per resolved tenant key at
-- admission time, before any WorkflowStarted event.
--
-- `quota_key` mirrors the resolved-key placement pattern already used by
-- `concurrency_key` (issue #247), but lives directly on
-- `harvest_workflow_executions` rather than `harvest_task_queue`: quota
-- enforcement runs *before* the execution row (and therefore before any task
-- row) exists, so the count query must be servable purely from
-- already-admitted execution rows.
--
-- Additive nullable columns only -- no new WorkflowEvent variant, no replay
-- impact. Rows with no QuotaPolicy configured carry quota_key = NULL and are
-- excluded by the partial indexes below, so a no-quota workflow pays zero
-- storage/index overhead (issue #946 AC9).
--
-- KNOWN LIMITATION (issue #1226): this migration deliberately does NOT
-- backfill quota_key for executions that were already active at the moment
-- a quota policy is first declared/deployed for their workflow type. The
-- key-resolution expression (`quota::resolve_quota_key`, e.g.
-- `"input.tenant_id"`) is Rust application code, not something a pure-SQL
-- migration can evaluate against each row's stored `input` JSON payload --
-- backfilling it here would require re-implementing an arbitrary dot-path
-- JSON field resolver in SQL and keeping it byte-for-byte in sync with the
-- real one forever. The practical effect is a bounded rollout-window gap:
-- an execution that started running *before* its workflow type's
-- QuotaPolicy was deployed keeps `quota_key = NULL` for the rest of its
-- run and is therefore invisible to `load_quota_usage`'s
-- `(workflow_name, quota_key, state)` count until it completes, fails, or
-- is otherwise collected -- it neither counts against nor is blocked by
-- the newly-declared cap. See issue #1226 for the proposed registry-aware
-- startup-reconciliation fix.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS quota_key TEXT NULL;

-- Backs both `max_active_executions` (a direct COUNT(*)) and the
-- `max_history_bytes` join (this index resolves the matching execution rows;
-- the SUM(pg_column_size(...)) itself reads harvest_events via the existing
-- idx_harvest_events_exec index on (workflow_exec_id, event_id)).
CREATE INDEX IF NOT EXISTS idx_harvest_we_quota_active
    ON harvest_workflow_executions (workflow_name, quota_key, state)
    WHERE quota_key IS NOT NULL;

-- Denormalized workflow_name + quota_key on harvest_dead_letters, mirroring
-- the existing owner/severity precedent (issue #372, migration
-- 20260601000002_harvest_ownership_metadata) so `max_dead_letters` counting
-- never depends on the originating execution row still existing. DLQ entries
-- are the durable long-term failure record and routinely outlive their
-- execution's retention window (issue #737) -- a join through
-- harvest_workflow_executions would silently under-count once the execution
-- row is retention-collected.
ALTER TABLE harvest_dead_letters
    ADD COLUMN IF NOT EXISTS workflow_name TEXT NULL;

ALTER TABLE harvest_dead_letters
    ADD COLUMN IF NOT EXISTS quota_key TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_harvest_dl_quota
    ON harvest_dead_letters (workflow_name, quota_key)
    WHERE quota_key IS NOT NULL;
