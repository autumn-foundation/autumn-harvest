-- issue #481: per-execution ambient context headers
--
-- context_headers: string key-value map attached at workflow start, propagated
-- automatically to all activities and child workflows without threading through
-- function signatures. Nullable JSONB (NULL = empty map for legacy executions).
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS context_headers JSONB NULL;

ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS context_headers JSONB NULL;
