-- Name-leading candidate index for quota_reconcile (issue #1631).
--
-- The scan now seeks once per registered workflow name. Each seek reads
-- `workflow_name = $name AND id > $cursor ORDER BY id LIMIT $batch`.
-- This index serves that read in `id` order with no sort. The work per
-- tick is bounded by names times batch size. Unrelated candidate rows in
-- other types are never read.
--
-- It replaces idx_harvest_we_quota_reconcile_candidates (`(id)` only).
-- Nothing else reads that index, so the write cost stays the same: one
-- partial index on the same predicate.
--
-- Like its predecessor, this index self-shrinks. A row leaves it when its
-- `quota_key` is set or its execution turns terminal.
--
-- On a live deployment prefer the concurrent form, which cannot run inside
-- Diesel's migration transaction. Build the new index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_quota_reconcile_name_id
--       ON harvest_workflow_executions (workflow_name, id)
--       WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');
-- An interrupted concurrent build leaves an invalid index, and
-- `IF NOT EXISTS` then skips it. Check `pg_index.indisvalid`. Drop and
-- rebuild an invalid index. Then drop the old index without a long lock:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_we_quota_reconcile_candidates;
CREATE INDEX IF NOT EXISTS idx_harvest_we_quota_reconcile_name_id
    ON harvest_workflow_executions (workflow_name, id)
    WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');

DROP INDEX IF EXISTS idx_harvest_we_quota_reconcile_candidates;
