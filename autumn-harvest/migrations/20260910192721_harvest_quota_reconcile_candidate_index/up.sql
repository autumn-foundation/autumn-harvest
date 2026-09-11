-- Partial index for quota_reconcile's candidate scan (issue #1226).
--
-- quota_reconcile::CANDIDATE_SQL filters `quota_key IS NULL AND state IN
-- ('RUNNING', 'PAUSED')`. The existing idx_harvest_we_state (migration
-- 20260409000000_harvest_initial) only covers `state = 'RUNNING'`, so the
-- PAUSED branch of that IN forced a full sequential scan of
-- harvest_workflow_executions on every reconcile tick -- unbounded by the
-- non-terminal row count, contrary to the module's own doc comment.
--
-- This index self-shrinks: a row leaves it the moment quota_reconcile (or
-- a fresh admission) sets its quota_key, so it always covers exactly the
-- current candidate set, not the table's full history. The `(id)` key is
-- not just eligibility bookkeeping: quota_reconcile's candidate scan orders
-- by `id` and resumes from a cursor, so this index also backs that keyset
-- scan directly -- a workflow-name-and-quota-key column list would not.
--
-- On a live deployment prefer the concurrent form, which cannot run inside
-- Diesel's migration transaction:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_quota_reconcile_candidates
--       ON harvest_workflow_executions (id)
--       WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');
CREATE INDEX IF NOT EXISTS idx_harvest_we_quota_reconcile_candidates
    ON harvest_workflow_executions (id)
    WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');
