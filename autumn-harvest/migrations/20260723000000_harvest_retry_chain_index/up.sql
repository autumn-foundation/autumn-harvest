-- Index the workflow-level retry chain link (issue #843).
--
-- `retry_of_exec_id` (issue #523) links a retry attempt to the predecessor
-- whose failure scheduled it. Before #843 the only reader was the result-wait
-- path; #843 routes every interactive/mutating operation (signal, cancel,
-- terminate, pause, resume, query, update, `result_snapshot`) through the same
-- successor lookup, so `WHERE retry_of_exec_id = $1` moved onto the hot path of
-- the management API.
--
-- The costly case is the MISS: proving "this FAILED run has no retry successor"
-- requires visiting every candidate row, which without an index is a full
-- (parallel) sequential scan of the hub table on every post-mortem read of a
-- failed run. Measured on 500k rows: ~63 ms parallel seq scan vs ~0.03 ms index
-- scan.
--
-- Partial (`WHERE retry_of_exec_id IS NOT NULL`) because only retry attempts
-- carry the column, mirroring `idx_harvest_wfx_continued_from` — the
-- structurally identical successor-link index for the continue-as-new chain
-- (issue #701).
--
-- On a live deployment prefer the concurrent form, which cannot run inside
-- Diesel's migration transaction:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_wfx_retry_of
--       ON harvest_workflow_executions (retry_of_exec_id)
--       WHERE retry_of_exec_id IS NOT NULL;
--
-- Additive, index-only: no column added, no data rewritten, no behaviour change.
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_retry_of
    ON harvest_workflow_executions (retry_of_exec_id)
    WHERE retry_of_exec_id IS NOT NULL;
