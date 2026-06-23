-- Add a row insert-time column to harvest_task_queue (issue #501 review).
--
-- The schedule-to-start SLI needs each task's true eligibility instant. An
-- immediate task is backdated (`scheduled_at = NOW() - skew`) so it stays
-- claimable under clock skew, while a delayed/retried task sets `scheduled_at`
-- to an explicit future instant. GREATEST(scheduled_at, created_at) recovers the
-- correct eligibility in both cases: the insert time for an immediate task, the
-- explicit time for a delayed/retried one.
--
-- Added nullable with NO default first (instant, no table rewrite / ACCESS
-- EXCLUSIVE lock on a hot queue), then SET DEFAULT clock_timestamp() so only
-- future inserts are stamped. clock_timestamp() (not NOW()/transaction_timestamp())
-- is the actual statement-execution wall clock, so a task inserted late inside a
-- large workflow-persistence transaction is stamped at its real insert time rather
-- than the transaction's start — avoiding inflated schedule-to-start / oldest-age
-- readings. Pre-upgrade PENDING rows keep created_at = NULL and the SLI falls back
-- to scheduled_at via COALESCE until they drain.
ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE harvest_task_queue ALTER COLUMN created_at SET DEFAULT clock_timestamp();

CREATE INDEX IF NOT EXISTS idx_harvest_we_canary_order
    ON harvest_workflow_executions (created_at DESC, id DESC)
    WHERE state = 'RUNNING';
