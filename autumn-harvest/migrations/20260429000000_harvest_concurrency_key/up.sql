-- Add per-activity cluster-wide concurrency caps.
--
-- concurrency_key: optional grouping key shared by activities that should
-- share a budget (e.g. "stripe" for all Stripe-touching activities).
-- When NULL the row takes the existing claim path with zero overhead.
--
-- concurrency_cap: the maximum number of RUNNING tasks allowed at once for
-- the key. Set to the activity's max_concurrent at enqueue time. NULL means
-- no cap (only used when concurrency_key is also NULL).
--
-- Both ALTER TABLE statements are online-safe (no harvest_* table lock beyond
-- the standard short metadata lock of ADD COLUMN).
ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS concurrency_key TEXT;

ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS concurrency_cap INT4;

-- Partial index on RUNNING rows with a concurrency key so the saturation
-- check subquery in claim_task() uses an index scan instead of a seq scan.
-- Rows with NULL concurrency_key are excluded (zero index overhead for the
-- no-cap path).
CREATE INDEX IF NOT EXISTS harvest_task_queue_concurrency_key_running
    ON harvest_task_queue (concurrency_key)
    WHERE state = 'RUNNING' AND concurrency_key IS NOT NULL;
