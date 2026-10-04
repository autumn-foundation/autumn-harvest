-- Record which claim started its activity handler (issue #1809).
--
-- A claim sets started_at and increments attempt. The handler starts later,
-- in the transaction that appends ActivityStarted. A timeout can fire in the
-- gap between the two. Such a timeout says nothing about the downstream, so
-- it must not feed the circuit breaker.
--
-- The start transaction writes the claim's attempt to this column. Each
-- claim increments attempt, so the column equals attempt only after the
-- current claim started its handler. No path needs to clear it.
--
-- A nullable column with no default changes only the catalog. The ALTER
-- still takes an ACCESS EXCLUSIVE lock on a hot table, so it waits at most
-- 5 s for that lock. Then the migration fails, and the operator runs it
-- again. Traffic does not queue behind a blocked ALTER.
SELECT set_config('lock_timeout', '5s', true);

ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS handler_started_attempt INT4;
ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS timed_out_claims TIMESTAMPTZ[];

COMMENT ON COLUMN harvest_task_queue.handler_started_attempt IS
    'The attempt whose activity handler started (issue #1809). Written with '
    'ActivityStarted. Equal to attempt only after the current claim started '
    'its handler. NULL when no attempt started.';
COMMENT ON COLUMN harvest_task_queue.timed_out_claims IS
    'The started_at of each claim that the timeout enforcer timed out after '
    'its handler started, newest last (issue #1809). The worker that held a '
    'claim takes its own started_at out of here to tell a timeout from any '
    'other lost claim. Each entry used one attempt, so there are at most '
    'max_attempts entries.';
