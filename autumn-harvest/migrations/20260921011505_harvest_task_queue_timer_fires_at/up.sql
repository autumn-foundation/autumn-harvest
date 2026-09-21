-- Add a timer-provenance column to harvest_task_queue (issue #1402).
--
-- queue::reschedule_task sets scheduled_at to a timer's fires_at, and
-- nothing else touches scheduled_at that way. stall_diagnosis trusts an
-- exact scheduled_at == fires_at match as proof this row is that timer's
-- wake target.
--
-- Some paths then move scheduled_at again without changing the wake
-- reason: a queue-pause resume credits held time back
-- (queue_pause::resume_shift_scheduled_at_query), a crashed worker's claim
-- is reclaimed (poison_pill::requeue_orphan_stmt), or a capability-miss
-- release retries the row (queue::release_task_for_capability_miss_query).
-- Each can drift scheduled_at away from fires_at by an unbounded amount,
-- breaking the exact-match proof for a row that is still, genuinely, that
-- timer's wake target.
--
-- This column is the fix: queue::reschedule_task stamps it alongside
-- scheduled_at, and it survives every drift above because none of them
-- writes it. A row whose wake reason actually changes (a signal, a child,
-- or an external handoff resolves instead) is repended through a path
-- that clears it, so a stale value can never outlive the timer it named.
ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS timer_fires_at TIMESTAMPTZ;

COMMENT ON COLUMN harvest_task_queue.timer_fires_at IS
    'The fires_at of the durable timer this row is currently armed for '
    '(issue #1402). Set only by queue::reschedule_task. Survives a '
    'later scheduled_at drift with the same wake reason. NULL when no '
    'timer owns this row, or once a different wake reason repends it.';
