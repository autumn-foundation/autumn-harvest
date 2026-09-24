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

-- Backfill (issue #1402): a row already armed by reschedule_task
-- before this migration ran gets NULL here, even though its scheduled_at
-- still exactly matches an unfired timer's fires_at. is_the_missed_timer_wake
-- trusts that exact match outright today, so the row is safe right now. But
-- a later drift (a queue-pause resume credit, an orphan reclaim, a
-- capability-miss release) can move scheduled_at away from fires_at without
-- changing the wake reason -- and with no marker to survive that drift, the
-- exact false negative issue #1402 fixes reopens for that one row, until it
-- is freshly rescheduled.
--
-- This closes that gap for every row the exact-match branch already trusts:
-- a scheduled_at that exactly equals a still-unfired timer's fires_at on the
-- same execution is unambiguous, so stamping timer_fires_at from it merely
-- makes durable a fact the classifier already accepts unconditionally.
UPDATE harvest_task_queue AS tq
SET timer_fires_at = tq.scheduled_at
FROM harvest_timers AS t
WHERE tq.task_type = 'workflow'
  AND tq.state IN ('PENDING', 'RUNNING')
  AND tq.workflow_exec_id = t.workflow_exec_id
  AND tq.scheduled_at = t.fires_at
  AND NOT t.fired;
