-- Activity schedule-to-close timeout (issue #378).
--
-- Adds an absolute wall-clock deadline for the entire life of an activity
-- (all retry attempts + back-off sleeps combined).  The column is NULL for
-- activities that do not declare `schedule_to_close`, preserving today's
-- unbounded behaviour.
--
-- The deadline is computed once at initial enqueue time as
-- `NOW() + schedule_to_close` and is never refreshed on retry, so the
-- timeout scanner can enforce it with a simple timestamptz comparison.

ALTER TABLE harvest_task_queue
    ADD COLUMN schedule_to_close_at TIMESTAMPTZ NULL;

-- Partial index for the timeout scanner: only rows with a deadline set and
-- still in an active state (RUNNING or PENDING) are hot; terminal rows
-- (COMPLETED, FAILED, CANCELLED) don't need scanning.
CREATE INDEX harvest_task_queue_schedule_to_close_idx
    ON harvest_task_queue (schedule_to_close_at)
    WHERE schedule_to_close_at IS NOT NULL
      AND state IN ('RUNNING', 'PENDING');
