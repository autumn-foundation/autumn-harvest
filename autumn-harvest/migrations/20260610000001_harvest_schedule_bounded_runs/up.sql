-- issue #478: bounded schedule runs (end_at + max_runs)
--
-- end_at:           absolute UTC cutoff; if next_run_at >= end_at the scheduler
--                   stops firing and marks the schedule exhausted.
-- max_runs:         total run budget; once runs_started reaches max_runs the
--                   scheduler stops firing and marks the schedule exhausted.
-- runs_started:     monotonically-increasing count of executions actually started
--                   by this schedule (not merely claimed or skipped).
-- exhausted_at:     set to NOW() when the schedule transitions to the terminal
--                   exhausted state; NULL = still active.
-- exhausted_reason: machine-readable string: 'end_at_reached' or
--                   'max_runs_exhausted'.  NULL = not exhausted.
ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS end_at            TIMESTAMPTZ     NULL,
    ADD COLUMN IF NOT EXISTS max_runs          INTEGER         NULL,
    ADD COLUMN IF NOT EXISTS runs_started      INTEGER         NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS exhausted_at      TIMESTAMPTZ     NULL,
    ADD COLUMN IF NOT EXISTS exhausted_reason  TEXT            NULL;

-- Partial index: fast scan for exhausted schedules (operator visibility, NOT
-- used for the scheduler hot path — the hot path already filters is_paused and
-- auto_paused_at; we add an exhausted_at filter too).
CREATE INDEX IF NOT EXISTS idx_harvest_schedules_exhausted
    ON harvest_schedules (exhausted_at)
    WHERE exhausted_at IS NOT NULL;
