-- Mark the first workflow task of a freshly admitted run (issue #1824).
--
-- The claim order reads this flag. Within one priority level, a marked row
-- whose attempt is 0 sorts as if it were due 30 seconds later. So the
-- continuations of running workflows go first under a backlog.
--
-- Only the workflow start path sets the flag. A child spawn, a
-- continue-as-new, a reset fork, a workflow retry and a DLQ redrive extend
-- admitted work, so their rows keep the default.
--
-- The default is FALSE. A row from before this migration is therefore never
-- demoted, and the claim order of in-flight work does not change at upgrade.
-- A constant default needs no table rewrite on Postgres 11 or later.
--
-- ADD COLUMN takes an ACCESS EXCLUSIVE lock for a moment. A held lock fails
-- the migration after 5 s, so claims and enqueues do not queue behind it. The
-- operator then clears the blocker and re-runs the migration.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS new_start BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN harvest_task_queue.new_start IS
    'TRUE on the first workflow task of a freshly admitted run (issue #1824). '
    'Set only by the workflow start path. The claim order lets continuations '
    'go first while such a row has attempt = 0.';
