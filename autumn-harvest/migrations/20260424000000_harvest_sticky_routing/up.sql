-- Add sticky cross-worker routing columns to harvest_task_queue for legacy
-- databases that were created before the initial migration was amended to
-- include them. Fresh installs applying the updated initial migration already
-- have these columns; this migration is a no-op there.
--
-- Sticky routing lets workflow follow-up tasks preferentially land on the
-- worker whose in-process LRU cache holds the workflow state. When the pinned
-- worker does not claim the task before `sticky_until` elapses, any worker may
-- claim it, so a crashed or slow sticky worker never blocks progress.

ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS sticky_worker_id TEXT,
    ADD COLUMN IF NOT EXISTS sticky_until     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS sticky_timeout   INTERVAL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'harvest_task_queue'::regclass
          AND conname = 'harvest_tq_sticky_pair_ck'
    ) THEN
        ALTER TABLE harvest_task_queue
            ADD CONSTRAINT harvest_tq_sticky_pair_ck
            CHECK ((sticky_worker_id IS NULL) = (sticky_until IS NULL));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_harvest_tq_sticky_poll ON harvest_task_queue
    (sticky_worker_id, queue_name, priority DESC, scheduled_at)
    WHERE state = 'PENDING' AND sticky_worker_id IS NOT NULL;
