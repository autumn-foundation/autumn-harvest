-- Poison-pill task quarantine (issue #367).
--
-- A poison-pill task crashes the worker process (panic, OOM, segfault, hard
-- exit) instead of returning a clean Err. The row it was running on stays in
-- RUNNING state after the worker dies. The worker-liveness-driven reclaim
-- scanner re-queues such orphaned rows, but a deterministic crasher would loop
-- forever, taking down one worker per attempt.
--
-- crash_strikes counts how many times a task has been reclaimed from a dead
-- worker without ever completing (distinct from `attempt`, which counts clean
-- claims). Once it reaches the operator-configured threshold the reclaim
-- scanner quarantines the task to harvest_dead_letters instead of re-queueing.
--
-- Defaults to 0. Existing rows and existing deployments see no behaviour change
-- until the reclaim scanner observes a dead worker holding the row.

ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS crash_strikes INT NOT NULL DEFAULT 0;

-- Partial index supports the orphan-reclaim scan, which only ever inspects
-- RUNNING rows that were claimed by some worker.
CREATE INDEX IF NOT EXISTS harvest_task_queue_running_worker_idx
    ON harvest_task_queue (worker_id)
    WHERE state = 'RUNNING' AND worker_id IS NOT NULL;
