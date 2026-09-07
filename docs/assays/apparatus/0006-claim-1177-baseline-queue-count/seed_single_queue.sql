-- Identical generation to ledger #5's seed.sql (same generate_series, same
-- literal priority/scheduled_at), except queues=1 so all rows land in a
-- single queue instead of being spread round-robin across four --
-- reproducing issue #1177's own single-queue fixture description.
-- Invoke with: psql -v backlog=10000 -f seed_single_queue.sql
TRUNCATE harvest_task_queue;

INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at)
SELECT 'bench-q-0',
       'activity',
       'PENDING',
       0,
       NOW() - INTERVAL '1 second'
FROM generate_series(0, :backlog - 1) AS s(i);

ANALYZE harvest_task_queue;
