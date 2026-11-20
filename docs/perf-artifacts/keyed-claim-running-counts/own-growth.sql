-- Add 50 RUNNING rows to each of the 20 keys of queue w, for 1,000 in all.
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, worker_id, activity_name, concurrency_key, concurrency_cap, started_at)
SELECT 'w', 'activity', '{}'::jsonb, 'RUNNING', 'w' || (g % 50), 'act', 'ck' || (g % 20), 1000000, now() FROM generate_series(1, 1000) g;
VACUUM ANALYZE harvest_task_queue;
