-- Add 9,000 RUNNING rows of 40 other keys, for 10,000 in all.
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, worker_id, activity_name, concurrency_key, concurrency_cap, started_at)
SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'RUNNING', 'w' || (g % 50), 'act' || (g % 60), 'bgk' || (g % 40), 10, now() FROM generate_series(1, 9000) g;
VACUUM ANALYZE harvest_task_queue;
