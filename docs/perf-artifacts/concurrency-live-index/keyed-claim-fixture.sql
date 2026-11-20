SELECT setseed(0.42);
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, activity_name, scheduled_at)
SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'PENDING', 'act' || (g % 60), now() - (random() * interval '6 hours') FROM generate_series(1, 100000) g;
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, activity_name, concurrency_key, concurrency_cap, scheduled_at)
SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'PENDING', 'act' || (g % 60), 'bgk' || (g % 40), 10, now() - (random() * interval '6 hours') FROM generate_series(1, 2000) g;
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, worker_id, activity_name, concurrency_key, concurrency_cap, started_at)
SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'RUNNING', 'w' || (g % 50), 'act' || (g % 60), 'bgk' || (g % 40), 10, now() FROM generate_series(1, 1000) g;
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, activity_name, concurrency_key, concurrency_cap, scheduled_at)
SELECT 'w', 'activity', '{}'::jsonb, 'PENDING', 'act', 'ck' || (g % 20), 1000000, now() - interval '1 second' FROM generate_series(1, 2000) g;
VACUUM ANALYZE harvest_task_queue;
