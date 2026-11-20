SELECT setseed(0.42);
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, state, input, queue_name)
SELECT gen_random_uuid(), 'wf' || (g % 30), 'wid-' || g, 0,
 CASE WHEN g % 100 = 0 THEN 'PAUSED' WHEN g % 7 = 0 THEN 'COMPLETED' ELSE 'RUNNING' END,
 '{}'::jsonb, 'q' || floor(20 * power(random(), 3))::int FROM generate_series(1, (100000 * 0.5)::int) g;
CREATE TEMP TABLE ex AS SELECT id, row_number() OVER (ORDER BY workflow_id) AS rn FROM harvest_workflow_executions;
CREATE UNIQUE INDEX ON ex (rn); ANALYZE ex;
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, priority, scheduled_at, workflow_exec_id, activity_name, schedule_to_close_at, concurrency_key, concurrency_cap, rate_limit_key, created_at)
SELECT 'q' || floor(20 * power(random(), 3))::int, CASE WHEN g % 5 < 2 THEN 'workflow' ELSE 'activity' END, '{}'::jsonb,
 'PENDING', (random() * 3)::int,
 CASE WHEN random() < 0.05 THEN now() + interval '1 hour' ELSE now() - (random() * interval '6 hours') END,
 CASE WHEN g % 5 < 2 THEN (SELECT id FROM ex WHERE rn = 1 + (g % (SELECT count(*) FROM ex))) END,
 CASE WHEN g % 5 >= 2 THEN 'act' || (g % 60) END,
 CASE WHEN random() < 0.10 THEN now() + interval '2 hours' END,
 CASE WHEN random() < 0.05 THEN 'ck' || (g % 40) END,
 CASE WHEN random() < 0.05 THEN 10 END,
 CASE WHEN random() < 0.03 THEN 'rl' || (g % 5) END, NULL
FROM generate_series(1, 100000) g;
UPDATE harvest_task_queue SET created_at = scheduled_at WHERE state = 'PENDING';
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, worker_id, activity_name, concurrency_key, concurrency_cap, started_at)
SELECT 'q' || floor(20 * power(random(), 3))::int, 'activity', '{}'::jsonb, 'RUNNING', 'w' || (g % 50), 'act' || (g % 60),
 CASE WHEN random() < 0.3 THEN 'ck' || (g % 40) END, CASE WHEN random() < 0.3 THEN 10 END, now()
FROM generate_series(1, (100000 * 0.1)::int) g;
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, activity_name, completed_at)
SELECT 'q' || floor(20 * power(random(), 3))::int, 'activity', '{}'::jsonb, 'COMPLETED', 'act' || (g % 60), now() - (random() * interval '1 hour')
FROM generate_series(1, 100000) g;
DROP TABLE ex;
