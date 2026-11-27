TRUNCATE harvest_task_queue CASCADE;
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'default', 'activity', 'noop', gen_random_uuid(), '{}'::jsonb,
       'COMPLETED', 0, 1, 3, NOW() - INTERVAL '1 hour', 0, FALSE
FROM generate_series(1, 6000) AS i;
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'default', 'activity', 'noop', gen_random_uuid(), '{}'::jsonb,
       'PENDING', (i % 3), 0, 3, NOW() - (i || ' seconds')::interval, 0, FALSE
FROM generate_series(1, 24) AS i;
ANALYZE harvest_task_queue;
