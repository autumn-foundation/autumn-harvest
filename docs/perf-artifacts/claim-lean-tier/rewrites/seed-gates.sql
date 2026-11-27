-- The fixture with live gates: one paused activity, one paused queue, and
-- 2,000 pending rows. Run the scripts with Q='{default,other}' and an
-- m-bind of one unmarked and one marked name.
TRUNCATE harvest_task_queue CASCADE;
INSERT INTO harvest_activity_pauses (activity_name, reason, paused_by) VALUES ('paused-a', 'lean claim bench', 'bench');
INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by) VALUES ('other', 'lean claim bench', 'bench');
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'default', 'activity', 'noop', gen_random_uuid(), '{}'::jsonb,
       'COMPLETED', 0, 1, 3, NOW() - INTERVAL '1 hour', 0, FALSE
FROM generate_series(1, 6000) AS i;
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, crash_strikes, wake_requested, new_start)
SELECT gen_random_uuid(), 'default',
       CASE WHEN i % 5 < 3 THEN 'workflow' ELSE 'activity' END,
       CASE WHEN i % 5 < 3 THEN NULL
            ELSE (ARRAY['noop','noop','noop','paused-a','busy','nohandler','other-a'])[1 + (i % 7)] END,
       CASE WHEN i % 5 < 3 THEN NULL ELSE gen_random_uuid() END, '{}'::jsonb,
       'PENDING', (i % 3), 0, 3, NOW() - (i || ' seconds')::interval, 0, FALSE, i % 5 = 0
FROM generate_series(1, 2000) AS i;
ANALYZE harvest_task_queue;
