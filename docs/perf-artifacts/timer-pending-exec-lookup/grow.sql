-- Add :n sleeping short runs, each with one open timer in the future.
\set ON_ERROR_STOP 1
SELECT setseed(0.17);
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input)
SELECT ('00000000-0000-4000-9000-' || lpad(to_hex(g), 12, '0'))::uuid, 'sleeper', 'sl-' || g, 0, '{}'::jsonb
FROM generate_series(1, :n) AS g;
INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired)
SELECT gen_random_uuid(), ('00000000-0000-4000-9000-' || lpad(to_hex(g), 12, '0'))::uuid,
       'timer-1', now() + interval '1 hour' + random() * interval '7 days', false
FROM generate_series(1, :n) AS g ORDER BY 4;
VACUUM ANALYZE harvest_workflow_executions;
VACUUM ANALYZE harvest_timers;
