-- Deterministic fixture for the claim PAUSED-gate measurement.
-- Usage: psql -v n=100000 -v paused=50 -f fixture.sql <db with all migrations applied>
-- No random(): every column derives from the row number, so reruns are identical.
--   executions: 0.1% PAUSED, 2% RUNNING, rest COMPLETED  (production-shaped: history dominates)
--   task_queue : 1500 PENDING workflow tasks on RUNNING runs + 50 parked on PAUSED runs
--                + 3000 COMPLETED tasks (dead tuples excluded; plain history rows)
TRUNCATE harvest_task_queue, harvest_workflow_executions CASCADE;
INSERT INTO harvest_workflow_executions
  (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at)
SELECT md5('e'||g)::uuid, 'wf', 'wf-'||g, md5('r'||g)::uuid, 0,
       CASE WHEN g % 1000 = 0 THEN 'PAUSED' WHEN g % 50 = 1 THEN 'RUNNING' ELSE 'COMPLETED' END,
       '{}', 'default', now() - (g||' seconds')::interval, now() - (g||' seconds')::interval
FROM generate_series(1, :n) g;
CREATE TEMP TABLE base AS SELECT date_trunc('hour', now()) - interval '2 hours' AS t0;
INSERT INTO harvest_task_queue (id, queue_name, task_type, workflow_exec_id, input, state, scheduled_at)
SELECT md5('tq'||rn)::uuid, 'default', 'workflow', id, '{}', 'PENDING',
       (SELECT t0 FROM base) + rn * interval '1 millisecond'
FROM (SELECT id, row_number() OVER (ORDER BY p, id) AS rn FROM (
        (SELECT id, 0 AS p FROM harvest_workflow_executions WHERE state='PAUSED' ORDER BY id LIMIT :paused)
        UNION ALL
        (SELECT id, 1 AS p FROM harvest_workflow_executions WHERE state='RUNNING' ORDER BY id LIMIT 1500)) u) v;
INSERT INTO harvest_task_queue (id, queue_name, task_type, activity_name, input, state, scheduled_at)
SELECT md5('ta'||g)::uuid, 'default', 'activity', 'a', '{}', 'PENDING', (SELECT t0 FROM base) + (g+5000) * interval '1 millisecond'
FROM generate_series(1,20) g;
INSERT INTO harvest_task_queue (id, queue_name, task_type, workflow_exec_id, input, state, scheduled_at, completed_at)
SELECT md5('tc'||row_number() OVER ())::uuid, 'default', 'workflow', e.id, '{}', 'COMPLETED', now() - interval '1 hour', now() - interval '1 hour'
FROM (SELECT id FROM harvest_workflow_executions WHERE state='COMPLETED' ORDER BY id LIMIT 3000) e;
ANALYZE harvest_task_queue; ANALYZE harvest_workflow_executions;
