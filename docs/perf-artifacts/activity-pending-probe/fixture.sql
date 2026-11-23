SELECT setseed(0.2026);
SET session_replication_role = replica;
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input, state)
SELECT md5('run' || g)::uuid, 'ledger_wf', 'wf-' || g, 0, '{}'::jsonb,
       CASE WHEN g % 10 = 0 THEN 'RUNNING' ELSE 'COMPLETED' END
FROM generate_series(1, 3000) g;
WITH runs AS (
  SELECT g, md5('run' || g)::uuid AS exec_id,
         CASE WHEN g <= 1000 THEN 10 WHEN g <= 2000 THEN 50 ELSE 200 END AS acts
  FROM generate_series(1, 3000) g
), ev AS (
  SELECT r.exec_id, e AS event_id,
         CASE WHEN e = 0 THEN 'WorkflowStarted'
              WHEN (e - 1) % 3 = 0 THEN 'ActivityScheduled'
              WHEN (e - 1) % 3 = 1 THEN 'ActivityStarted'
              ELSE 'ActivityCompleted' END AS t,
         (e::float8 / (3 * r.acts + 1)) + random() * 0.02 AS pos
  FROM runs r, generate_series(0, 3 * r.acts) e
)
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT exec_id, event_id, t,
  jsonb_build_object('type', t, 'data',
    CASE t
      WHEN 'WorkflowStarted' THEN jsonb_build_object('input', repeat(md5(event_id::text), 8))
      WHEN 'ActivityScheduled' THEN jsonb_build_object('activity_id', md5(exec_id::text || event_id)::uuid, 'name', 'step', 'input', repeat(md5(random()::text), 6))
      WHEN 'ActivityStarted' THEN jsonb_build_object('activity_id', md5(exec_id::text || (event_id - 1))::uuid, 'worker_id', 'w-1')
      ELSE jsonb_build_object('activity_id', md5(exec_id::text || (event_id - 2))::uuid,
             'output', CASE WHEN random() < 0.1 THEN repeat(md5(random()::text), 256) ELSE repeat(md5(random()::text), 32) END)
    END),
  timestamp '2026-11-01' + pos * interval '1 day'
FROM ev ORDER BY pos;
SET session_replication_role = origin;
DELETE FROM harvest_events WHERE workflow_exec_id IN (SELECT md5('run' || g)::uuid FROM generate_series(1, 3000) g WHERE g % 13 = 0);
VACUUM (ANALYZE) harvest_events;
