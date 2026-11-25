-- Production-shaped background history for the e2e throughput cell.
-- 1,000,000 executions: 97.5% terminal in the last 72 h, 2% RUNNING parked
-- on a far timer, 0.5% PAUSED. 6 events per terminal run, 1 per live run.
SELECT setseed(0.42);
CREATE TEMP TABLE bg AS
SELECT g, gen_random_uuid() AS id,
       now() - interval '72 hours' + (g * interval '0.2592 seconds') AS ts,
       random() AS r1, random() AS r2,
       CASE WHEN g > 975000 THEN (CASE WHEN g % 5 = 0 THEN 'PAUSED' ELSE 'RUNNING' END)
            WHEN g % 50 = 0 THEN 'FAILED'
            WHEN g % 97 = 0 THEN 'CONTINUED_AS_NEW'
            WHEN g % 211 = 0 THEN 'TIMED_OUT'
            ELSE 'COMPLETED' END AS st
FROM generate_series(1, 1000000) g;
INSERT INTO harvest_workflow_executions
  (id, workflow_name, workflow_id, shard_id, state, input, queue_name, started_at, created_at,
   completed_at, deadline_at, chain_deadline_at, paused_at, search_attrs, sla_deadline_at)
SELECT id,
       'bg_wf_' || CASE WHEN r1 < 0.6 THEN 0 ELSE g % 40 END,
       'bg-' || g, 0, st, '{"order": 1}'::jsonb, 'bg_q' || (g % 8), ts, ts,
       CASE WHEN st IN ('RUNNING','PAUSED') THEN NULL ELSE ts + interval '30 seconds' END,
       CASE WHEN r2 < 0.3 THEN now() + interval '7 days' ELSE NULL END,
       CASE WHEN r2 < 0.05 THEN now() + interval '30 days' ELSE NULL END,
       CASE WHEN st = 'PAUSED' THEN now() - interval '10 minutes' ELSE NULL END,
       CASE WHEN r1 < 0.2 THEN jsonb_build_object('customer', 'c' || (g % 1000)) ELSE NULL END,
       CASE WHEN r2 < 0.1 THEN ts + interval '10 minutes' ELSE NULL END
FROM bg;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT b.id, e.n, e.t,
       jsonb_build_object('type', e.t, 'data', jsonb_build_object('pad', repeat('x', 120))),
       b.ts + e.n * interval '1 second'
FROM bg b
CROSS JOIN LATERAL (VALUES
  (1, 'WorkflowExecutionStarted'), (2, 'ActivityTaskScheduled'), (3, 'ActivityTaskCompleted'),
  (4, 'ActivityTaskScheduled'), (5, 'ActivityTaskCompleted'), (6, 'WorkflowExecutionCompleted')) e(n, t)
WHERE b.st NOT IN ('RUNNING','PAUSED') OR e.n <= 1;
INSERT INTO harvest_timers (workflow_exec_id, timer_id, fires_at, fired)
SELECT id, 't1', now() + interval '7 days', false FROM bg WHERE st = 'RUNNING';
DROP TABLE bg;
VACUUM ANALYZE harvest_workflow_executions;
VACUUM ANALYZE harvest_events;
VACUUM ANALYZE harvest_timers;
