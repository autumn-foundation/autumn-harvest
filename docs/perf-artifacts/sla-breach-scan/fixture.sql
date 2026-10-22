-- Deterministic fixture for the SLA-breach scan measurement. No random().
-- Usage: psql -v n=100000 -f fixture.sql <db with all migrations applied>
--   0.1% PAUSED, 2% RUNNING, rest COMPLETED (history dominates).
--   20% of runs carry an SLA of 1 hour (g % 5 = 0).
--   Completed SLA runs: 95% finish in 10 minutes (on time), 5% in 2 hours
--   (late) and the scanner already flagged them (sla_breached = true).
--   RUNNING SLA runs: deadline one hour ahead, not yet breached.
TRUNCATE harvest_task_queue, harvest_workflow_executions CASCADE;
INSERT INTO harvest_workflow_executions
  (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
   started_at, created_at, completed_at, sla, sla_deadline_at, sla_breached, sla_breached_at)
SELECT md5('e'||g)::uuid, 'wf', 'wf-'||g, md5('r'||g)::uuid, 0, s.state, '{}', 'default',
       now() - (g||' seconds')::interval, now() - (g||' seconds')::interval,
       CASE WHEN s.state = 'COMPLETED'
            THEN now() - (g||' seconds')::interval + CASE WHEN g % 100 < 95 THEN interval '10 minutes' ELSE interval '2 hours' END END,
       CASE WHEN g % 5 = 0 THEN interval '1 hour' END,
       CASE WHEN g % 5 = 0 THEN CASE WHEN s.state = 'COMPLETED' THEN now() - (g||' seconds')::interval + interval '1 hour'
                                      ELSE now() + interval '1 hour' END END,
       (g % 5 = 0 AND s.state = 'COMPLETED' AND g % 100 >= 95),
       CASE WHEN g % 5 = 0 AND s.state = 'COMPLETED' AND g % 100 >= 95 THEN now() END
FROM generate_series(1, :n) g,
     LATERAL (SELECT CASE WHEN g % 1000 = 0 THEN 'PAUSED' WHEN g % 50 = 1 THEN 'RUNNING' ELSE 'COMPLETED' END AS state) s;
ANALYZE harvest_workflow_executions;
