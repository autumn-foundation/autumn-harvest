-- Boundary rows for the equivalence check. Run after fixture.sql.
-- `flag` says whether the scanner must flag the row. Fixed ids, no random().
CREATE TEMP TABLE edge (id uuid, flag boolean, note text) ON COMMIT DROP;
INSERT INTO harvest_workflow_executions
  (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at,
   completed_at, sla, sla_deadline_at, sla_breached)
SELECT md5('edge'||k)::uuid, 'wf', 'edge-'||k, md5('edge-run'||k)::uuid, 0, st, '{}', 'default',
       now() - interval '1 day', now() - interval '1 day', done, interval '1 hour', due, breached
FROM (VALUES
  (1,  'RUNNING',          NULL::timestamptz,               now() - interval '1 minute',  false),
  (2,  'COMPLETED',        now() - interval '10 minutes',   now() - interval '20 minutes', false),
  (3,  'COMPLETED',        now() - interval '30 minutes',   now() - interval '20 minutes', false),
  (4,  'COMPLETED',        '2026-01-01 00:00:00+00'::timestamptz, '2026-01-01 00:00:00+00'::timestamptz, false),
  (5,  'COMPLETED',        '2026-01-01 00:00:00.000001+00'::timestamptz, '2026-01-01 00:00:00+00'::timestamptz, false),
  (6,  'FAILED',           NULL,                            now() - interval '1 minute',  false),
  (7,  'PAUSED',           NULL,                            now() - interval '1 minute',  false),
  (8,  'COMPLETED',        now() - interval '10 minutes',   now() - interval '20 minutes', true),
  (9,  'COMPLETED',        now() - interval '10 minutes',   NULL,                          false),
  (10, 'CANCELLED',        now() - interval '10 minutes',   now() - interval '20 minutes', false),
  (11, 'TIMED_OUT',        now() - interval '10 minutes',   now() - interval '20 minutes', false),
  (12, 'CONTINUED_AS_NEW', now() - interval '10 minutes',   now() - interval '20 minutes', false),
  (13, 'TERMINATED',       now() - interval '10 minutes',   now() - interval '20 minutes', false),
  (14, 'RUNNING',          NULL,                            now() + interval '1 hour',     false)
) AS v(k, st, done, due, breached);
INSERT INTO edge
SELECT md5('edge'||k)::uuid, f, n FROM (VALUES
  (1,true,'running, deadline past'),(2,true,'completed late'),(3,false,'completed on time'),
  (4,false,'completed exactly at the deadline'),(5,true,'completed 1 microsecond late'),
  (6,true,'failed with no completed_at, deadline past'),(7,false,'paused, deadline past'),
  (8,false,'already breached'),(9,false,'no deadline'),(10,true,'cancelled late'),
  (11,true,'timed out late'),(12,true,'continued as new late'),(13,true,'terminated late'),
  (14,false,'running, deadline ahead')) t(k,f,n);
