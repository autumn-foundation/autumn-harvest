-- Production-shaped harvest_timers fixture. Seeded and deterministic.
-- Run on a database with every migration applied.
--
-- Shape:
--   19,000 short runs with 0 to 4 timers each.
--      900 medium runs with 100 timers each.
--      100 long-lived runs with 10,000 timers each, near the 50k-event cap.
-- Each run keeps one open timer that fires in the future.
-- 2,000 timers are due and not yet fired, a timer-ingest lag.
-- Every other timer has fired. Rows go in time order, so the timers of one
-- run spread over the heap, as they do in production.
\set ON_ERROR_STOP 1
SELECT setseed(0.42);

INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
       CASE WHEN g <= 100 THEN 'long_lived'
            WHEN g <= 1000 THEN 'medium'
            ELSE 'short' END,
       'wf-' || g, 0, '{}'::jsonb
FROM generate_series(1, 20000) AS g;

-- k = number of timers of run g.
CREATE TEMP TABLE plan AS
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid AS exec_id,
       g,
       CASE WHEN g <= 100 THEN 10000
            WHEN g <= 1000 THEN 100
            ELSE (g % 5) END AS k
FROM generate_series(1, 20000) AS g;

-- Timer i of run g fires at a time spread over 30 days. The last timer of
-- each run with k > 0 is open in the future.
INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired)
SELECT gen_random_uuid(), p.exec_id, 'timer-' || i,
       CASE WHEN i = p.k THEN now() + interval '1 hour' + (random() * interval '1 day')
            ELSE now() - interval '30 days' + ((i::float8 / p.k) * interval '29 days')
                 + (random() * interval '1 hour') END,
       i < p.k
FROM plan p, generate_series(1, p.k) AS i
ORDER BY 4;

-- The ingest lag: 2,000 due timers on short and medium runs, unfired.
INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired)
SELECT gen_random_uuid(),
       ('00000000-0000-4000-8000-' || lpad(to_hex(101 + (n * 7919) % 19900), 12, '0'))::uuid,
       'lag-' || n, now() - (random() * interval '10 minutes'), false
FROM generate_series(1, 2000) AS n;

VACUUM ANALYZE harvest_workflow_executions;
VACUUM ANALYZE harvest_timers;

-- Dead tuples: the last day of firings has not been vacuumed yet.
UPDATE harvest_timers SET fired = false
WHERE fired AND fires_at > now() - interval '1 day' - interval '1 hour';
VACUUM ANALYZE harvest_timers;
UPDATE harvest_timers SET fired = true
WHERE NOT fired AND fires_at > now() - interval '1 day' - interval '1 hour'
  AND fires_at <= now() - interval '10 minutes' AND timer_id NOT LIKE 'lag-%';
ANALYZE harvest_timers;
