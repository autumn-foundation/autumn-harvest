-- The size of the index when signals arrive one at a time, in random order.
-- Run on an empty database with every migration, such as `createdb -T`.
INSERT INTO harvest_workflow_executions
    (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
     started_at, created_at, completed_at)
SELECT gen_random_uuid(), 'size', 'size_' || n, gen_random_uuid(), 0, 'COMPLETED', '{}'::jsonb,
       'default', NOW(), NOW(), NOW()
FROM generate_series(1, 100000) AS n;
CREATE TEMP TABLE e AS SELECT id, row_number() OVER () AS rn FROM harvest_workflow_executions;
-- 25 signals for each of 4,000 executions.
INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload)
SELECT e.id, 's', '{}'::jsonb FROM e, generate_series(1, 25) WHERE e.rn <= 4000 ORDER BY random();
SELECT '25 per execution' AS shape, pg_relation_size('idx_harvest_signals_exec') / 8192 AS pages;
TRUNCATE harvest_signals;
-- One signal for each of 100,000 executions.
INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload)
SELECT e.id, 's', '{}'::jsonb FROM e ORDER BY random();
SELECT '1 per execution' AS shape, pg_relation_size('idx_harvest_signals_exec') / 8192 AS pages;
