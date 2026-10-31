-- Issue #2367 fixture. 4,000 completed executions with 25 signals each.
-- 24 of each 25 signals are consumed. Execution 4001 has no signals.
INSERT INTO harvest_workflow_executions
    (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
     started_at, created_at, completed_at)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
       'measure', 'measure_' || n, gen_random_uuid(), 0, 'COMPLETED', '{}'::jsonb,
       'default', NOW(), NOW(), NOW()
FROM generate_series(1, 4001) AS n;
INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload, consumed)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(e), 12, '0'))::uuid,
       'sig_' || s, jsonb_build_object('n', s, 'pad', repeat('x', 200)), s < 25
FROM generate_series(1, 4000) AS e, generate_series(1, 25) AS s;
VACUUM ANALYZE harvest_signals;
VACUUM ANALYZE harvest_workflow_executions;
