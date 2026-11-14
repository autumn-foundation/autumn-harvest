-- A production-shaped harvest_workflow_executions for the plan checks.
--
-- Usage: psql -v n=<executions> -f fixture.sql, on a migrated database.
-- The seed is fixed, so two runs at one size give the same rows.
--
-- The shape follows the issue #2668 fixture, with the lineage that the
-- retention gate reads:
--   * 96% terminal, 3.5% RUNNING, 0.5% PAUSED.
--   * 50 workflow names, skewed: the first name holds about a fifth.
--   * 30% of runs share a workflow id in continue-as-new chains of 10.
--   * 25% of runs are children of the first 2% of runs.
--   * 0.2% of runs are forks of an older run, 0.1% carry a legal hold.
--   * 1% of FAILED runs have a dead letter.
\set ON_ERROR_STOP 1
SELECT setseed(0.42);
TRUNCATE harvest_workflow_executions CASCADE;

CREATE TEMP TABLE seed AS
SELECT g,
       random() AS r_state,
       random() AS r_name,
       random() AS r_chain,
       random() AS r_parent,
       random() AS r_fork,
       random() AS r_hold,
       (random() * 30)::int AS age_days
  FROM generate_series(1, :n) AS g;

INSERT INTO harvest_workflow_executions
       (id, workflow_name, workflow_id, shard_id, state, input,
        created_at, started_at, completed_at, legal_hold_set_at)
SELECT md5('exec-' || g)::uuid,
       'wf_' || floor(50 * power(CASE WHEN r_chain < 0.3 THEN (hashint4((g / 10)::int) & 1023) / 1024.0 ELSE r_name END, 3))::int,
       CASE WHEN r_chain < 0.3 THEN 'chain-' || (g / 10) ELSE 'run-' || g END,
       0,
       -- Only the last link of a chain can be open, as the active key needs.
       CASE WHEN r_chain < 0.3 AND g % 10 <> 9 THEN 'CONTINUED_AS_NEW'
            WHEN r_state < 0.850 THEN 'COMPLETED'
            WHEN r_state < 0.910 THEN 'FAILED'
            WHEN r_state < 0.930 THEN 'CANCELLED'
            WHEN r_state < 0.940 THEN 'TIMED_OUT'
            WHEN r_state < 0.955 THEN 'CONTINUED_AS_NEW'
            WHEN r_state < 0.960 THEN 'TERMINATED'
            WHEN r_state < 0.995 THEN 'RUNNING'
            ELSE 'PAUSED' END,
       '{"k":"v"}'::jsonb,
       NOW() - make_interval(days => age_days),
       NOW() - make_interval(days => age_days),
       CASE WHEN (r_chain < 0.3 AND g % 10 <> 9) OR r_state < 0.960
            THEN NOW() - make_interval(days => age_days) END,
       CASE WHEN r_hold < 0.001 THEN NOW() END
  FROM seed;

-- The lineage columns, so the RI and partial-index paths see real rows.
UPDATE harvest_workflow_executions e
   SET parent_id = md5('exec-' || (1 + (s.r_parent * 1000000)::bigint % greatest(:n / 50, 1)))::uuid,
       parent_close_policy = 'ABANDON'
  FROM seed s
 WHERE e.id = md5('exec-' || s.g)::uuid
   AND s.r_parent < 0.25
   AND s.g > :n / 50;

UPDATE harvest_workflow_executions e
   SET start_source = 'fork',
       start_source_ref = md5('exec-' || (1 + (s.r_fork * 1000000)::bigint % (s.g - 1)))::uuid::text
  FROM seed s
 WHERE e.id = md5('exec-' || s.g)::uuid
   AND s.r_fork < 0.002
   AND s.g > 1;

INSERT INTO harvest_dead_letters
       (original_task_id, queue_name, task_type, workflow_exec_id, input, error, attempts)
SELECT gen_random_uuid(), 'default', 'activity', id, '{}'::jsonb, 'boom', 3
  FROM harvest_workflow_executions
 WHERE state = 'FAILED' AND md5(id::text) < '03';

VACUUM ANALYZE harvest_workflow_executions;
VACUUM ANALYZE harvest_dead_letters;

SELECT state, count(*) FROM harvest_workflow_executions GROUP BY 1 ORDER BY 2 DESC;
SELECT count(*) FILTER (WHERE parent_id IS NOT NULL) AS children,
       count(*) FILTER (WHERE start_source = 'fork') AS forks,
       count(*) FILTER (WHERE legal_hold_set_at IS NOT NULL) AS holds,
       count(DISTINCT (workflow_name, workflow_id)) AS identities
  FROM harvest_workflow_executions;
