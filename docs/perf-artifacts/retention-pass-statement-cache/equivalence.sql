-- Result equivalence of the custom and the generic plan (the cached form).
--
-- Usage: psql -f equivalence.sql, on a database that fixture.sql seeded.
-- PL/pgSQL keeps a plan for each static statement, and plan_cache_mode
-- selects its kind. Each probe runs over every execution of the fixture
-- under both modes. The result sets must be identical.
--
-- The rows cover a leaf with no children, a parent with many children,
-- each link of a continue-as-new chain, a fork source, a fork of a fork,
-- a held run, and a chain-link cutoff equal to a completed_at value.
\set ON_ERROR_STOP 1

CREATE OR REPLACE FUNCTION pg_temp.probe(target uuid)
RETURNS TABLE (active_children bigint, open_forks uuid[], chain_links bigint,
               chain_links_at_boundary bigint, hold text)
LANGUAGE plpgsql AS $fn$
DECLARE
  r record;
BEGIN
  SELECT workflow_name, workflow_id, completed_at INTO r
    FROM harvest_workflow_executions WHERE id = target;

  SELECT COUNT(*) INTO active_children
    FROM harvest_workflow_executions
   WHERE parent_id = target
     AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED');

  WITH RECURSIVE descendant(id, depth) AS (
      SELECT f.id, 1 FROM harvest_workflow_executions f
       WHERE f.start_source = 'fork' AND f.start_source_ref = target::text
      UNION
      SELECT f.id, d.depth + 1 FROM descendant d
        JOIN harvest_workflow_executions f
          ON f.start_source = 'fork' AND f.start_source_ref = d.id::text
       WHERE d.depth < 65
  ) SELECT array_agg(f.id ORDER BY f.id) INTO open_forks
      FROM descendant d
      JOIN harvest_workflow_executions f ON f.id = d.id
     WHERE (f.state IN ('RUNNING', 'PAUSED', 'MIGRATING', 'MIGRATED')
            OR (f.state = 'FAILED' AND EXISTS (SELECT 1 FROM harvest_dead_letters dl
                 WHERE dl.workflow_exec_id = f.id AND upper(dl.task_type) <> 'CALLBACK')));

  SELECT COUNT(*) INTO chain_links
    FROM harvest_workflow_executions
   WHERE workflow_name = r.workflow_name
     AND workflow_id = r.workflow_id
     AND id <> target
     AND (state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED', 'MIGRATED')
          OR completed_at IS NULL
          OR completed_at >= NOW() - interval '7 days');

  SELECT COUNT(*) INTO chain_links_at_boundary
    FROM harvest_workflow_executions
   WHERE workflow_name = r.workflow_name
     AND workflow_id = r.workflow_id
     AND id <> target
     AND (state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED', 'MIGRATED')
          OR completed_at IS NULL
          OR completed_at >= r.completed_at);

  SELECT concat_ws('|', legal_hold_set_at, legal_hold_until, workflow_name, workflow_id, state,
                   started_at, completed_at, shard_id, output, error, search_attrs, parent_id,
                   migrated_from_shards, staging_vacated_state) INTO hold
    FROM harvest_workflow_executions
   WHERE id = target
   LIMIT 1
     FOR UPDATE;

  RETURN NEXT;
END
$fn$;

BEGIN;
SET LOCAL plan_cache_mode = force_custom_plan;
CREATE TEMP TABLE custom_out AS
SELECT e.id, p.* FROM harvest_workflow_executions e, LATERAL pg_temp.probe(e.id) p;
COMMIT;

-- A new session function would reuse the custom plans, so drop the cache.
DISCARD PLANS;

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
CREATE TEMP TABLE generic_out AS
SELECT e.id, p.* FROM harvest_workflow_executions e, LATERAL pg_temp.probe(e.id) p;
COMMIT;

\echo '== rows compared, and rows that differ (must be 0)'
SELECT (SELECT count(*) FROM custom_out) AS rows_compared,
       (SELECT count(*) FROM (SELECT * FROM custom_out EXCEPT ALL SELECT * FROM generic_out) a) AS custom_only,
       (SELECT count(*) FROM (SELECT * FROM generic_out EXCEPT ALL SELECT * FROM custom_out) b) AS generic_only;

\echo '== the edge cases that the comparison covered'
SELECT count(*) FILTER (WHERE active_children = 0) AS no_active_child,
       count(*) FILTER (WHERE active_children > 0) AS with_active_child,
       count(*) FILTER (WHERE open_forks IS NOT NULL) AS with_open_fork,
       count(*) FILTER (WHERE chain_links > 0) AS with_chain_link,
       count(*) FILTER (WHERE chain_links_at_boundary > 0) AS with_boundary_link,
       count(*) FILTER (WHERE hold LIKE '2%') AS held
  FROM custom_out;
