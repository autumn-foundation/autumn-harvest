-- The five statements of the retention candidate loop that plan per call.
--
-- Usage: psql -f plans.sql, on a database that fixture.sql seeded.
-- Each statement runs as a prepared statement twice: once with a custom
-- plan, as an unnamed statement runs today, and once with the generic plan
-- that a cached statement can keep after its fifth run. The text of each
-- statement is the text that the engine sends, with its literals in place.
--
-- The bind values are the worst cases of the fixture: the parent with the
-- most children, the identity with the longest chain, and the run with
-- the most forks. A lookup by key reads one row either way.
\set ON_ERROR_STOP 1
\pset pager off

SELECT parent_id AS busy_parent
  FROM harvest_workflow_executions
 WHERE parent_id IS NOT NULL
 GROUP BY parent_id ORDER BY count(*) DESC, parent_id LIMIT 1 \gset
SELECT workflow_name AS chain_name, workflow_id AS chain_id, min(id::text) AS chain_member
  FROM harvest_workflow_executions
 GROUP BY workflow_name, workflow_id ORDER BY count(*) DESC, workflow_id LIMIT 1 \gset
SELECT start_source_ref AS fork_source
  FROM harvest_workflow_executions
 WHERE start_source = 'fork'
 GROUP BY start_source_ref ORDER BY count(*) DESC, start_source_ref LIMIT 1 \gset
SELECT id AS leaf
  FROM harvest_workflow_executions
 WHERE state = 'COMPLETED' AND parent_id IS NOT NULL
 ORDER BY id LIMIT 1 \gset

-- retention::active_child_count_sql
PREPARE active_child(uuid) AS
SELECT COUNT(*) AS count
  FROM harvest_workflow_executions
 WHERE parent_id = $1
   AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED');

-- fork::OPEN_FORK_DESCENDANTS_SQL
PREPARE open_forks(text) AS
WITH RECURSIVE descendant(id, depth) AS (
    SELECT f.id, 1 FROM harvest_workflow_executions f
     WHERE f.start_source = 'fork' AND f.start_source_ref = $1
    UNION
    SELECT f.id, d.depth + 1 FROM descendant d
      JOIN harvest_workflow_executions f
        ON f.start_source = 'fork' AND f.start_source_ref = d.id::text
     WHERE d.depth < 65
) SELECT f.id FROM descendant d
    JOIN harvest_workflow_executions f ON f.id = d.id
   WHERE (f.state IN ('RUNNING', 'PAUSED', 'MIGRATING', 'MIGRATED')
          OR (f.state = 'FAILED' AND EXISTS (SELECT 1 FROM harvest_dead_letters dl
               WHERE dl.workflow_exec_id = f.id AND upper(dl.task_type) <> 'CALLBACK')));

-- retention::chain_link_count_sql
PREPARE chain_link(text, text, uuid, timestamptz) AS
SELECT COUNT(*) AS count
  FROM harvest_workflow_executions
 WHERE workflow_name = $1
   AND workflow_id = $2
   AND id <> $3
   AND (
        state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED', 'MIGRATED')
       OR completed_at IS NULL
       OR completed_at >= $4
   );

-- retention::delete_candidate_execution, the legal-hold read FOR UPDATE.
-- The two unmapped columns are sql::<T>() fragments in the engine.
PREPARE hold_for_update(uuid) AS
SELECT legal_hold_set_at, legal_hold_until, workflow_name, workflow_id, state,
       started_at, completed_at, shard_id, output, error, search_attrs, parent_id,
       migrated_from_shards, staging_vacated_state
  FROM harvest_workflow_executions
 WHERE id = $1
 LIMIT 1
   FOR UPDATE;

-- retention::delete_candidate_execution, the child lineage null-out.
PREPARE null_parent(uuid, uuid, text[]) AS
UPDATE harvest_workflow_executions
   SET parent_id = $1
 WHERE parent_id = $2
   AND state = ANY($3);

\echo '################ custom plans (the engine today)'
SET plan_cache_mode = force_custom_plan;
\i plans_body.sql

\echo '################ generic plans (a cached statement after five runs)'
SET plan_cache_mode = force_generic_plan;
\i plans_body.sql
