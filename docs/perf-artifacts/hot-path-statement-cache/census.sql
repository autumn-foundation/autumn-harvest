-- The statement census of one e2e_bench run (issue #2651).
--
-- Needs pg_stat_statements in shared_preload_libraries, with
-- pg_stat_statements.track_planning = on. Reset before the run:
--
--   SELECT pg_stat_statements_reset();
--
-- The bench drops its database at the end. pg_stat_statements keeps the rows
-- of a dropped database, so the filter below selects exactly those rows.
SELECT calls,
       plans,
       round(total_plan_time::numeric, 1) AS plan_ms,
       round(total_exec_time::numeric, 1) AS exec_ms,
       rows,
       left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 400) AS query
  FROM pg_stat_statements
 WHERE dbid NOT IN (SELECT oid FROM pg_database)
 ORDER BY total_plan_time + total_exec_time DESC
 LIMIT 120;

-- The totals of the run.
SELECT sum(calls) AS statements,
       sum(plans) AS plans,
       round(sum(total_plan_time)) AS plan_ms,
       round(sum(total_exec_time)) AS exec_ms
  FROM pg_stat_statements
 WHERE dbid NOT IN (SELECT oid FROM pg_database);

-- The statements that Postgres plans on every call.
SELECT round(calls / 1440.0, 2) AS per_workflow,
       plans,
       round(total_plan_time::numeric) AS plan_ms,
       round(total_exec_time::numeric) AS exec_ms,
       regexp_replace(query, '[[:space:]]+', ' ', 'g') AS query
  FROM pg_stat_statements
 WHERE dbid NOT IN (SELECT oid FROM pg_database)
   AND plans >= calls * 0.9
   AND calls >= 500
 ORDER BY total_plan_time DESC;
