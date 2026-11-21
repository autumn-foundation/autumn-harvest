-- The statement census of one retention pass of retention_reclaim_bench.
--
-- Needs pg_stat_statements in shared_preload_libraries, with
-- pg_stat_statements.track_planning = on. census.sh resets it first.
-- :flat is the oid of the bench database of the unpartitioned arm.
--
-- The pass visits each expired execution once, so a statement of the
-- candidate loop runs once or twice per candidate. The seed and the
-- concurrent load run other counts, so the calls filter selects the loop.
\set ON_ERROR_STOP 1

\echo '== The candidate loop, by planning plus execution buffers'
SELECT calls,
       plans,
       shared_blks_hit + shared_blks_read AS buffers,
       round(total_plan_time::numeric) AS plan_ms,
       round(total_exec_time::numeric) AS exec_ms,
       wal_bytes,
       temp_blks_written,
       left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 160) AS query
  FROM pg_stat_statements
 WHERE dbid = :flat
   AND calls IN (:candidates, 2 * :candidates)
 ORDER BY shared_blks_hit + shared_blks_read DESC;

\echo '== The loop totals. RI rows are the cascade actions of the execution delete.'
SELECT query LIKE '%FROM ONLY%' AS ri_cascade,
       count(*) AS statements,
       sum(calls) AS calls,
       sum(plans) AS plans,
       sum(shared_blks_hit + shared_blks_read) AS buffers,
       round(sum(total_plan_time)) AS plan_ms,
       round(sum(total_exec_time)) AS exec_ms
  FROM pg_stat_statements
 WHERE dbid = :flat
   AND calls IN (:candidates, 2 * :candidates)
 GROUP BY 1
 ORDER BY 1;

\echo '== The loop statements that Postgres plans on every call'
SELECT calls,
       plans,
       shared_blks_hit + shared_blks_read AS buffers,
       round(total_plan_time::numeric) AS plan_ms,
       regexp_replace(query, '[[:space:]]+', ' ', 'g') AS query
  FROM pg_stat_statements
 WHERE dbid = :flat
   AND calls IN (:candidates, 2 * :candidates)
   AND plans >= calls * 0.9
 ORDER BY shared_blks_hit + shared_blks_read DESC;

\echo '== The whole bench database, seed and load included'
SELECT sum(calls) AS statements,
       sum(plans) AS plans,
       sum(shared_blks_hit + shared_blks_read) AS buffers,
       sum(wal_bytes) AS wal_bytes
  FROM pg_stat_statements
 WHERE dbid = :flat;
