-- The claim statements of one e2e_bench run, by form (issue #2680).
--
-- Run it after census.sh, before the next reset. The window form carries
-- the claim_window_fallback column. The full form carries legacy_candidate.
SELECT CASE WHEN query LIKE '%claim_window_fallback%' THEN 'window'
            WHEN query LIKE '%legacy_candidate%' THEN 'full'
            ELSE 'other' END AS form,
       sum(calls) AS calls,
       sum(plans) AS plans,
       round(sum(total_exec_time)::numeric, 1) AS exec_ms,
       round((sum(total_exec_time) / sum(calls))::numeric, 3) AS exec_ms_per_call,
       sum(rows) AS rows
  FROM pg_stat_statements
 WHERE dbid NOT IN (SELECT oid FROM pg_database)
   AND query LIKE 'WITH worker_info AS%'
 GROUP BY 1
 ORDER BY 1;
