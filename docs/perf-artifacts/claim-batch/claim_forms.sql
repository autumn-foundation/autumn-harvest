-- The claim statements of one e2e_bench run, by form. The batch form carries
-- claim_ranked, the window form claim_window_fallback alone, the full form
-- legacy_candidate.
SELECT CASE WHEN query LIKE '%claim_ranked%' THEN 'batch'
            WHEN query LIKE '%claim_window_fallback%' THEN 'window'
            WHEN query LIKE '%legacy_candidate%' THEN 'full'
            ELSE 'other' END AS form,
       sum(calls) AS calls,
       round(sum(total_exec_time)::numeric, 1) AS exec_ms,
       round((sum(total_exec_time) / sum(calls))::numeric, 3) AS exec_ms_per_call,
       sum(rows) AS rows
  FROM pg_stat_statements
 WHERE dbid NOT IN (SELECT oid FROM pg_database)
   AND query LIKE 'WITH worker_info AS%'
 GROUP BY 1
 ORDER BY 1;
SELECT sum(calls) AS all_calls, round(sum(total_exec_time)::numeric, 1) AS all_exec_ms
  FROM pg_stat_statements WHERE dbid NOT IN (SELECT oid FROM pg_database);
