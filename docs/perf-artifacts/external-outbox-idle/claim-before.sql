-- The claim as it shipped before this change: the text of
-- external_signal_outbox_claim_query() at trunk-dev 816131c.
SELECT pg_stat_statements_reset();
PREPARE claim(int4[], int8[]) AS
SELECT e.id, e.workflow_exec_id, e.event_id, e.event_type, e.event_data, e.timestamp
FROM harvest_events e
JOIN LATERAL ( SELECT 1 AS running_exec FROM harvest_workflow_executions x
  WHERE x.id = e.workflow_exec_id AND x.state = 'RUNNING' AND x.shard_id = ANY($1) LIMIT 1 ) running ON TRUE
WHERE e.event_type = 'ExternalSignalRequested'
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY($2))
  AND NOT EXISTS ( SELECT 1 FROM harvest_events res
     WHERE res.workflow_exec_id = e.workflow_exec_id
       AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
       AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id' )
ORDER BY e.timestamp, e.id LIMIT 1 FOR UPDATE OF e SKIP LOCKED;
SET plan_cache_mode = force_generic_plan;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) EXECUTE claim('{0}', '{}');
SELECT pg_stat_statements_reset();
EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}');
EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}');
SELECT calls, shared_blks_hit + shared_blks_read AS buffers,
       (shared_blks_hit + shared_blks_read) / calls AS per_call, rows, temp_blks_written
FROM pg_stat_statements WHERE query LIKE '%ExternalSignalRequested%' AND query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE 'EXPLAIN%';
