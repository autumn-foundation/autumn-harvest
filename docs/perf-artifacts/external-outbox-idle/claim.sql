-- The claim as it ships after this change: the text of
-- external_signal_outbox_claim_query(), on one line. Ten calls of the
-- generic plan, as the scanner keeps the statement prepared.
SELECT pg_stat_statements_reset();
PREPARE claim(int4[], int8[]) AS
SELECT e.id, e.workflow_exec_id, e.event_id, e.event_type, e.event_data, e.timestamp FROM harvest_external_outbox_pending p JOIN LATERAL ( SELECT 1 AS running_exec FROM harvest_workflow_executions x WHERE x.id = p.workflow_exec_id AND x.state = 'RUNNING' AND x.shard_id = ANY($1) LIMIT 1 ) running ON TRUE JOIN LATERAL ( SELECT ev.id, ev.workflow_exec_id, ev.event_id, ev.event_type, ev.event_data, ev.timestamp FROM harvest_events ev WHERE ev.id = p.event_row_id AND ev.event_type = 'ExternalSignalRequested' AND (ev.event_data->'data'->>'signal_id') IS NOT NULL AND NOT EXISTS ( SELECT 1 FROM harvest_events res WHERE res.workflow_exec_id = ev.workflow_exec_id AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed') AND res.event_data->'data'->>'signal_id' = ev.event_data->'data'->>'signal_id' ) LIMIT 1 ) e ON TRUE WHERE p.request_type = 'ExternalSignalRequested' AND NOT (p.event_row_id = ANY($2)) ORDER BY p.requested_at, p.event_row_id LIMIT 1 FOR UPDATE OF e SKIP LOCKED;
SET plan_cache_mode = force_generic_plan;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) EXECUTE claim('{0}', '{}');
SELECT pg_stat_statements_reset();
EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}');
EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}'); EXECUTE claim('{0}', '{}');
SELECT calls, shared_blks_hit + shared_blks_read AS buffers,
       (shared_blks_hit + shared_blks_read) / calls AS per_call, rows, temp_blks_written
FROM pg_stat_statements WHERE query LIKE '%ExternalSignalRequested%' AND query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE 'EXPLAIN%';
