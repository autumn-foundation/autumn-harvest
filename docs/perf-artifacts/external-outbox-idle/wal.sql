-- WAL and time of the request path: 10,000 requests, then 10,000
-- resolutions, each in one statement. A CHECKPOINT before each step
-- puts full-page images on an equal footing. Run on a schema without and
-- with the pending table.
\set ON_ERROR_STOP 1
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at)
SELECT md5('w'||gs)::uuid, 'wal_wf', 'wal_'||gs, md5('ww'||gs)::uuid, 0, 'RUNNING', '{}'::jsonb, 'default', NOW(), NOW()
FROM generate_series(1,10000) gs;
CHECKPOINT;
SELECT pg_current_wal_insert_lsn() AS l0 \gset
\timing on
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT md5('w'||gs)::uuid, 1, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',jsonb_build_object('signal_id','w-'||gs)), NOW()
FROM generate_series(1,10000) gs;
\timing off
SELECT pg_current_wal_insert_lsn() AS l1 \gset
CHECKPOINT;
SELECT pg_current_wal_insert_lsn() AS l2 \gset
\timing on
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT md5('w'||gs)::uuid, 2, 'ExternalSignalDelivered',
       jsonb_build_object('type','ExternalSignalDelivered','data',jsonb_build_object('signal_id','w-'||gs)), NOW()
FROM generate_series(1,10000) gs;
\timing off
SELECT pg_current_wal_insert_lsn() AS l3 \gset
SELECT pg_wal_lsn_diff(:'l1', :'l0') / 10000 AS request_wal_bytes_per_row,
       pg_wal_lsn_diff(:'l3', :'l2') / 10000 AS resolution_wal_bytes_per_row;
