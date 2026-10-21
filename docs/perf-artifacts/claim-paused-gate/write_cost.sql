-- WAL bytes for the state transitions that touch harvest_workflow_executions.state.
-- Run once without and once with idx_harvest_we_paused; compare the printed deltas.
-- Snapshots use the WAL insertion position. The write position lags it, so a delta could
-- miss records that are still in the WAL buffers and count them in the next window.
-- Needs the fixture loaded (run.sh does that). Rolled back, so reruns are identical.
-- The fixture at 300k executions starts with 300 PAUSED runs. The last update therefore
-- moves 5,300 rows back to RUNNING: the 300 original ones plus the 5,000 paused above.
BEGIN;
SELECT pg_current_wal_insert_lsn() AS l0 \gset
UPDATE harvest_workflow_executions SET state='COMPLETED'
 WHERE id IN (SELECT id FROM harvest_workflow_executions WHERE state='RUNNING' ORDER BY id LIMIT 5000);
SELECT pg_current_wal_insert_lsn() AS l1 \gset
UPDATE harvest_workflow_executions SET state='PAUSED'
 WHERE id IN (SELECT id FROM harvest_workflow_executions WHERE state='COMPLETED' ORDER BY id LIMIT 5000);
SELECT pg_current_wal_insert_lsn() AS l2 \gset
UPDATE harvest_workflow_executions SET state='RUNNING' WHERE state='PAUSED';
SELECT pg_current_wal_insert_lsn() AS l3 \gset
SELECT :'l1'::pg_lsn - :'l0'::pg_lsn AS wal_running_to_completed_5000,
       :'l2'::pg_lsn - :'l1'::pg_lsn AS wal_completed_to_paused_5000,
       :'l3'::pg_lsn - :'l2'::pg_lsn AS wal_paused_to_running_5300;
ROLLBACK;
