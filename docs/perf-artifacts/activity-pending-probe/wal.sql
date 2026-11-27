-- WAL bytes for 10,000 appended events (fixture mix: 1/3 each of Scheduled, Started, Completed), rolled back.
\set ON_ERROR_STOP 1
CHECKPOINT;
SELECT pg_current_wal_insert_lsn() AS l0 \gset
BEGIN;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT md5('run' || (1 + g / 300))::uuid, 1000000 + g,
       (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted'])[1 + g % 3],
       jsonb_build_object('type', (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted'])[1 + g % 3],
         'data', jsonb_build_object('activity_id', md5('a' || (g / 3))::uuid, 'output', repeat(md5(g::text), 32))),
       now()
FROM generate_series(1, 10000) g;
SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), :'l0') AS wal_bytes;
ROLLBACK;
