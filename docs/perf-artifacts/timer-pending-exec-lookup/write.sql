-- WAL bytes for the timer write path: 10,000 timer inserts, then 10,000
-- fired updates, as the worker issues them (one row per statement).
\set ON_ERROR_STOP 1
CHECKPOINT;
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input)
VALUES ('00000000-0000-4000-a000-000000000001', 'writer', 'writer-1', 0, '{}');
SELECT pg_current_wal_lsn() AS l0 \gset
DO $$ BEGIN FOR i IN 1..10000 LOOP
  INSERT INTO harvest_timers (workflow_exec_id, timer_id, fires_at, fired)
  VALUES ('00000000-0000-4000-a000-000000000001', 'w-' || i, now() - interval '1 second', false);
END LOOP; END $$;
SELECT pg_current_wal_lsn() AS l1 \gset
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT id FROM harvest_timers WHERE workflow_exec_id = '00000000-0000-4000-a000-000000000001' LOOP
    UPDATE harvest_timers SET fired = true WHERE id = r.id;
  END LOOP; END $$;
SELECT pg_current_wal_lsn() AS l2 \gset
SELECT pg_wal_lsn_diff(:'l1', :'l0') AS insert_wal_bytes,
       pg_wal_lsn_diff(:'l2', :'l1') AS fire_wal_bytes,
       round(pg_wal_lsn_diff(:'l1', :'l0') / 10000.0, 1) AS insert_wal_per_timer,
       round(pg_wal_lsn_diff(:'l2', :'l1') / 10000.0, 1) AS fire_wal_per_timer;
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size, pg_relation_size(indexrelid) AS bytes
FROM pg_stat_user_indexes WHERE relname = 'harvest_timers' ORDER BY 1;
