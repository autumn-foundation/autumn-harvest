-- The write cost of the index: 1,000 signal inserts, then 1,000 consumes.
BEGIN;
INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload)
SELECT '00000000-0000-4000-8000-000000000005', 'warm_' || n, '{}'::jsonb
FROM generate_series(1, 10) AS n;
ROLLBACK;
SELECT pg_stat_statements_reset();
BEGIN;
INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(1 + n % 4000), 12, '0'))::uuid,
       'new_' || n, '{}'::jsonb
FROM generate_series(1, 1000) AS n;
UPDATE harvest_signals SET consumed = true WHERE signal_name LIKE 'new_%' AND NOT consumed;
ROLLBACK;
SELECT shared_blks_hit + shared_blks_read AS buffers, rows, wal_records, wal_bytes,
       left(regexp_replace(query, '\s+', ' ', 'g'), 60) AS query
  FROM pg_stat_statements
 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
   AND query ~ '^(INSERT|UPDATE)' AND toplevel
 ORDER BY query;
