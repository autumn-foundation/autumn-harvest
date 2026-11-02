-- The write cost of the indexes: 1,000 dead-letter inserts.
-- The checkpoint makes the first change to each page write a full-page image.
\o /dev/null
BEGIN;
INSERT INTO harvest_dead_letters (original_task_id, queue_name, task_type, input, error, attempts)
SELECT gen_random_uuid(), 'warm', 'activity', '{}'::jsonb, 'warm', 1
FROM generate_series(1, 10);
ROLLBACK;
SELECT pg_stat_statements_reset();
CHECKPOINT;
BEGIN;
INSERT INTO harvest_dead_letters
    (original_task_id, queue_name, task_type, workflow_exec_id, activity_name, input, error,
     attempts)
SELECT gen_random_uuid(), 'queue_' || (n % 20), 'activity', gen_random_uuid(), 'charge_card',
       jsonb_build_object('n', n, 'pad', repeat('x', 200)),
       'upstream returned 503 for request ' || n || ': ' || repeat('y', 60), 3
FROM generate_series(1, 1000) AS n;
ROLLBACK;
\o
SELECT shared_blks_hit + shared_blks_read AS buffers, rows, wal_records, wal_fpi, wal_bytes,
       left(regexp_replace(query, '\s+', ' ', 'g'), 60) AS query
  FROM pg_stat_statements
 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
   AND query ~ '^INSERT' AND toplevel
 ORDER BY query;
