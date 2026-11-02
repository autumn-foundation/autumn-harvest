-- Run the statements once to load the catalog caches, then measure a second run.
\o /dev/null
\i statements.sql
SELECT pg_stat_statements_reset();
\i statements.sql
\o
SELECT shared_blks_hit + shared_blks_read AS buffers, rows,
       left(regexp_replace(query, '\s+', ' ', 'g'), 100) AS query
  FROM pg_stat_statements
 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
   AND toplevel AND query !~ '^(BEGIN|ROLLBACK|SELECT pg_stat)'
 ORDER BY query;
SELECT relname, relpages FROM pg_class
 WHERE relname LIKE 'harvest_dead_letters%' OR relname LIKE 'idx_harvest_dl_%'
 ORDER BY relname;
