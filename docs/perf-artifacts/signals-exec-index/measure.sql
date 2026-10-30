-- Run the statements once to load the catalog caches, then measure a second run.
\i statements.sql
SELECT pg_stat_statements_reset();
\i statements.sql
SELECT shared_blks_hit + shared_blks_read AS buffers, rows, toplevel,
       left(regexp_replace(query, '\s+', ' ', 'g'), 100) AS query
  FROM pg_stat_statements
 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
   AND shared_blks_hit + shared_blks_read > 10
 ORDER BY query;
SELECT relname, relpages FROM pg_class
 WHERE relname IN ('harvest_signals', 'harvest_signals_pkey', 'idx_harvest_signals_pending',
                   'idx_harvest_signals_exec')
 ORDER BY relname;
