-- The statements of plans.sql, run once each. A transaction holds the
-- locking statements, and a rollback undoes the update.
\echo '== active_child, the busiest parent'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE active_child(:'busy_parent');
\echo '== active_child, a leaf'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE active_child(:'leaf');
\echo '== open_forks, the most forked run'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE open_forks(:'fork_source');
\echo '== open_forks, a leaf'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE open_forks(:'leaf');
\echo '== chain_link, the longest chain'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE chain_link(:'chain_name', :'chain_id', :'chain_member', NOW() - interval '7 days');
BEGIN;
\echo '== hold_for_update, a leaf'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE hold_for_update(:'leaf');
\echo '== null_parent, the busiest parent'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF, SUMMARY OFF) EXECUTE null_parent(NULL, :'busy_parent', '{COMPLETED,FAILED,CANCELLED,TIMED_OUT,CONTINUED_AS_NEW,TERMINATED}');
ROLLBACK;
