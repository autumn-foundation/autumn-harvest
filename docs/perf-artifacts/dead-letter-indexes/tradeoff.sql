-- The cost that idx_harvest_dl_failed_at can add (issue #2401).
-- A filter that no index serves, ordered by failed_at, with a LIMIT. The
-- matches are the 5,000 newest rows, and the read wants the oldest. With the
-- index, the planner can walk it from the oldest end and filter each row.
-- This is the shape of dlq::query_dead_letters_for_bulk with only an
-- activity filter. The second run shuffles the heap first, as churn does.
\o /dev/null
SELECT id FROM harvest_dead_letters WHERE activity_name = 'refund'
 ORDER BY failed_at ASC LIMIT 100;
\o
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id FROM harvest_dead_letters WHERE activity_name = 'refund'
 ORDER BY failed_at ASC LIMIT 100;
BEGIN;
\o /dev/null
CREATE TEMP TABLE shuffled AS SELECT * FROM harvest_dead_letters ORDER BY random();
TRUNCATE harvest_dead_letters;
INSERT INTO harvest_dead_letters SELECT * FROM shuffled;
ANALYZE harvest_dead_letters;
SELECT id FROM harvest_dead_letters WHERE activity_name = 'refund'
 ORDER BY failed_at ASC LIMIT 100;
\o
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id FROM harvest_dead_letters WHERE activity_name = 'refund'
 ORDER BY failed_at ASC LIMIT 100;
ROLLBACK;
