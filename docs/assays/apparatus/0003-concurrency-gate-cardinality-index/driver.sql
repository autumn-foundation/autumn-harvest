-- Runs the whole assay -- schema, every seed, every control/candidate
-- EXPLAIN -- in one continuous psql session/connection, so control and
-- candidate measurements genuinely share backend-local state and the
-- shared-buffer pool the way the pre-registration and report describe.
-- Invoke via run_assay.sh (which just does `psql -f driver.sql`), not
-- directly, since \o paths below are relative to the invoking directory.
\set ON_ERROR_STOP 1

\i schema.sql

\set backlog 10000
\set queues 4
\set keys 256
\set running_rows 0
\i seed.sql
\o results/idle_256-control.explain.txt
\i control.sql
\o

\set keys 256
\set running_rows 2000
\i seed.sql
\o results/hot_256-control.explain.txt
\i control.sql
\o

\set keys 5000
\set running_rows 2000
\i seed.sql
\o results/hot_5000-control.explain.txt
\i control.sql
\o

\set keys 5000
\set running_rows 0
\i seed.sql
\o results/idle_5000-control.explain.txt
\i control.sql
\o

\i candidate_index.sql

\set keys 256
\set running_rows 0
\i seed.sql
\o results/idle_256-candidate.explain.txt
\i candidate.sql
\o

\set keys 256
\set running_rows 2000
\i seed.sql
\o results/hot_256-candidate.explain.txt
\i candidate.sql
\o

\set keys 5000
\set running_rows 2000
\i seed.sql
\o results/hot_5000-candidate.explain.txt
\i candidate.sql
\o

\set keys 5000
\set running_rows 0
\i seed.sql
\o results/idle_5000-candidate.explain.txt
\i candidate.sql
\o
