-- Runs the whole assay -- schema, function def, every seed, every
-- control/candidate measurement -- in one continuous psql session, so
-- control and candidate genuinely share backend-local state and the
-- shared-buffer pool. Invoke via run_assay.sh, not directly, since \o
-- paths below are relative to the invoking directory.
\set ON_ERROR_STOP 1

\i schema.sql
\i claim_deferred.sql

-- ===== idle_256: 10,000 backlog, 256 keys, 0 RUNNING =====
\set backlog 10000
\set queues 4
\set keys 256
\set running_rows 0
\i seed.sql

\o results/idle_256-control.explain.txt
\i control.sql
\o

\timing on
\o results/idle_256-control_raw.txt
\i control_raw.sql
\o
\timing off

\o results/idle_256-candidate_select.explain.txt
\i candidate_select.sql
\o

\set probe_key 'bench-ck-0'
\o results/idle_256-recheck.explain.txt
\i recheck.sql
\o

\timing on
\o results/idle_256-claim_deferred.txt
SELECT * FROM claim_deferred(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']);
\o
\timing off

-- ===== hot_256: 10,000 backlog, 256 keys, 2,000 RUNNING =====
\set keys 256
\set running_rows 2000
\i seed.sql

\o results/hot_256-control.explain.txt
\i control.sql
\o

\timing on
\o results/hot_256-control_raw.txt
\i control_raw.sql
\o
\timing off

\o results/hot_256-candidate_select.explain.txt
\i candidate_select.sql
\o

\set probe_key 'bench-ck-0'
\o results/hot_256-recheck.explain.txt
\i recheck.sql
\o

\timing on
\o results/hot_256-claim_deferred.txt
SELECT * FROM claim_deferred(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']);
\o
\timing off

-- ===== hot_5000: 10,000 backlog, 5,000 keys, 2,000 RUNNING =====
\set keys 5000
\set running_rows 2000
\i seed.sql

\o results/hot_5000-control.explain.txt
\i control.sql
\o

\timing on
\o results/hot_5000-control_raw.txt
\i control_raw.sql
\o
\timing off

\o results/hot_5000-candidate_select.explain.txt
\i candidate_select.sql
\o

\set probe_key 'bench-ck-0'
\o results/hot_5000-recheck.explain.txt
\i recheck.sql
\o

\timing on
\o results/hot_5000-claim_deferred.txt
SELECT * FROM claim_deferred(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']);
\o
\timing off

-- ===== L4: adversarial retry-depth fixture =====
\set backlog 10000
\set queues 4
\set keys 256
\i seed_adversarial.sql

\timing on
\o results/l4_adversarial-claim_deferred.txt
SELECT * FROM claim_deferred(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']);
\o
\timing off
