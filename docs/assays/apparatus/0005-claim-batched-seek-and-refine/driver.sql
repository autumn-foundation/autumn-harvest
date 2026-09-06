-- Runs the whole pre-registered assay -- schema, function def, every seed,
-- every control/candidate measurement -- in one continuous psql session,
-- so control and candidate genuinely share backend-local state and the
-- shared-buffer pool. Invoke via run_assay.sh, not directly.
--
-- \echo lines below label results/run.log's \timing output (run_assay.sh
-- captures this session's stdout to that file; \o does not redirect
-- \timing or \echo, only query results -- see run_assay.sh's own note).
\set ON_ERROR_STOP 1

\i schema.sql
\i claim_batched.sql

\set batch_size 50

-- ===== idle_256: 10,000 backlog, 256 keys, 0 RUNNING (L1) =====
\set backlog 10000
\set queues 4
\set keys 256
\set running_rows 0
\i seed.sql

\o results/idle_256-control.explain.txt
\i control.sql
\o

\o results/idle_256-batch_claim.explain.txt
\i batch_claim.sql
\o

\o results/idle_256-forced_index.explain.txt
\i forced_index_diagnostic.sql
\o

\echo -- idle_256 control_raw --
\timing on
\o results/idle_256-control_raw.txt
\i control_raw.sql
\o
\echo -- idle_256 claim_batched --
\o results/idle_256-claim_batched.txt
SELECT * FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
\timing off

-- ===== hot_256: 10,000 backlog, 256 keys, 2,000 RUNNING (L3) =====
\set keys 256
\set running_rows 2000
\i seed.sql

\o results/hot_256-control.explain.txt
\i control.sql
\o

\o results/hot_256-batch_claim.explain.txt
\i batch_claim.sql
\o

\echo -- hot_256 control_raw --
\timing on
\o results/hot_256-control_raw.txt
\i control_raw.sql
\o
\echo -- hot_256 claim_batched --
\o results/hot_256-claim_batched.txt
SELECT * FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
\timing off

-- ===== hot_5000: 10,000 backlog, 5,000 keys, 2,000 RUNNING (L2) =====
\set keys 5000
\set running_rows 2000
\i seed.sql

\o results/hot_5000-control.explain.txt
\i control.sql
\o

\o results/hot_5000-batch_claim.explain.txt
\i batch_claim.sql
\o

\o results/hot_5000-forced_index.explain.txt
\i forced_index_diagnostic.sql
\o

\echo -- hot_5000 control_raw --
\timing on
\o results/hot_5000-control_raw.txt
\i control_raw.sql
\o
\echo -- hot_5000 claim_batched --
\o results/hot_5000-claim_batched.txt
SELECT * FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
\timing off

-- ===== L4: ledger #4's exact 50-row adversarial fixture, one batch =====
\set backlog 10000
\set queues 4
\set keys 256
\i seed_adversarial_50.sql

\echo -- l4_adversarial_50 claim_batched --
\timing on
\o results/l4_adversarial_50-claim_batched.txt
SELECT * FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
\timing off

-- ===== L5: 200-row adversarial fixture, four batches =====
\set backlog 10000
\set queues 4
\set keys 256
\i seed_adversarial_200.sql

\echo -- l5_adversarial_200 claim_batched --
\timing on
\o results/l5_adversarial_200-claim_batched.txt
SELECT * FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
\timing off

-- ===== Equivalence check: candidate claims the same row control would,
-- in every non-adversarial scenario. Re-seed each and compare in the same
-- transaction (rolled back, so it doesn't consume the row). =====
\set keys 256
\set running_rows 0
\i seed.sql
BEGIN;
\o results/equivalence_idle_256.txt
SELECT 'control' AS side, id FROM harvest_task_queue WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']) AND state='PENDING' AND scheduled_at <= NOW() ORDER BY priority DESC, scheduled_at ASC LIMIT 1;
SELECT 'candidate' AS side, claimed_id FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
ROLLBACK;

\set keys 256
\set running_rows 2000
\i seed.sql
BEGIN;
\o results/equivalence_hot_256.txt
SELECT 'control' AS side, id FROM harvest_task_queue WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']) AND state='PENDING' AND scheduled_at <= NOW() ORDER BY priority DESC, scheduled_at ASC LIMIT 1;
SELECT 'candidate' AS side, claimed_id FROM claim_batched(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], :batch_size);
\o
ROLLBACK;
