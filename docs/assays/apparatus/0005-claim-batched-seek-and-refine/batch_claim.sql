-- Candidate's batch-fetch step, isolated for EXPLAIN at the non-adversarial
-- scenarios (first batch only, no keyset cursor needed -- see
-- claim_batched.sql for the multi-batch cursor and why `id` breaks ties
-- `priority`/`scheduled_at` alone cannot).
--
-- NOTE (post-review, Codex, round 2): this file previously also modeled
-- the concurrency-gate recheck as a batch-scoped CTE computed once from a
-- pre-claim snapshot. That mechanism was a genuine correctness gap (see
-- claim_batched.sql's own note) and claim_batched.sql no longer uses it --
-- the authoritative check is now a per-candidate `pg_try_advisory_xact_lock`
-- + fresh `COUNT`, which is inherently procedural (order- and
-- side-effect-dependent) and cannot be represented as a single `EXPLAIN`ed
-- query, the same reason ledger #4 kept its own recheck cost in a separate
-- isolated file (`recheck.sql`) rather than folding it into
-- `candidate_select.sql`. This file therefore measures only the fetch
-- step's own cost; the recheck's per-candidate cost is the same query
-- shape ledger #4 already measured in isolation (~1 buffer at both 256 and
-- 5,000 key cardinality), and its actual contribution to end-to-end cost
-- is visible in this assay's own `claim_batched()` wall-clock numbers.
--
-- NOTE (post-review, Codex, round 2, second finding): the archived
-- `EXPLAIN` output for this query shows a `Seq Scan` (or, forced, an
-- `Index Scan` that still reads every matching row) feeding a top-N
-- `Sort`, not a bounded index-ordered seek, at this apparatus's 10,000-row
-- backlog depth -- see the report's post-review section, including a
-- direct test showing this is not explained by the `id` tiebreak added
-- below (the same `Sort`+`actual rows=10000` shape appears with or without
-- it).
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, concurrency_key, concurrency_cap, priority, scheduled_at
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC, id ASC
LIMIT :batch_size
FOR UPDATE SKIP LOCKED;
