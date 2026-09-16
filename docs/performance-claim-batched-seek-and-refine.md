# Batched seek-and-refine claim: measured against the single-row path

Issue #1340 asks for the architectural fix `docs/performance.md`'s
"Any residual predicate defeats sort-elision" section names but declines to
attempt: a batched seek-and-refine restructuring of the claim candidate
scan. `docs/assays/0005-claim-batched-seek-and-refine.md` (ledger #5)
prototyped this shape and killed it on a pre-registration fencepost bug, not
a mechanism defect. This page measures a real, tested implementation
(`queue::claim_task_batched`, additive -- not wired into the default claim
path) against the single-row `claim_task_query()` it sits beside.

## Headline result

At the 256-key idle scenario (10,000-row backlog, 4 queues, 0 `RUNNING`),
the batch candidate fetch costs slightly more than the single-row scan: 290
buffers against 275 (1.05x). Neither query gets an index-driven bounded
scan at this backlog depth -- both plan as a full `Seq Scan` feeding a
`Sort`, matching ledger #5's own finding. The batch pays for locking
`B=50` rows with `FOR UPDATE SKIP LOCKED` instead of one.

At the 256-key hot-contention scenario (same backlog, 2,000 `RUNNING` rows
spread across the same keys), the batch candidate fetch costs slightly
FEWER buffers than the single-row scan (10,077 against 10,410) and far
less wall-clock: 23.5ms against 159.0ms in an isolated
`EXPLAIN (ANALYZE, BUFFERS)`. A real end-to-end drive of the compiled
`claim_task` and `claim_task_batched` functions against the same fixture
shows the same direction, at a similar margin: mean 915.0ms per batched
claim against 1,831.0ms per single-row claim (2.00x), over 400 real
claims each.

The mechanism: the single-row path always evaluates
`concurrency_pending_keys` and `concurrency_running_counts` -- an
aggregate over every distinct key with due `PENDING` work, scanning the
`RUNNING` population behind each one -- for every claim, whether or not the
winning row even carries a concurrency key. The batched path defers that
entirely to `claim_batched_candidate_attempt_query`, which touches only the
one winning candidate's own key.

**This does not resolve the O(backlog) scaling question ledger #5 left
open.** Both queries plan as a full scan at this fixture depth; the win
measured here is the concurrency-key aggregate's cost, not a `LIMIT`
pushdown. See [What this does not establish](#what-this-does-not-establish).

These numbers are measured AFTER a review finding on this PR: the first
draft's per-candidate walk debited a rate-limit token for every candidate
tried, including one the concurrency gate always rejected. An adversarial
batch sharing a saturated `concurrency_key` and a `rate_limit_key` could
have leaked far more than the single-row path's documented one-token
bound. `queue::claim_batched_candidate_concurrency_probe_query` closes
that gap with a cheap, read-only concurrency check that runs first, adding
one small round trip for a concurrency-keyed candidate that passes it. The
numbers above already include that extra round trip.

## Reproduction

Fixture, matching `docs/performance.md`'s own headline shape (issue #247):

```sql
-- 10,000-row backlog, 4 queues, 256 concurrency keys, uncapped.
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, concurrency_key,
   concurrency_cap, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop',
       gen_random_uuid(), '{}'::jsonb, 'PENDING', (i % 100), 0, 3,
       NOW() - INTERVAL '1 second', 'ckey-' || (i % 256), 1000000, 0, FALSE
FROM generate_series(1, 10000) AS i;

-- Hot-contention addition: 2,000 RUNNING rows on the same 256 keys.
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, worker_id, attempt, max_attempts, scheduled_at, started_at,
   concurrency_key, concurrency_cap, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop',
       gen_random_uuid(), '{}'::jsonb, 'RUNNING', 0, 'holder-' || i, 1, 3,
       NOW() - INTERVAL '10 second', NOW() - INTERVAL '5 second',
       'ckey-' || (i % 256), 1000000, 0, FALSE
FROM generate_series(1, 2000) AS i;
```

`autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh` seeds
this fixture, runs `EXPLAIN (ANALYZE, BUFFERS)` for both queries at idle and
hot-contention, runs the real end-to-end latency capture below, and
archives every output to `docs/perf-artifacts/claim-batched-seek-and-refine/`:

| file | scenario | source |
|:--|:--|:--|
| `control_idle.explain.txt` | idle | `EXPLAIN` of `claim_task_query()` |
| `batch_idle.explain.txt` | idle | `EXPLAIN` of `claim_task_batched_candidates_query()` |
| `control_hot.explain.txt` | hot-contention | `EXPLAIN` of `claim_task_query()` |
| `batch_hot.explain.txt` | hot-contention | `EXPLAIN` of `claim_task_batched_candidates_query()` |
| `end_to_end_latency.txt` | hot-contention | real `claim_task`/`claim_task_batched` calls |

The buffer totals above are each file's root-node cumulative `Buffers:`
line, which already includes every CTE the plan actually references
-- not a sum across every node in the plan, which double-counts through
nested CTE Scan references. `JIT` is disabled for the capture
(`SET jit = off`); JIT compilation overhead otherwise dominates wall-clock
on a single cold `EXPLAIN ANALYZE` and makes the two queries incomparable.

The end-to-end 400-claim numbers come from
`claim_batched_tests::zz_capture_claim_batched_end_to_end_latency`, an
`#[ignore]`d test the perf-repro script runs -- not an ad hoc, unreproducible
run. It drives the real compiled `queue::claim_task` and
`queue::claim_task_batched` functions against the same hot-contention
fixture, single-row path first, so the batched path's own numbers do not
benefit from a warmer cache. Absolute milliseconds reflect this
measurement's own container, not reference hardware -- read the ~2.0x
ratio, not the absolute figures, the same caveat every prior ledger entry
and `docs/performance.md` page carries.

## What this does not establish

Matching `docs/assays/0005-claim-batched-seek-and-refine.md`'s own honesty
about its gaps, carried forward rather than re-litigated:

- **Backlog-depth-independent scaling.** This page measures one backlog
  depth (10,000 rows). Whether either query's cost grows sub-linearly,
  linearly, or matches at a much deeper backlog is untested here.
- **Real concurrent-claimer throughput.** `tests/integration/claim_batched_tests.rs`'s
  `concurrent_batched_claimers_never_exceed_the_concurrency_cap` proves
  *correctness* under real concurrent claimers (the cap is never exceeded).
  It does not measure *throughput* under contention -- whether locking up
  to `batch_size` rows per attempt instead of one measurably changes claim
  fairness or latency under a real worker fleet remains the open question
  issue #1340 names before this could become the default claim path.
- **The adversarial in-batch-rejection + large-`RUNNING`-population
  combination.** Ledger #5 flagged this as untested; it remains untested
  here too.

## See also

- `docs/performance.md` -- the claim-path measurement discipline this page
  follows, and the "Any residual predicate defeats sort-elision" section
  that names issue #1340.
- `docs/assays/0005-claim-batched-seek-and-refine.md` -- the prototype this
  implementation corrects and builds on (per-candidate authoritative
  recheck, four-column keyset cursor with an `id` tiebreak).
- `autumn-harvest/src/queue.rs`'s module doc above
  `claim_task_batched_candidates_query` -- scope, mechanism, and what a
  reviewer needs to sign off before this becomes the default claim path.
