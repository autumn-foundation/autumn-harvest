## Engine — Claim cost no longer grows with backlog depth (issue #1971)

**Claim order does not change.** The claim picks the same row as before.
Its cost changes. One fix changes behavior: a capped concurrency key can no
longer run tasks over its cap.

**The problem.** The default claim statement scanned and sorted every due
`PENDING` row of the polled queues on each claim. At a 100K backlog the sort
spilled to disk. Assay #11 traced Harvest's single-box throughput gap
against Temporal to this cost.

**The fix.** `queue::claim_task_query` now reads a bounded **seek window**
first:

- Each polled queue has four heads: one per task type, for continuations
  and for new starts. Each head reads at most `queue::CLAIM_SEEK_WINDOW`
  (32) rows from one ordered range of the new index
  `idx_harvest_tq_claim_seek`. A pin head reads the live pins of this
  worker.
- Inside one head, the claim due time follows `scheduled_at`. So each head
  is already in claim order.
- Each head scan skips rows that a row-local gate rejects, such as a pin to
  another worker or a paused activity.
- A guard proves that no row outside the window sorts before the window
  candidate. The proof uses the last row of each full head. The candidate
  scan reads the rows that pass by primary key. Its row test gives the
  planner no partial index on `PENDING` rows. So stale statistics leave only
  the primary-key probe or a scan of the whole table.
- When the guard cannot prove it, or priority ageing is on, the same
  statement runs the old full scan. A one-time filter gates it, so it does
  not run otherwise.
- The rate-limit debit, the advisory-lock recheck and the `claimed` update
  are unchanged. The by-id claim keeps the full-scan form.

**The claim transaction.**

- The default claim statement is now cached once per connection.
  `diesel::sql_query` never caches, so the old claim planned its statement
  on every call. The claim returns a fixed column list, so a migration that
  adds a column does not break the cached statement.
- The transaction first sends `queue::CLAIM_PLAN_SETTINGS_SQL`, one batch:
  `SET LOCAL jit = off`, `SET LOCAL plan_cache_mode = force_generic_plan`
  and `SET LOCAL enable_bitmapscan = off`. The plan carries the cost
  estimate of the full scan, which passed `jit_above_cost`. A bitmap scan
  would read every due row of a queue head instead of the window.
- A claim with priority ageing on always runs the full scan, which reads
  faster with bitmap scans. It sends
  `queue::CLAIM_AGEING_PLAN_SETTINGS_SQL`, without the bitmap setting, and
  caches as its own statement.
- The `expired_runs` CTE reads each deadline index in its own branch. An
  `OR` of the two needs a bitmap scan.
- **Cap fix.** The claim counted a capped key under the snapshot of its own
  statement, which predates the advisory lock of the key. The count missed
  a claim that committed in between. A claim on a capped key now counts
  again after the claim, in a fresh snapshot, and gives the row back over
  the cap. Every claim path runs this re-check: the default, the by-id and
  the batched claim. It adds one round trip to a claim on a capped key
  only.

**Measured** on the deep-backlog generator of issue #1956 (PR #2045),
Postgres 16. Buffers per claim, from `EXPLAIN`, polling the four largest
queues:

| pending rows | old claim | new claim |
|--:|--:|--:|
| 10,000 | 1,551 | 721 |
| 100,000 | 15,820, spills to disk | 2,884 |
| 1,000,000 | 124,641, spills to disk, 6.5 s | 3,649, 38 ms |

Throughput with 8 claimers on the same fixture:

| pending rows | old claim, JIT on | old claim, JIT off | new claim |
|--:|--:|--:|--:|
| 10,000 | 19.6 claims/s | 136 claims/s | 689 claims/s |
| 100,000 | 1.2 claims/s | 7.1 claims/s | 573 claims/s |
| 1,000,000 | not run | 0.6 claims/s | 372 claims/s |

The issue #1215 case, 199 paused activities at a 100K backlog, no longer
spills. See
[`docs/performance.md`](../performance.md#the-seek-window-issue-1971) for
64 queues, latency, fairness and the cases that still fall back.

**Migration.** `20261008042107_harvest_task_queue_claim_seek_index` adds one
partial index on `PENDING` rows. It uses the guarded build of issue #1810.
No column change, no data migration, no `WorkflowEvent` variant, no replay
impact. See [0.8.0 §1.2](../upgrading/0.8.0.md#12-the-claim-reads-a-bounded-window-and-sets-its-own-planner-settings).

**Tests.** `claim_seek_tests` covers the depth sweep, the issue #1215 spill
and a one-type backlog. It covers stale statistics and a kind-filtered
claim behind the other kind. It covers the cached statement before and
after a column is added, and concurrent claimers on a capped key at depth.
It covers the claim order: a continuation storm, a deep pin, a saturated
head, `$6` and saturated types at the head, ageing, a kind filter and
randomized drains against a reference order. The shape tests in `queue.rs`
pin the window, the guard, the fallback gate and the result columns. They
also pin the splices, the window row test, the ageing statement and the cap
re-check.
