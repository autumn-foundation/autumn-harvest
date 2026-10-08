## Engine — Claim cost no longer grows with backlog depth (issue #1971)

**No behavior change.** The claim picks the same row as before. Only its
cost changes.

**The problem.** The default claim statement scanned and sorted every due
`PENDING` row of the polled queues on each claim. At a 100K backlog the sort
spilled to disk. Assay #11 traced Harvest's single-box throughput gap
against Temporal to this cost.

**The fix.** `queue::claim_task_query` now reads a bounded **seek window**
first:

- Each polled queue has two heads: continuations and new starts. A third
  head holds the live pins to this worker. Each head reads at most
  `queue::CLAIM_SEEK_WINDOW` (32) rows, in index order.
- Inside one queue head, the claim due time follows `scheduled_at`. So each
  head is already in claim order. The new index
  `idx_harvest_tq_claim_seek` serves the queue heads.
- Each head scan skips rows that a row-local gate rejects, such as a pin to
  another worker or a paused activity.
- A guard proves that no row outside the window sorts before the window
  candidate. The proof uses the last row of each full head.
- When the guard cannot prove it, or priority ageing is on, the same
  statement runs the old full scan. A one-time filter gates it, so it does
  not run otherwise.
- The rate-limit debit, the advisory-lock recheck and the `claimed` update
  are unchanged. The by-id claim keeps the full-scan form.
- The claim transaction sends `queue::CLAIM_PLAN_SETTINGS_SQL` first:
  `SET LOCAL jit = off` and `SET LOCAL plan_cache_mode =
  force_generic_plan`, one batch. The plan carries the cost estimate of the
  full scan, which passed `jit_above_cost`. A custom plan cost more to plan
  than the claim cost to run.

**Measured** on the deep-backlog fixture of issue #1956, Postgres 16:

| pending rows | claim buffers, before | claim buffers, after | 8 claimers, claims/s, before | 8 claimers, claims/s, after |
|--:|--:|--:|--:|--:|
| 1,000 | 330 | 367 | 780 | 952 |
| 10,000 | 10,413 | 450 | 1.6 | 1,348 |
| 100,000 | 103,416, spills to disk | 548 | 1.4 | 1,413 |

"Before" ran with the server default `jit = on`. With JIT off, the old
claim reached 127 claims/s at 10K and 13 at 100K. The issue #1215 case, 199
paused activities at a 100K backlog, no longer spills. Fairness does not
change: the window picks the row that the full scan would pick. See
[`docs/performance.md`](../performance.md#the-seek-window-issue-1971) for
latency, fairness and the cases that still fall back.

**Migration.** `20261008042107_harvest_task_queue_claim_seek_index` adds one
partial index on `PENDING` rows. It uses the guarded build of issue #1810.
No column change, no data migration, no `WorkflowEvent` variant, no replay
impact.

**Tests.** `claim_seek_tests`: the depth sweep, the issue #1215 spill, a
one-type backlog, a continuation storm, a deep pin, a saturated head, `$6`
and saturated types at the head, ageing, a kind filter, randomized drains
against a reference order, and the transaction scope of the planner
settings. The claim-query shape tests in `queue.rs` pin the window, the
guard, the fallback gate and the splices.
