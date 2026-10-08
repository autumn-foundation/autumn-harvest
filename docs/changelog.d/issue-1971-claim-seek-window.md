## Engine — Claim cost no longer grows with backlog depth (issue #1971)

**No behavior change.** The claim picks the same row as before. Only its
cost changes.

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
  scan reads the rows that pass by primary key.
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
  `SET LOCAL jit = off` and `SET LOCAL plan_cache_mode =
  force_generic_plan`. The plan carries the cost estimate of the full scan,
  which passed `jit_above_cost`.

**Measured** on the deep-backlog fixture of issue #1956, Postgres 16, 8
claimers:

| pending rows | claims/s, before | claims/s, after | claim buffers, before | claim buffers, after |
|--:|--:|--:|--:|--:|
| 1,000 | 533 | 797 | 365 | 675 |
| 10,000 | 1.4 | 1,142 | 10,405 | 469 |
| 100,000 | 1.3 | 1,334 | 103,404, spills to disk | 538 |

"Before" re-planned the old claim on every call with the server default
`jit = on`. The issue #1215 case, 199 paused activities at a 100K backlog,
no longer spills. Fairness does not change: the window picks the row that
the full scan would pick. See
[`docs/performance.md`](../performance.md#the-seek-window-issue-1971) for
latency, fairness and the cases that still fall back.

**Migration.** `20261008042107_harvest_task_queue_claim_seek_index` adds one
partial index on `PENDING` rows. It uses the guarded build of issue #1810.
No column change, no data migration, no `WorkflowEvent` variant, no replay
impact. See [0.8.0 §1.2](../upgrading/0.8.0.md#12-the-claim-reads-a-bounded-window-and-sets-its-own-planner-settings).

**Tests.** `claim_seek_tests` covers the depth sweep, the issue #1215 spill
and a one-type backlog. It covers stale statistics, a kind-filtered claim
behind the other kind, and the cached statement before and after a column
is added. It covers the claim order: a continuation storm, a deep pin, a
saturated head, `$6` and saturated types at the head, ageing, a kind filter
and randomized drains against a reference order. The claim-query shape
tests in `queue.rs` pin the window, the guard, the fallback gate, the
result columns and the splices.
