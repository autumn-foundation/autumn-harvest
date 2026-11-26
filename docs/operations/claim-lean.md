# Lean claim

The default claim sends a window statement, and the full statement only
when the window statement asks for it. See
[`performance-claim-window-split.md`](../performance-claim-window-split.md)
(issue #2680). The lean claim puts a third, smaller statement in front of
both. It is on by default.

Postgres starts every plan node of a statement on each run. Many nodes of
the window statement serve features that a row may not use: concurrency
keys, rate limits, build pins and capability requirements. The lean
statement has none of those nodes.

## What the lean statement does

A row is plain when its `concurrency_key`, `rate_limit_key`,
`required_build_id` and `required_capabilities` are all `NULL`. The lean
statement reads the same seek window as the window statement, in its own
snapshot:

- When it takes a row, that row is the row that the window statement
  takes. Each pin gate and deadline gate is the same text. Each pause gate
  gives the same result. See
  [How it reads the pauses](#how-it-reads-the-pauses).
- When its window holds no row, it takes no row, as the window statement
  does.
- When its window holds a row and it locks none, it claims no row and
  changes no row, and it sets a flag. The claim then sends the unchanged
  window statement in the same transaction.

So both orders claim the same row. The lean statement changes the cost of a
claim only.

Three causes set the flag:

- a window row that is not plain;
- a parked row: the workflow task of a paused run, or a task of a run past
  its deadline;
- window rows that peer claims hold, when no other window row is free.

So a queue of plain rows only can still set the flag.

The window statement can lock a row and not claim it. A run deadline can
pass after the lock, and an owner guard can refuse the row. The lean
statement does the same: it returns no row and a clear flag.

### The locks of a lean statement that sets its flag

A lean statement that sets its flag takes no bucket lock. A window with a
row that is not plain locks no row either.

There is one exception. A peer can commit a change of a window row between
the snapshot of the lean statement and its row lock. The statement then
locks that row, checks its new version and skips it. The claim keeps the
lock of that row until its transaction ends, as the window statement does.
With the lean statement first, that is one statement later.

## How it reads the pauses

The lean statement reads the pause lists in a way that needs fewer plan
nodes. The window statement keeps its text.

- **One read for each scan.** The lean statement reads each pause list
  through a scalar subquery. Postgres runs it once, and each pause gate is
  a test of its own scan. The window statement joins each list to each
  scan.
- **No aggregate.** The lean statement builds each list with
  `ARRAY(SELECT ...)`. An empty list is an empty array in both forms.
- **One list of held activities.** A worker names each activity at its
  adaptive limit in the claim. The lean statement adds those names to the
  list of paused activities. A claim holds a row of either kind in the same
  way, so one test does the work of two.

These change the plan of the lean statement only. A paused queue, a paused
activity and an activity at its limit hold the same rows as before.

## The skip

A lean statement that sets its flag costs one statement more than a claim
with no lean statement. It reads the whole seek window before it can set
the flag, so its cost grows with the number of queues of the claim. In one
measurement it cost a quarter to a half of a window statement. A queue that
keeps a keyed row or a parked row at its head would pay that on each claim.
So each claim loop keeps a skip:

- After a lean statement sets its flag, the next claims of the loop send
  the window statement first.
- Each flag adds 4 claims to the skip, up to 64. The first flag skips 4
  claims. A second flag with no lean claim before it skips 8.
- Each lean statement that takes a row takes 1 claim off the next skip. An
  empty poll keeps it.

At least 4 claims with no lean statement follow each flag. So a loop sends
one wasted lean statement in 5 claims at most. A cause that stays, such as
a keyed row at a queue head, costs about one statement in 65 claims.

A loop can see plain windows and other windows in turn. Examples are a
shallow queue of plain and keyed rows, and a parked row at a head while
plain rows arrive one at a time. One lean claim takes only 1 claim off the
skip, so such a loop moves to the window statement first. A loop keeps the
lean statement first while it takes more than 4 rows for each flag. A loop
with more flags than that sends the window statement first on most claims,
as a worker with the lean statement off does.

The skip counts claims. It reads no clock and no random number. A claim
loop holds one skip:

- for each shard that it polls;
- for each claim kind: any kind, workflow only and activity only;
- for each queue, when the worker has queue weights. Such a worker claims
  queue by queue.

So one loop never changes the skip of another loop. A keyed queue does not
take the lean statement away from a plain queue of the same worker, and a
keyed activity head does not take it away from a workflow-only claim.

## Claims that send no lean statement

- a claim with priority ageing (`with_priority_aging_secs` above zero);
- a claim with fairness keys (`with_fairness_keys`);
- a [batch claim](claim-batch.md);
- a dispatch-channel claim of one named row, and an owner decider take;
- a caller of `queue::claim_task` and of the other claim functions that
  take no loop state. Only `queue::claim_task_with_lean` takes one.

## Turn it off

```rust
use autumn_harvest::builder::WorkerConfig;

let config = WorkerConfig::default().with_claim_lean(false);
assert!(!config.claim_lean);
```

- `with_claim_lean(false)` makes each claim send the window statement
  first, as before this change.
- The switch belongs to each worker. Workers with and without it can share
  a shard, and no migration is needed.
- `GET /admin/config` reports the value as `claim_lean`.
- `WorkerRuntimeConfig` has the same field. A struct literal of
  `WorkerConfig` or `WorkerRuntimeConfig` needs `claim_lean: true`.

Turn it off to compare the two orders on your workload, or to rule the lean
statement out of a problem. A worker whose queue heads are seldom plain
gains nothing from the lean statement, so turn it off there too. The `e2e_bench` binary reads
`HARVEST_BENCH_CLAIM_LEAN`: `0` or `off` runs the same binary with the lean
statement off.

## Memory

Each pooled connection keeps one more cached plan for each lean variant
that it runs. A lean plan is about half the size of the window plan of its
variant: 0.7 MB to 1.0 MB on PostgreSQL 16. A claim loop can run three
variants, one for each claim kind. `with_claim_lean(false)` keeps a
connection from building them.

## Known limits

A connection can plan the lean statement while `harvest_task_queue` is
analysed at 12 to 14 rows. That plan reads the whole table for the
candidate, until the next `ANALYZE` replaces it. The window statement has
the same limit below 12 rows. Each plan takes the same row, and only the
cost of the claim differs. The plan pins of `lean_claim_db_tests.rs` cover
a deep backlog only.

## Evidence

- `docs/perf-artifacts/claim-window-split/claim_forms.sql` counts the claim
  statements of a run by form: lean, window and full.

- `formal/verus/src/claim_window.rs` proves the model of the lean step and
  the bound of the skip. See
  [`testing/formal-methods.md`](../testing/formal-methods.md).
- `lean_claim_db_tests.rs` compares both orders over random queue states,
  for each of the twelve statement variants. It also commits a peer claim
  between the snapshot and the row lock of a lean statement, and it counts
  the statements that a claim loop sends. It counts the lean claims that
  each pause gate decided. On a deep backlog of one queue, it pins the plan
  of each variant: a `Tid Scan` for the candidate and a bounded index scan
  for each queue head.

## See also

- [`claim-batch.md`](claim-batch.md): one claim statement takes up to 2, 4
  or 8 rows.
- [`claim-slots.md`](claim-slots.md): a fleet-wide cap on the claims that
  run at once on one shard.
