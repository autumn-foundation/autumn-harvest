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

- When each row of the window is plain, it takes the row that the window
  statement takes. Each pin gate and deadline gate is the same text. Each
  pause gate gives the same result. See
  [How it reads the pauses](#how-it-reads-the-pauses).
- When the window is empty, it takes no row, as the window statement does.
- In every other case it takes no row and no lock, and it sets a flag. The
  claim then sends the unchanged window statement in the same transaction.

So both orders claim the same row. The lean statement changes the cost of a
claim only.

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
with no lean statement. A queue that holds keyed rows at its head would pay
that on each claim. So each claim loop keeps a skip:

- After a lean statement sets its flag, the next claim of the loop sends
  the window statement first.
- Each next flag doubles the skip: 1 claim, then 2, then 4, up to 64.
- A lean statement that takes a row ends the skip. An empty poll keeps it.

The skip counts claims. It reads no clock and no random number. Each claim
loop holds its own skip for each shard that it polls, so one loop never
changes the skip of another loop.

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
statement out of a problem. The `e2e_bench` binary reads
`HARVEST_BENCH_CLAIM_LEAN`: `0` or `off` runs the same binary with the lean
statement off.

## Evidence

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
