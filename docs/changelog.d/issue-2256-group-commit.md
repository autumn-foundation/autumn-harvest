## Engine — Group commit across executions in one decision partition (issue #2256)

`group_commit::GroupCommit` lets an owner commit the decisions of several
executions of one decision partition in one Postgres transaction.
[`DESIGN-2256.md`](../design/DESIGN-2256.md) section 1 is the design note.
ADR 0009 decision 14 records it.

- One lane per shard and partition, with `width` slots (default 4). A
  decision commits alone at once while a slot is free. A batch forms only
  when decisions wait, so group commit engages only under queue depth.
- A batch holds decisions of one tenure, one per execution, up to
  `max_batch` (default 32). It fences once and holds the owner row
  `FOR KEY SHARE` until it commits. An acquisition waits for the whole
  batch. A renewal does not.
- Each stage of a batch of two or more runs in a savepoint. A failing stage
  rolls back alone. A fence error in a stage rolls the batch back.
- No result goes out before the batch commits. A batch of one sends no
  savepoint.
- A batch runs on a task of its own, so a leader whose caller leaves does
  not cancel it. After a failed stage, the batch probes the transaction,
  so an aborted transaction never reports `Ok`.
- The worker persist paths do not use the lane yet. Issue #2318 tracks the
  wiring.

**No migration. No new `WorkflowEvent` variant. No replay impact.**

Tests:

- `group_commit_db_tests.rs` has ten tests. A failing decision rolls back
  alone. No result goes out before the commit. A stale batch commits
  nothing. The fence holds the owner row for the whole batch. A lone
  decision sends no savepoint. One execution never shares a batch. A
  caller that leaves before its batch never runs. A batch survives its
  leader's caller leaving. A fence error from a stage rolls the batch back.
  A failed checkout fails the batch.
- `dst::group`: a seeded simulator with three nemeses, seven invariants
  and six mutants. Each mutant fails on its own invariant. The design
  passes at widths 1, 2 and 4.
- `formal/verus/src/group_commit.rs`: the plan rules and the result rule.
- `group_commit_props.rs`: the planner against the Verus spec model.
- `benches/group_commit_bench.rs`: throughput at depth and p50 at one
  writer. Paired bursts give 1.16x at 16 writers and 1.33x at 64 writers.
  At one writer, the p50 is 0.998 of the lane with grouping off. See
  [`performance-group-commit.md`](../performance-group-commit.md).
