## Lean claim tier (follows issue #2680)

A worker claim loop now sends a lean claim statement in front of the window
statement of issue #2680. The lean statement drops the plan nodes of
concurrency keys, rate limits, build pins and capability requirements. It
is exact for a seek window whose rows use none of those. When its window
holds a row and it locks none, it claims no row and changes no row, and it
sets a flag. The causes are a row that uses one of those features, a parked
row of a paused or expired run, and rows that peer claims hold. The claim
then sends the unchanged window statement in the same transaction. So a
claim takes the same row with and without the lean statement.

A lean statement that sets its flag takes no bucket lock. It keeps one row
lock in one case: a peer commits a change of a window row between its
snapshot and its row lock. The claim keeps the lock of that skipped row
until its transaction ends, as the window statement does.

- **Text.** `queue::lean_claim_query_for` derives each of the twelve lean
  texts from the final window text of its variant. Each edit replaces an
  exact text and asserts its count, so a changed gate panics at first use.
  No existing claim text changed. The batch claim, the by-id claim and the
  by-execution claim have no lean form.
- **Plan shape.** A second step of the derivation changes how the lean
  text reads the pauses. It reads each pause array through a scalar
  subquery, with no join. It builds each array with `ARRAY(SELECT ...)`,
  with no aggregate. The paused activity array also holds the saturated
  names of `$6`, so each scan has one activity test less. Each step is one
  entry of `LEAN_PLAN_REWRITES`, and it asserts the count of each text that
  it replaces.
- **Dispatch.** `queue::claim_task_with_lean` takes the `LeanSkips` of the
  calling claim loop. A lean flag makes the next claims of that loop send
  the window statement first. Each flag adds 4 claims to the skip, up to
  64. Each lean statement that takes a row takes 1 claim off the next skip.
  So a loop sends one wasted lean statement in 5 claims at most, and a loop
  of plain and keyed windows in turn moves to the window statement first.
  The skip counts claims and reads no clock. Each poll loop and each
  follower holds one skip per shard and per claim kind. A worker with queue
  weights also holds one skip per queue. A caller with no state, such as
  `queue::claim_task`, keeps the window statement first.
- **Switch.** `WorkerConfig::with_claim_lean(false)` turns the lean
  statement off. It is on by default. `GET /admin/config` reports
  `claim_lean`. `WorkerConfig` and `WorkerRuntimeConfig` gain the public
  field `claim_lean`. Add `claim_lean: true` to a struct literal. The
  operator guide is `docs/operations/claim-lean.md`.
- **Census.** The `claim_forms.sql` scripts of
  `docs/perf-artifacts/claim-window-split/` and
  `docs/perf-artifacts/claim-batch/` count the lean form. The lean text
  does not start with the `worker_info` CTE, so the old filter dropped it.
- **Memory.** Each pooled connection keeps one more cached plan for each
  lean variant that it runs, 0.7 MB to 1.0 MB on PostgreSQL 16.
- **No migration, no new event.** The change adds no table, no column and
  no `WorkflowEvent` variant.

Evidence: `formal/verus/src/claim_window.rs` proves the model of the lean
step and the skip bound. `lean_claim_db_tests.rs` compares the lean-first
claim with the window-first claim over random queue states for the twelve
variants, in owner mode and with an open and a closed fence. It commits a
peer claim between the snapshot and the row lock of a lean statement. It
counts the statements of a claim loop on a plain queue, on a queue with a
keyed row at its head, over two claim kinds, and of a worker with queue
weights. It counts the lean claims that a paused queue, a
paused activity and each `$6` name decided. It pins the plan of each lean
text on a deep backlog of one queue, with fresh and with idle statistics.
