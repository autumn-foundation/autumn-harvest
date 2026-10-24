# Design: issue #2256, group commit across executions in one decision partition

A later stage of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md)
fixes the owner protocol. Issue #2255 moved workflow tasks to the owner
decider. This issue lets an owner commit the decisions of several executions
of one partition in one Postgres transaction.

**No migration. No new `WorkflowEvent` variant. No replay impact.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- After stage 1b, the decision persist is the floor. Issue #2248 measured
  4.14 ms per persist at one claim loop. Most of it is round trips, not
  slow statements.
- Each decision is its own transaction: `BEGIN`, the start fence, the
  writes, the staged wakes and `COMMIT`. So each decision pays two round
  trips and one fence statement that do no work for the decision itself.
- With `synchronous_commit = on`, each `COMMIT` also waits for a WAL flush.
  One transaction for N decisions waits for one flush.
- The start fence takes the owner row `FOR KEY SHARE` and holds it until
  the transaction ends. An acquisition takes the row `FOR UPDATE`, so it
  waits for every fenced transaction. A renewal takes `FOR NO KEY UPDATE`,
  so it does not wait.
- Inside `decision_owner::fenced_scope`, a second fence of the same
  connection and tenure sends no query.
- `diesel_async` runs a nested `transaction` call as a `SAVEPOINT`. An
  error rolls back to the savepoint, and the outer transaction stays usable.
- Postgres aborts only the current subtransaction on an ordinary error: a
  unique violation, a lock timeout, a statement timeout or a deadlock
  victim. `ROLLBACK TO SAVEPOINT` also releases the locks of that
  subtransaction.
- The post-commit wake sender reads `txid_status` of the write. So no wake
  goes out before the outer `COMMIT`. A note staged in a savepoint that
  rolls back still goes out. That costs one extra wake only.
- The task row serializes the decisions of one execution. So one execution
  has at most one live decision. A stale owner can still hold one in flight.
- The worker persist spans about 25 functions in `worker.rs`. Each opens
  its own transaction and does post-commit work after it returns.
- The DST owner simulator models one history. It has no batch.
- `formal/verus` proves mirrored kernels of `decision_owner.rs`.

### 0.2 Brainstorm: how can several decisions share one commit?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A lane per decision partition. A decision joins the lane. A leader commits the queued decisions as one batch. | **Adopted.** It matches "one partition per batch". |
| B2 | A batch spans several partitions of one owner. | Rejected. ADR 0009 says a commit takes one owner row. A second row adds a lock-order edge. |
| B3 | A savepoint per decision, with the nested `transaction` of `diesel_async`. | **Adopted.** A failing decision rolls back alone. |
| B4 | One fence at the start of the batch, then `fenced_scope` for the rest. | **Adopted.** The owner row stays locked until the batch commits. |
| B5 | A linger timer that waits for more decisions. | Rejected. It adds latency at low load. |
| B6 | A width per lane. A decision starts at once while fewer than `width` transactions of the lane run. Else it queues. | **Adopted.** A batch forms only when decisions wait. So group commit engages only under queue depth. |
| B7 | The next free slot takes every queued decision, up to `max_batch`. | **Adopted.** The wait that a decision has today becomes a shared commit. |
| B8 | A batch of one runs with no savepoint. | **Adopted.** At low load the transaction is the same as today. |
| B9 | A spawned leader task. | Rejected. The waiter that gets the slot leads. No task outlives its caller. |
| B10 | A decision reports its result only after the batch commits. | **Adopted.** An acknowledgement follows a durable write. |
| B11 | A batch takes at most one decision of each execution. | **Adopted.** A second one waits for the next batch, so each decision reads a fresh history. |
| B12 | A batch takes only decisions of the leader's tenure. | **Adopted.** A decision of another epoch waits for its own batch, and its fence decides. |
| B13 | A fence error inside a stage aborts the batch. | **Adopted.** The tenure is in doubt, so no decision of it may commit. |
| B14 | A per-decision `lock_timeout`. | Rejected for now. `SET LOCAL` survives `RELEASE SAVEPOINT`. `max_batch` bounds the coupling instead. |
| B15 | Wire the lane into all persist paths of `worker.rs` now. | Deferred to #2318. Each path must split its post-commit work first. |
| B16 | Pure kernels for the plan and the outcome, shared by the engine, the simulator and Verus. | **Adopted.** The simulator then checks the engine's own rules. |

### 0.3 Reverse brainstorm: how can group commit cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | A failing decision aborts the whole batch. | A savepoint per decision. A test fails one decision with a real unique violation. |
| H2 | A failing decision leaves a partial write. | `ROLLBACK TO SAVEPOINT` removes it. The test reads the history of the failing execution. |
| H3 | A decision reports success, then the batch `COMMIT` fails. | Results go out after the commit only. A test fails the `COMMIT` with a deferred constraint. |
| H4 | A takeover commits mid-batch, and the stale batch commits after it. | The start fence holds the owner row `FOR KEY SHARE` to the commit. A test shows that the acquisition lock waits. |
| H5 | A stale batch commits after a takeover. | The start fence refuses it. Every decision of the batch gets `OwnershipLost`. |
| H6 | Two decisions of one execution share a batch, and the second reads a stale history. | The plan takes one decision per execution. A property test and a simulator mutant check it. |
| H7 | Low load gets slower. | A decision starts at once while a slot is free. A batch of one has no savepoint. The bench compares p50 at one writer. |
| H8 | A long decision stalls the batch. | `max_batch` bounds the batch. A slow decision would hold a connection today too. |
| H9 | A batch holds the locks of early decisions longer. A deadlock follows. | Postgres picks a victim. When the victim is a batch decision, its savepoint rolls back alone. |
| H10 | A queued decision runs after its caller left. | A dropped caller removes its entry while it waits. A dropped caller does not cancel a started batch. |
| H11 | A dropped leader strands its followers. | The batch runs on a task of its own, so the leader's caller can leave. |
| H12 | A follower waits forever. | Each follower also competes for a slot. When its entry is still queued at a free slot, it leads. |
| H13 | Wakes of a rolled-back decision go out. | The note goes out. The woken reader finds no new event. It costs one wake. |
| H14 | A batch-wide error loses its cause. | Each follower gets an error that names the cause. A fence error keeps its variant, so the owner unloads. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue has three done-when items: a
  design note, a criterion bench, and a simulator batch with one failing
  decision.
- **Red (gut).** Coupled failures feel risky. A savepoint per decision and
  results after the commit keep each decision as it is today.
- **Black (risks).** Section 0.3. The main risks are an early
  acknowledgement, a stale batch and longer lock holds.
- **Yellow (value).** Under depth, N decisions share one `BEGIN`, one fence
  and one `COMMIT`. With a real WAL flush they share one flush.
- **Green (ideas).** A batch could later carry a group fence for several
  partitions, after a lock-order proof. A batch could also prefetch the
  history rows of its executions.
- **Blue (process).** Red tests first: the Postgres lane tests, the
  simulator and its mutants, the planner properties and the Verus proof.
  Then the lane, the simulator and the proof until green. Then the bench,
  the docs and a review from several angles.

## 1. Design note

### 1.1 The lane

`group_commit::GroupCommit` keeps one lane per shard and decision
partition. A lane has `width` slots and a queue.

1. A decision enters the queue with its tenure, its execution and its stage.
   The stage is the body of the decision transaction.
2. It then waits for a slot or for its result.
3. The waiter that gets a slot plans a batch with `plan_batch`. The batch
   holds its own entry first, then the oldest queued entries of the same
   tenure, at most one per execution. It takes at most `share`: the queue
   over `width`, rounded up, and at most `max_batch`. So the other slots
   still find work.
4. When the plan finds that another leader took its entry, it frees the
   slot and waits for its result.
5. The leader starts the batch on a task of its own. The task runs the
   batch on one pooled connection, then sends each result. So a leader
   whose caller leaves does not cancel the batch.

While a slot is free, a decision leads a batch of one at once. So a batch of
two or more forms only when all `width` slots are busy and two or more
decisions wait. That is the queue depth. The defaults are `width = 4` and
`max_batch = 32`.

### 1.2 One transaction, one fence

```text
BEGIN
  start fence: generation row FOR SHARE, owner row FOR KEY SHARE
  SAVEPOINT d1;  stage 1;  RELEASE SAVEPOINT d1
  SAVEPOINT d2;  stage 2 fails;  ROLLBACK TO SAVEPOINT d2
  SAVEPOINT d3;  stage 3;  RELEASE SAVEPOINT d3
COMMIT
send results: d1 ok, d2 its error, d3 ok
```

- The leader pins the tenure in a decision scope and calls
  `assert_decision_fence` first. The order of ADR 0009 decision 6 holds.
- The rest of the batch runs in `fenced_scope`. A fence inside a stage then
  sends no query.
- The owner row stays locked `FOR KEY SHARE` until `COMMIT`. An acquisition
  needs `FOR UPDATE`, so it waits for the batch, as it waits for one
  decision today. No takeover commits mid-batch.
- A renewal needs `FOR NO KEY UPDATE`, so it does not wait.
- A stale tenure fails the start fence. No stage runs, and each decision
  gets `OwnershipLost`.
- A batch takes one owner row. It never takes a second one.

### 1.3 How a savepoint isolates a failing decision

- Each stage of a batch of two or more runs inside a nested `transaction`,
  so `diesel_async` sends `SAVEPOINT`.
- A stage error sends `ROLLBACK TO SAVEPOINT`. Postgres then discards each
  write of that stage and frees its locks. The writes of the other stages
  stay.
- The failing decision gets its own error after the commit. It wrote
  nothing, so its caller re-pends its task as after a failed solo persist.
- A fence error from a stage aborts the whole batch (B13).
- An error that breaks the connection fails the outer transaction too.
  Then no decision commits.
- A failed `SAVEPOINT`, `RELEASE` or `ROLLBACK TO` statement can leave the
  transaction aborted. Postgres then answers `COMMIT` with a silent
  rollback. So after a failed stage, the batch sends `SELECT 1` first. It
  fails on an aborted transaction, and then every decision fails.

### 1.4 Results follow the commit

- A stage result waits until the batch commits.
- After `COMMIT`, each passed stage gets `Ok`. Each failed stage gets its
  own error.
- When the start fence, a savepoint statement or `COMMIT` fails, every
  decision gets an error. A fence error keeps its variant.
- So `acknowledges(stage_passed, batch_committed)` is
  `stage_passed && batch_committed`. Verus proves that one decision's
  result depends only on its own stage and the batch commit.

### 1.5 What a stage must do

- It must not send a result or start post-commit work. Its caller does that
  after `commit` returns `Ok`.
- It runs on the batch task. It sees no task-local of its caller, and it
  must not pin a tenure.
- It reads its own execution row first, as a decision does today.
- It must not swallow a SQL error. Postgres answers `COMMIT` on an aborted
  transaction with a silent rollback. A batch of two or more probes for
  that after a failed stage. A batch of one sends no probe, so it costs the
  same as a lone decision today. A best-effort statement runs in its own
  nested `transaction`.

### 1.6 Failure table

| Failure | Batch | Failing decision | Other decisions |
|---|---|---|---|
| A stage error | commits | its error, no write | `Ok` |
| A fence error in a stage | rolls back | its error | the same fence error |
| Start fence fails | rolls back | — | `OwnershipLost` or `ShardFenced` |
| `COMMIT` fails | rolls back | — | an error that names the cause |
| The connection drops at `COMMIT` | unknown | — | an error, as for a lone decision |
| A savepoint statement fails | rolls back | its error | an error that names the cause |
| Pool checkout fails | no transaction | — | an error that names the cause |
| Deadlock victim in a stage | commits | `40P01`, no write | `Ok` |
| The leader's caller drops | runs on | — | its own result |
| A stage panics | rolls back | — | an error: the outcome is unknown |

### 1.7 Scope

This issue builds the lane, proves it and measures it. The worker persist
paths do not use it yet. Each path must first split its post-commit work
from its transaction body. Issue #2318 tracks that wiring and the lane
metrics.

Each lane has its own slots, and all lanes share one pool. So the wiring
must size `width` times the owned partitions against the pool. A caller
must not hold a connection of the same pool while it calls `commit`.

## 2. Change

1. `src/group_commit.rs`: the pure kernels `plan_batch`, `accepts` and
   `acknowledges`, the config, and with `db` the lane `GroupCommit`.
2. `src/dst/group.rs`: a seeded simulator of the lane, with nemeses,
   invariants and mutants.
3. `formal/verus/src/group_commit.rs`: the mirrored kernels and the
   theorems.
4. `benches/group_commit_bench.rs`: throughput at depth and p50 at low load.
5. Docs: ADR 0009 decision 14, `simulation.md`, `formal-methods.md`,
   `architecture.md`, a performance page and a changelog fragment.

## 3. Test plan

| Phase | Test | Red | Green |
|---|---|---|---|
| Red | `group_commit_db_tests::a_failing_decision_rolls_back_alone` | Fail: no lane. | Pass. |
| Red | `group_commit_db_tests::no_result_goes_out_before_the_batch_commits` | Fail. | Pass. |
| Red | `group_commit_db_tests::a_stale_batch_commits_nothing` | Fail. | Pass. |
| Red | `group_commit_db_tests::the_fence_holds_the_owner_row_for_the_whole_batch` | Fail. | Pass. |
| Red | `group_commit_db_tests::a_lone_decision_runs_with_no_savepoint` | Fail. | Pass. |
| Red | `group_commit_db_tests::one_execution_never_shares_a_batch` | Fail. | Pass. |
| Red | `group_commit_db_tests::a_decision_whose_caller_leaves_before_its_batch_never_runs` | Fail. | Pass. |
| Review | `group_commit_db_tests::a_fence_error_from_a_stage_rolls_the_batch_back` | — | Pass. |
| Review | `group_commit_db_tests::a_batch_survives_its_leader_caller_leaving` | — | Pass. |
| Review | `group_commit_db_tests::a_failed_pool_checkout_fails_the_batch` | — | Pass. |
| Review | `dst::group::the_design_passes_at_every_width` | — | Pass. |
| Red | `dst::group::*`: the design passes each seed, each mutant fails on its invariant | Fail: no simulator. | Pass. |
| Red | `property::group_commit_props` | Fail: no planner. | Pass. |
| Red | Verus `group_commit.rs` and the mirror guard | Fail. | Pass. |

## 4. Red and green record

- Red, on a skeleton that compiled but did nothing: all 7 Postgres tests,
  all 6 property tests and 10 of the 15 simulator tests failed. The 5 that
  passed check the config parser and the defect table, not the lane.
- The Verus guard then failed: it bans every name that holds `admit`. The
  kernel `admits` became `accepts`.
- Green: the lane, the simulator and the proof passed every test. A
  sweep of 20,000 simulator seeds passed. Verus reports 51 verified, 0
  errors.
- A weaker bound and a negated result rule each fail Verus, so each claim
  has teeth.

## 5. Review findings

Four review agents checked the lane concurrency, the transaction semantics
and the fence, the simulator and the proofs, and the docs. The fixes:

- The leader ran the batch on its caller's future. A caller that left
  failed every follower, even after `COMMIT` reached the server. The batch
  also inherited the caller's task-locals. A batch now runs on a task of
  its own, with an owned slot permit.
- A stage that swallowed its own error could leave the transaction
  aborted. Postgres answers `COMMIT` on it with a silent rollback, so the
  other decisions got `Ok` with nothing durable. After a failed stage, the
  batch now probes the transaction first.
- The simulator ignored two open batches of one lane. At width 2, both
  could commit a decision of one execution. A stage now waits for an
  uncommitted write of another batch, and a wait cycle fails a deadlock
  victim. The design passes at widths 1, 2 and 4.
- The `shared-execution` mutant failed only on a check of the plan rule.
  `NoConflictInsideABatch` now shows its harm.
- The done-when simulator test passed on any commit line. It now needs a
  committed batch with an injected failure.
- The Verus result theorem took the commit as a free input. It now models
  a fence error of any stage, which ends the batch.
- New Postgres tests: a fence error from a stage, a fresh decision behind
  stale ones, a leader whose caller leaves, and a failed checkout.
- Docs: the batching condition, the failure table, the proof claims,
  issue #2318 for the wiring, and several long or passive sentences.

Accepted as is:

- Each lane has its own slots, and all lanes share one pool. The wiring of
  issue #2318 must size them together.
- The simulator always leads from the head of a lane. The property tests
  cover any leader.
- The drain bound of the simulator is empirical. Every sweep converges.

## 6. Measurements

[`performance-group-commit.md`](docs/performance-group-commit.md) has the
tables. In short, on a 4-CPU host with Postgres on the same host and
`synchronous_commit = on`:

| Done-when item | Result |
|---|---|
| A design note on savepoints and the fence | Pass. Section 1 and ADR 0009 decision 14. |
| A throughput gain at depth | Pass. 1.16x at 16 writers and 1.33x at 64, from paired bursts. |
| No p50 regression at low load | Pass. The group p50 is 0.998 of the solo p50 at one writer. |
| The simulator covers a batch with one failing decision | Pass. `a_batch_with_one_failing_decision_commits_the_others`. |

The first bench run found a defect. One leader took the whole queue, so
the other slots found no work, and grouping was slower than solo. A leader
now takes `share`: the queue over `width`, rounded up. Verus proves its
bounds.

Criterion runs one mode after another, and drift on this host moved the
modes apart by about 10%, even where they run the same code. So the bench
also alternates bursts of the two modes, and the figures above come from
those pairs.
