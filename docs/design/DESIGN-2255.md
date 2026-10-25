# Design: issue #2255, take workflow tasks off the contended claim path

Stage 1b of epic #2247. [ADR 0009](../adr/0009-decision-owner.md) fixes the
owner protocol. Issues #2251 to #2254 built the lease, the owned claim, the
owner wakes and the failover tests. Issue #2248 measured the baseline. This
issue moves workflow tasks off the shared claim loop.

**One migration. No new `WorkflowEvent` variant. No replay impact.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- A worker runs one leader claim loop and `max_concurrent_claims - 1`
  followers. Each loop claims one task at a time with one claim statement.
  That statement scans the queue heads `FOR UPDATE SKIP LOCKED`.
- In owner mode the claim takes a workflow row of an owned partition only.
  It is still the same head-scan statement, on the same loops, as an
  activity claim. Assay 0014 counts 4 workflow claims and 3 activity claims
  per 3-activity workflow. The claim loop is busy 93% of the time at one
  loop.
- A workflow claim is one `READ COMMITTED` transaction: the plan settings,
  the claim statement, a queue-pause try-lock, the queue-pause recheck, the
  workflow-pause recheck and the commit.
- `claim_task_by_id_with_owner` claims one known row with every gate of the
  claim. The claim guard trigger checks the holder of the owner row for each
  `PENDING` to `RUNNING` update of a workflow row.
- Each workflow run has one workflow-task row. A wake while the row is
  `RUNNING` sets `wake_requested`. The park returns the flag, and the
  decision then wakes the row again. So the row serializes the decisions of
  one execution.
- The owner wake of #2253 is a post-commit `NOTIFY` on `harvest_owner_p{n}`
  with the execution id. `wake_workflow_task`, `enqueue` and `notify_owner`
  stage it. The listener only ends the idle wait of the leader claim loop.
- The sender drops every note to the partition that the writer pinned. A
  decision that wakes its own row (`had_wake_requested`, a late signal) or
  enqueues a successor in its own partition (continue-as-new keeps the
  `workflow_id`) wakes nobody. In stage 1a the claim loop polls, so that
  costs nothing.
- A due timer sends no wake. `reschedule_task` sets `scheduled_at` to the
  fire time, and the claim loop finds the row by polling.
- No index serves "pending workflow rows by due time". The partition
  column `decision_partition` has no index.
- `send_signal` commits the `harvest_signals` row and the wake in one
  transaction before it returns. The decision ingests and consumes the row
  in a fenced transaction. No owner memory acknowledges a signal.
- With DR on, owner mode adds two statements to each decision: a second
  generation check and the owner fence. Issue #2248 measured +17.8% (median
  of nine runs) against DR alone. The pass condition is under 10%.
- With DR on, each event append opens a savepoint and checks the generation
  row again, even inside the fenced scope. The signal and timer ingest
  transaction is not in a fenced scope, so it fences the owner twice.

### 0.2 Brainstorm: how can the owner run decisions without the claim loop?

| # | Idea | Verdict |
|---|------|---------|
| B1 | An owner decider task per owned database. It takes known rows by id and dispatches them. | **Adopted.** It runs beside the claim loops and joins their shutdown. |
| B2 | The decider learns of work from the owner `NOTIFY` of its partitions. | **Adopted.** The wake carries the execution id. |
| B3 | The decider scans the pending workflow rows of its partitions by due time. | **Adopted.** It catches a lost `NOTIFY`, a due timer and the backlog at an acquisition. |
| B4 | A partial index on `scheduled_at` of pending workflow rows. | **Adopted.** A scan reads its limit, not the backlog. |
| B5 | Take a row with a new lean statement. | Rejected. It must repeat each gate and the pause rechecks of #619 and #1640. A drift loses a pause. |
| B6 | Take a row with the by-id owned claim, keyed by execution. | **Adopted.** Every gate, the guard and the post-claim rechecks stay. One index probe finds the row. |
| B7 | Skip the take. Decide on a `PENDING` row and update it in the persist. | Rejected. It breaks the `wake_requested` protocol and the dead-worker re-pend. |
| B8 | Run each take in its own task, bounded by the workflow permits. | **Adopted.** A serial take loop would cap throughput as the claim loop does. |
| B9 | The claim loop claims activities only where the decider runs. | **Adopted.** A claim loop with no workflow partitions in its mask reads every workflow row at the head. |
| B10 | The decider also takes unstamped rows. The by-id claim allows them. | **Adopted.** The existing stamp and re-pend of #2252 then moves the row to its owner. |
| B11 | Keep the self-skip of the sender for plain wakes. | Rejected. A self-wake is now the only fast path for a re-pend or a successor in the same partition. Only an evict wake to the writer's own partition is dropped. |
| B12 | An in-process channel for self-wakes. | Rejected. More code. The `NOTIFY` already reaches the listener. |
| B13 | An in-memory inbox of signals. | Rejected. Restate#4566. The decider keeps execution ids only, as hints. A hint never acknowledges anything. |
| B14 | Fence the generation row and the owner row in one statement. | **Adopted.** A new function takes both rows in ADR order. |
| B15 | Skip the per-append generation check inside a fenced scope that locked the same generation row. | **Adopted.** The start fence holds the row until the commit. |
| B16 | Wrap the signal and timer ingest in the fenced scope. | **Adopted.** It then fences once. |
| B17 | A worker switch for stage 1b. | Rejected. Owner mode is already the operator switch. Stage 1a stays as the owned claim when the decider cannot run. |
| B18 | Record a take as a claim. | Rejected. A take does not scan the heads. It gets its own recorder method and the `op="take"` label, so the claim count and the take count stay apart. |

### 0.3 Reverse brainstorm: how can this change cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | A signal acknowledged before an epoch bump is lost, because its wake went to the old owner. | The decider scans each partition at each tenure change. A hint is never the only record. A test drives this case. |
| H2 | A paused run sits at the head of the scan and starves the rest. | The scan skips paused runs and paused queues. A take that the gates refuse backs off that execution. |
| H3 | Two takes of one row run at once. | The row is the lock. A local in-flight set skips a second take in the same worker. |
| H4 | A stale owner takes after a takeover. | The guard matches the holder. The mask holds live tenures only. The commit fence stays the backstop. |
| H5 | A timer fires late. | The scan reads the next due time and sleeps until then, at most the scan gap. |
| H6 | The decider takes during shutdown. | It stops on the shutdown token. The drain starts after it returns. |
| H7 | The decider starves on permits that the claim loop holds. | The claim loop takes no workflow permit where the decider runs. |
| H8 | A worker with no listener URL never hears a wake. | No decider runs there. The claim loops keep the owned claim. A configured listener that drops scans every poll interval until it listens again. |
| H9 | The new index slows the hot table. | It holds pending workflow rows only. One row per run. The upgrade guide gives the out-of-band build. |
| H10 | The fused fence changes the error of a fenced shard. | A generation mismatch still returns `ShardFenced`. A lock timeout on either row fails closed as `OwnershipLost`. |
| H11 | The per-append skip misses a colocated shard with its own generation row. | The skip compares the resolved shard and the pinned generation of both shards. |
| H12 | Self-wakes double the `NOTIFY` load. | Only a decision that re-pends its own row, or enqueues in its own partition, sends one. |
| H13 | A refused take loops forever. | The backoff set holds each refused execution for one poll interval. |
| H14 | The decider takes before the worker registers. | It waits on the same registration gate as the claim loop. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue has four done-when items: claims
  per workflow 7 to 3, 1.5x workflows/s at equal pool connections, a red
  signal test and a fence cost under 10%.
- **Red (gut).** A second path to dispatch feels like a second bug surface.
  The take reuses the by-id claim, and the dispatch is the same function.
- **Black (risks).** Section 0.3. The main risks are a lost wake, a scan that
  starves, and a 4-CPU host that is CPU-bound before the claim loop is.
- **Yellow (value).** The claim loop serves activities only. Decisions run
  as fast as the workflow permits and the database allow. One row per run
  still serializes each execution.
- **Green (ideas).** Group commit across executions of one partition is
  #2256. A lean take is possible later, after a measurement shows that the
  take dominates.
- **Blue (process).** Red tests first: the signal test, the claim count and
  the per-append fence count. Then the migration, the decider, the claim
  admission, the wake rule and the fused fence until green. Then the bench,
  the assay runs, the docs and a review from several angles.

## 1. Change

1. Migration `20261023015935_harvest_owner_decider`: the partial scan index
   and the fused fence function.
2. `decision_owner.rs`: `fence_with_generation`.
3. `queue.rs`: `claim_workflow_task_of_execution`, `owner_scan` and
   `OwnerScan`. A workflow re-pend wakes the owner.
4. `worker.rs`: the owner decider per owned database, its hints from the
   owner wake task, activity-only claims where it runs, and the fenced
   ingest.
5. `notify.rs`: a plain wake to the writer's own partition is kept.
6. `replication.rs` and `store.rs`: the fused fence and the per-append skip.
7. `telemetry.rs`: `MetricsRecorder::record_owner_take_duration`, recorded as
   `op="take"`. A new `DbOp` variant would break each exhaustive match.
8. Docs: ADR 0009, `sticky-routing.md`, the telemetry table, the upgrade
   guide, the baseline page, a performance page and a changelog fragment.

## 2. Test plan

| Phase | Test | Red | Green |
|---|---|---|---|
| Red | `a_signal_acknowledged_just_before_an_epoch_bump_is_never_lost` | Fail: the worker claims the workflow task. | Pass. |
| Red | `an_owner_takes_workflow_tasks_off_the_claim_path` | Fail: 160 workflow claims for 40 runs. | Pass: 3 activity claims and 4 takes per run. |
| Red | `a_dr_fenced_owner_decision_sends_one_fence_statement` | Fail: no fused statement, 3 generation reads. | Pass: 1 statement, 1 read. |
| Review | `a_signal_hinted_to_an_owner_that_loses_its_epoch_is_never_lost` | — | Pass. |
| Review | `a_due_timer_fires_through_the_owner_scan`, `the_owner_scan_reads_owned_and_unstamped_due_rows_only` | — | Pass. |
| Review | `a_fused_fence_reports_a_bumped_generation_as_shard_fenced`, `a_fused_fence_waits_for_a_held_generation_row`, `a_colocated_shard_with_its_own_generation_row_still_checks_it` | — | Pass. |
| Review | Unit tests: the by-execution splice, the decider state, the backoff, the hints, the gate record | — | Pass. |
| Both | The owner, claim, wake, failover, simulator and DR suites | Pass. | Pass. |

## 3. Red and green record

- Red, on the engine of #2248: the three red tests failed for the planned
  reasons. The signal reached the run, but through a workflow claim.
- Green: the first build of the decider passed all three, and every owner
  suite.

## 4. Review findings

Four review agents checked the decider loop, the fence and the SQL, the
tests and docs, and the performance. The fixes:

- With owner mode off, the decider loop slept for zero and spun. It now
  waits for a tenure change or one poll interval, and sends no query.
- A scan ran inside the loop, so the takes in flight stalled until it
  ended. Scans and takes now run as futures beside the loop.
- The scan index led with `scheduled_at`. Each owner read the rows of other
  owners, and a timer backlog. The index now leads with the partition, and
  the scan reads a few rows of each owned partition.
- The fused fence waited at most 250 ms for the generation row. A DR bump
  that queued behind a long transaction then fenced healthy owners. The
  generation row now waits with no bound, as the separate check did.
- The append skip read the registry again. A pin that appeared after the
  start fence then skipped a check. The scope now records the row that the
  start fence locked.
- A refused row was taken again each poll interval. The backoff now doubles
  per miss, up to 30 s. A wake during a take queues it again after the take.
- The scan waited while hints filled the queue. It now runs at its time,
  and the queue is bounded.
- An owner wake still woke the activity-only claim loops. It no longer does
  where the decider runs.
- A delayed re-pend woke the owner, whose take then missed. Only a re-pend
  that is due now wakes it.
- A take skipped the fairness charge of the owned claim. It charges now.
- The blocked decider now marks a refused workflow kind for the slot tuner.
- The starter alert, the claim panel, the runbook, the ADR, the telemetry
  table and the bench labels now name the take.

Accepted as is:

- A take is a full claim transaction. It costs the same round trips as the
  old workflow claim. The gain is that no claim loop waits for it.
- The scan orders by due time, not by priority. Hints come first in, first
  out. The Codex review raised it again, with the new-start handicap of
  #1824. Issue #2308 tracks the fix.
- The dispatch channel can still claim a workflow row by id in owner mode.
  It races the decider, and one of them takes the row.
- No worker switch turns the decider off. Owner mode off is the fallback.

After the PR opened, the Codex review found that the decider ignored queue
weights. A worker with queue weights now runs no decider and keeps the
owned claim of #2252. `a_worker_with_queue_weights_keeps_the_owned_claim`
proves it. The next review found the same gap for fairness keys. A worker
with fairness keys now keeps the owned claim too.
`a_worker_with_fairness_keys_keeps_the_owned_claim` proves it.

CI then failed `scale_out_from_4_to_8_workers_recovers_within_two_ttls`.
The decider scanned each poll interval, also with live wakes. The test
polls every 25 ms, so 8 deciders sent about 300 scans a second.
`pg_stat_statements` showed the scan at 3.6 times the server time of the
takes. On one host, the rate after the scale-out fell from 0.73 of the
baseline on `trunk-dev` to 0.62. While a listener receives the owner
wakes, the scan gap is now 1 s or one poll interval, whichever is longer.
With no live listener, the scan still polls (H8).

The gap did not move the scan count: the test worker has no notification
URL. With no wake, the scan is the only source of work. A scan of 4 rows
then a take for each costs more than one head claim per task. So a decider
now runs only on a database with an owner wake listener.
`a_worker_with_no_wake_listener_keeps_the_owned_claim` proves it.

## 5. Measurements

[`performance-owner-decider.md`](../performance-owner-decider.md) has the
tables. In short, on a 4-CPU host with Postgres on the same host:

| Done-when item | Result |
|---|---|
| 7 claims to 3 | Pass. 3.00 task claims and 4.00 takes per workflow, at 1, 2 and 4 claim loops. |
| 1.5x workflows/s | Not met. 1.27x at 1 loop, 1.19x at 2, 1.05x at 4. The CPU limits both modes at about 53 workflows/s. |
| Signal before an epoch bump | Pass. Two tests, with and without a live old owner. |
| Fence cost under 10% | Pass. −9.3% against DR alone (median of six runs), and +1.1% against the owner fence alone. |

The 1.5x item came from the claim-loop formula. `pg_stat_statements` shows
the same statements in both modes. Owner mode runs more decisions at once,
so on 4 shared cores each statement slows, and the CPU becomes the limit.
A host with the database apart, a cheaper take, eager activity dispatch
and group commit (#2256) are the next levers.
