# Design: issue #2251, the owner lease table and the commit fence

Stage 1a of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) fixes the
protocol. [The lease ADR](docs/adr/0010-fenced-owner-lease.md) fixes the
rules R1 to R7. This issue adds the table, the lease code and the fence in
the decision commit.

**One migration. No new `WorkflowEvent` variant. Owner mode stays off.**
Issue #2252 adds the switch, the owned claim and the acquisition loop. Until
then no production path holds a tenure, so no commit takes the owner row.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `replication::assert_fence` is the "top of a persist" check. Each event
  insert calls it before its row lock, in `insert_event_rows`. It takes the
  DR generation row `FOR SHARE`.
- The workflow-task persist locks the execution row first
  (`check_paused_and_park`). Its first append checks the generation row.
- A decision writes history from many transactions: the persist, the pause
  path, the non-determinism block, inline local activities, inline external
  signals and the failure paths. Every one of them inserts events through
  `insert_event_rows`.
- `fail_execution_on_error` fails the run on any error that it does not
  know. A fence error must pass through it.
- `WorkflowCache` keys entries by execution id. It does not know the
  partition.
- The simulator `dst::owner` drives an `OwnerStore` from one thread. A
  store must not block. The guard `stage_1a_owner_store_gets_a_simulator_run`
  trips when a migration adds `harvest_decision_owner`.
- Verus proofs mirror an engine function body. A guard compares the bodies.
- Postgres 16 is local. The DB tests run against it.

### 0.2 Brainstorm: what can stage 1a hold?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Seed 512 rows in the migration. | **Adopted.** No upsert can then change an epoch. |
| B2 | Create a row on first acquisition. | Rejected. It needs an upsert on the hot path. |
| B3 | A trigger that refuses an epoch decrease or a jump of more than 1. | **Adopted.** The database guards the proof property. |
| B4 | Compute the next epoch in Rust with a proved kernel. | **Adopted.** Verus proves the code that runs. |
| B5 | Call the fence in each decision transaction by hand. | Rejected. Too many sites. One missed site writes a stale event. |
| B6 | A task-local tenure. `assert_fence` also fences the owner row while it is set. | **Adopted.** Every event insert of a decision is fenced. Foreign writers run in other tasks. |
| B7 | The late fence in `insert_event_rows` waits for the owner row with no bound. | Rejected. It can wait while it holds a task row. |
| B8 | The late fence uses `NOWAIT`. A lock miss is a fence. | Rejected in review. A failed acquisition recheck holds `FOR UPDATE` on a live row for a moment, so `NOWAIT` fences a healthy owner. |
| B8a | One fence function with a 250 ms lock wait, for the start fence and the late fence. A miss is a fence. | **Adopted.** A short hold passes. A takeover fails closed. The wait cannot deadlock, because it ends in a refusal. |
| B9 | An explicit blocking fence at the start of the persist, pause and block transactions. | **Adopted.** ADR 0009 decision 6 moves the fence reads there. |
| B10 | Store the partition on the execution row now. | Deferred to #2252. The claim filter needs it. The fence computes it from `workflow_id`. |
| B11 | Tag each cache entry with its partition. | **Adopted.** The unload drops only that partition. |
| B12 | Re-pend the fenced claim at once. | **Adopted.** The claim-fenced requeue already exists. |
| B13 | Expire every lease in the DR bump. | **Adopted.** ADR 0009 decision 4. A locking `SELECT` and an `UPDATE` in the bump transaction. |
| B14 | One `renew_all` and one `release_all` statement per worker. | **Adopted.** A locking `SELECT ... ORDER BY partition` runs first. |
| B15 | A Postgres `OwnerStore` in an integration test that calls the engine code. | **Adopted.** It drives `owner::sweep_with` with all nemeses. |

### 0.3 Reverse brainstorm: how can this change cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | A stale owner commits after a takeover. | The fence matches holder and epoch under `FOR KEY SHARE`. The takeover takes `FOR UPDATE` and waits. |
| H2 | A fenced commit leaves part of its events. | The fence error aborts the whole transaction. A test counts the rows. |
| H3 | A renewal bumps the epoch, and the owner fences itself. | The renewal SQL changes `lease_until` only. A test and the simulator check it. |
| H4 | The same holder takes back a lapsed row at the old epoch. | Each acquisition adds 1. A test acquires twice with one holder. |
| H5 | A fence error fails the workflow. | `fail_execution_on_error` passes it through. The dispatcher re-pends the task. |
| H6 | The fenced worker retries at the stale epoch. | The worker drops the tenure, so the next decision has no tenure to retry with. |
| H7 | The fenced worker keeps resident state of the partition. | It drops every cache entry of the partition. `evict_partition_drops_only_that_partition` checks it. A cache write checks the tenure under the cache lock. |
| H8 | A foreign writer takes the owner row. | Only a decision task sets the task-local tenure. |
| H9 | The late fence waits while it holds a task row, and deadlocks with an acquisition. | The fence waits at most 250 ms, then fails closed. Each decision transaction fences first. |
| H10 | A multi-row statement locks rows in scan order and deadlocks. | Lock with `ORDER BY partition` first. A pin test checks the order. |
| H11 | The DR bump deadlocks with a renewal. | The bump takes the generation table first. A renewal takes no lock that the bump holds. |
| H12 | An epoch overflow wraps to a negative value. | `next_epoch` returns `None` at `i64::MAX`. Verus proves it. |
| H13 | The proof drifts from the code. | The Verus mirror guard compares the bodies. |
| H14 | A frozen commit holds the row for ever. | The fence sets `idle_in_transaction_session_timeout` to the TTL. |
| H15 | A legacy binary breaks on the new table. | No old path reads it. The table is new and empty of tenures. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue asks for four red tests, a
  proof, two gates and a simulator run on the real store.
- **Red (gut).** The task-local feels like hidden control flow. It is
  also the only way to fence every insert without a missed site.
- **Black (risks).** Section 0.3. The main risks are a late lock after a
  task row, a fence error that fails a run, and the cost of one more query
  per append in owner mode. Issue #2248 measures that cost.
- **Yellow (value).** A stale owner can no longer write. The epoch rule is
  proved, guarded by a trigger and tested on Postgres.
- **Green (ideas).** Skip the late fence query when the transaction already
  holds the row. That needs a transaction id per connection. Deferred.
- **Blue (process).** Red tests first. Then the migration and the module
  until green. Then the Verus proof, docs and gates. Then a review from
  several angles.

## 1. Change

1. Migration `20261020132402_harvest_decision_owner`: the table, 512 seeded
   rows and an epoch guard trigger.
2. `decision_owner.rs`: the partition, epoch, holder, TTL and tenure types;
   the proved kernels; `acquire`, `renew_all`, `release_all`, `fence`,
   `expire_all`; the task-local tenure and `OwnedPartitions`.
3. `HarvestError::OwnershipLost` and the `harvest.decision_owner.fenced`
   counter.
4. The worker: the fence at the start of every decision transaction, the
   late fence in `assert_fence`, the unload and the re-pend.
5. The DR bump expires every owner lease.
6. `formal/verus/src/owner_fence.rs` proves the epoch compare.
7. Docs: the ADR, the lock-order table, the schema table, the upgrade guide,
   the telemetry table and a changelog fragment.

## 2. Review findings

Four review agents checked lock safety, the worker paths, the tests and the
docs. The fixes:

- The `NOWAIT` late fence could fence a healthy owner. The fence is now one
  function with a 250 ms wait, and it reads the SQLSTATE, not the message.
- The idle bound of the TTL could end a decision that uploads a payload
  inside its transaction. An upload now widens the bound while it runs.
- Tenures and cache tags were keyed by partition only. A worker serves
  several databases, so they are keyed by shard and partition now.
- A missing tenure meant no fence. Owner mode on a shard now fails closed.
- The pin came after the wake ingest. It now comes right after the load.
- Many decision transactions took other locks first, and projection writes
  took no owner row. Each one now fences first.
- A cache write could race the unload. It checks the tenure under the
  cache lock.
- Inline cancel arms swallowed the fence error. They pass it through.
- The DR bump expired leases even when it failed. It rolls back now.
- The epoch guard could overflow, and allowed a new holder at the old
  epoch. Both are fixed.
- The oracle and the TLA+ model let a lapsed holder renew. The code does
  not, so both models now require a live lease. TLC still passes.
- A release that cleared the holder fenced the releaser's own commit in
  flight. A release now keeps the holder and the epoch, as a lapse does.
  The models match. No other owner can acquire before that commit ends.
- The tests gained the same-holder and released-tenure fences, a renewal
  under a held fence, a bounded-wait test, a no-fence mutant store and an
  owner-mode test. Polls replaced fixed sleeps.

A second review round found four more gaps. The fixes:

- A local activity handler saw the pin, so its own writes took the owner
  row. It now runs in a nested scope that pins nothing.
- The owner key came from the bits of the execution id. It now comes from
  the shard of the claim's pool, which names the database.
- The body-timeout quarantine wrote `WorkflowFailed` with no fence. It now
  pins the run's tenure and fences first.
- The bounded-wait test probed any lock on the table. It probes its row.
- A pin outside a decision scope now fails closed. A decision with no
  tenure in owner mode re-pends after one second, not at once.

The Codex review of the PR found two more gaps, and CI found one. The fixes:

- A body-timeout quarantine with no tenure returned a lost claim and left
  the task `RUNNING`. It now re-pends the task under its claim, so the
  next owner can take it.
- A failed run load failed the task before the pin. The load did not
  return the workflow id, so that write could not fence. With owner mode
  on, the task now re-pends. A missing run still fails its task, because
  it has no history to protect.
- A build without the `db` feature failed clippy on the db-only cache
  items. They take the same `cfg_attr` as the other db-only items.

A second Codex pass found one more gap. The fix:

- A takeover after the quarantine pin fenced its transaction. The error
  became a failed write, so the tenure stayed and the claim stayed
  `RUNNING`. The quarantine now unloads the partition, as the dispatcher
  does, and re-pends the task. Both share `drop_fenced_tenure`.

## 3. Out of scope

- The owner-mode switch, the owned claim, the acquisition loop and the
  balance policy: #2252. Each colocated shard must map to one key there.
- Foreign-writer routing and cache invalidation: #2253.
- A claim-time epoch check: the owned claim of #2252.
