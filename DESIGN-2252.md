# Design: issue #2252, owned workflow-task claims and the owner-mode switch

Stage 1a of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) fixes the
protocol. Issue #2251 added the owner rows, the lease statements and the
commit fence. This issue turns owner mode on: a per-database switch, the
owned claim, a database guard on the legacy claim and the acquisition loop.

**One migration. No new `WorkflowEvent` variant. No replay impact.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- One claim statement takes either kind of task. A worker runs one leader
  loop and `max_concurrent_claims - 1` followers. Every loop calls
  `queue::claim_task_with_fairness`. The dispatch channel calls the by-id
  claim. The batched claim has no production caller.
- Only two statements move a task row to `RUNNING`: the `claimed` CTE of
  the claim and the attempt of the batched claim.
- The claim reads a bounded window of 32 rows per queue head. Each head
  scan applies the row gates first, so a skipped row takes no slot.
- The unfenced claim has six binds. `claim_seek_tests.rs` and
  `deep_backlog_support.rs` depend on that text. The DR fence is a splice,
  not a copy. `fenced_claim_query_preserves_every_unfenced_gate` pins it.
- No row stores a decision partition. A task row holds
  `workflow_exec_id`, not `workflow_id`. Postgres has no `seahash`.
- Each workflow run has one workflow-task row. Park and wake reuse it. Nine
  call sites insert a workflow-task row.
- The append-only guard of issue #1817 reads a transaction setting
  (`harvest.sanctioned_event_rewrite`). That is a working precedent for a
  claim signal.
- `OwnedPartitions` has `set_owner_mode` and `adopt`, but no production
  caller. `pin_for` already fails closed with owner mode on.
- The e2e throughput bench runs one worker. Each run makes four decisions,
  and the first decision has no resident state. So the hit rate over all
  decisions cannot exceed 75%.

### 0.2 Brainstorm: how can the claim find owned work?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A `decision_partition` column on `harvest_task_queue`, stamped at insert. | **Adopted.** The claim tests it with no join. Park and wake keep the row, so one stamp lasts for the run. |
| B2 | A column on the execution row, joined in the claim. | Rejected. Each skipped head row then costs one more index probe. |
| B3 | Compute `seahash` in SQL. | Rejected. Postgres has no 64-bit unsigned arithmetic. A port could drift from the Rust hash. |
| B4 | An owned claim as a splice on each claim text, as the DR fence is. | **Adopted.** The base text stays byte-identical. One splice covers the plain, kind, fair and by-id forms. |
| B5 | Lock every owned owner row in the claim. | Rejected. Up to 512 row locks per claim. |
| B6 | A guard trigger on the claim, installed by the enable step. | **Adopted.** ADR 0009 decision 11. A deployment with owner mode off pays nothing. |
| B7 | The claim signal is a transaction setting that holds the holder token. | **Adopted.** An old binary never sets it. The guard compares it with the owner row. |
| B8 | The guard raises an error on a legacy claim. | Rejected. The `UPDATE` of a new binary with a stale switch view would fail. A skipped row (`RETURN NULL`) selects nothing, as the ADR asks. |
| B9 | A row with no partition stays claimable by any owner. The worker stamps it and re-pends it. | **Adopted.** An old enqueuer cannot strand a task. The decision fence still protects history. |
| B10 | Skip the sticky pin for an owned decision. | **Adopted.** The owner is the affinity. The row stays narrow. |
| B11 | Balance by `⌈512 / live owners⌉` from `harvest_workers`. | **Adopted.** ADR 0009 decision 9. A worker advertises the capability in its labels. |
| B12 | A named successor on handover. | Rejected by ADR 0009 decision 9. |

### 0.3 Reverse brainstorm: how can this change cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | An old binary claims a workflow task on an owner-mode shard. | The guard skips a claim with no signal. A test runs the legacy claim against the guard. |
| H2 | An owner claims with the switch off, and two claim styles run at once. | The owned claim reads the switch row in its statement. With the switch off, it selects no workflow row. |
| H3 | A stale owner claims after a takeover. | The guard takes the owner row `FOR KEY SHARE` and matches the holder. The commit fence stays the backstop. |
| H4 | The guard waits on the owner row while it holds the task row, and deadlocks. | The guard uses `SKIP LOCKED`, so it never waits. A skip skips the task row too. |
| H5 | A task with no partition is never claimed. | Any owner may claim it. The worker stamps it and re-pends it at once. |
| H6 | A rebalance strands a partition. | A release keeps holder and epoch and ends the lease. Peers acquire on their next scan. |
| H7 | A worker keeps deciding past its local deadline. | `pin_for` and the claim list skip a tenure past its deadline. |
| H8 | The acquisition loop takes every partition and never lets go. | Each scan recomputes the target. A worker releases down to the target. |
| H9 | The claim gets slower. | The owned claim drops the sticky gate for workflow rows. Benchmarks compare 1, 2 and 4 loops. |
| H10 | The extra cold decisions stay, because one worker still races itself. | Measure the cause first. Fix it in the owned path. |
| H11 | Colocated shards map to two tenure keys. | One canonical shard keys each database. |
| H12 | The enable step races a live legacy decision. | The enable step re-pends only the claims of dead workers. A live worker finishes its decision and parks the task. |
| H13 | A failed enable leaves the guard on and the switch off, so no claim takes a workflow task. | Guard, re-pend and switch are one transaction. |
| H14 | A peer that never acquires keeps the balance flapping. | Peers are owner loops that wrote a candidate row in the last 3 s. A worker that cannot own is no candidate. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue has five done-when items: a
  zero-extra-cold test, a hit rate above 95%, no throughput regression, a
  legacy-claim test and a docs update.
- **Red (gut).** A trigger on the hottest table feels risky. It exists
  only while owner mode is on, and its `WHEN` clause skips every row that
  is not a workflow claim.
- **Black (risks).** Section 0.3. The 75% ceiling makes the hit-rate item
  unreachable as written. The design measures continuation decisions too.
- **Yellow (value).** One owner per execution. Resident hits survive the
  sticky window. The sticky predicate leaves the owned claim.
- **Green (ideas).** A pin head for owned partitions. Deferred: the
  existing heads already serve owned rows, and stage 1b leaves the claim.
- **Blue (process).** Red tests first: the splice, the guard, the
  balance and the cold-decision count. Then the migration, the claim and
  the loop until green. Then docs, benchmarks and a review from several
  angles.

### 0.5 Measured before the plan was final

- One worker with two claim loops made 800 decisions for 200 runs in a
  debug build, with or without owner mode. The extra cold decisions of the
  R&D record show only under release timing. The e2e bench measures them.
- The release e2e bench shows them: 81 extra cold decisions at 2 loops
  without owner mode and 52 with it. A trace of each task-row write found
  one shape. The final decision of a run claims its task, then releases it
  with no new event, and a second claim completes the run.
- `pgrowlocks` named the cause. The peer claim loop held the row lock. Its
  head scan locks a row `FOR UPDATE SKIP LOCKED` just after the other loop
  claimed it. The recheck drops the row, but the lock stays until that
  claim transaction ends. The terminal guard `claim_still_held_for_update`
  read the lock as a lost claim. The decision rolled back, released the
  task, and the retry replayed cold. A park waits for the lock, so only the
  terminal write lost. Owner mode does not change this race.
- A wake re-arms a sticky pin. So an activity that outlives the window
  does not lose its pin. A timer does: no wake re-arms it. With 200 ms
  windows and one-second timers on two workers, sticky routing resumed 40
  of 72 continuation decisions. Owner mode resumed 72 of 72.

## 1. Change

1. Migration `20261021151820_harvest_decision_owner_claim`: the
   `decision_partition` column, the switch table and the guard function.
2. `decision_owner.rs`: `OwnedClaim`, `plan_balance`, local deadlines in
   `OwnedPartitions`, `enable_owner_mode`, `disable_owner_mode`,
   `read_owner_mode` and `OwnerLoop`.
3. `queue.rs`: the owned splice on every claim text, `claim_task_with_owner`,
   `claim_task_by_id_with_owner`, `requeue_unowned_workflow_task`, the
   partition stamp in `EnqueueParams` and no pin on an owned park.
4. `worker.rs`: one owner loop per database, the owned claim in each claim
   loop and the dispatch path, one key per colocated group, and the release
   after the drain.
5. Each writer of a workflow-task row stamps the partition. A dead-letter
   redrive does not know the workflow id, so its row stays unstamped.
6. The capability label in each registration and heartbeat.
7. Docs: the ADR, `sticky-routing.md`, the `CLAUDE.md` invariant, the
   upgrade guide, the telemetry table and a changelog fragment.
8. The e2e bench: `HARVEST_BENCH_OWNER_MODE=1` and a decision note.
9. `claim_still_held_for_update`: after a skip, one blocking retry in a
   savepoint, bounded by `CLAIM_GUARD_LOCK_WAIT` (100 ms). The bound is far
   below `deadlock_timeout`, so the guard still ends an ABBA cycle first.
10. The legacy schemas of `integration_e2e` and the plugin timeline suite
    gain the `decision_partition` column.

## 2. Review findings

Four review agents checked locking, worker liveness, the claim SQL and the
tests and docs. The fixes:

- A failed enable could strand every workflow task. Enable is now one
  transaction. It re-pends only the claims of dead workers.
- Disable used `DROP TRIGGER` and its `ACCESS EXCLUSIVE` lock. It now uses
  `DISABLE TRIGGER`.
- The guard could refuse the head row that the SQL gate passed, so the
  claim returned empty. Each candidate scan now checks the holder too.
- The partition test was an array, so its cost grew with the partition
  count. It is now one bit of a 64-byte mask.
- Peers came from heartbeat labels, so a worker that never acquires made
  the balance flap. Peers now come from a candidate table that live
  owner loops write each second.
- A worker with no workflow slot or type owned partitions. It is now a
  participant: owned claims, no partitions.
- The single-pool path skipped the split-group check. It runs it now.
- Shutdown could race a scan. The scan adopts under the loop lock, and the
  release stops the loop for good.
- A release now waits one scan, so decisions in flight can commit.
- The foreign re-pend locks rows in id order, covers a restarted process
  with the same id, and retries on failure.
- Owner mode alone no longer turns the owned claim on. Only the loop does,
  with the switch. The #2251 fail-closed test keeps its meaning.
- One pass evicts many partitions from the cache.
- Docs: the claim exception in the lock order, failover bounds, limits,
  grants and hot tables.

## 3. Limits

- An owner claims every workflow task of its partitions. So owner mode
  needs one queue set, one build and one workflow type set on the
  database. The enable step checks this once. A worker that starts later with other queues is not
  stopped.
- A legacy claim whose best row is a workflow task gets nothing. So an old
  binary can miss an activity behind it. The enable step refuses while an
  old binary is live.
- An unstamped row taken by a non-owner counts on
  `harvest.decision_owner.fenced`, as #2251 counts every fail-closed
  decision.
- The owned claim reads past the workflow rows of other owners at a queue
  head. Each owner then reads about N times as many head rows.

## 4. Out of scope

- Foreign-writer routing and cache invalidation: #2253.
- The crash bound and a shorter TTL: #2254.
- A claim off the contended path: stage 1b, #2255.
