# Design: issue #2253, owner wakes and cache eviction for foreign writers

Stage 1a of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) fixes the
owner protocol. Issue #2251 added the lease and the commit fence. This issue
routes the writes of other processes to the owner of the partition.

**No migration. No new `WorkflowEvent` variant. No new append path.** The
owner wake is a post-commit `NOTIFY` on one channel per partition. A wake
that a lost `NOTIFY` drops costs latency only. The decision checks the
delta of each cache hit, so a lost eviction wake cannot cause a stale
decision.

---

## 0. Planning record

### 0.1 Facts found before the plan

- A foreign writer takes no owner row (ADR 0009 decision 6). The execution
  row lock serializes it with the decision.
- Every event append goes through `store.rs`. Each append stages a
  `Note::Events` for the post-commit sender of `notify.rs`. The note has
  the execution id. It does not have the `workflow_id`.
- The partition is `seahash(prefix + workflow_id) % 512`. Postgres cannot
  compute it. The execution row does not store it until #2252.
- Signals and due timers append no event. They re-pend the parked
  workflow task with `queue::wake_workflow_task` or a sibling.
- Pause and resume append `WorkflowExecutionPaused` and
  `WorkflowExecutionResumed`. Terminate appends `WorkflowCancelled`.
  Reset seals the source with `WorkflowResetTerminated` and forks a new
  execution id. Reactivate appends `WorkflowRedriven`.
- A cache hit takes the entry, loads the delta from `next_event_id`, and
  checks that the delta has no gap. A resident resume declines on any
  event that it does not expect. A decline replays over the snapshot.
- With owner mode on and no tenure, a decision fails closed (#2251). So a
  non-owner already cannot decide. It re-pends after one second.
- `OwnedPartitions` keys tenures by `OwnerKey` (shard and partition).

### 0.2 Brainstorm: how can the owner learn of a foreign write?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Each foreign writer calls a wake function by hand. | Rejected. About 60 sites. A missed site wakes no owner. |
| B2 | Stage an owner note next to each `Note::Events`, in `store.rs`. | **Adopted.** Every append is covered by default, as the trigger of #1817 covers every column. |
| B3 | Stage an owner note in the queue wake functions too. | **Adopted.** Signals and timers append no event. |
| B4 | Read `workflow_id` in the write transaction. | Rejected. One more statement on the write path, and its error aborts the write. |
| B5 | The sender reads `workflow_id` after the commit, in one statement per tick. | **Adopted.** The write path stays as it is. |
| B6 | One channel for all owners, with the partition in the payload. | Rejected. Every owner wakes for every write. |
| B7 | One channel per partition: `harvest_owner_p{n}`. | **Adopted.** The issue asks for a wake keyed by partition. |
| B8 | Evict on every foreign append. | Rejected. A parent with many children replays cold at each child. |
| B9 | Evict on a state change: a terminal, pause, resume or redrive event. | **Adopted.** The event type marks the state change. |
| B10 | A new execution-row version column for evictions. | Rejected. A migration on a hot table, and a trigger on each update. |
| B11 | Check the delta of each cache hit for a state-change event. Replay cold on one. | **Adopted.** The durable backstop for a lost wake. |
| B12 | Check that the owner key of a hit equals the key of the decision. | **Adopted.** A rebalance moves a run to another database. |
| B13 | Add owner channels to the queue listener connection. | Rejected. The eviction needs the cache, and the poll loop has four wait shapes. |
| B14 | A listener task per shard pool, which listens only on owned partitions. | **Adopted.** It connects only while the worker holds a tenure on that shard. |
| B15 | Send owner wakes always. | Rejected. It doubles the `NOTIFY` load of every deployment. |
| B16 | A process flag. Owner mode turns it on. | **Adopted** until #2252 reads the per-database switch row. |

### 0.3 Reverse brainstorm: how can this change cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | The owner wake fails the write. | The note uses the post-commit sender. A sender error counts a lost wake and never reaches the write. |
| H2 | The sender lookup runs inside the write transaction. | It runs on the sender's own connection, after `txid_status` reads the commit. |
| H3 | A lost eviction wake lets the owner decide on a stale snapshot. | The hit checks the delta for a state-change event and replays cold (B11). |
| H4 | A late eviction drops a fresh entry. | An eviction is always safe. It costs one cold decision. |
| H5 | An eviction drops a parked future under the cache lock. | The listener takes the entry and drops it outside the lock. |
| H6 | The owner wakes itself on each of its own appends. | The note records the pinned partition. The sender skips a plain wake to that partition. |
| H7 | The skip drops a wake that another owner needs. | The skip compares the target partition with the pinned one. A write into another partition still wakes it. |
| H8 | A non-owner decides. | #2251 fails it closed. The tests assert that the non-owner prepares no decision. |
| H9 | The listener holds a connection in every worker. | It connects only while the worker holds a tenure on the shard. |
| H10 | A worker that listens on the wrong database. | The listener uses the same URL rule as the queue listener of its shard. No URL means no listener. |
| H11 | The sender query is slow on many notes. | One `= ANY` lookup on the primary key per tick, only for owner notes. |
| H12 | A deleted execution leaves an unresolved note. | The sender drops a note whose row is gone. Only retention deletes a run, and only a terminal one. |
| H13 | Owner wakes fill the `NOTIFY` queue. | They are off unless owner mode is on. One wake per execution per tick. |
| H14 | A new writer bypasses `store.rs`. | It stages no owner note. The hit check (B11) still holds. The ADR table lists the raw inserts. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1 and the writer table in ADR 0009. The
  issue asks for a test per writer class, eviction tests for pause, reset
  and terminate, and a child-into-parent test.
- **Red (gut).** A wake that the sender resolves feels indirect. It is
  also the only design that keeps the write path and the fallback path
  unchanged.
- **Black (risks).** Section 0.3. The main risks are a stale snapshot after
  a lost wake, and more `NOTIFY` load. B11 and B16 answer them.
- **Yellow (value).** A foreign write reaches the owner of its partition and
  no other worker. A state change frees the parked future at once.
- **Green (ideas).** Store the partition on the execution row (#2252). The
  sender could then send in one statement. Deferred to #2252.
- **Blue (process).** Red tests first: the wake matrix, the eviction tests
  and the child test. Then the note, the sender lookup, the listener and the
  hit check until green. Then docs and gates. Then a review from several
  angles.

## 1. Change

1. `notify.rs`: `Note::Owner`, the sender lookup, the per-partition
   channel, the payload and `OwnerWakeListener`.
2. `worker.rs`: the owner wake task `OwnerWakes` per shard pool.
3. `store.rs` and `queue.rs`: stage an owner note with each state-change
   append and each wake of a workflow task.
4. `event.rs`: `WorkflowEvent::evicts_owner_cache`.
5. `worker.rs`: the listener task per shard pool, the eviction, the wake of
   the poll loop and the cold rule of a cache hit.
6. `decision_owner.rs`: the owner-wake process flag and the version of the
   tenure set.
7. The metric `harvest.decision_owner.woken{shard, kind}`.
8. Docs: the writer table in ADR 0009, a changelog fragment and the
   telemetry table.

## 2. Review findings

The inventory agent and four review agents checked the notify pipeline,
the worker and cache, the tests and the docs. Nothing was blocking. The
fixes:

- The poison-pill sweep re-pended a workflow task with no owner wake. A dead
  worker can be the old owner, so the re-pend now wakes the owner.
- The sender sent an evict wake to the owner for its own terminal. It now
  skips each wake to the partition that the writer pinned.
- The owner lookup ran before every wake of the tick. The other wakes now
  go first, and the lookup has a 500 ms `statement_timeout`.
- A runtime stop during the lookup lost notes with no count. A guard now
  counts them.
- The fallback counted each dropped owner note in `send_failures`. That
  fired alerts on a healthy process, so it now logs at debug level.
- The cold rule of a cache hit applied with owner mode off. It is now
  gated on owner mode, so a legacy hit behaves as before.
- The owner wake task polled every 250 ms with no tenure. It now waits for
  a tenure change.
- The task reconnected at once after a failed `LISTEN`. It now waits first.
- A multi-shard owner wake did not settle, and a multi-shard turn could take
  the wake of another shard. Both match the queue wake now.
- The tests raced the wake of a resume and missed early wakes. The signal
  now goes in before the resume, and each test waits for the listener.
- The activity test did not isolate the activity result. A held activity
  now makes its result a separate write.
- The ADR table gained the poison-pill and queue-resume rows. The
  changelog claim about owner mode off was wrong.

Accepted as is:

- The self-skip compares the partition number only. A wake to the same
  number on another database is lost. That costs latency only.
- The owner-wake flag is process-wide and never turns off. Issue #2252
  replaces it with the switch row. CI runs each suite in its own process.
- Session breaks and the cross-shard relay have no test of their own. They
  use the chokepoints of an activity failure and a child terminal.

## 3. Out of scope

- The owned claim and the switch row: #2252. Until then a non-owner can
  still claim a workflow task. It fails closed and re-pends it.
- Failover tests: #2254.
