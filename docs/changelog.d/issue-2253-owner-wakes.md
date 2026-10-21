## Engine — Foreign writers wake the partition owner and evict its cache (issue #2253)

Stage 1a of epic #2247. Decision 12 of
[ADR 0009](../adr/0009-decision-owner.md) records the design and the
inventory of foreign writers. No migration. No `WorkflowEvent` variant. No
new append path, so the append-only invariant keeps its two exceptions.

**Owner wake.** A write outside the owning decision wakes the owner of the
run's partition after it commits. The wake is a `NOTIFY` on
`harvest_owner_p{n}` in the database of the run. Two chokepoints stage it.
`queue::wake_workflow_task` and `queue::enqueue` stage a plain wake when a
workflow task of the run becomes claimable. The append functions of
`store.rs` stage an evict wake for a terminal, a pause, a resume or a
redrive. A rebalance cutover and activation stage an evict wake on each
database. The post-commit sender reads each `workflow_id` on its own
connection, so the write path pays nothing. A plain wake to the partition
that the writer's own decision pinned is not sent.

**Owner side.** Each worker runs one owner wake task per shard pool with a
notify URL. The task connects only while the worker holds a tenure on that
shard, and it listens on the channels of those partitions. An evict wake
drops the cache entry and its parked future. Each wake ends the idle wait
of the claim loop. `harvest.decision_owner.woken{shard, kind}` counts the
wakes. The kind is `wake` or `evict`.

**Durable backstop.** A `NOTIFY` can be lost. A cache hit whose delta holds
a state change now replays cold. A hit whose entry has another owner key
replays cold too.

**Process flag.** `decision_owner::enable_owner_wakes` turns owner wakes on
for the process. Owner mode turns it on. Issue #2252 replaces it with the
switch row of each database. With owner wakes off, nothing changes.

**Tests.** `owner_wake_worker_tests` runs an owner and a non-owner worker in
owner mode. An activity result, a signal, a cancel and a timeout scanner
write each wake the owner, and the non-owner prepares no decision. A pause,
a reset and a terminate from another connection each evict the owner's
entry. The next decision replays cold and records no non-determinism. A
child decided by one owner writes its terminal into a parent that another
owner holds. That owner wakes and its next decision reads the terminal. A
lost evict wake still replays cold. `owner_wake_db_tests` checks the wake
and its evict flag on the partition channel for each writer class. A
listener of another partition gets no wake. The docs guard
`foreign_writer_table_cites_real_functions` checks each writer that the ADR
table cites.
