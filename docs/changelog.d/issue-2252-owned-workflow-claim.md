## Engine — Owned workflow-task claims and the owner-mode switch (issue #2252)

Stage 1a of epic #2247. [ADR 0009](../adr/0009-decision-owner.md) fixes the
protocol. Owner mode replaces the sticky lease for workflow tasks. Activity
claims do not change. Owner mode is off until an operator turns it on.

**Migration.** `20261021151820_harvest_decision_owner_claim` adds the
nullable column `harvest_task_queue.decision_partition`, the one-row switch
table `harvest_decision_owner_mode`, the candidate table
`harvest_decision_owner_candidates` and the function
`harvest_task_queue_owner_claim_guard`. It creates no trigger. No
`WorkflowEvent` variant, no replay impact.

**Switch.** `decision_owner::enable_owner_mode` refuses while a live worker
lacks the `decision_owner_version` label, while live workers poll different
queues or run different builds, or while no owner loop is live. It stamps
live workflow tasks. Then one transaction installs the claim guard,
re-pends the legacy claims of dead workers and sets the switch.
`disable_owner_mode` clears the switch and turns the guard off in one
transaction.

**Claim.** In owner mode a worker claims a workflow task only from its own
partitions, or an unstamped one. The owned claim is a splice on each claim
text, so the legacy text stays byte-identical. It sets
`harvest.owner_claim` to the holder token, tests one bit of a 64-byte
partition mask, and checks the holder of a stamped row in each candidate
scan. The guard skips a workflow claim
with no token, so an old binary claims no workflow task. For a stamped row
it takes the owner row `FOR KEY SHARE SKIP LOCKED` and matches the holder.
The sticky gate does not apply to a workflow row, and an owned park writes
no pin.

**Loop.** Each worker runs one owner loop per database. It scans once a
second, records itself as a live candidate, renews every TTL / 3, drops a
tenure at its local deadline, and balances to `⌈512 / live candidates⌉`. A
joiner takes free partitions. An owner above target + 1 stops claiming the
extra ones and releases them one scan later. A worker with no workflow
slot or type is a participant: it claims in the owned form and owns
nothing. Any owner takes a partition whose
lease ended over one renewal interval ago. An acquisition re-pends the
workflow tasks that a dead worker left `RUNNING` there. A graceful stop
releases every partition after the drain. Colocated shards share one key. A
worker that covers part of a colocated group refuses owner mode there.

**Terminal guard.** The extra cold decisions of issue #2007 came from
`queue::claim_still_held_for_update`, in both modes. A peer claim scans the
queue head `FOR UPDATE SKIP LOCKED` and can lock a row just after another
loop claimed it. Postgres keeps that lock until the peer claim transaction
ends. The final decision of a run read the lock as a lost claim. It rolled
back, released the task, and the retry replayed cold. A skip now retries
once with a blocking lock in a savepoint, bounded by
`CLAIM_GUARD_LOCK_WAIT` (100 ms). A transfer still reads as lost. The bound
is far below `deadlock_timeout`, so the guard still ends an ABBA cycle.

**Evidence.** Unit tests for the splice, the balance plan, the local
deadline and the colocated groups. Postgres tests for the guard, the owned
claim, enable, disable and rebalance. Worker tests for zero extra cold
decisions under two claim loops and for resident hits across the sticky
window. The e2e bench gains `HARVEST_BENCH_OWNER_MODE=1` and a decision note.

**Hit rate.** Each run of the e2e throughput bench makes four decisions,
and its first decision has no resident state anywhere. So the hit rate over
all decisions cannot pass 75% in either mode. The bench now also reports
the continuation hit rate: hits over the decisions after the first of each
run. The worker test measures that rate across a sticky window.

**API.** `schema::harvest_task_queue` gains `decision_partition`, and
`TaskQueueItem` does not. A full-row `.load::<TaskQueueItem>()` on that
table now needs `.select(TaskQueueItem::as_select())`.
