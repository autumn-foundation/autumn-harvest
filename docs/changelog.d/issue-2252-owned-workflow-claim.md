## Engine — Owned workflow-task claims and the owner-mode switch (issue #2252)

Stage 1a of epic #2247. [ADR 0009](../adr/0009-decision-owner.md) fixes the
protocol. Owner mode replaces the sticky lease for workflow tasks. Activity
claims do not change. Owner mode is off until an operator turns it on.

**Migration.** `20261021151820_harvest_decision_owner_claim` adds the
nullable column `harvest_task_queue.decision_partition`, the one-row switch
table `harvest_decision_owner_mode` and the function
`harvest_task_queue_owner_claim_guard`. It creates no trigger. No
`WorkflowEvent` variant, no replay impact.

**Switch.** `decision_owner::enable_owner_mode` refuses while a live worker
lacks the `decision_owner_version` label, or while live workers poll
different queues or run different builds. It stamps live workflow tasks,
installs the claim guard, waits for legacy decisions, then re-pends what is
left and sets the switch. `disable_owner_mode` clears the switch and drops
the guard in one transaction.

**Claim.** In owner mode a worker claims a workflow task only from its own
partitions, or an unstamped one. The owned claim is a splice on each claim
text, so the legacy text stays byte-identical. It sets
`harvest.owner_claim` to the holder token. The guard skips a workflow claim
with no token, so an old binary claims no workflow task. For a stamped row
it takes the owner row `FOR KEY SHARE SKIP LOCKED` and matches the holder.
The sticky gate does not apply to a workflow row, and an owned park writes
no pin.

**Loop.** Each worker runs one owner loop per database. It scans once a
second, renews every TTL / 3, drops a tenure at its local deadline, and
balances to `⌈512 / live owners⌉`. A joiner takes free partitions. An owner
above target + 1 releases the extra ones. Any owner takes a partition whose
lease ended over one renewal interval ago. An acquisition re-pends the
workflow tasks that another worker holds `RUNNING` there. A graceful stop
releases every partition after the drain. Colocated shards share one key. A
worker that covers part of a colocated group refuses owner mode there.

**Evidence.** Unit tests for the splice, the balance plan, the local
deadline and the colocated groups. Postgres tests for the guard, the owned
claim, enable, disable and rebalance. Worker tests for zero extra cold
decisions under two claim loops and for resident hits across the sticky
window. The e2e bench gains `HARVEST_BENCH_OWNER_MODE=1` and a decision note.
