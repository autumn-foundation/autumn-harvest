## Engine — The decision owner lease and the commit fence (issue #2251)

Stage 1a of epic #2247. [ADR 0009](../adr/0009-decision-owner.md) fixes the
protocol. Owner mode stays off until issue #2252 adds its switch and its
claim, so no production path holds a tenure yet.

**Migration.** `20261020132402_harvest_decision_owner` adds
`harvest_decision_owner(partition, holder, epoch, lease_until)` and seeds rows
0 to 511. A trigger refuses an epoch that falls or grows by more than 1, and
a changed partition. No hot table is locked. No `WorkflowEvent` variant, no
replay impact.

**Lease.** `decision_owner::acquire` takes a free or lapsed row `FOR UPDATE`
and adds 1 to the epoch, also for the same holder. `renew_all` changes
`lease_until` only, so it never waits for a commit and never bumps the epoch.
`release_all` frees a row and keeps its epoch. Both lock their rows in
ascending partition order first. A lock miss is `Busy`, never an error. The
DR bump (`harvest dr fence`) ends every owner lease in its transaction. A
bump of a shard with no generation row now rolls back.

**Fence.** A decision pins its tenure in a task-local. Each decision
transaction calls `replication::assert_decision_fence` first: the DR
generation row, then the owner row `FOR KEY SHARE`, then the execution row.
Each event insert of the decision fences again. The fence is the PL/pgSQL
function `harvest_decision_owner_fence`. It waits at most 250 ms and then
fails closed, so a failed acquisition recheck does not fence a healthy
owner. A holder or epoch mismatch aborts the whole transaction with
`HarvestError::OwnershipLost`. The worker drops the tenure and every cache
entry of the partition, counts `harvest.decision_owner.fenced{shard}` and
re-pends the task under its claim. It never retries at the stale epoch.
`fail_execution_on_error` passes the error through, so a fence never fails a
run. A write in another task takes no owner row. With owner mode on and no
tenure, a decision fails closed. A worker keys tenures by shard and
partition, because it can serve several databases.

**Deviations from the issue text.** The commit takes `FOR KEY SHARE`, not
`FOR SHARE`, and the acquisition is a `FOR UPDATE` lock, not the scanner
upsert. ADR 0009 decision 5 requires both: `a_for_share_commit_blocks_a_renewal`
and `a_scanner_style_upsert_does_not_wait_for_a_commit` show why.

**Model.** A renewal now needs a live lease in the TLA+ model and the
oracle too, as ADR 0009 decision 5 and the Postgres store require. TLC
passes the design and fails each mutant on its invariant, as before.

**Proof.** `formal/verus/src/owner_fence.rs` proves `next_epoch` and
`fence_passes` for every input. Its spec shows that only an acquisition moves
the epoch, so after a takeover the fence refuses every commit of the old
tenure.

**Tests.** `decision_owner_db_tests.rs` proves on Postgres that a stale
owner's commit fails after a takeover, a renewal keeps the epoch, an
acquisition bumps it, and a fenced commit leaves no event row. It also pins
the lock order. `decision_owner_sim_db_tests.rs` runs the issue #2250
simulator, with all four nemeses, on the Postgres store.
`decision_owner_worker_tests.rs` shows a real worker fenced, unloaded and
never retried. A DR test shows that the bump ends every lease and keeps each
holder and epoch.
