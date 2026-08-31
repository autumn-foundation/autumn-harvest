## Phase 5.x — cross-shard child workflow placement (issue #956)

Child workflows were always pinned to the parent's shard, and `spawn_child_workflow_fan_out`
had made a 10k-child fan-out three lines of code — so a single orchestrator
concentrated its entire fan-out's event-append, task-claim and vacuum load on one
Postgres instance no matter how many shards the operator provisioned. Adding
shards helped top-level starts and did nothing for the child-heavy workloads that
need sharding most. This ships the opt-in escape hatch, built entirely out of
primitives that already existed.

**`ChildPlacement`, per spawn, default unchanged forever.** Every
`spawn_child_workflow*` entry point gains a `_placed` sibling taking a
`&ChildPlacement`; the originals delegate with `ChildPlacement::ParentShard`,
which short-circuits **before** the router is consulted — so a deployment that
never installs a router (every single-shard deployment, every existing test) is
byte-for-byte unaffected, right down to the placement counter never incrementing.
`Distributed` routes through the same `ShardRouter::pick_for_new_workflow` a
top-level start uses; `Shard`/`ResidencyKey` honour an explicit #697 pin and are
rejected — never re-hashed — when the router refuses them.

**Restart stability without a directory.** A top-level start hashes a
caller-supplied `workflow_id`, stable by construction; a child's `ExecutionId` is
minted fresh every dispatch, so hashing *it* would re-roll the shard on every
crash-retry. `Distributed` hashes a deterministic `"{parent_exec_id}#{n}"`
placement key instead, giving children the same restart-stability contract.
Placement is resolved exactly once per child, on the fresh dispatch; replay reuses
the recorded `child_id` verbatim, so widening `writable_shards` cannot move a
started child.

**One row, four edges.** The parent's decision transaction never spans two
databases. A cross-shard spawn writes one `harvest_cross_shard_children` row on
the parent's shard, in the same transaction as `ChildWorkflowStarted` /
`ChildWorkflowSpawnedDetached` — so no committed row means no child was ever
promised, and an orphan is impossible. That row is not a message; it is the
child's lifecycle record on the parent's side, and child start, cancel, terminal
notify and the `ParentClosePolicy` cascade are all transitions of it, driven by
`enforce_cross_shard_children` alongside #492's outbox scanners. Each is
at-least-once with a real dedupe key: the child's `ExecutionId` is the PK on the
target shard, an idempotent cancel absorbs redelivery, and the cascade only acts
on a `RUNNING`/`PAUSED` child.

**The terminal notify is a pull, not a push** — the load-bearing design choice.
Pushing a notify from the child's shard would leave exactly the crash window AC3
rules out (child terminal committed, notify lost). Instead the relay *reads* the
child's state from the target shard and then appends the parent's
`ChildWorkflowCompleted`/`Failed`, wakes it, and deletes the row in **one
transaction on the parent's shard**. Nothing is ever in flight, so a crash at any
instant leaves the durable row for the next sweep. Delivery is therefore
exactly-once from the parent's point of view even though the observation is
at-least-once.

**No silent fallback.** A target shard this node has no pool for raises the new
typed, retryable `HarvestError::ShardUnavailable`; the parent's decision cycle
rolls back with nothing recorded and is parked-and-rewoken (the same treatment
#946's `QuotaExceeded` gets) rather than terminally failed — and the child is
never quietly re-placed on the parent's shard.

**Zero event-schema impact.** No new `WorkflowEvent` variant and no change to the
adjacently-tagged JSON contract: `ChildWorkflowStarted` / `ChildWorkflowCompleted`
/ `ChildWorkflowFailed` / `ChildWorkflowCascadeApplied` are reused verbatim, and
the child's shard is recoverable from the `child_id`. A parent replays
byte-identically regardless of where its children physically live.

**Read path.** `GET /workflows/{id}/children` already traversed every shard but
propagated a pool error with `?`, so one unreachable shard `500`d the whole call.
It now degrades through `shard_fanout::collect_fanout_rows` and reports `status` +
`unavailable_shards` additively, matching `/tree` and the rest of #756's contract.

Migration `20260727000000_harvest_cross_shard_children` adds one table, empty in
every deployment that never opts in. Documented in the new *Cross-shard child
placement* section of `docs/sharding.md` (including the deliberate residency
carve-out and the operator queries over the in-flight gauge).

**Test evidence.** 38 no-database tests covering the pure placement resolver
(including a 10k-child ±10% distribution check against the success metric) and
every branch of the relay's decision table, plus workflow-context tests that drive
real handlers through `run_workflow_with_context` and assert on the emitted
`StartChildWorkflow` commands' encoded shards. `cross_shard_children_tests.rs`
adds multi-shard *runtime* coverage against genuinely separate shard databases:
children landing on their encoded shard and nowhere else, the parent completing on
delivered terminals, a crash between the child's terminal and the parent's notify
losing no wake, the close cascade crossing the boundary, and an unreachable target
shard never falling back to the parent's shard.
