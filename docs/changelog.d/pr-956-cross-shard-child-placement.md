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

The `n` counts **invocations**, not fresh dispatches — the same rule
`fan_out_seq` / `race_seq` / `child_timeout_seq` follow, and deliberately unlike
`activity_seq`. A fresh `WorkflowContext` is built per decision cycle, so a
counter that only advanced on a fresh dispatch restarts at zero every cycle and
hands `#1` to every child of a sequential `for … { spawn(…).await }` loop — each
of those is the only fresh dispatch in its own cycle — collapsing the whole loop
onto one shard while a fan-out (all children fresh in one cycle) still looked
perfectly uniform. Counting invocations makes the Nth spawn's key depend on its
position in the workflow rather than on which cycle dispatched it.

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

**No silent fallback, and no hot spin.** A target shard this node has no pool
for — including "no pool map at all", a reachable misconfiguration since the
router and the pool map are independent globals — raises the new typed
`HarvestError::ShardUnavailable`. So does a shard drained out of
`writable_shards`, and so does a `Distributed` placement with **no** writable
shard (where `pick_for_new_workflow`'s own fall back to `default_shard` is
correct for a top-level start and is exactly the silent fallback AC8 forbids
here — placement is baked into the child's id, so a wrong choice is not
recoverable afterwards). Static misconfiguration — no router, an unknown shard,
an undeclared residency key — stays a terminal `Config` error, because retrying
it never helps.

`ShardUnavailable` is deferred with a **bounded jittered backoff** on every one
of the four spawn paths, via a generalisation of #946's
`recover_from_child_quota_exceeded`. The backoff is the load-bearing part: an
unreachable shard is a static property of this process, so #946's
park-and-immediately-rewake would re-claim, replay the whole history, fail
identically and spin at poll cadence forever.

**Zero event-schema impact.** No new `WorkflowEvent` variant and no change to the
adjacently-tagged JSON contract: `ChildWorkflowStarted` / `ChildWorkflowCompleted`
/ `ChildWorkflowFailed` / `ChildWorkflowCascadeApplied` are reused verbatim, and
the child's shard is recoverable from the `child_id`. A parent replays
byte-identically regardless of where its children physically live.

**Read path.** `GET /workflows/{id}/children` already traversed every shard but
propagated a pool error with `?`, so one unreachable shard `500`d the whole call.
It now degrades through `shard_fanout::collect_fanout_rows` and reports `status` +
`unavailable_shards` additively, matching `/tree` and the rest of #756's contract.

Migration `20260728000000_harvest_cross_shard_children` adds one table, empty in
every deployment that never opts in. Documented in the new *Cross-shard child
placement* section of `docs/sharding.md` (including the deliberate residency
carve-out and the operator queries over the in-flight gauge).

**Review outcomes worth naming.** Four defects were found by review and fixed
before this landed, each with a regression test: the relay's work-list intersected
`target_shard` with the caller's shard assignments, which `monitor_shard_scope`
narrows to one shard — so it selected *only* rows whose target is the parent's own
shard, i.e. never a cross-shard one, and swept nothing in the deployments it exists
for. Two concurrent sweeps (every worker assigned a shard runs the relay over the
same rows) could each append the parent's terminal event; the outbox delete is now
the exactly-once claim, taken **after** the parent's `FOR UPDATE` so the
engine-wide execution-row → outbox-row lock order is preserved. The
"is this child genuinely new?" test looked only at the parent's shard, so every
re-park re-classified a remote child as new and appended a duplicate
`ChildWorkflowStarted` — corrupting the parent's history and failing its next
replay. And `ExecutionId::new()`'s unencoded sentinel compared raw against the
normalised `shard_id` column, routing a *default-placement* child of an unencoded
parent into the relay, where no pool for shard 65535 can exist.

Two fairness/robustness fixes came with them: the sweep splits actionable rows
from a rotating poll of in-flight ones (a single `ORDER BY created_at LIMIT N`
filled its whole window with rows that were merely waiting, so a 10k fan-out
would start 200 children and starve the rest behind them), and every non-progress
path now writes `attempts`/`last_error`/`last_attempt_at`, which both makes the
documented runbook query truthful and drives a per-row retry backoff. There is
still no dedicated metric for the relay; that is called out in `docs/sharding.md`
and tracked as a follow-up.

**Codex round 1 (two P1s, both real).** A failed batch read of the parents'
states returned an empty map, which the call site read as "every parent is
terminal" — and `Retire` deletes the outbox row outright with no second look, so
one transient read error would have permanently lost the terminal wake of every
awaited cross-shard child in the batch and cascade-cancelled detached children
whose parents were alive. `parent_terminal` is now `Option<bool>`, and only
`Some(true)` retires or cascades; start, cancel and terminal delivery still make
progress during a parent-read outage. Separately, a *drained* shard was rejected
inside `resolve_child_placement`, which runs in the workflow handler — where the
ABI erases the error type into a `String` and the executor maps it to a terminal
`WorkflowOutcome::Failed`, so the worker's typed `ShardUnavailable` recovery
never saw it and a maintenance window would have permanently failed every
workflow spawning a placed child. Writability is now enforced by
`preflight_target_shard` at the **persist** boundary, where the rejection is
retryable; the resolver still never swaps the requested shard for another, and
static misconfiguration still fails terminally as it should.

**Codex round 2 (one P1, the sharpest yet).** The child's own terminal
transaction rolled back. `wake_parent_for_child_completion`/`_failure` append to
the parent's history on the **child's** connection — correct while the two are
co-located, fatal once they are not: on the target shard the parent row does not
exist, `store::append_single_event` requires it, and the resulting `NotFound`
rolled back the whole child terminal. The child never settled, so the relay never
had a terminal to deliver and the parent parked forever — a silent, total failure
of the feature that only a live two-database run would surface. Those two
functions and `timeout::wake_parent_for_child_timeout` now skip the inline wake
when the parent is on another shard and leave it to the relay, which is where it
belonged all along. Auditing for the same *shape* rather than the reported
instance turned up a fourth site the review had not reached — the poison-pill
seal in `poison_pill.rs` appends to the parent the same way — now guarded
identically. (`execution::notify_awaited_parent_of_child_terminal`, the operator
cancel path, was already safe: it looks the parent row up with `.optional()` and
skips when absent.)

**Codex rounds 3-4 (four more P1s, two P2s).** The relayed child's
`WorkflowStarted` was written through the identity codec instead of the
configured registry, so on a deployment with a keyed codec (#948) opting into
cross-shard placement stored that input in the clear — silently, and only for
placed children. The per-run `deadline_at`/`sla_deadline_at` were absolute values
computed during the parent's decision, so a relay running behind could create a
child that was already past its deadline and have the scanners kill it before it
ran a step; the durations now travel on the spec and become absolute at creation
(the *chain* deadline stays absolute by design, since re-anchoring it would
defeat the cap). A `STARTED` row whose child had been retention-collected looked
identical to "the shard was unreachable", so the parent waited forever; a child
absent from a shard that *answered* is now reported to the parent as failed. And
a typed `WorkflowFailure` was flattened to an untyped one, because the `error`
column holds only the decoded message — the relay now recovers the envelope from
the child's own `WorkflowFailed` event, restoring #767 parity. `Abandon` rows are
retired as soon as the child exists rather than held for its whole lifetime,
which is unbounded in exactly the case that policy exists for.

One round-4 finding I pushed back on rather than "fixed": a fully-drained fleet
degenerating a `Distributed` placement to the default shard. Failing it is
terminal (the handler ABI), and requeuing it would *deadlock the drain* — a
drained shard is one that should let its in-flight work finish, and a parent
cannot finish while the children it awaits are refused. With zero writable shards
the default shard is also where an unplaced child would go and where the parent
already lives, so no cross-shard contract is broken; none was made. AC8's actual
requirement is that a fallback never happen "without trace", so the path now
carries a `warn!` naming the workflow and the shard, and the docs say so plainly.

**Codex round 5 (one P1, four P2s).** `parent_is_on_another_shard` exempted an
unencoded *parent*, but an unencoded parent that opts into a placement mints a
child encoded to a real remote shard — so its terminal went straight back into
the inline append the guard exists to prevent. Only the *child* side normalises
the sentinel now; an unencoded parent's row lives on the router's default shard,
which is what the child's encoded shard is compared against. The four P2s were
all "placement changed something other than location": a remotely placed detached
child inherited the awaited-child execution and chain timeouts (the local
detached path deliberately persists none), skipped the
`max_workflow_attempts_ceiling` clamp, began a disconnected trace, and had its
relay-driven cancellations and terminations counted through a no-op recorder so
fleet-wide terminal metrics depended on where a child happened to land. All four
now match the local path exactly.

**Test evidence.** 38 no-database tests covering the pure placement resolver
(including a 10k-child ±10% distribution check against the success metric) and
every branch of the relay's decision table, plus workflow-context tests that drive
real handlers through `run_workflow_with_context` and assert on the emitted
`StartChildWorkflow` commands' encoded shards. `cross_shard_children_tests.rs`
adds multi-shard *runtime* coverage against genuinely separate shard databases:
children landing on their encoded shard and nowhere else, the parent completing on
delivered terminals, a crash between the child's terminal and the parent's notify
losing no wake *and the parent then consuming it*, a cross-shard race loser
cancelled on its own shard, a re-park never double-recording a remote child, the
close cascade crossing the boundary, and an unreachable target shard never falling
back to the parent's shard. `fanout_degradation_integration.rs` covers the
`/children` read path degrading to `200 partial` — flat and depth traversal — and
staying `complete` on the happy path.
