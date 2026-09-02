# Harvest Sharding Guide

## Overview

Harvest can spread workflow state across N independent Postgres databases (shards). A single workflow's event log, task queue rows, timers, signals, and DLQ entries all live on the same shard, so per-workflow ACID guarantees are preserved without cross-shard transactions.

For the full sharding architecture, see `CLAUDE.md` §Sharding and `autumn-harvest/src/shard.rs`.

**When should you shard?** [`performance.md`](performance.md) publishes measured
task-claim latency against pending-backlog depth. Claim cost grows superlinearly
with backlog depth and every worker pays the full scan, so beyond a certain depth
adding workers stops helping and sharding (or shedding the backlog) is the
answer. Measure your own steady-state backlog against that table before
provisioning a second shard.

---

## Per-Key Concurrency Limits and Sharding (issue #247)

### Limits are shard-local

Concurrency limits declared via `#[workflow(concurrency(key = "input.tenant_id", limit = 10))]` are enforced **within a single shard**. The claim query in `harvest_task_queue` counts `RUNNING` rows on the local shard only; it has no visibility into rows on other shards.

This means:

| Deployment | Effective global cap |
|---|---|
| Single-shard (default) | `limit` — enforced globally |
| Multi-shard (N shards) | Up to `limit × N` — each shard enforces `limit` independently |

### Achieving a true global cap in a multi-shard deployment

If your use-case requires a hard global cap across all shards (e.g., "at most 10 concurrent workflows for any tenant, regardless of which shard they land on"), route all executions for the same concurrency key to a single shard.

Use **explicit shard placement** (below) — supply a `residency_key` derived from the same field you cap on. Every execution carrying that key lands on the same shard, so the local `limit` is also the global limit:

```bash
curl -X POST /api/harvest/workflows/order_flow/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id": "order-42", "input": {"tenant_id": "acme"}, "residency_key": "acme"}'
```

**Trade-off**: routing all of one tenant's workflows to the same shard concentrates load. Size shards to handle the worst-case tenant burst, or use rate limiting above Harvest to bound how fast new workflows can be started per tenant.

### Latest-wins supersede is shard-local too (issue #811)

The same scope contract applies to the `on_conflict = "cancel_running"` overflow strategy:

```rust
#[workflow(concurrency(key = "input.doc_id", limit = 1, on_conflict = "cancel_running"))]
```

When a new run for key `K` is admitted, the supersede pass serializes on a shard-local advisory lock (`pg_advisory_xact_lock(hashtext(K))` — the **same** namespace the claim-time concurrency gate uses) and scans **only the admitting shard's** `harvest_workflow_executions` for non-terminal runs of the same workflow type carrying `K`. It has no visibility into, and never cancels, a run on another shard.

| Deployment | Effective latest-wins guarantee |
|---|---|
| Single-shard (default) | At most `limit` non-terminal runs per key, globally — the newest admission wins |
| Multi-shard (N shards) | At most `limit` **per shard**, so up to `limit × N` globally; a run on shard A is never superseded by an admission on shard B |

**Cross-shard latest-wins is explicitly out of scope.** A cross-shard supersede would need a distributed lock plus a cross-shard cancellation transaction, and neither exists (nor is planned) — the whole sharding design deliberately avoids cross-shard transactions.

If you need a *global* "only one run per key, newest wins" guarantee in a multi-shard deployment, use the same fix as the cap above: pin every execution for the key to one shard with an explicit `residency_key`, so the shard-local guarantee **is** the global one.

Two further scoping notes:

- **Supersede is scoped to `(workflow_name, concurrency_key)`, not the key alone.** A *different* workflow type that merely resolved the same key string — and never opted in to latest-wins — is never cancelled. This is what makes the feature migration-free: the resolved key lives on `harvest_task_queue`, so no new column on `harvest_workflow_executions` is needed.
- **A single admission sheds at most `SUPERSEDE_SCAN_LIMIT` (32) runs.** Latest-wins is a per-key fair-share control, not a bulk-cancel tool: one start must never open an unbounded transaction cancelling hundreds of executions. Any excess beyond that is shed by the *next* admission for the same key, so the population still converges. The candidate scan itself is bounded to `limit + 32` rows for the same reason, so flipping a long-backlogged `Defer` key to `cancel_running` never materialises the whole backlog while holding the advisory lock.
- **The superseded population is "non-terminal runs", not "runs currently occupying a dispatch slot".** The claim-time gate (issue #247) counts `RUNNING` *task* rows with a live worker; the supersede scan counts `RUNNING`/`PAUSED` *execution* rows. So a paused run — or one whose workflow task is still deferred at the claim gate — occupies no dispatch slot yet is still superseded. That is the intended reading: latest-wins enforces "at most `limit` **non-terminal runs** per key, newest wins", which is the operator-visible population.

### Operator note: combining `cancel_running` with `ctx.mutex` (issue #691)

Postgres's one-argument advisory locks share a single 64-bit space, and the durable mutex (`ctx.mutex`, issue #691) takes a *blocking* lock in that same space. A latest-wins admission holds its concurrency lock while cancelling an incumbent, and that cancellation runs the incumbent's terminal chokepoint — which can take a mutex lock. The reverse order also exists (a mutex holder reaching a terminal state whose completion trigger starts a `cancel_running` workflow), so the two orders are inverted.

If both happen concurrently Postgres detects the cycle and aborts one side with SQLSTATE `40P01`, surfacing as a database error on the *start*. The aborted transaction rolls back atomically — no partial supersede, no orphaned cancellation — and the start is safe to retry. It is a **liveness** hazard, not a correctness one, and it needs a workflow that both declares `on_conflict = "cancel_running"` and participates in `ctx.mutex` on the same terminal path. If you see `40P01` on such a start, retry it; if it recurs, split the mutex usage out of the latest-wins workflow.

---

## Explicit shard placement and data residency (issue #697)

By default a new workflow's shard is chosen by rendezvous hashing over `(workflow_name, workflow_id)`. That is the right default — it spreads load evenly and needs no configuration — but a hash cannot know which physical database sits in which jurisdiction, so it cannot satisfy a data-residency obligation. For that, the start path accepts an **explicit placement**.

### Two ways to place a workflow

| Placement | Field | Use when |
|---|---|---|
| **Concrete shard** | `shard_id: 1` | You already know the shard number (ops tooling, migrations, tests). |
| **Residency key** | `residency_key: "eu"` | You know the *jurisdiction / tenant*, not the shard number. The router maps the key to a shard. |
| **Auto** (default) | *omit both* | Anything with no residency obligation. Byte-for-byte the pre-#697 routing. |

The two fields are **mutually exclusive** — supplying both is a `400`.

### Resolution rule and stability guarantee

A residency key is resolved through an **operator-declared map** on the router, not through a hash:

```rust
let router = ShardRouter::new(
    vec![ShardId::new(0), ShardId::new(1)],   // readable
    vec![ShardId::new(0), ShardId::new(1)],   // writable
    ShardId::new(0),                          // default
)
.with_residency_map([
    ("eu".to_string(), ShardId::new(0)),
    ("us".to_string(), ShardId::new(1)),
]);
```

This is a declared map rather than a hash for two reasons:

1. **A hash cannot express jurisdiction.** Only the operator knows that shard 0's database is the one physically hosted in the EU. A hash would assign keys to shards arbitrarily.
2. **A hash is not stable when the shard set widens.** Rendezvous hashing deliberately re-balances: adding a shard moves roughly `1/N` of keys to it. That is correct for load distribution and *fatal* for residency — a key that resolved to the EU shard could silently start resolving to the US shard the moment a third shard is added.

The declared map gives the guarantee residency needs:

> **A residency key resolves to the same shard across process restarts and across any widening of `writable_shards`.** The mapping changes only when an operator edits the map.

A key that is **not in the map** is rejected with a typed error — it is never hashed as a fallback, because a silent hash is exactly the failure mode that produces a compliance breach.

### Rejections (never a silent fallback)

`POST /workflows/{name}/start` returns `400` with a JSON body — never a `500`, never a silent re-hash, never a quiet fall back to the default shard — for:

| Condition | Error |
|---|---|
| `shard_id` not in `readable_shards` | *shard N is not a placeable shard for this deployment* |
| `shard_id` readable but not in `writable_shards` (a draining shard) | *shard N is not currently accepting new workflows; it is being drained* |
| `residency_key` not in the declared map | *residency key 'K' is not declared for this deployment* |
| `residency_key` blank / whitespace-only | `residency key must not be blank` |
| `residency_key` longer than 128 characters | rejected at the request boundary |
| Both `shard_id` and `residency_key` supplied | mutually exclusive |
| `shard_id` negative, or the reserved `65535` sentinel | rejected at the request boundary (not a placeable shard) |
| `shard_id` / `residency_key` on a debounced or batched workflow | mutually exclusive (see *Caveats*) |

Pinning to a **readable but non-writable** shard is rejected rather than accepted because placing new work on a shard the operator is draining contradicts the drain.

The messages above are deliberately terse: they name what the caller asked for and why it was refused, but never enumerate the deployment's shard set, drain state, or declared residency keys — a start endpoint is often reachable by a lower-trust caller than the operator surfaces. The full detail (including the known set) is written to the server log via `tracing::warn!` for the operator.

A pin to a shard the router accepts but that has **no pool of its own** — a real state mid a shard-add rollout, since the router's shard set and the pool map are configured independently — fails closed with a `503`, not a `201`. Falling back to the default shard's database here would write residency-bound data to the wrong country while still reporting the pinned shard to the caller.

Misconfiguring the map itself (a blank key, or a target outside `readable_shards`) panics at `ShardRouter` construction, so it fails at boot rather than at the first request. A declared target that is readable but *not* writable is accepted (it is a legitimate mid-drain state) but logged as a warning: starts under that key will be rejected until the shard is writable again.

### Observing the chosen shard

The chosen shard is observable three ways after a start (AC5) — no new column was added:

- The start response carries `shard_id` **when a placement was requested**. An unpinned start omits the field entirely, so its response is byte-for-byte the pre-#697 shape. A **throttled** start's deferred `202` echoes it too, since there is no `execution_id` to decode yet.
- The `ExecutionId` encodes the shard in its first two bytes (`exec_id.shard()`), which is how every later id-based lookup routes with no directory.
- The execution row's existing `shard_id` column.

### Verifying fleet consistency *before* accepting pinned starts

The three surfaces above tell you where one run landed. They cannot tell you whether the **fleet agrees on the map**, and that is the failure mode with the worst blast radius: if two replicas are deployed with different `residency_map`s, the same `residency_key` places workflows in **different jurisdictions** depending on which replica happens to serve the request — silently, because both replicas report identical `readable_shards` / `writable_shards` / `default_shard`.

`GET /api/harvest/admin/config` (issue #695) therefore surfaces the declared map under `shard_topology.residency_map`:

```bash
curl -s .../admin/config | jq -S '.shard_topology'
```

```json
{
  "default_shard": 0,
  "readable_shards": [0, 1],
  "residency_map": { "eu": 0, "us": 1 },
  "shard_forwards": {},
  "writable_shards": [0, 1]
}
```

`shard_forwards` (issue #964) is the same argument applied to a strictly worse
failure mode. It is empty until a shard has been decommissioned; after one, two
replicas that disagree about where a **retired** shard's ids resolve will answer
the *same* `ExecutionId` from *different* databases — one finding the run, the
other a confident `404` — while reporting identical `readable`/`writable`/
`default` sets. Diff it as the last step of a decommission.

The projection is `BTreeMap`-ordered, so it is byte-stable and a plain diff across replicas is meaningful:

```bash
diff <(curl -s "$REPLICA_A/admin/config" | jq -S .shard_topology) \
     <(curl -s "$REPLICA_B/admin/config" | jq -S .shard_topology)
```

Run this after any deploy that touches the map, and before you start relying on pinned starts. Residency keys are operator-declared jurisdiction labels, not secrets or caller input, and the endpoint is admin-gated — but note the deliberate asymmetry with the *rejection* messages above, which never enumerate the declared key set to a lower-trust start caller.

A coverage guard keeps this honest: `ShardRouter::parts()` and `ShardTopologyView::from_router` both destructure exhaustively (no `..`), so adding a placement-affecting field to the router is a **compile error** until it is surfaced here. `residency_map` itself was missing from the snapshot when it was first introduced, which is why the guard exists.

### Residency is transitive across the workflow tree

Everything spawned under a pinned parent stays on the parent's shard — this is asserted by tests, not left implicit:

| Descendant | Inherits |
|---|---|
| Awaited child workflow | ✅ |
| Detached child workflow | ✅ |
| `ctx.race()` child branch | ✅ |
| Child-or-deadline race (`execute_child_workflow_timeout`) | ✅ |
| `continue_as_new` successor | ✅ |
| Workflow-level retry run (#523) | ✅ |
| Reset fork (#148) | ✅ |

So pinning the **root** of a workflow tree confines the whole tree.

> **One deliberate exception (issue #956).** A spawn can opt *out* of that inheritance per call, with `ChildPlacement`. The default is unchanged and permanent — every entry in the table above still reads ✅ unless the calling code explicitly passes a non-default placement. A residency-bound tree must therefore keep the default; use `ChildPlacement::ResidencyKey` when a child genuinely has its own declared jurisdiction, and never `ChildPlacement::Distributed`. See *Cross-shard child placement* below.


### Worked example — a two-region EU/US deployment

1. **Provision two databases**, one per region, and run `harvest migrate run` against each (DSN via `HARVEST_DATABASE_URL`).

2. **Declare the shards and the residency map** when building the router:

   ```rust
   let router = ShardRouter::new(
       vec![ShardId::new(0), ShardId::new(1)],
       vec![ShardId::new(0), ShardId::new(1)],
       ShardId::new(0),
   )
   .with_residency_map([
       ("eu".to_string(), ShardId::new(0)),   // database hosted in the EU
       ("us".to_string(), ShardId::new(1)),   // database hosted in the US
   ]);
   ```

3. **Start each workflow with the caller's region as the residency key.** Derive the key from whatever your app already knows about the customer — do *not* try to infer it from the workflow payload, which Harvest deliberately never inspects:

   ```bash
   # An EU customer's order — confined to the EU database.
   curl -X POST /api/harvest/workflows/order_flow/start \
     -H 'Content-Type: application/json' \
     -d '{"workflow_id": "order-1001", "input": {...}, "residency_key": "eu"}'
   # → 201 {"execution_id": "...", "shard_id": 0, ...}
   ```

   or from the CLI:

   ```bash
   harvest workflow start order_flow --workflow-id order-1001 --residency-key eu
   ```

4. **Prove confinement.** The response's `shard_id` is `0`; the returned `execution_id` encodes shard `0`; and the row exists in the EU database only. Every child, retry, and continue-as-new of that run stays in the EU database.

5. **Adding a third region later does not move existing keys.** Add shard 2 to `readable_shards`, wait for readiness, add it to `writable_shards`, then add `("apac", ShardId::new(2))` to the map. `eu` and `us` keep resolving exactly where they did.

### Caveats

- **`(workflow_name, workflow_id)` uniqueness is per-shard.** A pin moves a run off its hash-derived shard, so a *later, unpinned* start of the same `workflow_id` would route elsewhere and could create a duplicate. When the caller omits `workflow_id`, Harvest mints one that hashes to the pinned shard, closing the hole automatically. When the caller supplies an **explicit** `workflow_id`, be consistent: either always pin it or never pin it. (This is the same consistency requirement idempotency-key routing already documents.)
- **Placement is resolved before idempotency-key replay.** A retry of a keyed start must carry the same placement as the original delivery. A *committed* keyed replay still returns its original `200` even if the pinned shard has since been drained — the replay creates no new work, so it is validated against `readable_shards` only.
- **Only the HTTP start route and the CLI carry placement.** `POST /workflows/{name}/signal-with-start` and `/update-with-start` have no `shard_id` / `residency_key` field, and the in-process SDK start APIs (`StartWorkflowParams`, the typed client stubs) carry none either — all of them route by hash. An entity workflow created through the documented signal-with-start pattern therefore **cannot** be pinned today. If a residency-bound workflow must be reachable that way, start it explicitly first (pinned) and let signal-with-start attach to the existing run. `WorkflowHandleClient::resolve_shard_placement` resolves and validates a placement for pre-flight tooling, but does not itself place anything.
- **Deferred starts cannot be pinned.** Debounce (#499) and batch (#518) admit a start without creating an execution, so there is nothing to place at request time; combining either with `shard_id` / `residency_key` is a `400` rather than a silently discarded pin. A throttled start (#607) *is* pinned — it defers the same concrete placement to its scanner.
- **Rollout ordering.** Placement is enforced by the node handling the start. During a rolling deploy, a pinned request that lands on a pre-#697 node is accepted and hashed, silently ignoring the pin. Upgrade the whole fleet before you begin sending pinned starts, and treat the first pinned start as the cutover point.
- **Residency keys are an operator-declared, low-cardinality set.** The map is held in memory on every node and validated at boot; it is sized for regions/jurisdictions (single digits to dozens), not per-tenant keys. For per-tenant placement, map the tenant to a region in your own application layer and pass the region as the key.
- **Out of scope**: migrating a *running* workflow between shards, per-shard worker assignment, geo-replication / cross-region failover, and inferring residency from payload contents. Harvest never reads your payload to decide placement — the caller states it explicitly.

### Cross-shard global limits — explicit out of scope

Distributed counting across shards requires either a coordination service (Redis, a dedicated Postgres coordinator) or accepting bounded inaccuracy (approximate counts via gossip). Both add operational complexity that conflicts with Harvest's goal of being a Postgres-native engine. Cross-shard global limits are therefore **out of scope** for this feature; the per-shard guarantee is the contract.

If you need approximate cross-shard fair-share rather than a hard global cap, the metrics observable via `GET /admin/concurrency` can feed an external rate limiter in the layer above Harvest.

### Worker crash and slot release

When a worker crashes and its heartbeat times out, the `timeout.rs` scanner transitions the claimed task back to `PENDING` state. The concurrency cap check counts only `RUNNING` rows, so the slot is immediately available for another worker to claim. No operator intervention is required.

This is handled entirely within the shard where the task lives — no cross-shard coordination is needed for crash recovery.

---

## Cross-shard child placement (issue #956)

Children are pinned to the parent's shard by default, and that default is permanent. That is the right default — it keeps a whole workflow tree's ACID, residency and observability story shard-local — but it means a single orchestrator concentrates its entire fan-out's write load on one database. Adding shards helps top-level starts and does nothing for the child-heavy workloads that need sharding most.

`ChildPlacement` is the opt-in, **per-spawn** escape hatch.

```rust
use autumn_harvest::shard::ChildPlacement;

// Today's behaviour. The router is never even consulted.
let a: Receipt = ctx.spawn_child_workflow(&process_one_info(), item).await?;

// The same call, spread across `writable_shards`.
let b: Vec<Receipt> = ctx
    .spawn_child_workflow_fan_out_placed(
        &process_one_info(),
        items,
        &ChildPlacement::Distributed,
    )
    .await?;
```

Every `spawn_child_workflow*` entry point has a `_placed` sibling taking a `&ChildPlacement`; the original methods delegate with `ChildPlacement::ParentShard`.

| Variant | Resolution |
|---|---|
| `ParentShard` (default) | The parent's shard. Short-circuits **before** the router is consulted, so a deployment with no router installed is unaffected. |
| `Distributed` | `ShardRouter::pick_for_new_workflow` over `writable_shards` — the same rendezvous function a top-level start uses. |
| `Shard(id)` | An explicit pin, validated exactly like `ShardPlacement::Shard` (unknown or drained ⇒ rejected). |
| `ResidencyKey(key)` | An explicit residency pin, resolved through the declared map (undeclared ⇒ rejected, never hashed). |

### Restart stability

A top-level start's rendezvous key is the caller-supplied `workflow_id`, which is stable by construction. A child's `ExecutionId` is minted fresh on every dispatch, so hashing *it* would re-roll the shard whenever a decision cycle is retried after a crash. `Distributed` instead hashes a deterministic per-parent key, `"{parent_exec_id}#{n}"`, so a retried cycle re-derives the identical shard for every slot — the same restart-stability contract top-level starts have.

Placement is decided exactly once in a child's lifetime, on the **fresh dispatch**. Replay reuses the `child_id` recorded in `ChildWorkflowStarted` verbatim and never re-derives anything, so widening `writable_shards` cannot move an already-started child.

### What crosses the shard boundary, and how

The parent's decision transaction stays shard-local. Always. It never opens a second database.

Instead, a cross-shard spawn writes **one row** into `harvest_cross_shard_children` on the parent's shard, in the same transaction as the parent's `ChildWorkflowStarted` / `ChildWorkflowSpawnedDetached` event. That row is not a message — it is the cross-shard child's lifecycle record on the parent's side, and all four cross-shard edges are transitions of it. `enforce_cross_shard_children` (part of the ordinary scanner tick, alongside #492's outbox scanners) drives them:

| Edge | What the relay does | Dedupe key |
|---|---|---|
| Child start | Creates the child on the target shard, then marks the row `STARTED` | the child's `ExecutionId` is the primary key over there |
| Cancel | Delivers an idempotent `cancel_workflow_execution` | a terminal target absorbs the cancel |
| Terminal notify | Reads the child's terminal state, appends `ChildWorkflowCompleted`/`Failed` to the parent, wakes it, deletes the row — one transaction on the **parent's** shard | the append and the delete commit together |
| Close cascade | Applies `RequestCancel`/`Terminate` on the target shard, records `ChildWorkflowCascadeApplied`, deletes the row | the cascade only acts on a `RUNNING`/`PAUSED` child |

Note that the terminal notify is a **pull**, not a push. A push from the child's shard would leave a crash window between the child's terminal commit and the parent's notify; here there is nothing in flight to lose, so a crash at any instant simply leaves the durable row for the next sweep.

### Consistency contract

- **Per-execution ACID stays shard-local.** The parent's decision transaction touches one database; so does the child's. There is no two-phase commit and no cross-shard join.
- **Cross-shard effects are at-least-once with dedupe** (the table above) — the same contract `enforce_external_signals_outbox` / `enforce_external_cancels_outbox` (#492) established.
- **Latency, not correctness, is the price.** A cross-shard child's start and its terminal wake are each one scanner tick away rather than one transaction away.
- **Placement never falls back silently.** A target shard this node has no pool for fails the spawn with the typed, retryable `HarvestError::ShardUnavailable`; the parent's decision cycle rolls back with nothing recorded and is re-driven once the shard is reachable. It is never quietly re-placed on the parent's shard.
- **Zero event-schema impact.** No new `WorkflowEvent` variant and no change to the adjacently-tagged JSON contract. The child's shard is recoverable from the `child_id` recorded in `ChildWorkflowStarted`, so a parent replays byte-identically regardless of where its children live.
- **Replay never re-resolves placement.** The router is consulted once, on the fresh dispatch; every later wake reuses the `child_id` already in history. This is what makes the byte-identical replay above true, and it also means a topology edit after dispatch — a shard removed, a residency mapping changed, a worker that has not installed its router yet — cannot fail a parent that is merely replaying.
- **Restore every shard to the same instant.** A point-in-time restore that rolls shards back to *different* instants can leave a child alive on a target shard whose parent-side `ChildWorkflowStarted` and lifecycle row were rolled away — an orphan the relay cannot see, because it is driven entirely off that row. `harvest backup verify` derives its child checks from parent-side events, so it does not currently catch the reverse direction either (tracked in issue #1263). The same hazard exists for same-shard children; more databases simply make skew easier to produce. Restoring all shards to one common instant avoids it entirely.

### Operating it

`harvest_cross_shard_children` is the in-flight gauge:

```sql
-- In-flight cross-shard children on this shard, by target and lifecycle state.
SELECT target_shard, status, cancel_requested, count(*)
FROM harvest_cross_shard_children
GROUP BY 1, 2, 3
ORDER BY 1, 2;

-- Rows that are not making progress name their own last failure.
SELECT child_exec_id, target_shard, attempts, last_attempt_at, last_error
FROM harvest_cross_shard_children
WHERE attempts > 0
ORDER BY attempts DESC
LIMIT 20;

-- Oldest still-unsettled child, per target shard: the backlog-age signal.
SELECT target_shard, min(created_at) AS oldest, count(*) AS in_flight
FROM harvest_cross_shard_children
GROUP BY 1
ORDER BY 2;
```

Every settled child deletes its row, so a steadily growing count means the relay is not draining — check `last_error` for an unreachable target shard first. `attempts` is written on **every** non-progress path (a failed step, an unreadable target shard, an unrecognised stored value), and it also drives the retry backoff: a row is re-tried after `min(attempts, 6) x 5s`, so one permanently-broken row backs off instead of consuming a slot in every sweep.

There is **no dedicated metric** for the relay yet; the queries above and the `Timeout` scanner's existing `harvest.scanner.tick` liveness are the current signals. A `harvest.cross_shard_child.*` counter family is tracked in issue #1263, along with the payload-offloader gap on the relayed child's start event and the retention-window question for undelivered terminals. Per-execution metrics are unaffected: the relay threads the scanner's real recorder into its cancel, terminate and quota-admission calls, so `harvest.workflow.terminal` counts do not depend on where a child was placed.

### Reading a cross-shard tree

`GET /workflows/{id}/children` and `GET /workflows/{id}/tree` already traverse every shard, so a cross-shard child is visible without any new endpoint. Both now degrade rather than `500` when a shard is unreachable: they return the children they could see and name the rest in `unavailable_shards`, with `status` dropping to `partial` — the #756 contract, which cross-shard placement makes routine rather than exotic.

> **Behaviour change for existing `/children` callers.** Before this, an unreachable shard produced a `500`. Now it produces a `200` whose `items` array is *incomplete*. A client that treated `200` as "this is the whole child list" must read `status` (or check `unavailable_shards` is empty) before drawing that conclusion. The fields are additive; the status code is the part that changed.

The parent **detail** view (`GET /workflows/{id}`) resolves no children — it returns the execution row's own `parent_id` and nothing else — so there is nothing there to degrade.

## Shard rebalancing — migrating quiescent workflows (issue #964)

Before this, the sharding contract ended at *"cross-shard rebalancing of existing
workflows is out of scope"*. That had two consequences that compound over time:
adding a shard only helps **new** starts, so a hot shard stays hot for as long as
its residents live — forever, for a continue-as-new entity workflow — and a shard
can never be decommissioned, because there is no way to move its residents off.

`harvest shard rebalance` is the operator-initiated primitive that fixes both.
It is deliberately narrow: it moves **quiescent** executions only.

```
copy ──▶ replay-verify ──▶ ONE atomic cutover commit ──▶ sealed source
```

Restricting to quiescent runs is the move that makes the rest provable. It
converts a live distributed-migration problem into a copy-verify-cutover of an
*inert, append-only event log* — exactly the artifact Harvest's replay engine
exists to verify — which is what removes the catch-up phase every online shard
move in the literature needs, and with it the dual-write and the 2PC.

The full design note, including the alternatives that were rejected and why, is
`docs/plans/2026-09-02-shard-rebalancing.md`.

### The identity contract: an `ExecutionId` never changes

**A migrated execution keeps its `ExecutionId`.** This is the load-bearing
decision and it is worth stating plainly, because it inverts what the first two
bytes mean:

> An `ExecutionId`'s encoded shard is the shard the run **originated** on — its
> routing entry point — not necessarily where it lives today.

Nothing that holds an id has to learn anything, because no id changed. A
parent's recorded `ChildWorkflowStarted.child_id`, a stored `WorkflowHandle`, an
external signal or cancel target, a webhook's stored reference, a schedule's
carryover lineage — all keep resolving, structurally rather than by enumeration.
There is no rewrite pass and no alias table.

Resolution is two-level:

| Level | Mechanism | Used when |
|---|---|---|
| 1 | **Origin-shard forwarding.** The sealed source row (`MIGRATED`) carries `migrated_to_shard`; an id-routed lookup lands on the origin shard as it always did and follows the pointer. | The origin shard is still readable — i.e. always, until it is decommissioned. |
| 2 | **Router-declared shard forwards.** `ShardRouter::with_shard_forwards([(retired, successor)])` maps a *removed* shard's ids straight to its successor. | After a decommission, when there is no origin database left to ask. |

Chains (A→B, then B→C) are followed up to `MAX_FORWARD_HOPS` (4) and then fail
closed with a typed, retryable error rather than looping. A completed migration
also best-effort repoints the **origin** shard straight at the new home, so
chains collapse rather than accumulate; correctness comes from following the
chain, performance from collapsing it.

### The two new execution states

| State | Where | Meaning |
|---|---|---|
| `MIGRATING` | target shard | A staged, inert copy. It holds the target's `(workflow_name, workflow_id)` active-uniqueness slot so nothing else can claim the identity mid-migration, and it has **no workflow task row at all**, so nothing can dispatch it. |
| `MIGRATED` | source shard | The sealed source after cutover: terminal-shaped, non-claimable, carrying the forwarding pointer. |

> **A deliberate deviation from the reset precedent.** Issue #964 suggests
> modelling the seal on the reset path's `TERMINATED`, "which already releases
> the uniqueness index". A migration must **not** do that. A reset forks a
> successor on the *same* shard, so its source has to release
> `(workflow_name, workflow_id)` or the successor could not be inserted. A
> migration puts the copy on a *different database*, whose index is its own — so
> nothing needs releasing, and releasing would be a correctness bug: a later
> start of the same business key still hashes back to the source shard, would
> find no active row, and would create a **second live run** alongside the
> migrated one. `MIGRATED` therefore stays inside the active-uniqueness index,
> and such a start fails closed instead.

`MIGRATED` is a **seal, never a delete**. The source keeps its full history, so
an audit of where a run used to live survives the move — and so does the
forwarding pointer an id resolves through. Do not purge a `MIGRATED` row while
any pre-migration id might still be held.

### What "quiescent" means, precisely

An execution is migratable only when every one of these holds. Each has a named
blocker, so a dry run explains itself completely rather than one reason at a
time.

| Requirement | Why |
|---|---|
| `state = 'RUNNING'` | Terminal runs have nothing to move. |
| No parent (`parent_id IS NULL`) | A child's terminal appends to its parent's history in a shard-local transaction; moving the child would break that edge. **Root executions only.** |
| No claimed workflow task | A worker is mid-cycle. |
| No workflow task dispatchable *now* | A wake is pending delivery. |
| At most one *parked* workflow task, with `wake_requested = false` | This is the migratable shape — see below. |
| No non-terminal activity or external tasks | In-flight work is out of scope by design. |
| No unconsumed signals | An undelivered wake. |
| No `INFLIGHT` completion deliveries | An outbound delivery must settle first. |
| No `ACTIVE` worker sessions | Session state lives on exactly one worker. |
| No live children, same-shard or cross-shard | Same reason as the parent rule. |
| Not non-determinism-blocked | A re-dispatch backoff is pending. |
| Holds no durable mutex (`ctx.mutex`, #691) | The lock row is shard-local and keyed by the holder; moving the holder away leaves the key held by an execution that no longer lives here, and every waiter on it blocked forever. |
| Queued for no durable mutex | The grant is delivered by waking the waiter **on this shard**, so a migrated waiter's grant would be delivered to a sealed row — a lost wake. |
| No dead-letter rows | Redriving a DLQ entry enqueues a task on this shard, which after a migration would target a sealed row. Redrive or discard it first. |
| No live children, **including paused ones** | A paused child still completes eventually, and its terminal appends to the parent's history on the shard the parent used to be on. |

**A run waiting on a timer or a signal IS migratable — that is the point.** Both
shapes keep a workflow task row: a timer park is `PENDING` with a future
`scheduled_at`, and a signal park is `RUNNING` with no worker
(`queue::park_workflow_task`). A predicate of "no task rows" would have refused
exactly the long-lived population this feature exists to move, so the parked row
is **copied**, not treated as a blocker.

A **paused** execution is *not* migratable. `PAUSED` is a state, not a column,
and the predicate admits `RUNNING` only — so a paused root reports `NotRunning`
and is skipped. Resume it, migrate it, and pause it again; the decommission
runbook's straggler table says so. (Widening the predicate would mean the copy
and the cutover carrying the original state through activation, which is real
work for a case an operator can resolve in one command.)

### The wake contract: never lost, never doubled

| When the wake arrives | What happens |
|---|---|
| Before or during the copy | The cutover re-evaluates the **full** quiescence predicate inside its own `UPDATE ... WHERE`, so it matches zero rows. The migration aborts, the source is untouched, and the wake is processed there normally. |
| After the cutover | The source is sealed, so the write resolves through the forwarding pointer to the target. Activation notices the pending signal and schedules the restored task at `NOW()` rather than leaving it waiting on a timer days out. |
| A keyed redelivery, after the move | Signals are copied **with** their `idempotency_key`, and the target enforces the same partial unique index, so a retried delivery collides exactly as it would have on the source. |

### Crash safety

The durable record is `harvest_shard_migrations`, on the **source** shard — the
database that stays authoritative right up to the cutover. Its phase says what is
outstanding:

| Phase at crash | Resume action | Source authoritative? |
|---|---|---|
| `PENDING` | re-stage (the inert target copy is discarded and re-made) | yes |
| `COPIED` | verify | yes |
| `VERIFIED` | re-check quiescence, then cut over | yes |
| `COMMITTED` | activate the target (idempotent) | **no** — the target is |
| `DONE` / `ABORTED` | retire the row | — |

Only `VERIFIED → COMMITTED` changes who is authoritative, and it is a single
statement on one database. Run `harvest shard rebalance-resume` after any crash.

> **The one honest gap.** Between the cutover commit and the target's activation
> the run is claimable on *neither* shard. That is a **liveness** gap, not a
> correctness one: the run is authoritative on exactly one shard at every
> instant, and claimability follows within one resume step. Closing it entirely
> would need a two-phase commit across two databases, which the sharding design
> rules out.

### What migrates, and what does not (the dedupe scopes)

| Mechanism | Migrates? | Accepted window |
|---|---|---|
| Full event history | ✅ verbatim | none — byte-identical and replay-verified before cutover |
| Execution row (every column) | ✅ verbatim | none |
| Durable timers | ✅ verbatim, including `fires_at` | none |
| Signals, including `idempotency_key` | ✅ verbatim | none |
| Payload refs (`harvest_payload_refs`) | ✅ | none — the blob store is shard-external |
| The parked workflow task row | ✅ | none |
| Terminal task rows (activity history) | ❌ stays on the sealed source | none — the durable record is in `harvest_events` |
| Workflow logs | ✅ | none — copied for a privacy reason as much as an operational one: they are free text, so a PII sink, and `erase.rs` scrubs them via the execution's own shard. Logs left behind would be out of reach of an erasure issued against the run. |
| Start idempotency keys | ❌ | none for an existing run: a keyed start still hashes to the same shard |
| Debounce rows | ❌ | at most one extra debounced start per key immediately after the move |
| Start-throttle tokens, rate-limit buckets | ❌ | already per-shard; unaffected for an existing run |
| Concurrency-key accounting | moves implicitly with the parked task row | the run's slot moves from A's cap to B's; the documented `limit × N` scope is unchanged |
| Completion-trigger fire ledger | ❌ | a trigger whose source run migrated may fire once more; triggers are already at-least-once |
| Audit log | ❌ | the **source** shard records every attempt in its own `harvest_audit_log`. Note the consequence for a decommission: retiring the shard retires its audit trail, so ship audit off-box (issue #953's export) before step 5 of the runbook. |

### Zero new event variants; the append-only invariant is untouched

A migration appends **nothing** to `harvest_events` and rewrites **nothing**. The
copy `INSERT`s new rows on a *different* database; the source's rows are read-only
throughout. Shard rebalancing is therefore **not** a fourth exception to the
append-only invariant in `CLAUDE.md` — it is an instance of it. All bookkeeping
lives in columns and in `harvest_shard_migrations`.

### Using it

Always dry-run first. The dry run walks the same code path up to the first write
and reports exactly the population a real run would move — it is not a separate
estimator that could drift.

```bash
# What would move?
harvest shard rebalance \
  --shard 0=postgres://.../harvest_shard0 \
  --shard 1=postgres://.../harvest_shard1 \
  --from 0 --to 1 --limit 100 --dry-run

# Do it, with an operator identity for the audit trail.
harvest shard rebalance \
  --shard 0=postgres://.../harvest_shard0 \
  --shard 1=postgres://.../harvest_shard1 \
  --from 0 --to 1 --limit 100 --actor alice@example.com

# After any crash or interruption.
harvest shard rebalance-resume \
  --shard 0=... --shard 1=... --from 0
```

The command connects to the shard databases **directly** rather than through the
management API: a rebalance is inherently a two-database operation and the node
serving the API has no reason to hold a pool for both. Possession of both DSNs is
the admin gate, and every non-dry-run attempt writes a
`shard.rebalance.migrate` row to the source shard's `harvest_audit_log` naming
the actor, the execution, and the outcome.

**Run the migration before deploying the binary.** `db_conn_for_execution` now
performs a forwarding lookup on multi-shard deployments, so a node running the
new binary against a database that has not yet gained `migrated_to_shard`
answers `503` on every single-execution management route. This is the ordinary
migrate-then-deploy order; it is called out because the pre-#964 code path
issued no query at all and so had no such requirement.

**A note on payload codecs.** `harvest shard rebalance` verifies with the
default (identity) codec registry, because the CLI has no key material and
putting decryption keys on an operator's workstation is not a default worth
having. That does not weaken the guarantee: the load-bearing half of
verification is the **raw byte-identity** check over the stored event tuples,
which is codec-independent. On a deployment using keyed codecs (issue #948) the
decode-and-replay half will refuse with an unknown-key error, so the migration
fails closed rather than proceeding unverified. Drive rebalancing from a process
that has the registry installed if you need both halves.

Both shards must be at the **same migration level**. The copy is deliberately
column-list-free (`to_jsonb` on the source, `jsonb_populate_record` on the
target) so a newly-added column is carried automatically rather than silently
dropped by a hand-maintained list — and the converse hazard, a target that lacks
a column and would silently discard it, is refused up front with a schema-parity
check.

### Decommissioning a shard — runbook

See `docs/runbooks/shard-decommission.md` for the full drill.

---

## Operational Checklist

### Enabling per-key concurrency on an existing deployment

1. Deploy the binary with `#[workflow(concurrency(...))]` attributes.
2. Existing in-flight executions continue without any per-key cap — their task queue rows have `concurrency_key IS NULL` and the claim query skips the cap check for them.
3. New executions started after the deploy get the cap applied immediately.
4. No migration is required — the `concurrency_key` and `concurrency_cap` columns already exist in `harvest_task_queue` (added in migration `20260429000000_harvest_concurrency_key`).

### Observing per-key in-flight counts

The management API exposes live stats:

```
GET /admin/concurrency
```

Response:
```json
[
  {
    "key": "tenant:acme",
    "max_concurrent": 10,
    "in_flight": 7,
    "pending": 23
  }
]
```

The `pending` field shows how many tasks are currently deferred because the key is at or above its cap.

Each row also carries a `workflows` array attributing the key to the workflow type(s) with live tasks on it, and the effective overflow strategy each declares (issue #811). It is omitted when empty:

```json
[
  {
    "key": "doc-a",
    "task_type": "workflow",
    "max_concurrent": 1,
    "in_flight": 1,
    "pending": 0,
    "workflows": [{ "workflow_name": "doc_index", "on_conflict": "cancel_running" }]
  }
]
```

A workflow whose registered `WorkflowInfo` declares no concurrency policy reports `"defer"` — the same value the start path resolves for it. Superseded runs are counted on `harvest.concurrency.superseded{workflow}`; the concurrency key is deliberately **not** a metric label (unbounded tenant input, per ADR-0001 §7), which is exactly why the per-key view lives here.

The `harvest.concurrency.in_flight` metric (tagged by concurrency key) is emitted by the concurrency sampler at regular intervals. **Note**: the raw key value is tagged on the metric — ensure your key cardinality is bounded (e.g., use `tenant_id`, not `execution_id`) to avoid metric cardinality explosion. See ADR-0001 §7.

### Historical per-tenant usage report (issue #596)

`GET /admin/concurrency` above is strictly **point-in-time**: it answers "how many tasks are in flight for this key right now?" It cannot answer the question every multi-tenant embedder eventually asks finance: "how much did tenant `acme` actually consume last month?" ADR-0001's cardinality rule deliberately keeps `MetricsRecorder` low-cardinality, so that answer cannot come from Prometheus either.

`GET /admin/usage` is the **historical companion** to `/admin/concurrency` — and the supported alternative to querying `harvest_workflow_executions` / `harvest_events` directly, which is an internal schema this project does not guarantee stability for. It aggregates already-durable data over a caller-supplied time window, computed read-only, with no new `WorkflowEvent` variant and no migration.

```
GET /admin/usage?from=2026-06-01T00:00:00Z&to=2026-07-01T00:00:00Z&group_by=search_attr:tenant_id
```

`from` and `to` are required (RFC 3339 timestamp or a relative duration like `24h`, measured back from now). `group_by` defaults to `workflow_name`; pass `search_attr:<key>` to bucket by a tenant key already carried in `search_attrs` (e.g. `search_attr:tenant_id`).

Response:
```json
{
  "status": "complete",
  "from": "2026-06-01T00:00:00Z",
  "to": "2026-07-01T00:00:00Z",
  "group_by": "search_attr:tenant_id",
  "groups": [
    {
      "group": "acme",
      "workflow_starts": 4210,
      "completed": 4102,
      "failed": 58,
      "cancelled": 12,
      "timed_out": 3,
      "activity_executions": 51820,
      "activity_executions_failed": 340,
      "activity_compute_seconds": 918422.7
    }
  ],
  "unavailable_shards": []
}
```

Executions that lack the requested `search_attrs` key are grouped under the literal group `"(unattributed)"` rather than silently dropped.

**Metric semantics** (each unit is counted exactly once, in the window it actually occurred — the chargeback-consistent choice):

- `workflow_starts`: executions whose `started_at` falls in `[from, to]`.
- `completed` / `failed` / `cancelled` / `timed_out`: derived from the durable terminal events (`WorkflowCompleted`/`WorkflowFailed`/`WorkflowCancelled`/`WorkflowExecutionTimedOut`) whose timestamp falls in `[from, to]`, not the mutable execution row — so a DLQ redrive that clears a `FAILED` row's state never erases a historical failure. `cancelled` additionally excludes a `WorkflowCancelled` event whose execution ended up sealed `TERMINATED` by a genuine `terminate` (which reuses the same event type as a real cancel) — but *not* one sealed `TERMINATED` by a later reset/DAG-retry, which always appends its own `WorkflowResetTerminated` marker, so a reforked-for-retry run doesn't lose its historical cancellation either. `TERMINATED` and `CONTINUED_AS_NEW` are not broken out separately.
- `activity_executions`: count of dispatch attempts (`ActivityStarted` events) in the window — retries reuse the same activity id but each attempt appends a fresh event, so a 3-attempt activity contributes 3.
- `activity_executions_failed`: count of terminal `ActivityFailed`/`ActivityTimedOut` events in the window that have a matching `ActivityStarted` (non-final retry attempts emit no event, so this counts exhausted-retry-or-timeout only). The start-match requirement excludes external activities, whose `ActivityTimedOut` can be appended with no `ActivityStarted` at all.
- `activity_compute_seconds`: for each activity whose terminal event falls in the window, the wall-clock span from that activity's most recent (final-attempt) start to its terminal event, summed. Retry backoff wall time is excluded by construction.
- Local activities and externally-completed activities are excluded from the activity counters — they never emit `ActivityStarted`, so they're not worker compute.

**Window ceiling**: a `from`/`to` window wider than a configurable ceiling (default 90 days, `HarvestApiState::set_usage_window_ceiling`) is rejected with `400`, naming the ceiling, so an operator cannot accidentally trigger a full-table scan across every shard.

**Shard-aware, no rollup**: like `/admin/concurrency`, `/admin/usage` fans out across every shard and merges. Unlike `GET /workflows/count`, it does **not** roll a long tail into an `other` bucket — a chargeback report that silently drops low-volume tenants would be actively wrong. Choose a bounded-cardinality `group_by` key (a tenant id, not an execution id).

CLI:
```
harvest usage --from 2026-06-01T00:00:00Z --to 2026-07-01T00:00:00Z --group-by search_attr:tenant_id
```
Renders a table by default; pass `--json` for piping.

### Adding a shard to a deployment that uses per-key concurrency

Follow the standard add-a-shard procedure in `CLAUDE.md`. The new shard starts with no task queue rows, so the cap is independent from day one. If you need to migrate in-flight workflows to the new shard, that is out of scope (cross-shard rebalancing is not supported).

---

## Debounce coordination is shard-local (issue #499)

Debounce pending-start records (`harvest_debounce`) are routed to the same shard as the debounce key (via `ShardRouter`), matching the per-key concurrency scope above. All burst admissions for the same `(workflow_name, debounce_key)` pair must land on the same shard for the `UNIQUE (workflow_name, debounce_key)` upsert collapse to work. Cross-shard global debounce coordination is **out of scope**: embedders requiring a global cap should ensure all executions for a given debounce key route to a single shard (see [Explicit shard placement and data residency](#explicit-shard-placement-and-data-residency-issue-697) above).

---

## Per-key activity rate limits are shard-local (issue #699)

Per-key **activity** rate-limit buckets (`dyn-rate:{key_expr}:{resolved}` in `harvest_rate_limit_buckets`, declared via `#[activity(rate_limit(key = "input.tenant_id", rps = …))]`) are enforced **within a shard**, exactly like the per-key concurrency limits and the workflow-start throttle above. An activity dispatched from a workflow runs on that workflow's own shard, so the token bucket for a given `(key_expr, resolved_value)` is consulted on that shard only. Buckets are never coordinated across shards.

**Consequence**: the effective per-tenant RPS across a multi-shard cluster is `per-shard-rate × number-of-shards-the-tenant's-workflows-land-on`. If a tenant's workflows spread across N shards via rendezvous hashing, that tenant's activities can run at up to N × the declared `rps`. Cross-shard global rate coordination is **out of scope** (it would require a coordination service or bounded-inaccuracy gossip, both of which conflict with the Postgres-native design). Embedders needing a true global per-tenant activity cap should route all of a tenant's executions to a single shard (see [Explicit shard placement and data residency](#explicit-shard-placement-and-data-residency-issue-697) above), so every one of that tenant's activities consults the same bucket.

---

## TTL'd pacing overrides apply per shard, fanned out to all of them (issue #945)

A runtime pacing override (`POST`/`DELETE /admin/rate-limits/{activity_name}/override` and `POST`/`DELETE /admin/start-throttle/{workflow_name}/override`) layers a temporary, self-expiring quota change on top of an **existing, statically-declared** rate limit or throttle. Because that underlying bucket is itself shard-local — each shard database holds its own independent `harvest_rate_limit_buckets` row for the same bucket key — a single override call **fans out and writes the same override to every shard**, mirroring the pre-existing `set_rate_limit`/`declare_compat` fan-out pattern (issue #332). This is what makes "one CLI call, effective everywhere in under a minute" true for a multi-shard deployment: the operator does not need to know which shards a given activity/workflow's traffic lands on.

**Consequence**: a partial fan-out failure (one shard's database unreachable while the others succeed) can leave the override live on some shards and absent on others until retried — the response reports per-shard failures (`shard_errors`) and the audit record for that call is marked failed when any shard did not receive the write, so this state is never silent. If every shard fails, the call returns `503` with no override applied anywhere. Because expiry is enforced by each shard reading its own row's `override_expires_at > NOW()` independently, there is no cross-shard coordination of *when* an override reverts — every shard's copy reverts on its own clock at the same wall-clock instant, with no drift beyond ordinary clock skew between shard hosts.

**Dynamically per-key-keyed policies are out of scope.** A pacing override targets a *single* declared bucket key. A rate limit declared with a dynamic per-key key expression (`#[activity(rate_limit(key = "input.tenant_id", ...))]`, issue #699 above) or a throttle declared with a `key_expr` fans out into many independent per-tenant buckets at runtime — there is no single bucket for an override to target, so `POST .../override` against such a policy is rejected `409 Conflict` rather than silently doing nothing or overriding an arbitrarily-chosen tenant's bucket.

---

## Durable mutex is shard-local (issue #691)

The durable named mutex `ctx.mutex(key).acquire()` (the `harvest_mutex_locks` / `harvest_mutex_waiters` tables) is enforced **within a shard**, exactly the same scope as the per-key concurrency limits above. The lock table, the FIFO waiter queue, and the `pg_advisory_xact_lock` that serializes each acquire/release/reclaim all live on the workflow's own shard, so "at most one holder per key" holds only among executions that resolve to that shard.

**Consequence**: two workflows that must serialize on a shared resource must land on the **same** shard for the mutex to actually serialize them. Route all contending executions for a given lock key to a single shard (see [Explicit shard placement and data residency](#explicit-shard-placement-and-data-residency-issue-697) above) — e.g. derive the mutex key from the same field you shard on. Cross-shard global mutual exclusion is **out of scope** for the same Postgres-native reasons as cross-shard concurrency and rate limits.

---

## Per-tenant resource quotas are shard-local (issue #946)

Declared per-tenant quotas (`#[workflow(quota(key = "input.tenant_id", max_active_executions = 100, max_history_bytes = …, max_dead_letters = …))]`, or `WorkflowInfo::with_quota(QuotaPolicy)`) are enforced **within a shard**, the same scope as the per-key concurrency limits, the workflow-start throttle, per-key activity rate limits, and the durable mutex above. The `quota_key` resolved at start time (via the identical `resolve_concurrency_key` dot-path resolver #247 already uses — there is no second resolver) is stamped on `harvest_workflow_executions.quota_key`; usage — the `RUNNING`/`SUSPENDED`/`PAUSED` execution count, the aggregate `harvest_events` payload bytes, and the DLQ row count for that key — is computed with a single indexed query **against the workflow's own shard only**, inside the same admission transaction the `FOR UPDATE`-locked start already runs. A cross-shard execution row for the same tenant key is invisible to the check.

**Consequence**: a tenant whose workflows spread across N shards via rendezvous hashing effectively gets `declared-limit × N` of each resource fleet-wide, not the declared limit globally. This is the exact "`per-shard-limit × shards`" behaviour the per-key concurrency and rate-limit sections above already document, applied to quotas. Cross-shard coordinated quotas are **explicitly out of scope** (per issue #946) for the same Postgres-native reasons as every other per-key primitive on this page: a genuinely global cap would require a coordination service or a bounded-inaccuracy gossip protocol, both of which conflict with the shard-local, no-cross-shard-transaction design this engine is built around.

**Achieving a true global cap** for a tenant's aggregate resource footprint requires routing every one of that tenant's executions to a **single** shard — see [Explicit shard placement and data residency](#explicit-shard-placement-and-data-residency-issue-697) above (derive the same `residency_key`/`shard_id` you pin on from the same tenant field the quota's `key` expression resolves). With all of a tenant's executions confined to one shard, that shard's quota check sees every one of them and the declared limit is the tenant's true fleet-wide ceiling.

**Adding a shard** to a deployment using per-tenant quotas follows the same procedure as the [per-key concurrency section](#adding-a-shard-to-a-deployment-that-uses-per-key-concurrency) above: a tenant whose workflows begin landing on the new shard (because it entered `writable_shards`) gets a *fresh* quota budget on that shard, independent of whatever usage already exists on its prior shard(s), until the rebalancing settles.

---

## Cross-shard keyset pagination for `GET /workflows`

When `page_size` (or `cursor` / `order`) is present on a `GET /workflows` request, the engine performs a **k-way merge** across all shards so the caller sees a single globally-ordered result set without knowing which shard each execution lives on.

### Per-shard query

Each shard receives the same keyset predicate and fetches **`page_size + 1`** rows:

```sql
SELECT *
FROM   harvest_workflow_executions
WHERE  <state/workflow_name/time-range filters>
  AND  (created_at, id) < ($cursor_created_at, $cursor_id)  -- DESC direction
       -- or (created_at, id) > ($cursor_created_at, $cursor_id)  -- ASC direction
ORDER  BY created_at DESC, id DESC                           -- or ASC
LIMIT  $page_size + 1;
```

The `idx_harvest_we_created_id` index on `(created_at DESC, id DESC)` makes this an O(log n) index scan regardless of how deep into history the operator has paged.

### K-way merge

Results from all N shards are merged in memory by sorting on the same `(created_at DESC, id)` key. The merged list is then evaluated against the overflow probe:

- If `merged_len > page_size`: a next page exists. Truncate to `page_size`, encode the last kept row as `next_cursor`.
- If `merged_len <= page_size`: this is the last page. `next_cursor = null`.

### Row budget under N shards

Each shard contributes at most `page_size + 1` rows to the merge, so the total rows read across all shards is at most `N * (page_size + 1)`. With `page_size = 50` and `N = 4` shards that is at most 204 rows read to return 50. The full history is reachable across pages — no execution is ever skipped.

### Cursor correctness across shards

The cursor encodes a concrete `(created_at, id)` pair from the **global** top-`page_size` list, not a per-shard position. When that pair falls on shard 2, shards 0, 1, and 3 use the same `(created_at, id)` anchor to exclude rows they already returned — this is safe because `(created_at, id)` is globally unique across all shards (UUID `id` guarantees this). Inserting new rows mid-pagination on any shard does not cause duplicates or gaps: the keyset anchor is immutable once the cursor is issued.

### Filter interaction

`started_after` / `started_before` apply as `WHERE started_at` predicates on every shard before the keyset filter and ORDER BY, so both filters and pagination compose cleanly in a single per-shard query.

### Index

The additive migration `20260618000000_harvest_workflow_list_keyset_index` creates:

```sql
CREATE INDEX IF NOT EXISTS idx_harvest_we_created_id
    ON harvest_workflow_executions (created_at DESC, id DESC);
```

This index must be present on every shard for deep-page performance to remain flat. The migration is idempotent (`IF NOT EXISTS`) and is applied by the normal migration step (`autumn migrate`, or `harvest migrate run` against a dedicated Harvest database).

## Cross-shard typed search-attribute predicates (`search_attr_filter`, issue #506)

The `search_attr_filter=key:op:value` predicates (comparison/set filtering over
`search_attrs`) are pushed down to **each shard's `WHERE` clause** and the
matching rows are k-way merged exactly as the keyset pagination path above — the
predicate is part of the shared per-shard query, so it composes with `state`,
time-range, `cursor`, `page_size`, and `order` with no extra round-trips and no
row duplicated or skipped under concurrent inserts.

### Per-shard SQL and index strategy

Every predicate stays on an index path rather than a full per-row scan by
reusing the existing `idx_harvest_we_search` GIN index
(`USING GIN (search_attrs)`, default `jsonb_ops`), which supports both `@>`
(containment) and `?` (key existence):

| `op` | Per-shard predicate | Index leg |
|------|--------------------|-----------|
| `eq` | `search_attrs @> '{"key": <typed>}'` | GIN `@>` |
| `ne` | `search_attrs ? 'key' AND NOT (search_attrs @> '{"key": <typed>}')` | GIN `?` narrows |
| `gt`/`gte`/`lt`/`lte` | `search_attrs ? 'key' AND jsonb_typeof(search_attrs -> 'key') = 'number' AND (search_attrs ->> 'key')::numeric <op> $val` | GIN `?` narrows, numeric recheck |
| `in` | `search_attrs ? 'key' AND search_attrs -> 'key' = ANY($vals::jsonb[])` | GIN `?` narrows |
| `exists` | `search_attrs ? 'key'` | GIN `?` |

The key is always a **bound** parameter (never string-interpolated), so dynamic
keys are injection-safe; the comparison operator literal comes from a fixed
internal enum. Because the existing GIN index already covers `@>` and `?`,
**no new index and no migration are required** — there is no column change to
`harvest_workflow_executions`, no `harvest_events` change, and no new
`WorkflowEvent` variant. This index must be present on every shard (it ships in
the initial `20260409000000_harvest_initial` migration).

### Typed coercion is shard-stable

The value→type coercion (number / boolean / string) is a pure function of the
predicate text, so every shard derives the identical typed comparison and the
merged result is deterministic. Numeric comparison matches only number-typed
stored values; a value stored as a string on one shard is excluded uniformly on
all shards, never a partial false match.

---

## Adding a Shard — Operational Runbook (issue #522)

Follow this procedure to add a new shard to a live deployment. Each step is safe to stop and retry.

### Step 1 — Provision and migrate

Provision a new Postgres database and run migrations against it:

```bash
export HARVEST_DATABASE_URL=postgres://user:pass@new-shard-host/harvest
harvest migrate run
```

Pass the DSN through the environment, not `--database-url`: a command line is
visible to every process on the host (`ps`, `/proc`) for as long as the
migration runs, and a shard DSN carries a password.

Harvest's migrations are embedded in the `harvest` binary, so this needs no
source tree. Add `--include-dir` for any set that is not (the plugin's
connector dead-letter table, an application's own); see
[Migrations](getting-started/10-operations.md#migrations).

### Step 2 — Add to readable_shards

Add the new shard to `readable_shards` (but **not** `writable_shards`) and deploy. The router can now resolve IDs that encode the new shard; nothing writes there yet.

### Step 3 — Wait for readiness gate

Run the shard health check against the new shard:

```bash
harvest shard health --candidate-shard <shard_id>
# or
GET /admin/shards/health?candidate_shard=<shard_id>
```

Wait for `readiness: "ready"`. A `degraded` row includes machine-readable `reason_codes` explaining what is blocking readiness. Three codes are relevant here:

| `reason_code` | Meaning | Resolution |
|---|---|---|
| `no_live_worker` | The shard is `Writable` and has claimable tasks, but **no live worker** covers this shard. | Widen a worker's coverage and redeploy — either add the shard to its `shard_assignments`, or remove the explicit `shard_assignments` narrowing entirely so "auto" coverage applies (issue #961). Verify with `GET /admin/config` → `worker.shard_assignments`. |
| `worker_queue_uncovered` | No healthy worker covers a required queue on this shard. | Same as above — check queue bindings. |
| `schema_migration_missing` | The shard is missing required migrations. | Re-run `harvest migrate run` against the shard (DSN via `HARVEST_DATABASE_URL`). |

The `no_live_worker` gate is the primary pre-flip readiness gate for issue #522: until at least one `Healthy + Active` worker lists the new shard in its `shard_assignments`, the shard will not report `ready`. This prevents silently stranding work on the new shard.

### Step 4 — Flip writable and verify

Add the new shard to `writable_shards` and deploy. The fleet **automatically drains the newly-writable shard** — no operator intervention is needed. Workers that cover the shard will claim and dispatch tasks from it within one `poll_interval`.

Shard coverage is configured in **Rust**, not in `autumn.toml` — there is no
`[harvest.worker] shard_assignments` key. Multi-shard also runs through
`HarvestRunner` only: `HarvestPlugin` rejects a multi-shard pool by design, so a
plugin-hosted app is always single-shard.

```rust
use autumn_harvest::types::ShardId;

// Explicit coverage — this worker polls exactly shards 0 and 1.
WorkerConfig::default().with_shard_assignments([ShardId::new(0), ShardId::new(1)])

// ...or omit the call entirely for "auto": cover every shard this process has
// a pool for. A worker left on auto picks the new shard up on redeploy with no
// code edit at all (issue #961).
WorkerConfig::default()
```

The pool itself is supplied to the runner:

```rust
HarvestRunnerResources::new(harvest_pool).with_sharded_pool(sharded_pool)
```

Once flipped:
- New workflows begin landing on the shard via rendezvous hash.
- In-flight workflows on existing shards continue draining through their own worker tasks.
- Workers covering both shards poll each shard's pool independently on every tick (round-robin, so a deep backlog on one shard cannot starve the other), preserving per-shard ACID locality.

#### Verify coverage — don't assume it (issue #961)

"New workflows begin landing on it" is only true if a live worker actually *covers* the shard. Run all three checks after the flip; each is falsifiable, none requires reading source:

**1. The worker's *effective* assignments include the shard.**

```bash
curl -s .../api/harvest/admin/config | jq '.worker.shard_assignments'
# => [0, 1]   # must contain the new shard id
```

`GET /admin/config` reports the **resolved** list, not the raw config value — so a worker left on auto (`shard_assignments` empty or absent) shows the concrete shards it will poll, and an explicit list that forgot the new shard is visible as a missing id rather than as silence. `HarvestRunner` also logs a `tracing::warn!` at boot naming any writable router shard the worker does not cover.

**2. Dispatch is actually happening on the shard.**

```promql
sum by (shard) (rate(harvest_shard_dispatched_total{shard="1"}[5m])) > 0
```

`harvest.shard.dispatched{shard}` increments once per dispatched task, per shard — the shard-dimension twin of `harvest.queue.dispatched{queue}`. A flat zero on a shard that has work is the signal that the poll loop is not reaching it. (Zero on an idle shard with no work is expected and benign — pair it with check 3.)

**3. Nothing is stranded.**

```promql
harvest_shard_stranded_pending{shard="1"} == 0
```

See *Observing stranded work* below. The `harvest_shard_undrained` starter alert fires on exactly the combination that matters: claimable work on a shard with no live poller.

### Observing stranded work

The `harvest.shard.stranded_pending` gauge (emitted by the stranded-work sampler on each worker) shows per-shard claimable task counts for shards that have **no live covering worker**:

```promql
harvest_shard_stranded_pending{shard="1"} > 0
```

A non-zero value means tasks are queued on that shard but no worker is draining them. Healthy steady state is `0` on all shards. Use this as an alerting signal: if it stays non-zero for more than `2 × poll_interval`, check the fleet's *effective* coverage with `GET /admin/config` → `worker.shard_assignments` (see [Verify coverage](#verify-coverage--dont-assume-it-issue-961)).

### Pre-flip checklist

Before adding a shard to `writable_shards`:

- [ ] `GET /admin/shards/health?candidate_shard=<id>` returns `readiness: "ready"` for the new shard.
- [ ] No `no_live_worker` reason code present (at least one live worker covers the shard).
- [ ] `harvest.shard.stranded_pending{shard="<id>"}` is `0` (no backlog from prior test writes).
- [ ] Schema migrations are applied (`schema_migration_missing` absent).
- [ ] `GET /admin/config` → `worker.shard_assignments` contains the new shard id (or the worker is on auto and its pool includes the shard).
- [ ] The `harvest_shard_undrained` starter alert is installed, so a post-flip coverage regression pages rather than silently stranding work.

A `readiness: "degraded"` result with `no_live_worker` in `reason_codes` is the engine's way of saying: "flip cancelled — no worker will claim work on this shard."
