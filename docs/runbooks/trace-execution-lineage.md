# Trace a failed descendant from a root alert

Use this runbook when a DLQ, stalled-workflow, or SLA alert hands you a single
execution id belonging to a **saga or fan-out** workflow — one that spawns
children which themselves spawn grandchildren — and you need to find *which
descendant actually failed*.

Before `GET /workflows/{id}/tree` (issue #621), the only relationship primitive
was `GET /workflows/{id}/children`, which returns **direct** children. Chasing a
4-level saga meant hand-walking `parent_id` links level by level — and because a
spawned child gets a fresh rendezvous-hashed `ExecutionId`, it can land on a
**different shard** than its parent, so a single-shard `parent_id` query silently
misses cross-shard children. The tree endpoint does that walk for you, across
every readable shard, in one call.

## 1. Is anything under here broken at all?

Start with the roll-up. It answers "does this family contain a failure?" without
transferring the whole tree:

```bash
curl -s "$HARVEST/api/harvest/workflows/$EXEC_ID/tree?summary=true" | jq
```

```json
{
  "root": {
    "execution_id": "0000...aaa",
    "workflow_name": "checkout_saga",
    "workflow_id": "order-42",
    "state": "RUNNING"
  },
  "counts": {
    "running": 3, "failed": 1, "completed": 12,
    "cancelled": 0, "timed_out": 0, "terminated": 0,
    "paused": 0, "continued_as_new": 0
  },
  "total_descendants": 16,
  "max_depth_reached": 3,
  "truncated": false,
  "status": "complete"
}
```

Or from the CLI:

```bash
harvest workflow tree "$EXEC_ID" --summary
```

Two things to read carefully:

- **`counts` are *descendant* counts** — the root's own state is reported
  separately on `root.state` and is **not** included. So `counts.failed == 0`
  with `root.state == "FAILED"` means the root itself failed but its children
  are healthy.
- **Every known state key is always present**, at `0` when absent, so
  `.counts.failed > 0` is safe to assert without a presence check.

## 2. Find the failed descendant

If the roll-up shows a failure, pull the tree and filter to it:

```bash
harvest workflow tree "$EXEC_ID"
```

```
RUNNING      0000...aaa  checkout_saga (order-42)
  └─ COMPLETED   0000...bb1  reserve_inventory (inv-42)
  └─ RUNNING     0000...bb2  fulfil_shipment (ship-42)
    └─ FAILED      0000...cc9  charge_card (charge-42) [detached:request_cancel]
```

Or, straight to the id with `jq`:

```bash
curl -s "$HARVEST/api/harvest/workflows/$EXEC_ID/tree" \
  | jq -r '[.root] | [recurse(.children[]?)] | .[] | select(.state=="FAILED") | .execution_id'
```

Then drill into that id with the single-execution surfaces:
`GET /workflows/{id}` (error detail), `/stack` (what it is blocked on),
`/awaitables` (open waits), `/timeline` (where the time went).

## 3. Understand the spawn topology

Each node carries the spawn relationship, which changes what a parent's closure
does to it:

- `await_mode: "awaited"` — the parent **suspended** on this child. It is not
  covered by the parent-close cascade.
- `await_mode: "detached"` + `parent_close_policy` — the parent did **not**
  suspend. When the parent closes, `request_cancel` / `terminate` / `abandon`
  decides this child's fate (issue #347).

A `FAILED` detached child under a still-`RUNNING` parent is the classic
"nobody noticed" case this view exists to surface.

## 4. Re-root at a mid-tree node

The endpoint takes **any** node, not just a top-level root. Rooting at a
mid-tree node returns just the subtree below it (the chosen root is always
`depth: 0` and still reports its own `parent_id`, so you can see you are inside a
larger tree):

```bash
harvest workflow tree 0000...bb2   # only fulfil_shipment and below
```

## 5. When the answer is truncated

The walk is bounded by `max_depth` (default 20, max 50) and `max_nodes`
(default 1000 **including** the root, max 10000). Hitting either is never
silent:

```json
{
  "truncated": true,
  "truncation_reason": "max_nodes",
  "truncated_parent_ids": ["0000...bb2", "0000...bb7"],
  "truncated_parents_capped": false,
  "limits": { "max_depth": 20, "max_nodes": 1000 }
}
```

`truncated_parent_ids` names the nodes whose subtrees were dropped — **re-root
the call at one of them** to continue the walk. Only parents that *provably*
have children are listed (a bounded existence probe confirms it), so you are
never sent chasing a subtree that does not exist.

Raise a bound when you need more:

```bash
harvest workflow tree "$EXEC_ID" --max-depth 30 --max-nodes 5000
```

Out-of-range bounds are rejected with `400` naming the offending parameter
rather than silently clamped — the `limits` echo always reflects what actually
ran.

> On a `max_nodes` truncation the named list can be *incomplete*: a parent whose
> children all fell beyond the per-shard fetch window is not observed.
> `truncated: true` is the authoritative "this tree is incomplete" signal;
> `truncated_parent_ids` is the actionable, bounded subset.

## 6. Reading a partial answer

The tree is assembled by reading each shard independently. Two consequences:

- **An unreachable shard degrades, it does not fail** (issue #756). You get
  `200` with `status: "partial"` and the shard named in `unavailable_shards`,
  plus every reachable shard's rows — a triage read must not `500` during
  exactly the incident it exists to diagnose. Treat a `partial` tree as a
  **lower bound**: descendants may exist on the shard that could not be read.

  ```json
  { "status": "partial",
    "unavailable_shards": [{ "shard_id": 1, "reason": "database connection for shard 1 could not be acquired" }] }
  ```

  The CLI prints a `PARTIAL (...)` block naming each unavailable shard.

- **The snapshot may be marginally time-skewed across shards.** There is no
  cross-shard transaction: a child on shard B is read a few milliseconds after
  its parent on shard A, so a child that terminated in that window can appear
  `RUNNING` (or a child spawned in that window can be missing) relative to a
  strictly-simultaneous read. This is accepted for incident triage — re-run the
  call to confirm anything that looks contradictory.

## Scope

Read-only and point-in-time. It performs no writes, appends no events, and adds
no `WorkflowEvent` variant or migration. To *act* on a subtree use the batch
surfaces (`POST /batch-operations`, issue #533) or per-execution
cancel/terminate (#504); for activity-level detail inside one execution use
`/stack` (#503) and `/timeline` (#739).
