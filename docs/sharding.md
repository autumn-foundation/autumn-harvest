# Harvest Sharding Guide

## Overview

Harvest can spread workflow state across N independent Postgres databases (shards). A single workflow's event log, task queue rows, timers, signals, and DLQ entries all live on the same shard, so per-workflow ACID guarantees are preserved without cross-shard transactions.

For the full sharding architecture, see `CLAUDE.md` §Sharding and `autumn-harvest/src/shard.rs`.

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

`ShardRouter::new` builds a rendezvous router keyed on `(workflow_name, workflow_id)`. To achieve tenant-pinned routing you would need a workflow ID naming convention that incorporates the tenant identifier — for example, always prefixing workflow IDs with the tenant: `"acme::order-42"`. Because rendezvous hashing is deterministic, all workflow IDs with the same prefix will not necessarily land on the same shard (the hash also mixes in `workflow_name`), so this approach only works reliably if you pin both `workflow_name` and the tenant-identifying part of `workflow_id`.

A fully custom shard-selection strategy is not yet exposed via `ShardRouter`; it is planned as a future API extension.

With tenant-consistent placement, every execution for `tenant_id = "acme"` lands on the same shard, so the local `limit` is also the global limit.

**Trade-off**: routing all of one tenant's workflows to the same shard concentrates load. Size shards to handle the worst-case tenant burst, or use rate limiting above Harvest to bound how fast new workflows can be started per tenant.

### Cross-shard global limits — explicit out of scope

Distributed counting across shards requires either a coordination service (Redis, a dedicated Postgres coordinator) or accepting bounded inaccuracy (approximate counts via gossip). Both add operational complexity that conflicts with Harvest's goal of being a Postgres-native engine. Cross-shard global limits are therefore **out of scope** for this feature; the per-shard guarantee is the contract.

If you need approximate cross-shard fair-share rather than a hard global cap, the metrics observable via `GET /admin/concurrency` can feed an external rate limiter in the layer above Harvest.

### Worker crash and slot release

When a worker crashes and its heartbeat times out, the `timeout.rs` scanner transitions the claimed task back to `PENDING` state. The concurrency cap check counts only `RUNNING` rows, so the slot is immediately available for another worker to claim. No operator intervention is required.

This is handled entirely within the shard where the task lives — no cross-shard coordination is needed for crash recovery.

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

The `harvest.concurrency.in_flight` metric (tagged by concurrency key) is emitted by the concurrency sampler at regular intervals. **Note**: the raw key value is tagged on the metric — ensure your key cardinality is bounded (e.g., use `tenant_id`, not `execution_id`) to avoid metric cardinality explosion. See ADR-0001 §7.

### Adding a shard to a deployment that uses per-key concurrency

Follow the standard add-a-shard procedure in `CLAUDE.md`. The new shard starts with no task queue rows, so the cap is independent from day one. If you need to migrate in-flight workflows to the new shard, that is out of scope (cross-shard rebalancing is not supported).

---

## Debounce coordination is shard-local (issue #499)

Debounce pending-start records (`harvest_debounce`) are routed to the same shard as the debounce key (via `ShardRouter`), matching the per-key concurrency scope above. All burst admissions for the same `(workflow_name, debounce_key)` pair must land on the same shard for the `UNIQUE (workflow_name, debounce_key)` upsert collapse to work. Cross-shard global debounce coordination is **out of scope**: embedders requiring a global cap should ensure all executions for a given debounce key route to a single shard (see the concurrency routing guidance above).

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

This index must be present on every shard for deep-page performance to remain flat. The migration is idempotent (`IF NOT EXISTS`) and runs automatically with `diesel migration run`.
