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
