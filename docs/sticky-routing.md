# Sticky Cross-Worker Routing

Sticky routing is an opt-in performance feature that keeps follow-up workflow tasks on the worker that already has the execution's event history in its in-process LRU cache (issue #235).

## Problem

Every time a workflow suspends and resumes, the worker that picks up the follow-up task must reconstruct the full event history from Postgres before it can replay the workflow function. For a workflow with N suspension points the Nth task must reload all N-1 prior events from the database — an `O(history_size)` read that grows with execution age.

The `WorkflowCache` LRU was introduced in Phase 2 to store this snapshot in memory, but without sticky routing the follow-up task can land on any worker in the fleet. The worker that wins the claim has a ~1/fleet_size chance of being the one that holds the cache entry.

## Solution

Sticky routing adds a hard affinity lease: when a worker claims a task it records its own worker ID on the execution row. While `sticky_until > NOW()`, the claim query's WHERE clause restricts that task to the owning worker — other workers cannot see it at all. Once the lease expires (`sticky_until <= NOW()` or `sticky_until IS NULL`), the task becomes claimable by any eligible worker. Within the claimable set, tasks whose `sticky_worker_id` matches the claiming worker are ordered first via `ORDER BY ... DESC`, so a worker that holds the warm cache wins ties.

Stickiness is therefore a **hard exclusion during the lease window, not a soft ordering hint**. Operators should size `lease_ttl` accordingly: a long TTL means pinned tasks are invisible to other workers for that duration if the owning worker is unavailable.

On a cache hit the worker loads only the *delta* events appended since the last suspension (timer firings and signals) and prepends the cached snapshot, reducing the per-task Postgres read from `O(history_size)` to `O(new_events)`.

## Configuration

Sticky routing is **off by default**. Enable it per-worker via `WorkerConfig::with_sticky_routing`:

```rust
use autumn_harvest::{StickyRoutingConfig, WorkerConfig};
use std::time::Duration;

let worker = WorkerConfig::default()
    .with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::from_secs(30),
    });
```

`lease_ttl` controls how long a worker holds the affinity lease on an execution. After the TTL expires another worker may claim the task (a cache miss, which is always correct — it is just slower). See the operational recommendations below for guidance on sizing this value.

To disable sticky routing after enabling it, pass `lease_ttl: Duration::ZERO`:

```rust
let worker = WorkerConfig::default()
    .with_sticky_routing(StickyRoutingConfig { lease_ttl: Duration::ZERO });
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `harvest.workflow.cache_hit` | counter | Task served from in-process LRU cache (delta load). |
| `harvest.workflow.cache_miss` | counter | Task required a full history reload from Postgres. |

Both metrics carry a `workflow` label (the workflow name). `execution.id` is deliberately excluded per ADR-0001 §7 (cardinality).

Monitor the **hit ratio** (`cache_hit / (cache_hit + cache_miss)`) per worker. After enabling sticky routing, you should see this climb toward 1 for long-running workflows that suspend many times. A ratio that stays near 0 may indicate the lease TTL is shorter than the median inter-task delay.

## Cache eviction

The `WorkflowCache` is a bounded LRU. When the cache is full the least-recently-used entry is evicted, causing the next task for that execution to fall back to a cold full-history load. Eviction does not cause data loss — it only affects performance.

The cache size is configured via `WorkerConfig::workflow_cache_size` (default: 1000 entries). For large fleets with many concurrent executions per worker, tune this up proportionally.

## Interaction with other features

**Shard assignments** (`WorkerConfig::shard_assignments`): sticky routing and shard assignments compose independently. Shard assignments determine which shard a worker polls; sticky routing determines which worker within a shard is preferred for a given execution.

**Build-id routing** (`WorkerConfig::with_build_id`): build-id routing decides whether a worker is *eligible* to claim a task at all (version compatibility). Sticky routing is a secondary preference within the eligible set. An eligible worker that holds the warm cache wins over an equally-eligible worker that does not.

**`continue_as_new`**: when a workflow rotates via `continue_as_new` the old execution's cache entry is evicted (terminal outcome). The new execution starts fresh with an empty cache, same as any other new execution.

## Operational recommendations

- Enable sticky routing for workflows that suspend frequently (timer waits, fan-out/fan-in, multi-step human-in-the-loop).
- For short-lived workflows that complete in a single task (no suspension), sticky routing has no effect — there is no follow-up task to benefit from the warm cache.
- Set `lease_ttl` to 2-3× your median worker restart time. A value shorter than a typical deploy window will cause a burst of cold reloads during rolling restarts.
- **Timer duration vs. lease TTL**: the affinity lease is written when the workflow parks (`sticky_until = NOW() + lease_ttl`). If a timer fires *after* `sticky_until`, any worker can claim the follow-up task regardless of which worker set the pin. For timer-heavy workflows whose timers routinely exceed `lease_ttl`, set `lease_ttl` to at least the 95th-percentile timer duration, or accept that long-timer suspension points will incur a cold reload.
- Watch the `harvest.workflow.cache_miss` counter during rolling deploys: it will spike as executions migrate to new workers and then settle as the new workers warm their caches.
