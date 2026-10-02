# Sticky Cross-Worker Routing

Sticky routing keeps follow-up workflow tasks on the worker that already has the execution's event history in its in-process LRU cache (issue #235). It is on by default with a 5 s window (issue #1798).

## Problem

Every time a workflow suspends and resumes, the worker that picks up the follow-up task must reconstruct the full event history from Postgres before it can replay the workflow function. For a workflow with N suspension points the Nth task must reload all N-1 prior events from the database — an `O(history_size)` read that grows with execution age.

The `WorkflowCache` LRU was introduced in Phase 2 to store this snapshot in memory, but without sticky routing the follow-up task can land on any worker in the fleet. The worker that wins the claim has a ~1/fleet_size chance of being the one that holds the cache entry.

## Solution

Sticky routing adds a hard affinity lease: when a workflow suspends, the worker writes its own ID to `sticky_worker_id` on the task queue row. While `sticky_until > NOW()`, the claim query's WHERE clause restricts that task to the owning worker — other workers cannot see it at all. Once the lease expires (`sticky_until <= NOW()` or `sticky_until IS NULL`), the task becomes claimable by any eligible worker. Within the claimable set, tasks whose `sticky_worker_id` matches the claiming worker are ordered first via `ORDER BY ... DESC`, so a worker that holds the warm cache wins ties.

Stickiness is therefore a **hard exclusion during the lease window, not a soft ordering hint**. Operators should size `lease_ttl` accordingly: a long TTL means pinned tasks are invisible to other workers for that duration if the owning worker is unavailable.

On a cache hit the worker loads only the *delta* events appended since the last suspension (timer firings and signals) and prepends the cached snapshot, reducing the per-task Postgres read from `O(history_size)` to `O(new_events)`.

## Configuration

Sticky routing is **on by default**. `WorkerConfig::default()` sets `sticky_timeout` to `DEFAULT_STICKY_TIMEOUT` (5 s), the same fallback window that Temporal uses for its sticky queue. Change the window per worker with `WorkerConfig::with_sticky_routing`:

```rust
use autumn_harvest::{StickyRoutingConfig, WorkerConfig};
use std::time::Duration;

let worker = WorkerConfig::default()
    .with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::from_secs(10),
    });
```

`lease_ttl` controls how long a worker holds the affinity lease on an execution. After the TTL expires another worker may claim the task (a cache miss, which is always correct — it is just slower). See the operational recommendations below for guidance on sizing this value.

To disable sticky routing, pass `lease_ttl: Duration::ZERO`. This also disables the warm workflow cache:

```rust
let worker = WorkerConfig::default()
    .with_sticky_routing(StickyRoutingConfig { lease_ttl: Duration::ZERO });
```

## Failover

A pin never blocks progress for longer than one sticky window.

- **Crash.** The pin stays until `sticky_until` passes. A peer then claims the task. A wake re-arms the pin of a parked task, so each execution pinned to the dead worker waits up to one window at its next wake. With the default window, that is up to 5 s.
- **Graceful shutdown.** The worker releases its pins when it starts to drain, and again after the drain (issue #1798). A wake during the drain does not re-arm a released pin. Peers can claim pending tasks at once, and a parked task as soon as it wakes. The release keeps the pins of worker sessions, because a session pin is a hard pin (issue #606).

The release is best effort. If it fails, the pins expire after one sticky window. A decision that still runs when the drain times out can pin its task again when it parks. That pin also expires after one window.

Each release is one `UPDATE` per pool. At worst it scans all `RUNNING` rows of the task queue, because a parked row is `RUNNING`.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `harvest.workflow.cache_hit` | counter | Task served from in-process LRU cache (delta load). |
| `harvest.workflow.cache_miss` | counter | Task required a full history reload from Postgres. |

Both metrics carry a `workflow` label (the workflow name). `execution.id` is deliberately excluded per ADR-0001 §7 (cardinality).

Monitor the **hit ratio** (`cache_hit / (cache_hit + cache_miss)`) per worker. With sticky routing on, the ratio climbs toward 1 for long-running workflows that suspend many times. A ratio that stays near 0 may indicate the lease TTL is shorter than the median inter-task delay.

## Decision cost

Sticky routing makes the history load of a warm decision `O(new_events)`. The workflow still replays from the top on every decision. Its CPU cost grows linearly with history length. The `decision_cost` and `decision_wall` groups in `autumn-harvest/benches/replay_bench.rs` measure this at 1k, 5k and 10k events:

```sh
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench replay_bench -- decision_
```

| History | `decision_cost` (replay) | `decision_wall` (suspends) |
|--------:|-------------------------:|---------------------------:|
| 1,000 events | 0.10 ms | 101.6 ms |
| 5,000 events | 0.56 ms | 101.7 ms |
| 10,000 events | 1.13 ms | 101.9 ms |

Measured on a 4-vCPU cloud container. Read the slope, not the absolute values. A suspending decision waits for the fixed 100 ms suspension timeout of issue #1797, so `decision_wall` reads about max(100 ms, replay). A cache that keeps the live workflow resident would make the replay work of a warm decision roughly constant (issue #1798, step 2). That step depends on #1797.

## Cache eviction

The `WorkflowCache` is a bounded LRU. When the cache is full the least-recently-used entry is evicted, causing the next task for that execution to fall back to a cold full-history load. Eviction does not cause data loss — it only affects performance.

The cache size is configured via `WorkerConfig::workflow_cache_size` (default: 1000 entries). For large fleets with many concurrent executions per worker, tune this up proportionally.

**Memory.** The cache is on by default. Each entry holds the full decoded history of one execution, with offloaded payloads loaded back in. The bound is an entry count, not a byte count. A worker that runs many long histories can use a lot of memory. Lower `workflow_cache_size` for such a worker. Decoded payloads stay in memory until the entry is evicted.

## Interaction with other features

**Shard assignments** (`WorkerConfig::shard_assignments`): sticky routing and shard assignments compose independently. Shard assignments determine which shard a worker polls; sticky routing determines which worker within a shard is preferred for a given execution.

**Build-id routing** (`WorkerConfig::with_build_id`): build-id routing decides whether a worker is *eligible* to claim a task at all (version compatibility). Sticky routing is a secondary preference within the eligible set. An eligible worker that holds the warm cache wins over an equally-eligible worker that does not.

**`continue_as_new`**: when a workflow rotates via `continue_as_new` the old execution's cache entry is evicted (terminal outcome). The new execution starts fresh with an empty cache, same as any other new execution.

## Operational recommendations

- Keep sticky routing on for workflows that suspend frequently (timer waits, fan-out/fan-in, multi-step human-in-the-loop).
- For short-lived workflows that complete in a single task (no suspension), sticky routing has no effect — there is no follow-up task to benefit from the warm cache.
- Keep `lease_ttl` short. A restarted worker starts with a cold cache, so a long window does not help a restart. After a crash, each pinned execution waits up to one window. A graceful shutdown adds no wait.
- **Timers.** Do not raise `lease_ttl` to cover long timers. A timer task that becomes due after `sticky_until` is claimable by any worker, and that costs one cold load. A long window costs more: after a crash, it blocks each pinned execution for that long.
- **Redis dispatch.** A worker that is not the owner hands a pinned reference on after one poll interval, with no backoff, until the owner claims it or the pin expires (issue #1798). See `docs/operations/redis-dispatch.md`.
- Watch the `harvest.workflow.cache_miss` counter during rolling deploys. It spikes as executions move to new workers, then settles as the new workers warm their caches.
