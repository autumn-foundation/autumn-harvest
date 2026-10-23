# Sticky Cross-Worker Routing

Sticky routing keeps follow-up workflow tasks on the worker that already has the execution's event history in its in-process LRU cache (issue #235). It is on by default with a 5 s window (issue #1798).

## Problem

Every time a workflow suspends and resumes, the worker that picks up the follow-up task must reconstruct the full event history from Postgres before it can replay the workflow function. For a workflow with N suspension points the Nth task must reload all N-1 prior events from the database — an `O(history_size)` read that grows with execution age.

The `WorkflowCache` LRU was introduced in Phase 2 to store this snapshot in memory, but without sticky routing the follow-up task can land on any worker in the fleet. The worker that wins the claim has a ~1/fleet_size chance of being the one that holds the cache entry.

## Solution

Sticky routing adds a hard affinity lease: when a workflow suspends, the worker writes its own ID to `sticky_worker_id` on the task queue row. While `sticky_until > NOW()`, the claim query's WHERE clause restricts that task to the owning worker — other workers cannot see it at all. Once the lease expires (`sticky_until <= NOW()` or `sticky_until IS NULL`), the task becomes claimable by any eligible worker. Within the claimable set, tasks whose `sticky_worker_id` matches the claiming worker are ordered first via `ORDER BY ... DESC`, so a worker that holds the warm cache wins ties.

Stickiness is therefore a **hard exclusion during the lease window, not a soft ordering hint**. Operators should size `lease_ttl` accordingly: a long TTL means pinned tasks are invisible to other workers for that duration if the owning worker is unavailable.

On a cache hit the worker loads only the *delta* events appended since the last suspension (timer firings and signals) and prepends the cached snapshot, reducing the per-task Postgres read from `O(history_size)` to `O(new_events)`.

A hit takes the entry out of the cache. Only a committed suspension, or a task that the wake ingest re-drives, puts it back. The delta event ids must run on from the cached `next_event_id` with no gap. A gap drops the resident workflow of the entry (see below).

## Resident workflows

A cache entry also keeps the suspended workflow itself (issue #1798, step 2). The next decision on the same worker sends the new result to the parked handler future and polls it again. It does not replay history, so its replay work stays roughly constant as history grows.

A warm decision must decide exactly as a cold replay would. The resident path therefore accepts a narrow set of suspensions:

- The cycle awaits one command: an activity, a timer or a signal. Or it awaits two or more activities, such as a `futures::join!` or `try_join_all` of parallel tool calls (issue #2008).
- Each other command is a marker, a side effect, progress, current details, a log line or a search-attribute upsert.
- The context has no open `ctx.race()`, park token, push signal handler, held mutex, cancel request or non-determinism record, and no unread history.
- A signal wait is not for a name that `try_wait_for_signal`, `try_receive_signal` or a drain probed with a scan that reached the end of history.
- The workflow is not hosted in a hot-swapped module.

A warm decision resumes only on an exact delta. The delta holds the events of the last suspension, in order. Then it holds the events that resolve parked awaits: an activity success, a timer fire or a signal. Each parked await resolves at most once. The start and heartbeat events of a parked activity may come before its result. The context inputs must not have changed, for example the deadline, shard, build, queue, parent, headers or handler.

A redundant wake brings the events of the last suspension and no result. It can bring start and heartbeat events. It resolves no await, so each parked activity waits again, as below. A lone timer or signal cannot wait again this way, so its redundant wake declines.

A delta can resolve only some activities of a join. The resume then parks the others again with a `WaitForActivity` command, which a cold replay emits too. Such a cycle must only wait. If a branch runs a command, fails, drops a parked sibling or reads history state, such as the replay position, the worker drops the cycle and replays cold (`ResumeDeclined::SiblingStillParked`). The cycle fires no side effect that replay suppresses, such as a log line or a business metric. So a dropped cycle does not repeat one. A delta with several results runs as one cycle per result, in history order, as a cold replay matches them.

Any other delta declines. A decline drops the resident workflow and runs a cold replay, which is always correct. Joins with a timer or a signal next to another await, races, activity failures, updates, cancels and child workflows therefore still replay cold.

**Observing it.** `harvest.workflow.resident` counts each decision attempt on a worker with resident state on (issue #2007). Its `outcome` label is `hit` for a resumed decision. Each other value names why the decision replayed: `cold`, `declined`, `multi_await`, `race`, `mutex`, `hot_swap`, `blocked` or `unsupported`. The hit rate is `hit` over all outcomes. A resumed cycle also records `harvest.replay = false` on its `harvest.workflow.execute` span. A decline logs `resident workflow declined; replaying cold (issue #1798)` at `debug` level, with the `ResumeDeclined` reason.

`harvest.workflow.resident.duration` times each of these decisions, with the same labels (issue #2257). The clock starts before the history load and stops at the end of the first drive. The window also holds the wake ingest and the pre-drive writes. The p99 of `outcome="cold"` is the cold p99 of the stage 2 gate in [`docs/rnd/2026-10-22-stage-2-gate.md`](rnd/2026-10-22-stage-2-gate.md).

**Limits.**

- Resident state lives only in worker memory. A restart, a failover, an LRU eviction or a decision that does not commit drops it. The next decision then replays cold.
- A timer that a cold replay re-arms writes no second `TimerStarted`. Its fire then declines and replays cold.
- A resident future keeps the state of a foreign future, such as a raw `tokio::time::sleep` in a `select!`. Such a workflow is not deterministic on a cold worker either. Use a Harvest timer instead.
- A join branch that runs a command after its own await, while a later branch has a command event, fails a cold replay. A warm decision cannot always see this shape. When the earlier branch's result arrives last, the warm decision runs on, and the failure shows at the next cold replay. Issue #1798 has the same limit for a single wait. Run the next command after the join, or check in-flight histories with `ReplayVerifier`.

## Owner mode replaces stickiness

Owner mode (issue #2252, [ADR 0009](adr/0009-decision-owner.md)) gives each execution one owner worker. It replaces the sticky lease for workflow tasks. Activity tasks do not change.

- **One owner per partition.** Each database has 512 decision partitions. The partition of an execution is a hash of its `workflow_id`. A worker holds a lease on each partition that it owns.
- **Owned take.** Only the owner takes a workflow task of its partitions. The sticky gate does not apply to a workflow row, and the park writes no pin. So no window expires, and every decision of an execution runs on its owner.
- **Unstamped rows.** Each new workflow task carries its partition. A row with none, from an old binary or a dead-letter redrive, is claimable by any owner. That owner stamps the row and re-pends it to the right owner.
- **Resident hits survive.** The owner keeps the resident workflow between decisions, however long the next activity, timer or signal takes. A decision replays cold only after an eviction, a restart or a change of owner.
- **Balance.** Each worker targets `⌈512 / live owners⌉` partitions. A live owner is a worker whose owner loop scanned in the last 3 s. A worker that joins takes free partitions. A worker above its target plus one stops taking work in the extra ones and releases them one scan later. A graceful stop releases every partition in one statement.
- **Who owns.** A worker with no workflow slot or no workflow type never owns a partition. It still claims activity tasks.
- **Failover.** A crashed owner drops out of the live owners within 3 s. Its leases lapse after the TTL, 10 s by default. A peer scans just after that lease end and acquires its partitions. A new owner starts cold. [`performance-owner-failover.md`](performance-owner-failover.md) records the measured bounds.
- **Fence.** Each decision commit checks the owner's lease epoch. A stale owner cannot write. See [ADR 0009](adr/0009-decision-owner.md).
- **Decider.** The owner does not claim its workflow tasks through the claim loop (issue #2255). An owner decider takes each one by its execution, after an owner wake or a scan of its partitions. The claim loops claim activities only. [`performance-owner-decider.md`](performance-owner-decider.md) records the claim counts and the throughput.

| | Sticky routing | Owner mode |
|---|---|---|
| Affinity | 5 s lease on the task row | Lease on the decision partition |
| After the window | Any worker claims, and the decision replays cold | The owner takes the task, and the decision resumes |
| Claim cost | A sticky predicate in each scan | No head scan for a workflow task. The owner takes it by its execution |
| Crash recovery | Up to one sticky window | About one lease TTL |

### Turning owner mode on

Owner mode is a switch per database. It is off by default. Turn it on with `decision_owner::enable_owner_mode`. The step:

1. Refuses while a live worker cannot run owner mode, while live workers poll different queues or run different builds, while live owners register different workflow or activity types, or while no owner loop is live. An owner claims every workflow task of its partitions, so each owner must be able to run each of them.
2. Stamps the partition of each live workflow task.
3. In one transaction, installs a guard on the claim of a workflow task, re-pends the legacy claims of dead workers, and sets the switch. A claim with no owner token then takes no workflow task. A binary from before owner mode sends no token, so a fleet can never mix the two claim styles. A live worker finishes its own legacy decision.

The guard statements take a `SHARE ROW EXCLUSIVE` lock on `harvest_task_queue`, bounded by a lock timeout. That lock does not block a reader. Run the step as the owner of the table.

`decision_owner::disable_owner_mode` clears the switch and turns the guard off in one transaction. Owners then release their partitions and claim as before.

**Limits.**

- The enable step checks queues, builds and handler types once. A worker that starts later with other queues, another build or other workflow or activity types is not stopped. Its partitions can then hold workflow tasks that it cannot claim. Keep one queue set, one build and one set of workflow and activity types per database while owner mode is on.
- For a rolling deploy of a new build, turn owner mode off first, deploy, then turn it on again.
- A binary from before owner mode claims no workflow task. Its claim can also miss an activity task behind a workflow task. So the enable step refuses while such a binary is live.

Workers read the switch about once a second. A worker in owner mode still caches workflow state and still needs `sticky_timeout` above zero. `harvest.workflow.resident` keeps the same labels in both modes.

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

To disable sticky routing, pass `lease_ttl: Duration::ZERO`. This also disables the warm workflow cache and resident workflows:

```rust
let worker = WorkerConfig::default()
    .with_sticky_routing(StickyRoutingConfig { lease_ttl: Duration::ZERO });
```

Resident workflows are on by default. To keep the event cache but replay every decision, turn them off:

```rust
let worker = WorkerConfig::default().with_resident_workflows(false);
```

`WorkerRuntimeConfig::resident_workflows` carries the same switch. The effective-config view reports the effective value as `resident_workflows`, so it reads `false` while sticky routing is off.

## Failover

A pin never blocks progress for longer than one sticky window.

- **Crash.** The pin stays until `sticky_until` passes. A peer then claims the task. A wake re-arms the pin of a parked task, so each execution pinned to the dead worker waits up to one window at its next wake. With the default window, that is up to 5 s.
- **Graceful shutdown.** The worker releases its pins when it starts to drain, and again after the drain (issue #1798). A wake during the drain does not re-arm a released pin. Peers can claim pending tasks at once, and a parked task as soon as it wakes. The release keeps the pins of worker sessions, because a session pin is a hard pin (issue #606). After the drain the worker also empties its `WorkflowCache` and stops resident capture. A stopped `Worker` that a caller still holds then keeps no parked workflow in memory.

The release is best effort. If it fails, the pins expire after one sticky window. A decision that still runs when the drain times out can pin its task again when it parks. That pin also expires after one window.

Each release is one `UPDATE` per pool. At worst it scans all `RUNNING` rows of the task queue, because a parked row is `RUNNING`.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `harvest.workflow.cache_hit` | counter | Task served from in-process LRU cache (delta load). The task resumes the resident workflow or replays the cached history. |
| `harvest.workflow.cache_miss` | counter | Task required a full history reload from Postgres. |
| `harvest.workflow.resident` | counter | One per decision with resident state on (issue #2007). The `outcome` label is `hit`, or the reason why the decision replayed. Labels: `workflow`, `queue`, `outcome`. |

All three metrics carry a `workflow` label (the workflow name). `execution.id` is deliberately excluded per ADR-0001 §7 (cardinality).

Monitor the **hit ratio** (`cache_hit / (cache_hit + cache_miss)`) per worker. With sticky routing on, the ratio climbs toward 1 for long-running workflows that suspend many times. A ratio that stays near 0 may indicate the lease TTL is shorter than the median inter-task delay.

## Decision cost

Sticky routing makes the history load of a warm decision `O(new_events)`. A cold decision replays the workflow from the top, so its CPU cost grows linearly with history length. A resident decision does not replay, so its cost stays roughly constant. The `decision_cost`, `decision_wall` and `decision_cost_warm` groups in `autumn-harvest/benches/replay_bench.rs` measure this at 1k, 5k and 10k events:

```sh
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench replay_bench -- decision_
```

| History | `decision_cost` (cold replay) | `decision_wall` (cold, suspends) | `decision_cost_warm` (resident) |
|--------:|------------------------------:|---------------------------------:|--------------------------------:|
| 1,000 events | 0.13 ms | 0.14 ms | 2.8 µs |
| 5,000 events | 0.61 ms | 0.69 ms | 2.6 µs |
| 10,000 events | 1.25 ms | 1.49 ms | 2.8 µs |

Measured on a 4-vCPU cloud container. Read the slope, not the absolute values. Issue #1797 removed the fixed 100 ms suspension timeout, so `decision_wall` now tracks the replay cost.

## Cache eviction

The `WorkflowCache` is a bounded LRU. When the cache is full the least-recently-used entry is evicted, causing the next task for that execution to fall back to a cold full-history load. Eviction does not cause data loss — it only affects performance.

The cache size is configured via `WorkerConfig::workflow_cache_size` (default: 1000 entries). For large fleets with many concurrent executions per worker, tune this up proportionally.

**Memory.** The cache is on by default. Each entry holds the full decoded history of one execution, with offloaded payloads loaded back in. The bound is an entry count, not a byte count. A worker that runs many long histories can use a lot of memory. Lower `workflow_cache_size` for such a worker. Decoded payloads stay in memory until the entry is evicted.

A resident entry also holds the parked handler future and its context. The context keeps its own copy of the history, so a resident entry holds two copies. Turn off resident workflows to drop the second copy.

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
