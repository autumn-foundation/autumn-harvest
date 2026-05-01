# autumn-harvest

Postgres-backed durable workflow engine, companion to the Autumn web framework. Provides event-sourced workflow execution with activities, signals, timers, child workflows, and DAG scheduling.

## Workspace Structure

```
autumn-harvest/          <- workspace root (this file lives here)
  autumn-harvest/        <- core library crate
    src/
      lib.rs
      types.rs           <- Phase 1
      error.rs           <- Phase 1
      policy.rs          <- Phase 1
      event.rs           <- Phase 1
      context.rs         <- Phase 1 stubs, Phase 2 full impl
      info.rs            <- Phase 1
      builder.rs         <- Phase 1
      prelude.rs         <- Phase 1
      schema.rs          <- Phase 1, db feature only
      models.rs          <- Phase 1, db feature only
      store.rs           <- Phase 2: event store (append/load history)
      replay.rs          <- Phase 2: deterministic replay engine
      executor.rs        <- Phase 2: workflow executor (replay + suspension)
      queue.rs           <- Phase 2: Postgres task queue (SKIP LOCKED)
      notify.rs          <- Phase 2: LISTEN/NOTIFY wrapper
      worker.rs          <- Phase 2: worker runtime (poll loop, semaphore dispatch)
      heartbeat.rs       <- Phase 2: batched heartbeat flusher
      timeout.rs         <- Phase 2: timeout enforcement scanner
      cache.rs           <- Phase 2: LRU workflow state cache
      dlq.rs             <- Phase 2: dead letter queue
      pool.rs            <- Phase 2: separate pool config with shared ceiling
    migrations/
      20260409000000_harvest_initial/
    tests/
      integration_e2e.rs <- testcontainers integration tests
      replay_tests.rs    <- replay engine integration tests
      macros_*.rs        <- proc-macro integration tests
  autumn-harvest-macros/ <- proc-macro crate
    src/
      lib.rs
      workflow.rs
      activity.rs
      collect.rs
```

Two crates in the workspace. `autumn-harvest` is the public library. `autumn-harvest-macros` is a separate proc-macro crate consumed by `autumn-harvest` via `prelude.rs`.

### Phase Status

- **Phase 1** (complete): types, error, event, policy, context stubs, models, macros, builder
- **Phase 2** (complete): event store, replay engine, workflow context, activity context, task queue (SKIP LOCKED), LISTEN/NOTIFY, worker runtime, heartbeating, timeout enforcement, workflow versioning (ctx.version), LRU workflow cache, dead letter queue, separate worker pool with shared ceiling, testcontainers integration tests
- **Phase 3** (implemented): DAG scheduler/runtime, `DagBuilder`, `#[dag]` macro, trigger rules, signals/queries, management HTTP API, Autumn adapter crate with `HarvestExt` lifecycle integration
- **Phase 3.5** (implemented): Local activities (`#[activity(local = true)]`, `ctx.execute_local_activity_raw`, `WorkflowCommand::RunLocalActivity`, three new `WorkflowEvent` variants, builder cap validation) — see issue #98
- **Phase 4** (next): production hardening -- cancellation/saga semantics, sharding, sticky cross-worker routing, observability, metrics, dashboard (autumn-harvest-ui)

---

## Architecture

### Crate Relationship

`autumn-harvest` is the core engine crate. It re-exports everything from `autumn-harvest-macros` through `prelude.rs`. Autumn-specific integration lives in the separate `autumn-harvest-plugin` crate, which provides `HarvestPlugin`, the management API router, and app lifecycle wiring.

Macro-generated code must use `::autumn_harvest::` paths for everything. The proc-macro crate has no dependency on `serde_json` or `autumn-web` itself; it emits token streams that resolve via the `::autumn_harvest::` path. `lib.rs` re-exports `serde_json` at `::autumn_harvest::serde_json` and exposes its own local `task_duration()` parser at `::autumn_harvest::task_duration` for exactly this reason.

Do not change macros to emit `::serde_json::` or `::autumn_web::` paths — downstream crates will not have those as direct dependencies and the code will fail to compile.

### Companion Function Pattern

`#[workflow]` generates a hidden companion function alongside the user's function:

```
pub fn __autumn_workflow_info_{name}() -> ::autumn_harvest::WorkflowInfo
```

`#[activity]` generates:

```
pub fn __autumn_activity_info_{name}() -> ::autumn_harvest::ActivityInfo
```

`workflows![name1, name2]` expands to `vec![__autumn_workflow_info_name1(), __autumn_workflow_info_name2()]`.

`activities![name1, name2]` expands to `vec![__autumn_activity_info_name1(), __autumn_activity_info_name2()]`.

### Key Design Decisions

**1. UUID PKs for execution IDs, not i64**

`ExecutionId`, `ActivityExecId`, and `HarvestTimer`/`HarvestSignal`/`DagRun`/`TaskQueueItem` all use `Uuid` as their primary key. Execution IDs must be generated before DB insert (distributed, shard-safe). The i64 convention for application domain tables does NOT apply here.

The `harvest_events` table is the exception: its `id` column is `BIGSERIAL i64` because events are strictly local and append-only — the sequence is never shared across shards.

**2. `db` feature gates all Diesel code**

`schema.rs` and `models.rs` are compiled only when `features = ["db"]`. `default = ["db"]`, so it compiles in by default. Tests on Windows run `--no-default-features` to avoid OpenSSL dependency. CI tests the `db` feature on Linux.

**3. Adjacently-tagged event JSON**

`WorkflowEvent` uses `#[serde(tag = "type", content = "data")]`. This emits `{"type": "ActivityScheduled", "data": {...}}`. Postgres can extract the event type with `payload->>'type'` without parsing the full payload. Never change this tagging — stored events depend on it.

**4. Append-only event invariant**

Never remove or reorder `WorkflowEvent` variants. Stored JSON in `harvest_events.event_data` must always deserialize into the same variant names after deployment. Add new variants at the end.

**5. `WorkflowContext` replay modes**

`is_replaying()` returns the current mode. Normal mode: generate new events. Replay mode: return recorded results without re-executing side effects. The replay engine (Phase 2) will drive `set_replaying()` and manage the event history pointer. Phase 1 contexts always start in normal mode.

**6. `WorkflowHandlerFn` / `ActivityHandlerFn` are fn pointers**

Both types are `fn` (not `Box<dyn Fn>`). The macro generates a closure body cast to `fn` pointer. This keeps `WorkflowInfo` and `ActivityInfo` `Sync` without needing `Arc`. Serialization errors in the dispatch shim propagate as `Err(String)` — they are never swallowed.

**7. Multi-param dispatch packs into JSON array**

Single-param workflows/activities: input is passed as a single JSON value and deserialized directly. Multi-param: input is expected to be a JSON array `[arg1, arg2, ...]`, indexed by position.

### Sharding

Harvest can spread workflow state across N independent Postgres databases. A single workflow's event log, task queue rows, timers, signals, and DLQ entries all live on the same shard, so per-workflow ACID guarantees are preserved without cross-shard transactions. Cross-shard rebalancing of existing workflows is out of scope.

Key mechanics:

- **Shard identity is embedded in `ExecutionId`.** `ExecutionId::new_for_shard(ShardId)` writes the shard number into the UUID's first two bytes; `ExecutionId::shard()` reads it back. Any caller holding an `ExecutionId` can route to the owning shard in O(1) with no directory lookup. `ExecutionId::new()` emits the `ShardId::UNENCODED` sentinel (`0xFFFF`) which `ShardRouter` resolves to the configured default shard — that keeps tests and replay harnesses working and gives a safe fallback for pre-sharding rows.
- **`ShardRouter` picks the initial shard for a new workflow** using `seahash` rendezvous hashing over `readable_shards`. The hash is stable across process restarts so outbox retries are idempotent even when `writable_shards` is being widened or narrowed; picks outside `writable_shards` are re-hashed on the writable subset.
- **`ShardedDbPool` is a `BTreeMap<ShardId, DbPool>`** with `pool_for(ShardId)`, `pool_for_execution(ExecutionId)`, `iter_shards()`, and a `single(pool)` helper. Single-shard deployments use the `single` shape — it's semantically identical to the pre-sharding runtime and requires no config change.
- **Backward compatibility.** Existing deployments see zero behavior change. The plugin wires a `ShardRouter::single()` and `ShardedDbPool::single(existing_pool)` by default; the `shard_id` column on `harvest_workflow_executions` is retained for observability and is derived from `exec_id.shard()`.

Operational "add a shard" procedure (new workflows only):

1. Provision a new Postgres database and run `diesel migration run` against it.
2. Add the new shard to `readable_shards` and keep `writable_shards` pointing at the existing shards. Restart the plugin — the router can now resolve ids that encode the new shard, even though nothing writes there yet.
3. Add the new shard to `writable_shards`. New workflows begin landing on it via rendezvous hash. In-flight workflows on the old shards continue to drain through their own worker tasks.

Current implementation scope: `ExecutionId`/`ShardId` encoding, `ShardRouter`, `ShardedDbPool`, shard-aware start/read paths in the plugin, and `WorkerConfig.shard_assignments`. Per-shard worker poll loops, per-shard scheduler tick loops with DAG→shard pinning, and cross-shard observability remain as follow-up work.

---

## Module Guide

| Module | Phase | Purpose |
|--------|-------|---------|
| `types.rs` | 1 | Newtypes: `WorkflowId` (String), `ExecutionId` (Uuid v4), `ActivityExecId` (Uuid v4), `TimerId` (String), `WorkerId` (String) |
| `error.rs` | 1 | `HarvestError` (thiserror), `HarvestResult<T>`, `TimeoutType` enum |
| `policy.rs` | 1 | `RetryPolicy`, `TriggerRule`, `Schedule`, `TaskStatus`, `compute_retry_delay` |
| `event.rs` | 1 | `WorkflowEvent` enum (17 variants, adjacently-tagged serde), `type_name()` |
| `context.rs` | 1+2 | `WorkflowContext` (replay, suspension, version gate, timers), `ActivityContext` (heartbeat channel, cancellation) |
| `info.rs` | 1 | `WorkflowInfo`, `ActivityInfo`, `WorkflowHandlerFn`, `ActivityHandlerFn` type aliases |
| `builder.rs` | 1 | `HarvestBuilder` (fluent), `WorkerConfig` (queues, concurrency, timeouts) |
| `prelude.rs` | 1 | Core glob re-export surface including macros |
| `schema.rs` | 1 | Diesel `table!` macros -- 8 tables |
| `models.rs` | 1 | `Queryable`/`Selectable` read structs and `Insertable` `New*` write structs for all 8 tables |
| `store.rs` | 2 | Event store: `append_events`, `load_history`, `events_to_rows` with sequential event IDs |
| `replay.rs` | 2 | Deterministic replay engine: `HistoryMatcher` walks event history, detects non-determinism |
| `executor.rs` | 2 | Workflow executor: `run_workflow` drives replay + live execution, handles suspension |
| `queue.rs` | 2 | Postgres task queue: `enqueue`, `claim` (FOR UPDATE SKIP LOCKED), `complete`, `fail` |
| `notify.rs` | 2 | LISTEN/NOTIFY wrapper: `Listener` (async stream), `Notifier` (pg_notify), channel naming |
| `worker.rs` | 2 | Worker runtime: poll loop, semaphore-bounded concurrent dispatch, graceful shutdown |
| `heartbeat.rs` | 2 | Batched heartbeat flusher: debounced channel receiver, bulk DB update |
| `timeout.rs` | 2 | Timeout enforcement scanner: start-to-close, schedule-to-start, heartbeat timeout queries |
| `cache.rs` | 2 | LRU workflow state cache: bounded capacity, access-order eviction |
| `dlq.rs` | 2 | Dead letter queue: `DeadLetterEntry` builder, move-to-DLQ on retry exhaustion |
| `pool.rs` | 2 | Separate DB pool config: web pool + worker pool with shared ceiling, minimum guarantees |
| `telemetry.rs` | 4 | OpenTelemetry surface: `TraceContextCarrier`, `TraceContextPropagator`, `MetricsRecorder`, `TelemetryConfig` — no-op by default, opt-in via `HarvestBuilder::telemetry` |
| `migrations/` | 1 | SQL -- run with `diesel migration run` |

### Macro Modules (`autumn-harvest-macros`)

| File | Purpose |
|------|---------|
| `lib.rs` | Entry points: `#[workflow]`, `#[activity]`, `workflows![]`, `activities![]` |
| `workflow.rs` | `workflow_macro` — emits user fn + companion `WorkflowInfo` fn |
| `activity.rs` | `activity_macro` — parses `retry`, `start_to_close`, `heartbeat_timeout`, `schedule_to_start`, `queue` attrs; emits user fn + companion `ActivityInfo` fn |
| `collect.rs` | `workflows_macro` / `activities_macro` — expand to `vec![companion_calls...]` |

---

## Macro Usage

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> {
    // ... orchestrate activities
    Ok(())
}

#[activity(start_to_close = "30s", queue = "email-workers")]
async fn send_email(ctx: &ActivityContext, addr: String) -> Result<(), String> {
    // ... I/O, external calls
    Ok(())
}

let app = autumn_web::app()
    .workflows(workflows![onboarding])
    .activities(activities![send_email])
    .worker(WorkerConfig::default())
    .harvest_api("/api/harvest");
```

The `workflows`, `activities`, `worker`, and `harvest_api` methods above are provided by `autumn-harvest-plugin::HarvestPlugin`, not the core crate.

Supported `#[activity]` attribute keys:
- `start_to_close = "30s"` — duration string parsed by `task_duration()`
- `heartbeat_timeout = "10s"`
- `schedule_to_start = "5m"`
- `retry = RetryPolicy::exponential(3, Duration::from_secs(1))` — any expression
- `queue = "email-workers"` — task queue name
- `local = true` — run inline on the workflow worker (see Local Activities below)

Duration strings: `"30s"`, `"5m"`, `"1h"`. Parsed via Harvest core's local `task_duration()` helper.

`#[workflow]` takes no attributes in Phase 1.

### Local Activities

Local activities run **inline on the workflow worker task** — they are never enqueued to `harvest_task_queue` and never dispatched to a remote worker. Their results are still recorded durably in `harvest_events` (`LocalActivityScheduled`, `LocalActivityCompleted`, `LocalActivityFailed`) so deterministic replay works identically to regular activities.

```rust
#[activity(local = true, start_to_close = "5s", retry = RetryPolicy::fixed(3, Duration::from_millis(100)))]
async fn compute_checksum(ctx: &ActivityContext, data: Vec<u8>) -> Result<String, String> {
    // pure CPU work — no I/O, no heartbeats
    Ok(hex::encode(sha256(&data)))
}
```

**Decision matrix — local vs regular activity:**

| | Local activity | Regular activity |
|---|---|---|
| Execution location | Inline on the workflow worker | Dispatched to task queue / remote worker |
| Typical duration | < 1 s | Any duration |
| Hard timeout cap | `WorkerConfig::max_local_activity_start_to_close` (default 60 s) | No cap enforced by Harvest |
| Heartbeating | **Not supported** | Supported |
| `schedule_to_start` timeout | **Not supported** | Supported |
| Custom task queue | **Not supported** | Supported |
| Retry policy | Supported | Supported |
| Durability / replay | Full (events appended to history) | Full (events appended to history) |

**Use a local activity when:**
- The work is in-process (pure computation, fast cache lookups, format conversions, orchestration glue)
- Latency matters and you want to avoid round-trips through the task queue
- The operation reliably completes within the 60 s default cap

**Use a regular activity when:**
- The work involves real I/O (HTTP, DB, filesystem)
- You need the activity to run on a different worker pool or machine
- The operation might take more than 60 s or needs heartbeating to signal liveness
- You want `schedule_to_start` timeout enforcement

---

## Development Commands

```bash
# Build without DB (works on Windows, no OpenSSL required)
cargo build -p autumn-harvest --no-default-features

# Build with DB (Linux/macOS with OpenSSL)
cargo build -p autumn-harvest

# Tests (no DB, works everywhere)
cargo test -p autumn-harvest --no-default-features
cargo test -p autumn-harvest-macros

# Tests with DB feature (requires running Postgres + OpenSSL)
cargo test -p autumn-harvest --features db

# Lint
cargo clippy -p autumn-harvest -- -D warnings
cargo clippy -p autumn-harvest-macros -- -D warnings

# Format check
cargo fmt --check

# Format
cargo fmt

# Migrations (requires diesel-cli and a running Postgres instance)
cd autumn-harvest && diesel migration run
```

The `testing` feature in `autumn-harvest/Cargo.toml` gates `WorkflowContext::new_test()` and `ActivityContext::new_test()` for use outside `#[cfg(test)]` blocks (e.g., in integration test binaries).

---

## Adding New Workflow Types or Activities

1. Annotate the async function with `#[workflow]` or `#[activity(..)]`.
2. The function must take `ctx: &WorkflowContext` or `ctx: &ActivityContext` as its first argument.
3. Return type must be `Result<T, E>` where both `T` and `E` implement `serde::Serialize` / `serde::Deserialize` and `E: ToString`.
4. Add the function name to `workflows![...]` or `activities![...]` in the builder call.
5. If the activity uses shared state (DB pool, HTTP client), register it on the builder with `.state(value)` (Phase 2) and access via `ctx.state::<T>()`.

---

## DB Schema Quick Reference

| Table | PK type | Purpose |
|-------|---------|---------|
| `harvest_workflow_executions` | `Uuid` | One row per workflow run |
| `harvest_events` | `i64` (BIGSERIAL) | Append-only event log per execution |
| `harvest_task_queue` | `Uuid` | Pending/active work items for workers |
| `harvest_dag_runs` | `Uuid` | DAG run instances |
| `harvest_schedules` | `Uuid` | DAG cron/interval schedule config |
| `harvest_signals` | `Uuid` | Pending signals queued for delivery |
| `harvest_timers` | `Uuid` | Durable timers registered by workflows |
| `harvest_dead_letters` | `Uuid` | Tasks that exhausted all retry attempts |

`harvest_workflow_executions` is the hub — six tables join back to it via `workflow_exec_id`.

---

## Phase 2 Modules

| Module | Purpose |
|--------|---------|
| `store.rs` | Event store (append/load history) |
| `replay.rs` | Deterministic replay engine (`HistoryMatcher`) |
| `executor.rs` | Workflow executor (`run_workflow` with replay + suspension) |
| `queue.rs` | Postgres task queue (SKIP LOCKED) |
| `notify.rs` | LISTEN/NOTIFY wrapper |
| `worker.rs` | Worker runtime (poll loop, semaphore-bounded dispatch) |
| `heartbeat.rs` | Batched heartbeat flusher |
| `timeout.rs` | Timeout enforcement scanner |
| `cache.rs` | LRU workflow state cache |
| `dlq.rs` | Dead letter queue |
| `pool.rs` | Separate pool configuration with shared ceiling |

---

## Testing

```bash
# Unit tests (no DB required for most)
cargo test -p autumn-harvest

# Integration tests (requires Docker for testcontainers)
cargo test -p autumn-harvest --test integration_e2e

# Replay tests
cargo test -p autumn-harvest --test replay_tests

# Macro tests
cargo test -p autumn-harvest-macros
```

---

## Design Decisions (Phase 2)

**DD-1: Oneshot suspension model**
Coroutine stays in memory; durability comes from the event history. When an activity is scheduled during live execution, the workflow function suspends via a oneshot channel. The executor re-invokes the workflow from the top on each replay cycle, replaying recorded events until it reaches the suspension point.

**DD-2: Separate DB pools with shared ceiling**
Worker pool and web pool are independently sized but share a total connection ceiling (`PoolConfig`). This prevents a burst of worker activity from starving HTTP request handling. `pool.rs` enforces minimum guarantees (at least 1 connection per pool) and distributes remainder to the web pool.

**DD-3: Workflow versioning via ctx.version()**
`WorkflowContext::version()` emits a `VersionMarker` event on first live call and replays the recorded version on subsequent runs. This allows workflow code to branch on version (`if ctx.version() >= 2 { ... }`) to handle non-determinism across deploys without breaking replay of in-flight executions.

**DD-4: Basic in-process LRU cache**
`WorkflowCache` is a bounded LRU cache for workflow state, keyed by `ExecutionId`. Cross-worker sticky routing (ensuring the same execution always lands on the same worker) is deferred to Phase 3/4. For now, cache misses just reload from the event store.

---

## Phase 4 Scope (next)

- **Cancellation semantics**: explicit workflow/activity cancellation and propagation
- **Saga primitives**: compensations and failure orchestration
- **Cross-worker routing**: sticky execution affinity and shard-aware placement
- **Sharding follow-up**: per-shard worker poll loops, per-shard scheduler tick loops with DAG→shard pinning, and multi-shard observability (the core `ShardId`/`ShardRouter`/`ShardedDbPool` primitives and the shard-aware write/read paths in the plugin are already in place).
- **Operational surface**: richer metrics, observability, and dashboard/UI work
