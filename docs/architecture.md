# autumn-harvest engineering reference

Postgres-backed durable workflow engine, companion to the Autumn web framework. Provides event-sourced workflow execution with activities, signals, timers, child workflows, and DAG scheduling.

This is the architecture and API reference for working on the engine itself:
workspace layout, crate relationships, design decisions, the module guide, and
the macro-usage patterns.

It is restored here from `CLAUDE.md` as of 89442c4, which `562c781` reduced to
workflow instructions: `CLAUDE.md` is agent instructions, not project
documentation. That reduction also took the phase list, which
[`docs/shipped-work.md`](shipped-work.md) restored — this file is the other
half, and the cross-references throughout `docs/` and the source comments point
back at the sections below.

## Workspace Structure

```
autumn-harvest/          <- workspace root
  autumn-harvest/        <- core library crate
    src/
      lib.rs
      types.rs           <- Phase 1
      error.rs           <- Phase 1
      erase.rs           <- Phase 3.30: targeted PII erasure (issue #495)
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
      testing.rs         <- Phase 3.5 (testing feature): WorkflowReplayer harness
      build_routing.rs   <- Phase 3.7: worker build-id routing (issue #171)
      handle_typed.rs    <- Phase 3.14: type-safe client stubs and handles (issue #341)
    migrations/
      20260409000000_harvest_initial/
      20260509000000_harvest_build_routing/
      20260518000001_harvest_workflow_execution_timeout/
      20260613000000_harvest_workflow_sla/
    tests/
      integration_e2e.rs <- testcontainers integration tests
      replay_tests.rs    <- replay engine integration tests
      build_routing_tests.rs <- build-id routing unit + integration tests
      sticky_routing_tests.rs <- sticky routing unit + integration tests (issue #235)
      scheduler_ha_tests.rs <- HA scheduler claim exclusivity tests (issue #350)
      macros_*.rs        <- proc-macro integration tests
  autumn-harvest-macros/ <- proc-macro crate
    src/
      lib.rs
      workflow.rs
      activity.rs
      collect.rs
```

Two crates in the workspace. `autumn-harvest` is the public library. `autumn-harvest-macros` is a separate proc-macro crate consumed by `autumn-harvest` via `prelude.rs`.

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

`#[query(workflow = "name")]` generates:

```
pub fn __autumn_query_handler_info_{name}() -> ::autumn_harvest::QueryHandlerInfo
```

`#[update(workflow = "name")]` generates:

```
pub fn __autumn_update_handler_info_{name}() -> ::autumn_harvest::UpdateHandlerInfo
```

`queries![name1, name2]` expands to `vec![__autumn_query_handler_info_name1(), __autumn_query_handler_info_name2()]`.

`updates![name1, name2]` expands to `vec![__autumn_update_handler_info_name1(), __autumn_update_handler_info_name2()]`.

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

**Sanctioned in-place mutation exception — payload erasure (`erase.rs`, issue #495):** `erase_workflow_payloads` is the **only** operation (alongside heartbeat checkpoints in `queue::record_heartbeat`) permitted to mutate existing `harvest_events.event_data` rows in-place. It replaces payload-bearing field values within the `data` object with a tombstone `{"_harvest_erased": true}` — the event `type`, variant structure, event IDs, timestamps, and sequence number are never touched. This exception is **terminal-only** (execution must be COMPLETED/FAILED/CANCELLED/TIMED_OUT/CONTINUED_AS_NEW/TERMINATED) to protect replay determinism of any resumable run, and is **irreversible**. No new `WorkflowEvent` variant is introduced.

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
3. Run `harvest shard health --candidate-shard <id>` or `GET /admin/shards/health?candidate_shard=<id>` and wait for `readiness: "ready"`. `degraded` rows include machine-readable `reason_codes`; `unavailable` rows keep reachable shards visible while naming the broken shard.
4. Add the new shard to `writable_shards`. New workflows begin landing on it via rendezvous hash. In-flight workflows on the old shards continue to drain through their own worker tasks.
5. **Verify coverage rather than assuming it** (issue #961): confirm the worker's *effective* `shard_assignments` includes the new shard (`GET /api/harvest/admin/config` → `worker.shard_assignments`; an empty `shard_assignments` in config resolves to "every shard this process has a pool for"), confirm `harvest_shard_dispatched_total{shard="<id>"}` is non-zero once work lands there, and confirm `harvest_shard_stranded_pending{shard="<id>"}` stays at `0`. A writable shard with pending work and no live poller is what the `harvest_shard_undrained` starter alert fires on.

`/api/harvest/health` is liveness-style by default. To make it fail with `503` when writable shard readiness is not `ready`, configure `[harvest.readiness] require_shard_readiness = true` or set `AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS=true`.

Current implementation scope: `ExecutionId`/`ShardId` encoding, `ShardRouter`, `ShardedDbPool`, shard-aware start/read paths in the plugin, `WorkerConfig.shard_assignments`, and shard health readiness for worker/scheduler rollout coverage. **Per-shard worker poll loops, per-shard background control loops, and per-shard scheduler ticks with DAG→shard pinning all ship** (issues #522 / #796 / #1157 / #797, reconciled and closed by #961):

- **Worker poll loops are per-shard.** `Worker::run_multi_shard` runs one claim attempt per assigned shard per tick with a rotating start index, so a deep backlog on one shard cannot starve dispatch on another (round-robin, a stronger equal-share guarantee than #515's per-queue weights, and it composes with them since queue weights decide *which row* within a claimed shard). Each shard also gets its own LISTEN/NOTIFY listener, fleet registration, heartbeat and drain.
- **`shard_assignments` defaults to "auto"** (issue #961). An **empty** `shard_assignments` means "cover every shard this process has a pool for" — resolved by `builder::resolve_shard_assignments` at the two choke points where the pool is final: `HarvestRunner` (right after it installs the sharded pool, so its coverage warning sees the effective set) and `Worker::new`. Resolution is deliberately **not** applied in `From<WorkerConfig> for WorkerRuntimeConfig` — the runner assigns `sharded_pool` *after* that conversion, so resolving there would see no pool and accomplish nothing. It is idempotent, so double-application is harmless. Setting an explicit list is never widened, so one-worker-process-per-shard deployments still narrow deliberately; `HarvestRunner` emits a `tracing::warn!` naming any writable router shard the worker does not cover. With **no** sharded pool the list stays **empty** — there is no shard identity to resolve, and fabricating a `[ShardId(0)]` would assert a shard number the process never established (the write-side twin of issue #1150, whose read-side consumers all normalise an empty array as "covers whatever shard the row was read from"). A single-shard deployment is therefore byte-for-byte unchanged, and its dispatch stays visible on `harvest.queue.dispatched{queue}` (#515); the per-shard `harvest.shard.dispatched{shard}` counter is emitted only when the worker actually has a shard identity.
- **Background control loops are per-shard.** Timeout/SLA enforcement, poison-pill reclaim, pause auto-resume, session-slot reconciliation, and the external signal/cancel outboxes each run against every assigned shard.
- **Scheduler ticks are per-shard**, and #350's HA `fire_claim_token` exclusivity holds **per shard by construction** — the claim is a row-level `UPDATE` and each `harvest_schedules` row lives in exactly one shard database, so no shard-scoped claim column and **no migration** is required. A schedule's home shard is deterministic: DAGs route through `pick_for_dag` (rendezvous), non-DAG schedules through the router's default shard, and `scheduled_fire_exec_id` encodes it into the fired `ExecutionId` on both the main dispatch path and the buffered-run drain.
- **Coverage is observable.** `harvest.shard.dispatched{shard}` reports the live per-shard dispatch split (the shard-dimension twin of `harvest.queue.dispatched{queue}`), `harvest.shard.stranded_pending{shard}` reports claimable work on a shard with no live poller, and shard health surfaces a `no_live_worker` reason code.

---

## Module Guide

| Module | Phase | Purpose |
|--------|-------|---------|
| `types.rs` | 1 | Newtypes: `WorkflowId` (String), `ExecutionId` (Uuid v4), `ActivityExecId` (Uuid v4), `TimerId` (String), `WorkerId` (String) |
| `error.rs` | 1 | `HarvestError` (thiserror), `HarvestResult<T>`, `TimeoutType` enum |
| `policy.rs` | 1 | `RetryPolicy`, `TriggerRule`, `Schedule`, `TaskStatus`, `compute_retry_delay` |
| `event.rs` | 1 | `WorkflowEvent` enum (41 variants, adjacently-tagged serde), `type_name()`. Variants added in issue #140: `UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed`. `SideEffectRecorded` + bounded `SideEffectKind` enum added in issue #384 (deterministic primitives). `ExternalCancelRequested`, `ExternalCancelDelivered`, `ExternalCancelFailed` added in issue #492. `ExternalSignalRequested` gained an **additive** optional `idempotency_key: Option<String>` field in issue #521 (no new variant) |
| `event.rs` | 1 | `WorkflowEvent` enum (35 variants, adjacently-tagged serde), `type_name()`. Variants added in issue #140: `UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed`. `SideEffectRecorded` + bounded `SideEffectKind` enum added in issue #384 (deterministic primitives). `WorkflowStarted` gains two additive optional fields in issue #488: `last_completion_result: Option<serde_json::Value>` and `last_error: Option<String>` (both `#[serde(default, skip_serializing_if = "Option::is_none")]`, frozen at schedule-fire time). |
| `context.rs` | 1+2 | `WorkflowContext` (replay, suspension, version gate, timers), `ActivityContext` (heartbeat channel, cancellation) |
| `info.rs` | 1 | `WorkflowInfo`, `ActivityInfo`, `WorkflowHandlerFn`, `ActivityHandlerFn` type aliases |
| `builder.rs` | 1 | `HarvestBuilder` (fluent), `WorkerConfig` (queues, concurrency, timeouts) |
| `prelude.rs` | 1 | Core glob re-export surface including macros |
| `schema.rs` | 1 | Diesel `table!` macros -- 11 tables (includes `harvest_build_policies`, `harvest_build_compat`) |
| `models.rs` | 1 | `Queryable`/`Selectable` read structs and `Insertable` `New*` write structs for all 11 tables |
| `store.rs` | 2 | Event store and read helpers: `append_events`, `load_history`, `load_history_since` (delta load for cache-hit path, issue #235), `load_timestamped_history` (read-only, undecoded, `(timestamp, WorkflowEvent)` rows ordered by id — issue #739), `events_to_rows` with sequential event IDs, `load_workflow_children` for parent -> child operator queries |
| `timeline.rs` | 3.50 | Per-execution timeline read model (issue #739): pure `db`-free `derive_timeline(...) -> Timeline`; `Timeline`/`TimelineStep`/`TimelineRollup`/`SlowestStep`/`StepKind`/`StepOutcome`/`TimelineEventRow` (re-exported from `lib.rs`). Reconstructs a run's wall-clock breakdown (per-step wait/exec split where derivable, busy/wait totals, slowest step) purely from recorded `harvest_events` timestamps. No new event variant, no migration, no write path. Route `GET /workflows/{id}/timeline`. |
| `stall_diagnosis.rs` | 3.51 | Per-execution stall diagnosis — the pure root-cause classifier (issue #809): `ExecutionHealth`, the discriminated `BlockedOn` enum, `PendingActivityFacts`/`ExternalHandoffFacts`/`PendingChildFacts`/`AwaitedSignalFacts`/`PendingTimerFacts`/`WorkflowTaskFacts`/`ReplayWaitFacts`/`NdBlockFacts`/`DiagnosisInputs`, `classify_pending_activity`, `classify_execution`, `classify_workflow_task`, `workflow_task_hard_impediment`, `workflow_wake_was_missed`, `activity_precedence`, `summarize`, `TIMER_OVERDUE_GRACE_SECONDS` (all re-exported from `lib.rs`). No `db` feature, no DB access, no event variant, no migration. Route `GET /workflows/{id}/diagnose`. |
| `replay.rs` | 2 | Deterministic replay engine: `HistoryMatcher` walks event history, detects non-determinism |
| `executor.rs` | 2 | Workflow executor: `run_workflow` drives replay + live execution, handles suspension |
| `queue.rs` | 2 | Postgres task queue: `enqueue`, `claim` (FOR UPDATE SKIP LOCKED), `complete`, `fail` |
| `notify.rs` | 2 | LISTEN/NOTIFY wrapper: `Listener` (async stream), `Notifier` (pg_notify), channel naming |
| `worker.rs` | 2 | Worker runtime: poll loop, semaphore-bounded concurrent dispatch, graceful shutdown |
| `workers.rs` | 4 | Worker fleet registry: `register_worker`, `heartbeat_worker`, `transition_status`, `list_workers`, `get_worker`, `fleet_health`, `spawn_worker_heartbeat` |
| `heartbeat.rs` | 2 | Batched heartbeat flusher: debounced channel receiver, last-write-wins timestamp + checkpoint payload DB update |
| `timeout.rs` | 2 | Timeout enforcement scanner: start-to-close, schedule-to-start, heartbeat timeout queries, and workflow execution timeouts (`enforce_workflow_execution_timeouts`, issue #243) |
| `cache.rs` | 2 | LRU workflow state cache: bounded capacity, access-order eviction. `CachedWorkflowState` holds `events: Vec<WorkflowEvent>` + `next_event_id: i32` for delta-load support (issue #235). |
| `dlq.rs` | 2 | Dead letter queue: `DeadLetterEntry` builder, move-to-DLQ on retry exhaustion |
| `pool.rs` | 2 | Separate DB pool config: web pool + worker pool with shared ceiling, minimum guarantees |
| `erase.rs` | 3.30 | Targeted PII erasure (issue #495): `ERASURE_TOMBSTONE_KEY`, `erasure_tombstone()`, `tombstone_payload_fields(event_value)` (pure, no-DB), `is_terminal_state(state)`, `EraseOutcome`/`SkippedChild`/`EraseFailure`; DB-gated `erase_workflow_payloads(conn, exec_id, reason)`. **Sanctioned in-place mutation exception** to the append-only invariant (alongside heartbeat checkpoints): only `data` field contents are mutated, never event structure. Terminal-only, irreversible, idempotent, cascades to terminal children on the same shard. |
| `payload_store.rs` | 3.37 | Large-payload claim-check offloading (issue #524): `PayloadStore` async trait (embedder-supplied backend, no cloud client in core), `PayloadOffloader` (`offload_event_value`/`inflate_event_value`/`extract_offload_ref`/`refs_in_event_value`), reference-envelope + sha256 checksum helpers. Composes after `PayloadCodec`; no new `WorkflowEvent` variant. Store seam in `store.rs` (`append_events_offloaded`/`load_history_inflated`); GC via `harvest_payload_refs` (migration `20260627000001`). |
| `completion_callback.rs` | 3.46 | Durable completion callbacks (issue #605): `validate_target_url`/`SsrfPolicy`/`HostAllowlist`/`SsrfRejection` (pure SSRF guard, HTTPS-only + allowlist-required by default), HMAC-SHA256 envelope signing (`build_envelope`/`sign`/`CallbackSecret`), `EventFilter`/`CallbackTarget`/`resolve_all_targets`/`resolve_effective_targets` (pure config resolution), boxed-future `CompletionCallbackDeliverer` trait (no HTTP client in core, mirrors `PayloadStore`/`HistoryArchiver`), `classify_outcome`/`OutcomeAction` (pure retry/backoff/dead-letter decision), `enqueue_completion_deliveries` (folded into `completion_trigger::evaluate_triggers_for_execution`'s existing terminal transaction), `fire_due_completion_deliveries` (two-transaction scanner folded into `timeout::enforce_timeouts_once`), `list_deliveries_for_execution`/`redrive_delivery` (management API), `GLOBAL_CALLBACK_CONFIG` static. New table `harvest_completion_deliveries` (migration `20260705000000`) plus `completion_callbacks jsonb NULL` on `harvest_workflow_executions`. No new `WorkflowEvent` variant, no replay impact. See `docs/completion-callbacks.md`. |
| `update.rs` | 3.6 | Update primitive: `UpdateRegistry` (type-erased validators + async handlers), `BoxUpdateHandler`, `BoxUpdateValidator`. `WorkflowContext` methods: `register_update_handler`, `register_update_handler_no_validator`, `validate_update`, `execute_admitted_update`. `HistoryMatcher` methods: `match_update(update_id)`, `drain_admitted_updates()`. Error variants: `HarvestError::UpdateRejected`, `HarvestError::UpdateHandlerNotFound` |
| `query.rs` | 3.10 | Query registry: `QueryRegistry`, `QueryHandler`. `WorkflowContext` methods: `register_query` (no-arg), `register_query_handler<Req,Resp>` (typed), `execute_query_with_args`, `list_query_names`. Error variants: `QueryHandlerNotFound`, `WorkflowNotRunning`, `QueryHandlerPanicked`, `QueryTimedOut`. `WorkerConfig::query_timeout` (default 5 s). `telemetry::METRIC_QUERY_DURATION` constant. No `WorkflowEvent` variants — queries leave zero footprint in `harvest_events`. |
| `signal_handler.rs` | 3.42 | Push-based signal handler registry (issue #546): `SignalHandlerRegistry` (type-erased, synchronous, fire-and-forget, first-registration-wins), `BoxSignalHandler`, `invoke_signal_handler` (panic-safe invocation). `WorkflowContext` methods: `register_signal_handler<Req>` (typed), `register_signal_handler_raw` (untyped, storage-only, no inline dispatch), `list_signal_handler_names`. Dispatch runs through `WorkflowContext::pump_signal_handlers` (triggered by `match_history`'s post-hook, plus an executor-level `flush_pending_signal_handlers` backstop at end of cycle) and `HistoryMatcher::claim_pending_signal(name)`, a cursor-bound claim against `pending_signals` (populated by the same `prepare_match`/`drain_early_signals` sweep every other `match_*` call uses) — never a full-history scan — so a handler cannot fire ahead of an unconsumed activity/timer/etc. in history. Claims across all registered names are collected before any dispatch and sorted by event index, so cross-handler-name ordering always follows history order, not registration order. Marks claimed events consumed so `match_signal`/`wait_for_signal` never double-delivers the same event. No new `WorkflowEvent` variant — `SignalReceived` is reused. |
| `webhook_trigger.rs` | 3.46 | Inbound webhook trigger descriptors (issue #344): `WebhookCtx` (verified request metadata), `WebhookHandlerError` (`Deserialize`/`Rejected`), `WebhookHandlerFn` (fn pointer, mirrors `WorkflowHandlerFn`), `WebhookTarget::{Starts, SignalsWithStart}`, `WebhookTriggerInfo`, pure `validate_webhook_triggers`. Unconditional (not `db`-gated) — consumed by the `#[webhook]` macro and `autumn-harvest-plugin::webhook_receiver` (feature `webhooks`). Autumn-web 0.5's `[security.webhooks]`/`SignedWebhook` owns verification; this module owns mapping + idempotent dispatch. |
| `sessions.rs` | 3.46 | Worker sessions -- fleet-side, engine-internal concerns for co-locating an activity pipeline on one worker (issue #606). Pure (no-DB) predicates: `AcquireEligibility`/`session_acquire_eligible`, `BrokenSessionReason`/`broken_session_reason`, `lease_expired`, `acquire_retry_backoff`. In-process slot tracking: `SessionSlotCounter` (plain `Arc<AtomicI64>`, not a `Semaphore`/`OwnedSemaphorePermit` map -- avoids the `clippy::significant_drop_tightening` trap `slot_tuner.rs`'s `TunedSlotRuntime` already hit), `try_acquire_session_slot`/`release_session_slot`. DB-gated (`db` feature): `record_session_acquired`/`record_session_completed`, `broken_session_candidates_query`/`enforce_broken_sessions` (folded into `timeout::enforce_timeouts_once`). Complements the author-facing `WorkflowContext::create_session`/`Session` API in `context.rs`. |
| `build_routing.rs` | 3.7 | Worker build-id routing: `BuildCompatibilitySet` (in-memory eligibility checker), `BuildPolicy`, `BuildCompatEntry`, `BuildReachability`. DB functions: `set_build_policy`, `get_build_policy`, `list_build_policies`, `declare_compat`, `revoke_compat`, `load_compat_set`, `build_reachability`, `all_build_reachability`, `all_build_reachability_sharded` (cross-shard fan-out), `merge_reachability`. New newtypes in `types.rs`: `BuildId`, `DeploymentName`. **Percentage build ramp (issue #604, Phase 3.45):** `BuildPolicy.target_build_id`/`.ramp_percent`, `ramp_bucket`, `validate_ramp_percent`, `BuildPolicy::resolve_assigned_build` (deterministic per-`ExecutionId` ramp decision), `set_build_ramp`, `clear_build_ramp`. See `docs/runbooks/safe-deploy.md` for the operator deploy playbook (incl. the percent-ramp scenario). |
| `telemetry.rs` | 4 | OpenTelemetry surface: `TraceContextCarrier`, `TraceContextPropagator`, `MetricsRecorder`, `TelemetryConfig` — no-op by default, opt-in via `HarvestBuilder::telemetry`. Implements all 8 ADR-0001 span kinds (issue #136); see `docs/adr/0001-otel-trace-contract.md` for the full attribute schema and propagation rules. Metric catalogue (ADR-0001 §7): `harvest.workflow.started` (counter, `worker.rs`), `harvest.workflow.duration` (histogram, `worker.rs`), `harvest.workflow.terminal` (counter, `worker.rs`/`timeout.rs`/`execution.rs`, issue #519, labels: `workflow.name`, `queue`, `outcome` — 6 bounded values: completed/failed/cancelled/timed_out/terminated/continued_as_new), **Activity-outcome trio** (issue #528): `harvest.activity.duration` (histogram, `worker.rs`, labels: `activity`, `queue`, `status`), `harvest.activity.failed` (counter, `worker.rs`, richer labels: `activity`, `workflow.type`, `error.type`, `non_retryable` — per-attempt terminal/non-retryable failure signal), `harvest.activity.attempts` (counter, `worker.rs`, labels: `activity`, `queue`, `outcome` — 2 values: `completed`/`failed`; fires for **both** outcomes so success-rate = `attempts{outcome=completed}/attempts` within a single family), `harvest.activity.retries` (counter, `worker.rs` `handle_activity_result`, labels: `activity`, `queue`; fires once per retry actually enqueued after the `schedule_to_close` deadline check — retry-storm signal). `harvest.timer.started` (counter, `worker.rs`), `harvest.queue.depth` (gauge, `worker.rs` sampler), `harvest.queue.schedule_to_start` (histogram, `worker.rs` `dispatch_task` — recorded after the concurrency permit is acquired so it captures worker-local backpressure, issue #501, label: `queue`; wall-clock seconds from task eligibility to execution start, discounting the immediate-enqueue skew allowance via `queue::schedule_to_start_secs` — the canonical worker-capacity SLI), `harvest.queue.oldest_pending_age` (gauge, `worker.rs` sampler, issue #501, label: `queue`; age of oldest *claimable* eligible task — excludes PAUSED executions mirroring `claim_task`, skew-discounted; resets to 0 when queue drains), `harvest.dlq.entries` (gauge, `worker.rs` sampler), `harvest.worker.slots_in_use` / `harvest.worker.slots_available` (gauges, `worker.rs` `spawn_worker_slot_sampler`, issue #531, label: `slot_type` — `workflow`/`activity`; pure in-memory read of the two dispatch `Semaphore`s' `available_permits()` against the configured max, invariant `slots_in_use + slots_available == configured_max` per slot type within one sampler interval; `execution.id` is never a label), `harvest.schedule.runs` (counter, `scheduler.rs`), `harvest.schedule.skipped` (counter, `scheduler.rs`), `harvest.retention.deleted` (counter, `retention.rs`), `harvest.workflow.cache_hit` (counter, `worker.rs`, issue #235), `harvest.workflow.cache_miss` (counter, `worker.rs`, issue #235), `harvest.workflow.timeout` (counter, `timeout.rs`, issue #243), `harvest.workflow.sla_breached` (counter, `timeout.rs`, issue #487, labels: `workflow`, `queue`; observation-only, emitted exactly once per run on soft-SLA breach), `harvest.schedule.fire_attempts` (counter, `scheduler.rs`, issue #350, labels: `schedule`, `outcome`), `harvest.task.quarantined` (counter, `poison_pill.rs`, issue #367, labels: `queue`, `reason`), `harvest.activity.circuit.tripped` (counter, `worker.rs`, issue #369, label: `activity.name`), `harvest.activity.circuit.closed` (counter, `circuit_breaker.rs`, issue #369, label: `activity.name`), `harvest.workflow.debounced` (counter, `api.rs` plugin, issue #499, label: `workflow` — the debounce key is deliberately *not* a label, as it is derived from user/tenant input and would be unbounded), `harvest.workflow.debounce_fired` (counter, `debounce.rs` scanner, issue #499, labels: `workflow`, `queue`), `harvest.payload.offloaded` (counter+bytes-offloaded measure, `payload_store.rs`, issue #524, labels: `payload.field`, `store.id`; incremented once per offloaded field on write), `harvest.payload.offload_fetch_duration` (histogram, `payload_store.rs`, issue #524, label: `store.id`; records inflate latency on read), `harvest.webhook.received` / `harvest.webhook.rejected` (counters, `webhook_receiver.rs` plugin, issue #344, labels: `path` — bounded to registered `#[webhook]` bindings — and `outcome` via the bounded `WebhookOutcome` enum; `rejected` never fires for `accepted`/`idempotent_replay`), `harvest.workflow.start_throttled` (counter, `api.rs`/`scheduler.rs`, issue #607, label: `workflow` only — the resolved throttle key is deliberately *not* a label, unbounded cardinality; per-key backlog is exposed via `GET /admin/start-throttle` instead), `harvest.update.duration` (histogram, `worker.rs` `emit_update_result_metrics`, issue #781, labels: `workflow`, `name`, `queue`, `outcome` (`completed`/`failed`); admit→terminal update latency — the latency companion to the #684 `harvest.update.completed`/`failed` counters, emitted on the same post-commit path; rejected updates excluded). Cardinality rule: `execution.id` is span-only; `MetricsRecorder` API enforces this by construction. **Custom user metrics (issue #532):** three additive default no-op trait methods `record_user_counter`/`record_user_gauge`/`record_user_histogram`; `USER_METRIC_PREFIX = "harvest.user."` constant; `UserMetricError` enum (thiserror); `validate_user_metric(name, labels)` pure validation (reserved prefix, length, forbidden label keys, label cap); `UserMetrics<'a>` handle with suppression gate + `is_enabled()` short-circuit + validation + prefix; all re-exported from `lib.rs`. |
| `concurrency.rs` | 3.13 | Per-key concurrency limits (issue #247): `ConcurrencyPolicy { key_expr, limit }` attached to `WorkflowInfo`; `resolve_concurrency_key(expr, input)` resolves a dot-notation field path against the JSON input at workflow-start time. Limits enforced within a shard via the existing `concurrency_key`/`concurrency_cap` claim-query path. See `docs/sharding.md` for the cross-shard scope contract. |
| `debounce.rs` | 3.31 | Debounced workflow starts (issue #499): `DebouncePolicy { key_expr, window, max_wait }` (`Copy`, mirrors `ConcurrencyPolicy`); `resolve_debounce_key(expr, input)` delegates to `concurrency::resolve_concurrency_key`; `compute_fire_deadline(now, window, first_seen, max_wait)` pure deadline logic (no DB); DB-gated `admit_debounced_start` (upsert, trailing-edge, last-input-wins), `fire_due_debounced_starts` (scanner: claim, start, delete in one txn), `list_pending_debounce` (management API). Key scoping: shard-local. **Semantics**: trailing-edge, burst collapse, last-input-wins, max-wait cap (default 1h, overridable via `WorkerConfig::default_debounce_max_wait`). See `examples/debounce_webhook.rs`. |
| `throttle.rs` | 3.47 | Workflow-start throttle — pace admissions, defer the excess (issue #607): `ThrottlePolicy { refill_per_sec, burst, key_expr: Option<&'static str>, schedule_to_start: Option<Duration> }`; pure `parse_rate("100/m")`/`ThrottlePolicy::from_rate_str` (rate-string parsing, burst defaults to the per-period count); `resolve_throttle_key` delegates to `concurrency::resolve_concurrency_key`; `bucket_key`/`compute_expiry` pure helpers (no DB). DB-gated `reserve_or_defer` (the admission primitive: FIFO backlog guard → `queue::try_consume_rate_limit_token` against the reused `harvest_rate_limit_buckets` table → `Reserved` (proceed immediately) or `Deferred` (durable row in `harvest_start_throttle`, written before `WorkflowStarted`)), `fire_due_throttled_starts` (scanner mirroring `debounce::fire_due_debounced_starts`: claim oldest-first, drop if past `schedule_to_start`, else debit-and-start-or-leave-for-next-tick), `throttle_backlog_by_key`/`list_pending_throttle` (management API). Key scoping: shard-local (per-shard buckets — cross-shard global rate coordination is an explicit, documented out-of-scope limitation). **Semantics**: token-bucket/GCRA pacing, defer-don't-drop (contrast debounce's collapse-to-one — a throttle preserves *every* start as its own pending row), id-reuse short-circuits refund their token. See `examples/throttle_fanout.rs`. |
| `metrics_rs_adapter.rs` | 4 | `metrics-rs` feature flag adapter: `MetricsRsRecorder` bridges `MetricsRecorder` → `metrics` crate global registry. See `docs/telemetry.md` for recipe. |
| `poison_pill.rs` | 3.17 | Poison-pill task quarantine (issue #367): pure `quarantine_decision`/`ReclaimAction` (no DB dep), `orphaned_running_tasks_query` (worker-liveness reclaim, independent of per-task timeouts), `reclaim_orphaned_tasks` (increment `crash_strikes`, requeue-or-quarantine), `spawn_poison_pill_reclaimer`. Quarantine → `harvest_dead_letters` with `DeadLetterReason::PoisonPill` + terminal `WorkflowFailed` (no new event variant). `WorkerConfig::poison_pill_threshold` (default 3, 0 disables). Shard-local. |
| `circuit_breaker.rs` | 3.18 | Per-activity circuit breaker (issue #369): `CircuitBreakerRegistry` (closed/open/half-open, rolling-window failure count, single half-open probe, `on_dispatch`/`on_result`, `force_open`/`force_close`, `snapshot`/`list`), `CircuitPhase`, `DispatchDecision`, `CircuitTransition`, `CircuitSnapshot`. Pure/in-process, per-shard; consulted by the worker before dispatch and shared with the management API via `HandlerRegistry::circuit_breakers()`. No new event variant, no migration. |
| `slot_tuner.rs` | 3.42 | Adaptive worker dispatch-slot tuner (issue #548): `SlotTuner` trait, `DefaultSlotTuner` (pool-pressure shrink / saturated-and-waiting grow / hold), `SlotTunerConfig { min_slots, max_slots, tuner }` (`::new`/`::with_tuner`), pure helpers `initial_target`/`apply_action`/`validate_band`/`tuned_available`, `TunedSlotRuntime` (owns withheld `OwnedSemaphorePermit`s; `resize_toward`/`release_all_withheld`), `spawn_slot_tuner_loop`. Opt-in via `WorkerConfig::with_slot_tuner`; `None` (default) is byte-identical to the pre-#548 fixed-concurrency semaphore. No new event variant, no migration, no replay surface — purely an in-process semaphore control constructed inside `worker.rs::spawn_monitoring_tasks` (never stored on `Worker` itself, to avoid clippy's significant-drop propagation into every `Worker`-holding test). See `docs/operations/adaptive-slot-tuner.md`. |
| `migrations/` | 1 | SQL -- run with `diesel migration run` |

### Macro Modules (`autumn-harvest-macros`)

| File | Purpose |
|------|---------|
| `lib.rs` | Entry points: `#[workflow]`, `#[activity]`, `#[query]`, `workflows![]`, `activities![]` |
| `workflow.rs` | `workflow_macro` — emits user fn + companion `WorkflowInfo` fn |
| `activity.rs` | `activity_macro` — parses `retry`, `start_to_close`, `heartbeat_timeout`, `schedule_to_start`, `queue` attrs; emits user fn + companion `ActivityInfo` fn |
| `collect.rs` | `workflows_macro` / `activities_macro` / `webhooks_macro` — expand to `vec![companion_calls...]` |
| `query.rs` | `query_macro` — pass-through attribute that validates the annotated item is a function; used for documentation and future typed query discovery |
| `webhook.rs` | `webhook_macro` (issue #344) — emits user fn + companion `WebhookTriggerInfo` fn + public `{fn}_info()` alias; rejects `verifier =`/`idempotency_header =` at compile time (superseded by autumn-web's `[security.webhooks]`) |

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

Supported `#[workflow]` attribute keys (all optional):
- `execution_timeout = "30m"` / `chain_execution_timeout = "7d"` / `sla = "2h"` — duration strings
- `concurrency(key = "input.tenant_id", limit = 10)` — per-key concurrency (issue #247)
- `debounce(key = …, window = …, max_wait = …)` / `batch(…)` / `throttle(rate = "100/m", …)` — admission policies
- `max_input_bytes = 4_194_304` — per-workflow payload cap raiser (issue #252)
- `owner = "team"` / `runbook = "url"` / `severity = "page"` / `description = "…"` — ops metadata
- `retry = RetryPolicy::exponential(3, Duration::from_secs(1))` — workflow-level retry (issue #523)
- `mcp` / `mcp = true` — expose as an MCP tool (issue #597)
- `activities = [send_email, charge_card]` / `children = [generate_report]` — opt-in declared
  dependencies, resolved against the registered activity/workflow catalogs at deploy-time
  preflight (issue #802; see `docs/getting-started/10-operations.md`)
- `allow_nondeterministic_apis` — suppress the determinism guardrails for this function

### Embedder Primitives — SignalWithStart (issue #244)

`signal_with_start_workflow_execution` is the atomic *start-or-attach + signal*
primitive built for webhook receivers and idempotent event-driven flows. It
collapses the racy fetch-then-start-then-signal trio into one shard-local
transaction. The same primitive is exposed over HTTP as
`POST /api/harvest/workflows/{workflow_name}/signal-with-start`.

**Outcome matrix** — `reuse_policy` × prior execution state:

| Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
|-------------|------------------|-------------------|---------------------------|----------------------|
| none | start + signal | start + signal | start + signal | start + signal |
| RUNNING / SUSPENDED | signal existing | `Err(AlreadyExists)` | signal existing | cancel + start + signal |
| COMPLETED | start fresh + signal | `Err(AlreadyExists)` | start fresh + signal | start fresh + signal |
| FAILED | start fresh + signal | `Err(AlreadyExists)` | start fresh + signal | start fresh + signal |
| CANCELLED | start fresh + signal | `Err(AlreadyExists)` | start fresh + signal | start fresh + signal |
| TERMINATED | start fresh + signal | start fresh + signal | start fresh + signal | start fresh + signal |

For terminal priors, `AllowDuplicate` and `AllowDuplicateFailedOnly` diverge
from the standalone `start_or_load_workflow_execution` semantics (which return
the existing terminal run): signal-with-start escalates internally to a
fresh start so the spec's "no signal silently dropped" invariant holds.

`TERMINATED` is the *sealed* state set by the reset path: the row is released
from the partial unique index, so `RejectDuplicate` no longer treats it as a
duplicate. The reset operator explicitly opted the prior row out of the
uniqueness scope, matching the broader `start_or_load_workflow_execution`
semantics.

**Idempotency dedupe is scoped to the logical workflow**, not the
`workflow_exec_id`. A webhook retry carrying the same `idempotency_key` that
arrives after the original execution has reached a terminal state will be
recognised as a duplicate and short-circuited: no fresh execution is started
and no second signal is queued, even though the fresh-start escalation would
otherwise create a new `exec_id`. The dedupe joins `harvest_signals` to
`harvest_workflow_executions` so the per-shard partial unique index
(`workflow_exec_id, idempotency_key`) is augmented with a
`(workflow_name, workflow_id)` scope.

`SignalWithStartOutcome.started_fresh` distinguishes a freshly inserted run
from one attached to an existing live execution; `signal_delivered` reports
whether the signal row was actually queued (it is `false` when the prior
execution is terminal or the `idempotency_key` matched a row that was
already enqueued).

**Event ordering.** On fresh start the call appends only `WorkflowStarted` in
this transaction; the signal is staged as a `harvest_signals` row that the
worker's existing `ingest_pending_signals` path promotes to `SignalReceived`
*before* the workflow function is first dispatched. No new `WorkflowEvent`
variant is introduced — the issue's append-only invariant is preserved by
construction.

**Idempotency key.** `idempotency_key: Option<String>` is backed by a partial
unique index on `harvest_signals (workflow_exec_id, idempotency_key) WHERE
idempotency_key IS NOT NULL`. Two webhook deliveries carrying the same key
produce exactly one `SignalReceived` event.

**Input schema validation (issue #373) — fresh start only.**
`SignalWithStartParams.workflow_info: Option<&WorkflowInfo>` lets a caller
supply the target workflow's registered info so `input` (`start_input`) is
validated against its published JSON Schema, when one is set. Validation
runs *inside* the start transaction's `FOR UPDATE` lock and *only* when the
call actually creates a fresh execution (`SignalWithStartOutcome::started_fresh
== true`) — never on an attach, since `start_input` is never written there. A
schema-invalid fresh start returns `HarvestError::InputValidationFailed { violations }`
and rolls back with no execution row persisted. `None` skips validation
entirely (a schema-less workflow, or a caller — e.g. the typed client stub,
whose `input: I` is already checked by Rust's type system — that intentionally
never validates). The HTTP route below always passes the registered
`WorkflowInfo`, so schema validation applies uniformly to every JSON caller
without ever rejecting a legitimate signal to an already-running execution
because its *signal* payload doesn't match the *start*-input schema (a real
bug fixed in issue #918's review of the webhook receiver, which reuses this
same primitive for `#[webhook(signals = ...)]` targets).

HTTP route:
- `POST /api/harvest/workflows/{workflow_name}/signal-with-start` with body
  `{ workflow_id, start_input, signal_name, signal_payload, id_reuse_policy?, idempotency_key?, queue?, memo?, search_attrs?, execution_timeout_secs? }`
  → `201 Created` (fresh start) or `200 OK` (attached) with response
  `{ execution_id, workflow_name, workflow_id, state, started_fresh, signal_delivered }`.
- `409 Conflict` when `id_reuse_policy = reject_duplicate` rejects an
  existing execution.

See `examples/signal_with_start_webhook.rs` for a worked Stripe webhook
example.

### Standalone Start — Conflict Policy (issue #685)

The plain `POST /workflows/{workflow_name}/start` route (and the CLI
`harvest workflow start`) accepts **two orthogonal axes** for handling a
`(workflow_name, workflow_id)` collision:

- **`reuse_policy`** (unchanged, 4 values) governs a **terminal** prior run
  (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`SUSPENDED`).
- **`conflict_policy`** (new) governs an **active** (`RUNNING`/`PAUSED`) prior
  run. `unspecified` (the default) defers to the reuse policy's *native* active
  behavior, so omitting the field is byte-for-byte identical to today.

The two axes are independent: `conflict_policy` has no effect on a terminal
prior, and `reuse_policy` has no effect on an active prior (except via the
`unspecified` fallback).

**Effective ACTIVE-prior behavior** — `reuse_policy` (rows) × `conflict_policy`
(columns). Cells: `Err(AlreadyExists)` (409) / *return existing* (attach, 200) /
*cancel + start fresh* (201).

| reuse_policy \ conflict_policy | `unspecified` (default) | `fail` | `use_existing` | `terminate_existing` |
|--------------------------------|-------------------------|--------|----------------|----------------------|
| `allow_duplicate`              | return existing (attach) | Err(AlreadyExists) | return existing (attach) | cancel + start fresh |
| `reject_duplicate`             | Err(AlreadyExists)      | Err(AlreadyExists) | return existing (attach) | cancel + start fresh |
| `allow_duplicate_failed_only`  | return existing (attach) | Err(AlreadyExists) | return existing (attach) | cancel + start fresh |
| `terminate_if_running`         | cancel + start fresh    | Err(AlreadyExists) | return existing (attach) | cancel + start fresh |

**Backward continuity.** The `unspecified` column *is* today's four `reuse_policy`
variants exactly — `(reuse, conflict=unspecified)` is byte-for-byte the pre-#685
behavior for every reuse policy. Terminal priors are unaffected by
`conflict_policy` entirely.

**AC-3 — the idempotent-starter shape.** `reuse=terminate_if_running` +
`conflict=use_existing` gives *fresh-for-all-terminal* + *attach-active*: a
terminal prior is replaced with a fresh run (the `terminate_if_running` *terminal*
half is unchanged — it always starts fresh), while `use_existing` overrides
`terminate_if_running`'s *active* half from **cancel** to **attach**, so a still-
running prior is returned rather than cancelled. This is the canonical
"start-or-attach a singleton entity workflow" pattern with a single HTTP call.

**HTTP mapping.**
- *attach* → `200 OK`, `created: false` (and `started_fresh: false` **when the
  request sent a `conflict_policy` field** — sending the field, even
  `unspecified`, opts into the attach-vs-fresh distinguisher; omitting it keeps
  the response byte-for-byte identical to today).
- *cancel + start fresh* → `201 Created`, `started_fresh: true`.
- *fail over an active prior* → `409 Conflict`.
- **Admin auth (capability-precise gate, #685 review).** Admin is required
  **iff the request can cancel a live run** — i.e. the resolved active-prior
  behavior (the matrix cell) is *cancel + start fresh* (`Terminate`). That is
  exactly `conflict=terminate_existing` (any reuse), or
  `reuse=terminate_if_running` with the **default/omitted** conflict
  (`unspecified` → native `Terminate`). Without admin these → **`401`**. The
  other two active resolutions never cancel live work and are **non-admin**:
  *attach* (`use_existing`, or the native-attach reuse policies) and *fail*
  (`fail`). In particular the flagship idempotent-starter
  `reuse=terminate_if_running` + `conflict=use_existing` resolves to *attach*
  and is **not** admin-gated — a non-admin webhook/cron caller can use it
  directly. The gate is computed from the parsed policies via
  `effective_active_conflict_behavior(reuse, conflict) == ActiveConflictBehavior::Terminate`,
  so an invalid policy value returns `400` before the gate is reached.
- an unknown `conflict_policy` value → `400`.

**Deferred-start restriction.** A non-default `conflict_policy` combined with a
**throttle / debounce / batch** policy returns `400`: those defer the start, so
there is no active prior to resolve at request time. `unspecified` (or omitting
the field) is always accepted. `conflict_policy` combined with `idempotency_key`
(#808) is allowed — they compose.

**Concurrency note (#685 review).** Concurrent `terminate_existing` starts of
the same `(workflow_name, workflow_id)` against one live prior are
last-writer-wins and **converge to a single surviving run via a bounded internal
retry** — no transient `NotFound` is surfaced. A loser whose post-INSERT
`load_workflow_execution_by_key_for_update` observes the winner seal the prior
row it locked (a pre-existing seal race, more reachable now that
`terminate_existing` has no pre-check) is retried internally: under READ
COMMITTED each SELECT gets a fresh snapshot, so the loser picks up the winner's
committed replacement RUNNING row and starts fresh against it (a bounded cap
guards the pathological "sealed without a replacement" case, falling back to the
pre-existing `NotFound`). It does **not** corrupt data, deadlock, or double-run —
the seal + insert is transactional, and `use_existing` never enters this branch.

### Typed Dispatch

Use the companion functions generated by `#[workflow]` and `#[activity]` instead of raw string names when dispatching from within a workflow. The companion name is `{fn_name}_info()` (the public alias for the hidden `__autumn_{workflow|activity}_info_{name}()` function).

| Raw (string-based)                                            | Typed (info-based)                                           |
|---------------------------------------------------------------|--------------------------------------------------------------|
| `ctx.execute_activity_raw("send_email", json!(...), "q")`    | `ctx.execute_activity(&send_email_info(), input).await?`     |
| `ctx.execute_activity_raw_with_opts("send_email", ..., "q")` | `ctx.execute_activity_with_opts(&send_email_info(), input, queue_override, retry, timeout).await?` |
| `ctx.execute_local_activity_raw("checksum", ...)`            | `ctx.execute_local_activity(&checksum_info(), input).await?` |
| `ctx.spawn_child_workflow_raw("child", json!(...))`           | `ctx.spawn_child_workflow(&child_info(), input).await?`      |
| `ctx.wait_for_signal("approved")`                             | `ctx.receive_signal::<Approval>("approved").await?`          |
| `ctx.wait_for_signal_timeout("approved", timeout)`            | `ctx.receive_signal_timeout::<Approval>("approved", timeout).await?` |
| `ctx.spawn_child_workflow_timeout("child", json!(...), timeout)` | `ctx.execute_child_workflow_timeout::<O>(&child_info(), input, timeout).await?` |

`execute_activity` delegates to `execute_activity_with_opts` with all overrides `None`, so `ActivityInfo` defaults (queue, retry policy, start-to-close) are consistently applied. The `queue_override` in `execute_activity_with_opts` takes priority over `info.default_queue` which takes priority over `"default"`.

```rust
#[activity(start_to_close = "30s", queue = "email-workers")]
async fn send_email(ctx: &ActivityContext, addr: String) -> Result<(), String> { /* ... */ }

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> {
    // Queue and retry defaults come from send_email_info() — no magic strings.
    ctx.execute_activity(&send_email_info(), format!("user-{user_id}@example.com")).await?;

    // Override the queue for priority routing; keep all other defaults.
    ctx.execute_activity_with_opts(
        &send_email_info(),
        format!("user-{user_id}@example.com"),
        Some("priority-email"),
        None,
        None,
    ).await?;

    // Child workflows — output type is inferred from the annotation.
    let report: ReportResult = ctx.spawn_child_workflow(&generate_report_info(), user_id).await?;

    // Typed signal receive — payload deserialized directly.
    let approval: Approval = ctx.receive_signal("approval").await?;
    Ok(())
}
```

Local activities also have a `_with_opts` variant for per-call retry/timeout overrides:
```rust
ctx.execute_local_activity_with_opts(&checksum_info(), data, Some(retry_policy), Some(timeout)).await?;
```

### Signal-or-Deadline Waits (issue #476)

`receive_signal_timeout` / `wait_for_signal_timeout` bound a signal wait with a durable deadline — the primitive for human-in-the-loop and callback-driven flows (approval gates, payment confirmations, webhook callbacks with an SLA):

```rust
#[workflow]
async fn document_review(ctx: &WorkflowContext, doc_id: String) -> Result<String, String> {
    // Two lines: await approval, else auto-reject after 24 hours.
    let decision: Option<Decision> = ctx
        .receive_signal_timeout("approval", Duration::from_secs(24 * 60 * 60))
        .await
        .map_err(|e| e.to_string())?;
    match decision {
        Some(d) => Ok(format!("decided by {}", d.approver)),
        None => Ok("auto_rejected".to_string()),   // deadline fired first
    }
}
```

`Ok(Some(payload))` when the signal arrives before the deadline; `Ok(None)` when the durable timer fires first. The untyped `wait_for_signal_timeout` returns `HarvestResult<Option<Value>>`, mirroring the `wait_for_signal` / `receive_signal` pairing.

**Determinism contract.** The race composes the existing `TimerStarted`/`TimerFired` and `SignalReceived` events — **no new `WorkflowEvent` variant, no migration**. The winner is decided by **recorded history order**: whichever of `SignalReceived` or `TimerFired` appears first in `harvest_events` wins on every replay, regardless of wall-clock timing on the replaying worker. A history containing both events always replays to the same branch (`HistoryMatcher::match_signal_or_timer`, returning the `SignalOrTimerMatch` enum). When the signal wins, the stray `TimerFired` from the still-armed durable timer is consumed transparently; when the timer wins, **no signal payload is consumed** — a late delivery remains observable by a subsequent `receive_signal*` call.

Mechanics: each call deterministically derives a race timer ID (`__signal_timeout:{seq}:{signal_name}` from a per-context counter) and, on first live execution, suspends with a `StartTimer` + `WaitForSignal` command batch (a suspension shape the worker already supports; the timer row is deduped by `timer_id` on re-park). The mixed park self-wakes if a signal arrived while the task was still executing, so an early approval is processed immediately rather than at the deadline. Wake-up ingest appends due timer fires and pending signals in occurrence order (DB-clock `received_at` vs DB-clock-anchored `fires_at`), so a worker claiming the woken task late cannot flip an on-time signal to the timeout branch. `timeout` is rounded up to whole seconds. Composition caveat: like `ctx.timer` and `wait_for_signal` today, the race cannot share one suspension batch with `ScheduleActivity` or `StartChildWorkflow` commands (`tokio::join!` with `execute_activity`/`spawn_child_workflow` is rejected by the worker's suspension-shape handler) and is deferred — not raced — by an inline `RunLocalActivity` sibling (`extract_run_local_activity` drops wait commands, so the deadline is only armed after the local activity completes). All three are pre-existing engine limitations shared by every wait primitive. The replay matcher already tolerates interleaved sibling events (activities, local activities, child workflows, markers, side effects, sibling timers, and stashed signals compete at their recorded history positions) so histories from a future mixed-batch implementation replay correctly. `WorkflowTestEnv` supports both branches without real sleeping: `queue_signal(...)` exercises the signal branch, omitting it auto-fires the deadline timer. See `examples/approval_with_timeout.rs`.

### Child-or-Deadline Waits (issue #779)

`execute_child_workflow_timeout` / `spawn_child_workflow_timeout` bound a **child-workflow** await with a durable deadline — the primitive for a sub-orchestration that must not run past an SLA (a per-tenant onboarding flow, a batch pipeline, a partner callback that itself spawns activities and timers). It mirrors `receive_signal_timeout` (#476), one level up: instead of racing a signal against a timer, it races a **child workflow's terminal outcome** against a timer.

```rust
#[workflow]
async fn parent(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    // Await a child sub-orchestration, else run plan B after 10 minutes.
    match ctx.execute_child_workflow_timeout::<FulfillResult>(
        &fulfill_child_info(), order.clone(), Duration::from_secs(600),
    ).await.map_err(|e| e.to_string())? {
        Some(result) => Ok(result.tracking_id),          // child finished in time
        None => Ok(fallback_dispatch(&order)),           // deadline fired first; child was request-cancelled
    }
}
```

`Ok(Some(output))` when the child reaches a terminal **success** before the deadline; `Ok(None)` when the durable timer fires first (the still-running child is **request-cancelled** on the deadline); `Err(_)` when the child **fails** before the deadline — the typed failure is preserved end-to-end (issue #767), so `err.workflow_error_type()`/`workflow_details()`/`is_workflow_non_retryable()` are all readable on the parent side. The untyped `spawn_child_workflow_timeout` returns `HarvestResult<Option<Value>>`, mirroring the `spawn_child_workflow` / `spawn_child_workflow_raw` pairing.

**Determinism contract.** The race composes the existing child-completion events (`ChildWorkflowStarted`/`ChildWorkflowCompleted`/`ChildWorkflowFailed`) and the existing `TimerStarted`/`TimerFired` events — **no new `WorkflowEvent` variant, no migration** (core-only, no engine-schema change). The winner is decided by **recorded history order**: whichever of the child terminal or `TimerFired` appears first in `harvest_events` wins on every replay, regardless of wall-clock timing on the replaying worker, so the outcome is order-stable across workers and replays (`HistoryMatcher::match_child_or_timer`, returning the `ChildOrTimerMatch` enum, exported from `lib.rs`). When the child wins, the still-armed deadline timer is **proactively torn down** via a `CancelRaceLosers { timers: [...] }` bookkeeping command (`queue::delete_pending_timer`) — pushed on every resolution cycle (strict-replay-safe, since the delete appends no event, mirroring `ctx.race()`'s `race_timer_signal_impl`) so the unfired `harvest_timers` row cannot pin the terminal parent via `retention::has_inflight_dependencies`; when the timer wins, the loser child's terminal is consumed transparently and the child is request-cancelled via a `CancelRaceLosers` bookkeeping command in the same deadline-resolving cycle (gated on `child_already_terminal` so the cancel is emitted on exactly the one live cycle the child is still running).

**Canary replay of an in-flight child-timeout is not a false non-determinism (Codex P2).** A running workflow parked on `spawn_child_workflow_timeout` replays to `ChildOrTimerMatch::InProgress` at the recorded-history frontier (the `ChildWorkflowStarted`/`TimerStarted` pair is recorded, but neither resolution is). The deploy replay canary (`run_workflow_canary`) samples exactly such executions and expects them to *suspend* → `ReplaySucceeded`, so the `InProgress` arm suppresses its strict-replay non-determinism error under `self.canary_mode && position >= len` — mirroring `check_strict_replay_no_match`'s canary-at-end exception exactly, and falling through to re-park. Genuine-divergence detection is untouched: a real code-vs-history divergence resolves as `ChildOrTimerMatch::Diverged` (always nd-errors, never canary-excepted), and strict `WorkflowReplayer` runs (non-canary) still treat an unresolved race as a fixture problem.

**Over-deadline child terminal ordering (Codex P1 + P2-D).** Because the matcher is pure recorded-order, a child that completes/fails **after** its deadline must not be allowed to win just because its terminal was appended out-of-band before the overdue `TimerFired`. **Every** out-of-band path that appends a child terminal to a live parent therefore calls `worker::materialize_due_child_timeout_deadlines` to append any currently-**due** `__child_timeout:` deadline as a `TimerFired` **before** the child terminal: the worker child-wake path (`worker::wake_parent_for_child_completion`/`wake_parent_for_child_failure`, P1), the operator cancel/terminate path (`execution::notify_awaited_parent_of_child_terminal`, P2-D), and the child's own execution-timeout path (`timeout::wake_parent_for_child_timeout`, P2-D). So a deadline that has passed is observed by every replay regardless of when the parent is claimed or how the child terminated (completion, failure, operator cancel/terminate, or the child's own execution timeout) — a deadline reached ⇒ `None` even when the parent is claimed late, matching the #476 signal-or-deadline guarantee. The due rows are selected `FOR UPDATE` (EvalPlanQual makes `fired = false` exactly-once against the parent-claim `ingest_due_timers_and_signals`), scoped strictly to the `__child_timeout:` prefix (unrelated `ctx.timer()` rows are never fired or reordered), and a child with no due deadline materializes nothing (byte-identical to the pre-fix wake path). The already-recorded loser child terminal that follows is consumed by `match_child_or_timer`'s existing timer-win branch (`consume_loser_child_terminal`, which handles both completed/failed losers), so no cancel is pushed for it.

**Lock-ordering convention for `materialize_due_child_timeout_deadlines` (Codex round-11 P2 ABBA fix).** The materializer acquires the **parent execution row `FOR UPDATE` FIRST, then the due `harvest_timers` rows `FOR UPDATE`** — the unified `harvest_workflow_executions` → `harvest_timers` order (documented in a convention comment at the materializer, mirroring the `harvest_external_tasks` task-row → execution-row convention documented in `timeout.rs` from the issue #609 round-9 hardening). This is load-bearing: the operator cancel/terminate path (`notify_awaited_parent_of_child_terminal`) already holds the parent execution row `FOR UPDATE` before calling the materializer, whereas the worker-wake (`wake_parent_for_child_completion`/`_failure`) and child-execution-timeout (`wake_parent_for_child_timeout`) callers reach it with **no** outer parent lock. Had the materializer taken the timer lock first, those two orderings would invert (ABBA): two concurrent wakes of the same overdue parent — one via a normal child completion/failure (timer-first) and one via an operator cancel/terminate of a *sibling* child (execution-row-first) — would deadlock, and Postgres would abort a healthy terminal notification. Locking the parent execution row first at the top of the materializer (a same-transaction no-op re-lock for the operator path) unifies every call site onto execution-row → timer, so no cycle is possible; a gone parent short-circuits to `Ok(0)` and the caller's own `append_single_event` surfaces the `NotFound` unchanged. The materializer is the **only** `harvest_timers FOR UPDATE` writer (the parent-claim `ingest_due_timers_and_signals` uses a plain `.load()` + `append_events`, taking no `FOR UPDATE` on either table), so forcing execution-first there introduces no new inversion. Deterministically pinned by `materializer_locks_execution_row_before_timers_no_abba` in `tests/integration/child_timeout_tests.rs`, which holds the parent execution row lock open and proves — via a `FOR UPDATE NOWAIT` probe while the materializer is blocked — that the timer row is not yet locked (a probe that would fail under the pre-fix timer-first order).

Mechanics: each call deterministically derives a race timer ID (`__child_timeout:{seq}:{workflow_name}` from a per-context `child_timeout_seq` counter, a namespace disjoint from `__signal_timeout:`) and, on first live execution, spawns the child and arms the deadline in **one** mixed `StartChildWorkflow + StartTimer` suspension batch — the child is spawned by this call itself, atomically co-persisted with the timer (a dedicated worker `extract_child_timeout_race`/`persist_child_timeout_race` path, with a child-terminal self-wake re-check so a child that finishes in the park window is not missed). `timeout` is rounded up to whole seconds.

**Composition caveats** (mirroring `receive_signal_timeout`, enforced fail-loud):
- The race **cannot share one suspension batch with a *new* `ScheduleActivity` or `StartChildWorkflow`**. The child is spawned and raced inside the primitive's own single mixed batch, so you cannot `tokio::join!` two `execute_child_workflow_timeout` calls, nor race a child-timeout against other new work in one batch — `extract_child_timeout_race` requires exactly one child + one timer (whose `timer_id` carries the reserved `context::CHILD_TIMEOUT_TIMER_PREFIX` = `"__child_timeout:"`) + bookkeeping, and any extra command **or** a timer without that prefix falls through to the worker's generic "unsupported commands" failure (fail-loud, never silent corruption). This means a hand-rolled `tokio::join!(ctx.spawn_child_workflow(..), ctx.timer("mytimer", n))` (a plain child + an ordinary timer) is **not** mistaken for the child-timeout primitive (Codex P2-C) — it stays fail-loud, so its ordinary timer is never silently left undeleted on a child-win.
- On the deadline the default is **request-cancel** of the losing child (there is no "abandon on deadline" variant — a potential follow-up).
- **Known limitation — a workflow-handler panic before the deadline-cancel is persisted drops it (shared, pre-existing, not child-timeout-specific).** The deadline branch pushes a `CancelRaceLosers { children: [...] }` bookkeeping command and returns `Ok(None)`; the child is durably request-cancelled when that cycle's commands persist (via `apply_race_loser_cancellations`, wired into `persist_terminal_outcome_commands`/`persist_bookkeeping_and_requeue_workflow`/`persist_scheduled_activities`), so the request-cancel-on-deadline promise holds for every *normal* resolution — including the common `None => Ok(fallback)` shape, which persists the cancel on the terminal transaction. It does **not** hold when the workflow function **panics** after the deadline branch: the issue #782 panic-containment contract (R5) deliberately treats a panicked cycle's command buffer as untrustworthy and **discards all of its pending commands** on both the retry (`requeue_workflow_task_after_panic`) and terminal (`pending_cmds = Vec::new()` in `process_workflow_task`, before `persist_terminal_outcome_commands` is even reached) paths. A panic that recurs deterministically until the `workflow_panic_max_attempts` budget is exhausted therefore seals the parent `FAILED` with the over-deadline child never request-cancelled — the awaited child (`parent_close_policy = NULL`) is also not covered by `apply_parent_close_cascade`, so it runs to its own natural terminal (or its own `execution_timeout`, #243) as orphaned, wasted work. This is the **same broad class** as the documented fan-out limitation (a panicking/failing cycle before persist drops its `StartChildWorkflow` commands) — the fix is a cross-cutting change to the #782 discard-panicked-commands contract, not a local `persist_terminal_outcome_commands` tweak (which already replays `CancelRaceLosers` correctly on every non-panic path), and is out of scope here.
- **Known limitation — deadline-cleanup dropped if the event hard cap trips on the same cycle (shared, pre-existing, not child-timeout-specific).** If the parent hits its event-history hard cap on the same decision cycle the deadline branch resolves, the seal routes through `fail_workflow_for_history_cap` → `move_workflow_to_dlq_for_history_cap`, which bypasses `persist_terminal_outcome_commands` and therefore drops the deadline branch's `CancelRaceLosers` cleanup — the over-deadline child is not request-cancelled and its `__child_timeout:` timer row is left `fired = false`. This is the same terminal-seal-drops-pending-commands class as the #782 panic path and the #601 fan-out limitation, only reachable on a run already being force-killed at the hard cap; the awaited child runs to its own natural terminal (or its `execution_timeout`, #243) as orphaned work, and remediation is an operator cancel/terminate of that child.
- **Detached children are not supported** by this primitive (out of scope) — it always awaits/races an awaited child.
- The deadline timer is **proactively torn down** on a child-win (a `CancelRaceLosers { timers: [...] }` bookkeeping command that durably deletes the `__child_timeout:` row, matching `ctx.race()`'s loser cleanup), so an unfired `harvest_timers` row can never pin the terminal parent through retention (`has_inflight_dependencies` blocks on any `fired = false` timer). Note: `wait_for_signal_timeout` itself still leaves its timer armed on signal-win — the same latent retention leak, tracked as a separate follow-up.

`WorkflowTestEnv` supports both branches without real sleeping: a resolving child mock exercises the child branch, omitting it auto-fires the deadline timer. See `examples/child_with_timeout.rs`.

### Query Handlers

Query handlers let operators and UIs read arbitrary workflow-internal state without writing any event to `harvest_events`. They are pure synchronous functions registered via `WorkflowContext::register_query_handler` (typed) or `register_query` (no-arg shorthand). Use `#[query]` as a documentation marker on the handler function.

```rust
#[derive(serde::Deserialize)]
struct ProgressQuery { include_summary: bool }

#[derive(serde::Serialize)]
struct ProgressResponse { processed: u64, total: u64 }

#[workflow]
async fn batch_processor(ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
    let processed = Arc::new(Mutex::new(0u64));
    let state = processed.clone();
    ctx.register_query_handler("progress", move |req: &ProgressQuery| {
        Ok(ProgressResponse { processed: *state.lock().unwrap(), total: 1000 })
    });
    ctx.register_query("status", || serde_json::json!("running"));
    // ... activities ...
    Ok(())
}
```

Management API:
- `POST /api/harvest/workflows/{exec_id}/query/{name}` with body `{"args": <value>}` → `{"result": <value>}`
- `GET /api/harvest/workflows/{exec_id}/queries` → sorted list of registered query names

Errors: `QueryHandlerNotFound` (404), `QueryHandlerPanicked` (503), `QueryTimedOut` (408), `HistoryUnavailable` (410).

**Terminal (closed) workflows are queryable for post-mortem state inspection (issue #612).** Querying a `COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`TERMINATED`/`CONTINUED_AS_NEW` execution no longer returns `409 WorkflowNotRunning`: the recorded history is replayed and the query is served (`200`) against the workflow's reconstructed internal state. A cooperatively-returning run (a `COMPLETED` run, or a `FAILED` run whose function returned `Err`) drives all the way to `Poll::Ready`, so the query reads the **final** state — a `FAILED` run therefore answers a "how far did you get?" query with its computed internal state, not the error string. A run the engine **sealed while its function was parked mid-command** — `CONTINUED_AS_NEW` (`continue_as_new` parks forever), `TIMED_OUT` (killed on an in-flight activity), or a mid-await external/hard `CANCELLED`/`FAILED` — replays to a *suspension* rather than `Poll::Ready`; because the recorded history still carries the terminal lifecycle event (the "terminal seal"), the query is served against the partial state reconstructed **at that recorded terminal point** (still `200`, never `410`). The drive is read-only — serving a terminal query appends **zero** events and performs **zero** writes. Caveats:
- **Unregistered name** on a terminal run → `404 QueryHandlerNotFound` (identical to the running contract — never a silent empty success).
- **History unqueryable** → `410 Gone` "history unavailable: …". This covers a terminal execution whose recorded history has **no terminal seal at all** — genuinely truncated: pruned by retention or released on reset — and one whose payloads were PII-erased (issue #495), which would otherwise compute against `{"_harvest_erased": true}` tombstones (detected via an O(1) check of the execution row's own tombstoned `input` column, not a full-history scan). `410` = "row present but history unqueryable"; a fully retention-**deleted** row (the row itself no longer exists) stays `404` = "row gone". It also covers **code drift** (Codex P2, PR #986 follow-up): if the workflow code changed since the run executed such that the driven handler settles to a servable state (`ReachedTerminal`, or a `Suspended` run sealed while parked) while genuine recorded **non-lifecycle** history remains unconsumed, the reconstructed state does not correspond to what actually happened → `410` rather than a misleading partial answer. A truthfully-replayed completed / sealed-while-parked run leaves only the trailing terminal-lifecycle seal unconsumed (excluded by `HistoryMatcher::has_non_lifecycle_unconsumed`), so it still serves `200`.
- **Spinning replay** → `408 QueryTimedOut`, bounded by `WorkerConfig::query_timeout` — never a hang. The bound applies to async-yielding replays; a workflow that busy-loops synchronously without ever `.await`-ing is out of scope, exactly as for the live executor.
- **Running/suspended** executions are unchanged: the drive stops at the first suspension and the query reads current partial state, regardless of outcome.

Configure the per-query timeout via `WorkerConfig::default().with_query_timeout(Duration::from_secs(10))` (default 5 s). Queries are replay-safe: they never emit `WorkflowCommand`s and leave zero footprint in `harvest_events`. The read-only, deadline-bounded replay driver is `executor::drive_query_replay` (returns `QueryReplayOutcome::{ReachedTerminal, Suspended, TimedOut}`); the pure classifier `executor::classify_terminal_query(outcome, sealed, has_unconsumed_history)` + `executor::history_reached_terminal_seal` maps a terminal execution's drive outcome to serve (200) / 408 / 410, with the `has_unconsumed_history` argument (from `ctx.history_has_unconsumed_events()`, computed after the drive) gating every `Serve` on the code-drift check.

### Current Details — operator status breadcrumb (issue #593)

`ctx.set_current_details(...)` publishes a freeform, human-readable "what is this run doing right now" string for operators triaging a live execution — the answer `GET /workflows/{id}` and `/stack` don't give (they report *state* and *what it's blocked on*, not *intent*). One call per phase, no handler to register or call by name:

```rust
#[workflow]
async fn fulfill_order(ctx: &WorkflowContext, order_id: String) -> Result<(), String> {
    ctx.set_current_details("step 1/2: charging card");
    // ... ctx.execute_activity(&charge_payment_info(), ...).await?;
    ctx.set_current_details("step 2/2: awaiting carrier pickup confirmation");
    // ... ctx.execute_activity(&ship_order_info(), ...).await?;
    ctx.set_current_details(""); // clear -- the run is about to complete
    Ok(())
}
```

An operator reads it back with the existing describe call:

```bash
curl http://localhost:8080/api/harvest/workflows/{exec_id} | jq -r '.execution.current_details'
```

**Semantics**: last-write-wins (each call overwrites the previous value); an **empty string clears** the field to `NULL` rather than persisting `""`; the value is **durable** — persisted to a `current_details` column on `harvest_workflow_executions` (migration `20260609000001_harvest_workflow_current_details`), so it survives worker restart and LRU cache eviction, not just held in process memory; **capped** at `DEFAULT_CURRENT_DETAILS_CAP_BYTES` (1 KiB, configurable via `HarvestBuilder::with_current_details_cap`) with deterministic UTF-8-boundary truncation — oversized input is truncated, never rejected, so a status breadcrumb can never wedge a workflow.

**Replay-safe by construction**: `set_current_details` is a no-op while `ctx.is_replaying()` is true — zero `WorkflowCommand`s, zero `WorkflowEvent`s on replay. The value rides an internal, replay-suppressed `WorkflowCommand::SetCurrentDetails` that is never appended to `harvest_events`; the worker resolves the **last** command in a cycle (`worker::latest_current_details_update`, a pure last-write-wins/empty-clears decision function) to a single targeted `UPDATE` (`store::update_current_details`), mirroring the heartbeat-checkpoint and `deadline_at` mutable-column pattern. **No new `WorkflowEvent` variant, no change to the adjacently-tagged event JSON contract, single additive migration, shard-local.** A `WorkflowReplayer` fixture whose workflow calls `set_current_details` (set, overwrite, and an empty-string clear) always reports `ReplaySucceeded` — see `tests/replayer_tests.rs::replay_workflow_with_current_details_calls_succeeds` and `examples/current_details_status.rs`.

Out of scope for this slice (per issue #593): a static, immutable run summary set once at start time; a dedicated query handler or SSE push for the field; indexing/filtering executions *by* `current_details` (that's the search-attributes surface, #506/#159); Vantage UI rendering.

### Signal Handlers (issue #546)

Signal handlers are the **push** counterpart to `wait_for_signal`/`receive_signal` (**pull**), completing the query/update/signal handler-registration trio. Register a handler once — typically at the top of the workflow body, alongside `register_query_handler`/`register_update_handler` — and it fires automatically for every matching `SignalReceived` event, at any point in the run, with zero hand-coded `select!`-style interleaving.

**Pull vs push — when to reach for which:**

| | Pull (`wait_for_signal` / `receive_signal`) | Push (`register_signal_handler`) |
|---|---|---|
| Shape | One `.await` at one code point | One registration call, dispatched whenever a match exists |
| Fits | "Block here until X happens" (approval gate, checkout) | "React to X at any time while doing other things" (cancel/pause/upgrade mid-run) |
| Return value | Caller gets the payload directly at the await point | Fire-and-forget; handler mutates author-captured state (e.g. `Arc<Mutex<T>>`) |
| Validation/rejection | N/A (signals are always accepted) | N/A — that's the `update` primitive's job |

```rust
#[derive(serde::Deserialize)]
struct CancelRequest { reason: String }

#[workflow]
async fn subscription(ctx: &WorkflowContext, tier: String) -> Result<String, String> {
    // Pure state mutation only -- see the "dispatch is per-cycle" caveat
    // below before adding a log line or external call inside a handler.
    let cancelled = Arc::new(Mutex::new(false));
    let state = cancelled.clone();
    ctx.register_signal_handler("cancel", move |req: CancelRequest| {
        *state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        let _ = req.reason;
    });

    // Untyped variant for dynamic dispatch:
    ctx.register_signal_handler_raw("pause", |_payload: serde_json::Value| {
        // ...
    });

    // ... billing loop / activities, checking `cancelled` as needed ...
    Ok(tier)
}
```

**Mechanics.** `WorkflowContext::register_signal_handler` (typed, `Req: Deserialize`) and `register_signal_handler_raw` (untyped `serde_json::Value`) store the handler in an in-memory `SignalHandlerRegistry` (`signal_handler.rs`); registration is **storage-only** and never dispatches inline. Dispatch happens via `WorkflowContext::pump_signal_handlers`, triggered by `match_history`'s post-hook, which runs after every history-consulting call (an activity/timer/child-workflow/signal-wait match, a deterministic primitive like `system_now`/`new_uuid`/`side_effect`, and once more — as a backstop — right after the workflow function returns, on both the completed and failed paths). The pump dispatches every registered handler whose target signal is currently **claimable**. "Claimable" is deliberately narrow: `HistoryMatcher::claim_pending_signal` only inspects `pending_signals`, the same stash `prepare_match`'s cursor-bound `drain_early_signals` sweep populates for every other `match_*` call (`match_activity`, `match_timer`, ...) — it can never reach ahead of wherever the workflow's own code-driven cursor progression has carried the matcher so far. Claims from every currently-registered handler name are collected and sorted by event index *before* any dispatch, so two differently-named handlers always fire in true historical order relative to each other, regardless of which order their `register_*` calls ran in and regardless of whether any command separates them. **No new `WorkflowEvent` variant** — `SignalReceived` is reused, so the append-only invariant and adjacently-tagged JSON contract are untouched. Handlers are **fire-and-forget** and **synchronous**: no validator, no completion event, no suspension shape to reason about. A panicking handler is caught at the dispatch boundary and logged rather than propagating past that call; a payload that fails to deserialize under the typed variant is logged and dropped.

**Dispatch timing — deferred, not inline (post-ship hardening).** An earlier implementation dispatched eagerly, either draining the full history at registration time (drained *every* recorded `SignalReceived` for a name regardless of cursor position — a handler registered at the top of a workflow function could fire on a signal recorded *after* an activity or timer the workflow hadn't reached yet in this replay cycle) or pumping inline per registration call (which fixed that but still broke ordering *across different handler names*: at the moment the first of two back-to-back `register_*` calls runs, the second handler doesn't exist yet, so its own eager pump can't know to wait for it and reorders by registration order instead of history order — both confirmed via code review on PR #890, the second confirmed to recur on essentially every replay cycle where 2+ differently-named signals are delivered close together, since that is the idiomatic "register all handlers at the top of the function" pattern this feature was built for). Both are fixed by making registration purely a storage operation and deferring all dispatch to the `match_history` post-hook: the first pump after every handler this cycle has registered considers all of them together. The trade-off: a handler's effect is no longer guaranteed visible on the *literal next line* with zero intervening `.await`/history-consulting calls — a workflow reading handler-mutated state should do so after at least one such call (a loop with an activity/timer between registration and the read is the idiomatic shape, and is what the executor's own end-of-cycle flush exists to backstop for a workflow with no such call at all).

**Dispatch is per-cycle, not once-ever.** There is no persisted "already delivered" marker for a signal handler (unlike `update`, whose handler is skipped on replay once an `UpdateCompleted`/`UpdateFailed` event exists). A fresh `SignalHandlerRegistry` and `HistoryMatcher` are built for every workflow replay/live cycle, so the *same* recorded `SignalReceived` event is redelivered on every subsequent cycle for the life of the execution — not just the first time it is seen. That is safe and correct for reconstructing in-memory state (the captured `Arc<Mutex<T>>` is itself rebuilt fresh at the top of the same cycle, so replaying the same signals into it always reconstructs the same final value, exactly like the rest of the workflow body's plain Rust logic). It is **not** safe for a non-idempotent side effect performed directly inside a handler (an external call, a log line meant to fire once) — that side effect repeats on every subsequent replay of the same history. Keep handlers limited to mutating captured state; route side effects through a regular activity instead.

**Mutex poisoning is not swallowed by the panic guard.** `invoke_signal_handler`'s `catch_unwind` stops a handler panic from propagating past the dispatch call, but it does not "un-poison" a `std::sync::Mutex` the handler happened to be holding when it unwound — a later `.lock().unwrap()` on that same mutex elsewhere in the workflow body will still panic. Recover with `.lock().unwrap_or_else(std::sync::PoisonError::into_inner)` (as the example above and `examples/signal_handlers_subscription.rs` do) if a handler shares a mutex with other workflow code.

**No signal silently dropped.** A signal delivered before its handler is registered (e.g. it arrived on an earlier workflow-task cycle, before this cycle's code reached the registration call) is buffered and dispatched once the handler exists, in the same workflow task — the same buffering the engine already uses for `wait_for_signal` (`pending_signals`). Handler dispatch is replay-deterministic: replaying a recorded history fires handlers at the identical drain points and produces identical commands.

**Coexistence with pull.** A signal name with no registered handler and no pending `wait_for_signal` falls back to today's buffered behavior unchanged — pull-based `receive_signal` still works exactly as before. The two consumption styles for the *same* name never double-deliver a single `SignalReceived` event: whichever style claims an event first (in code-execution order) is the one that receives it; draining marks the event consumed so the other style's later attempt sees nothing for it. This also holds against a `receive_signal_timeout`/`wait_for_signal_timeout` race (issue #476) for the same name: `HistoryMatcher` tracks, per signal name, which event indices fall inside a still-open race window (a `TimerStarted { timer_id: "__signal_timeout:{seq}:{name}" }` with no matching `TimerFired` recorded yet); those indices are reserved for the race and a push handler skips them, so mixing both styles for one signal name cannot silently flip a race outcome from `SignalWon` to `TimerWon`.

**Must be called from the main workflow body.** Calling `register_signal_handler`/`register_signal_handler_raw` from inside an `#[update]`/`register_update_handler` closure or a query handler is a silent no-op: those run against a separate, throwaway `WorkflowContext` created fresh per invocation, so the registration is discarded the instant the handler returns and never fires.

`ctx.list_signal_handler_names()` returns the sorted names of all registered handlers (parity with `list_query_names`).

Registration is **idempotent** — calling with the same `name` multiple times (e.g. on every replay cycle) is a no-op after the first call.

See `autumn-harvest/examples/signal_handlers_subscription.rs` for a complete subscription workflow reacting to `cancel`/`pause`/`upgrade` signals mid-run. Out of scope for this slice: `#[signal]` macro sugar for handler-side registration (client-stub-only for now), signal validators/rejection (that's `update`'s job), and any change to signal delivery/idempotency-key dedupe (#521) or the `harvest_signals` wire format.

### Fan-out / Parallel Activities

`WorkflowContext` exposes first-class fan-out for dispatching N activities in parallel and collecting results in input order.  Two semantics are available:

| Method | Semantics |
|--------|-----------|
| `execute_activity_fan_out(info, inputs)` | Fail-fast: returns `Ok(Vec<O>)` or the **first** `Err` |
| `execute_activity_fan_out_collect(info, inputs)` | Collect-all: returns `Ok(Vec<Result<O, String>>)` — per-slot errors |
| `execute_activity_fan_out_raw(activities)` | Raw fail-fast: `Vec<(String, Value, String)>` input |
| `execute_activity_fan_out_collect_raw(activities)` | Raw collect-all |
| `execute_activity_fan_out_windowed(info, inputs, max_in_flight)` | **Bounded** fail-fast: at most `W` in flight at a time |
| `execute_activity_fan_out_collect_windowed(info, inputs, max_in_flight)` | **Bounded** collect-all: at most `W` in flight at a time |
| `execute_activity_fan_out_raw_windowed(activities, max_in_flight)` | **Bounded** raw fail-fast |
| `execute_activity_fan_out_collect_raw_windowed(activities, max_in_flight)` | **Bounded** raw collect-all |

```rust
// Typed, homogeneous fan-out — all slots run the same activity
// (≤ 3 lines of code measured from the example):
let results: Vec<ItemResult> = ctx
    .execute_activity_fan_out(&process_item_info(), items)
    .await
    .map_err(|e| e.to_string())?;

// Collect-all — per-slot Vec<Result<O, String>>:
let per_slot: Vec<Result<ItemResult, String>> = ctx
    .execute_activity_fan_out_collect(&process_item_info(), items)
    .await
    .map_err(|e| e.to_string())?;

// Raw heterogeneous fan-out:
let results = ctx.execute_activity_fan_out_raw(vec![
    ("send_email".to_string(), json!(addr1), "email-workers".to_string()),
    ("send_sms".to_string(),   json!(phone), "sms-workers".to_string()),
]).await.map_err(|e| e.to_string())?;

// Bounded / windowed — process a wide collection at most 50 at a time
// (the durable, replay-safe equivalent of futures::stream::buffer_unordered):
let results: Vec<ItemResult> = ctx
    .execute_activity_fan_out_windowed(&process_item_info(), items, 50)
    .await
    .map_err(|e| e.to_string())?;
```

**Determinism rule — the input collection MUST be derived from already-recorded state** (workflow input, prior activity outputs, signals).  Never derive the collection from non-deterministic sources such as the system clock, `rand`, or an in-process counter.  If the collection is derived from a prior activity output, that output is in history and is therefore deterministic.

**Replay mechanics**: a `MarkerRecorded { name: "fan_out:{n}" }` event is appended before the activity events on the first live run.  On replay the recorded count is compared to the current collection length; if they differ, `HarvestError::NonDeterministic` is returned immediately rather than silently corrupting results.

**Cancellation**: both methods check `ctx.is_cancelled()` before dispatching and return `HarvestError::Cancelled` if the workflow has been cancelled.

#### Bounded / windowed fan-out (issue #750)

The `_windowed` variants add a `max_in_flight: usize` (`W`) argument to each of the four shapes above. Instead of scheduling **all** `N` inputs at once (which schedules `N` `harvest_task_queue` rows in a single suspension and hammers workers/downstreams), a windowed fan-out schedules the inputs in successive **waves** of at most `W`, so at no point are more than `W` of that call's activities in the scheduled-but-not-completed state. This is the durable, replay-safe equivalent of `futures::stream::buffer_unordered` — one method call replacing the ~30-line manual "loop / slice / call fan-out per chunk / stitch results" idiom.

- **All `N` inputs are still processed**, and results are returned in **input order**, identical to the unbounded shapes (fail-fast returns the first `Err`; collect-all returns per-slot `Vec<Result<O, String>>`).
- **`W == 0` (or `< 1`) clamps up to 1** — a defined, documented outcome, never a panic, hang, or silent no-op.
- **`W >= N` produces behavior and recorded history identical to the unbounded path** (a single wave = one `try_join_all` over all `N`).
- **Replay is window-independent — including across a window CHANGE**: the window governs live dispatch only and is **never** recorded. The recorded events are the *same* `MarkerRecorded { name: "fan_out:{seq}", details: N }` + per-input activity events as the unbounded path (no new `WorkflowEvent` variant, no migration), so a history produced by *any* window replays — and *resumes* — to identical results regardless of the `W` the replaying code is configured with, whether that `W` is **larger or smaller** than the one that produced the recorded partial state. This holds because a windowed call runs in **two phases**: it first *resumes* the already-scheduled input-order prefix as one homogeneous batch (every prefix slot is in history, so each resolves to `Matched` or emits only a `WaitForActivity` — never a `ScheduleActivity`), and only once that prefix is fully resolved does it dispatch the fresh remainder in `W`-sized waves. This is what prevents a mid-flight window *increase* from regrouping an in-flight slot together with never-scheduled fresh slots into a mixed `[WaitForActivity + ScheduleActivity]` suspension batch (which the worker cannot persist). A mismatch between recorded `N` and current-code `N` still surfaces `HarvestError::NonDeterministic`, exactly as the unbounded path does.
- **Window DECREASE across a partial suspension — resume drains the old width**: work already scheduled under the *old* (larger) window cannot be un-dispatched, so when a fan-out is re-driven under a *smaller* window the resume phase waits on the **entire** already-scheduled prefix (up to the old width) before the smaller window governs any further dispatch. During that resume, peak in-flight can briefly exceed the newly-lowered `W`. This is inherent and correct — the window bounds *new* dispatch, never work already in flight — and introduces no new stampede.
- **Cancellation is honored at the start of every drive cycle** (before dispatching any further wave), in addition to once up front; a fan-out whose (replay-reconstructed) context is cancelled returns `HarvestError::Cancelled` rather than launching further waves.
- **Fail-fast backpressure**: on the first activity failure the fail-fast variants abort and **later waves are never dispatched** — a deliberate backpressure semantic, so a bounded fail-fast may process *fewer* items than the unbounded path would (which schedules all `N` up front regardless). Collect-all dispatches every wave (per-slot failures don't short-circuit) so all `N` inputs are always processed.
- **Trade-off — convoy effect**: this is a chunked-wave scheduler, not a true sliding window. A wave does not refill until *every* activity in it completes, so one slow activity in a wave stalls that wave's remaining slots until it finishes (throughput is bounded by the slowest activity per wave, not per slot). For most fan-outs of similar-cost activities this is a non-issue; for highly heterogeneous latencies a smaller `W` or a true streaming primitive would keep more slots busy.
- The two `try_join_all` known limitations of the unbounded fan-out (documented in the fan-out sections above) carry over **per-wave** — narrowed to a single wave's width, not widened.

See `autumn-harvest/examples/fanout_batch.rs` for a complete end-to-end example covering all shapes (static N, dynamic N from a prior activity, collect-all with partial failure, and a windowed fan-out over a collection larger than the window).

### External Workflow Family — signal / cancel / await

`WorkflowContext` exposes three verbs for a running workflow to interact with an **arbitrary, independently-started sibling** execution by `ExecutionId` (no parent/child linkage required). All three are deterministic and replay-safe, resolve same-shard inline (in the caller's persist transaction) or cross-shard via a background outbox (mirroring each other, no cross-shard transaction), and lower onto append-only `WorkflowEvent` variants (no migration):

| Verb | Method | Already-terminal target | Payload | Effect on target |
|---|---|---|---|---|
| **signal** (#244) | `ctx.signal_external_workflow(target, name, payload)` | `ExternalSignalFailed { target_terminal }` (a terminal run can't receive) | Yes | Delivers a signal |
| **cancel** (#492) | `ctx.request_cancel_external_workflow(target)` | **no-op success** (goal met) | No | Cancels the run |
| **await** (#757) | `ctx.await_external_workflow::<T>(target)` / `await_external_workflow_value(target)` | resolves with the recorded **outcome** | No | **none** (observe-only) |

**`await` (issue #757)** durably blocks until `target` reaches a terminal state, then hands back its typed result or terminal cause:

```rust
// Fan-in coordinator: await three independently-started legs by id (one line each).
let a: LegResult = ctx.await_external_workflow(leg_a).await.map_err(|e| e.to_string())?;
let b: LegResult = ctx.await_external_workflow(leg_b).await.map_err(|e| e.to_string())?;
// Branch on a leg's TYPED terminal cause (an outcome, not a transport error):
let c_total = match ctx.await_external_workflow::<LegResult>(leg_c).await {
    Ok(c) => c.total,
    Err(e) if e.workflow_error_type() == Some("RegionUnavailable") => 0,
    Err(e) => return Err(format!("leg C failed fatally: {e}")),
};
```

**Semantics** (the key differences from `cancel`):
- Target `COMPLETED` → resolves with the **deserialized output** (`await_external_workflow_value` returns the raw `Value`). **Codec/offload caveat**: the reader reads the target's `execution.output` row column raw (core `append_events`/`load_history` use the identity codec; payload codecs are a plugin-layer concern), so on a codec-**encrypting** deployment the awaiter freezes the ciphertext envelope, and a large target output is copied inline into the awaiter's history **without offloading** (a documented future optimization — see the plan). This mirrors the `FAILED`-path `details` caveat below.
- Target `FAILED`/`TIMED_OUT`/`CANCELLED`/`TERMINATED` → returns a typed `Err` carrying the target's terminal cause as an **outcome** (not a transport error), and **all four are programmatically branchable** via `err.workflow_error_type()`: a `FAILED` target surfaces as `HarvestError::WorkflowFailed { name: "external-workflow:{target}", .. }` — the SAME typed error a failed child surfaces as (issue #767), so `workflow_error_type()` / `workflow_details()` / `is_workflow_non_retryable()` all read the target's decoded failure. The other three surface as `HarvestError::WorkflowFailed` whose `workflow_error_type()` is exactly `target_timed_out` / `target_cancelled` / `target_terminated` (so a coordinator can distinguish them without string-matching the human message).
- Target `RUNNING`/`PAUSED` → the caller parks durably and resolves within **one outbox poll interval** after the target reaches any terminal state.
- **Self-await** (`target == own ExecutionId`) → immediate `HarvestError::ExternalAwaitFailed { reason_code: "self_await" }` — records no history (nil-UUID sentinel).
- **Unknown target** still unknown after the configured grace window → `HarvestError::ExternalAwaitFailed { reason_code: "target_unknown" }` (transport failure, distinct from the terminal-cause outcomes above; `err.external_await_reason_code()` reads it).
- **Observe-only**: awaiting NEVER establishes parent/child linkage, cancels, triggers `ParentClosePolicy`, or has any lifecycle effect on the target.

**Determinism**: the resolved value/error is frozen into the AWAITER's own history via the three new append-only events `ExternalAwaitRequested` / `ExternalAwaitResolved { output }` / `ExternalAwaitFailed { reason_code, message?, error_type?, details?, non_retryable? }` (at the END of the enum — pre-upgrade histories deserialize/replay unchanged). `HistoryMatcher::match_external_await(target)` matches them (`Matched { output }` carries the real output value); a divergent target surfaces as `HarvestError::NonDeterministic` / `NonDeterminismKind::ExternalAwaitMismatch`. Implementation mirrors the cancel primitive: `ExternalAwaitId` newtype, `WorkflowCommand::AwaitExternalWorkflow` via `SignalBatchItem::Await`, `persist_external_signal_inline` Await arm (same-shard inline via `execution::read_external_await_outcome`, which follows a `CONTINUED_AS_NEW` chain to the true terminal), and `timeout::enforce_external_awaits_outbox` (cross-shard + still-running resolution). See `autumn-harvest/examples/await_external_workflow.rs`.

### Fan-out / Parallel Child Workflows

`WorkflowContext` exposes the same fan-out shape for **child workflows** (issue #601) — the missing sibling to activity fan-out for sub-orchestrations that need their own durable history rather than a single unit of I/O work. All N children are scheduled (each gets its own `ExecutionId` on the parent's shard, per the existing child-spawn shard-pinning contract) before any is awaited — genuinely concurrent, not a sequential `spawn_child_workflow` loop.

| Method | Semantics |
|--------|-----------|
| `spawn_child_workflow_fan_out(info, inputs)` | Fail-fast: returns `Ok(Vec<O>)` or the **first** `Err` |
| `spawn_child_workflow_fan_out_collect(info, inputs)` | Collect-all: returns `Ok(Vec<Result<O, String>>)` — per-slot errors |
| `spawn_child_workflow_fan_out_raw(children)` | Raw fail-fast: `Vec<(String, Value)>` input |
| `spawn_child_workflow_fan_out_collect_raw(children)` | Raw collect-all |

```rust
// Typed, homogeneous fan-out — all slots spawn the same child workflow type
// (≤ 3 lines of code measured from the example, matching the activity fan-out bar):
let results: Vec<ItemResult> = ctx
    .spawn_child_workflow_fan_out(&process_item_child_info(), items)
    .await
    .map_err(|e| e.to_string())?;

// Collect-all — per-slot Vec<Result<O, String>>:
let per_slot: Vec<Result<ItemResult, String>> = ctx
    .spawn_child_workflow_fan_out_collect(&process_item_child_info(), items)
    .await
    .map_err(|e| e.to_string())?;

// Raw heterogeneous fan-out:
let results = ctx.spawn_child_workflow_fan_out_raw(vec![
    ("region_rollup".to_string(), json!({"region": "us-east"})),
    ("region_rollup".to_string(), json!({"region": "eu-west"})),
]).await.map_err(|e| e.to_string())?;
```

**Activity vs. child-workflow fan-out — when to reach for which:**

| | Activity fan-out | Child-workflow fan-out |
|---|---|---|
| Unit of work | A single I/O call (HTTP, DB, external service) | A sub-orchestration that itself needs activities, timers, signals, or retries |
| History | One `ActivityScheduled`/`Completed` pair per slot | The child gets its **own** independent event history |
| Failure/retry granularity | Per-activity `RetryPolicy` | Per-child workflow-level retry (issue #523) / Saga compensation inside the child |
| Cancellation propagation | Activity cancellation | **None** — fanned-out children are awaited, not detached; `ParentClosePolicy` (issue #347) only ever governs detached children, so cancelling the parent does not stop in-flight fanned-out children (see below) |

**Replay-safety and determinism are byte-identical to activity fan-out** — the feature reuses the same mechanism end-to-end rather than introducing a parallel contract:
- **Append-only invariant honored**: reuses the existing `ChildWorkflowStarted` / `ChildWorkflowCompleted` / `ChildWorkflowFailed` events and the existing `MarkerRecorded { name: "fan_out:{n}" }` marker. **No new `WorkflowEvent` variant, no migration.**
- **Shared sequence counter**: activity and child fan-out draw from the *same* `fan_out_seq` counter on `WorkflowContext`, so `fan_out:{n}` numbering stays deterministic when both are used in one workflow (an activity fan-out followed by a child fan-out gets `fan_out:1` then `fan_out:2`, never two `fan_out:1`s).
- **Determinism rule** — the input collection MUST be derived from already-recorded state (workflow input, prior activity outputs, signals). On replay the recorded child count is compared against the current collection length; a mismatch returns `HarvestError::NonDeterministic` immediately, before any child is (re)spawned.
- **All-or-nothing dispatch**: on a fresh (first-time) fan-out, every child's serialized input is checked against `payload_max_workflow_input` *before* any child is dispatched and *before* the `fan_out:{n}` marker is even recorded — `peek_fan_out_count`/`record_fan_out_marker` split the "is this fresh?" check from the marker push so a validation failure never leaves an orphaned marker with zero matching children. An oversized input in slot N cannot leave slots `0..N` mid-dispatch (command pushed, `ExecutionId` allocated) while the fan-out call fails outright due to `try_join_all`'s poll-order-dependent short-circuit. The fan-out now deterministically dispatches all N children or none, with a failed fan-out's persisted history reading exactly `[..., WorkflowFailed]` — no marker, no phantom children. This does **not** by itself make such a failure replay cleanly through `WorkflowReplayer` — that's a broader, pre-existing engine limitation (a bare trailing `WorkflowFailed` is intentionally non-transparent to in-progress `match_*` calls; see `tests/replayer_tests.rs::known_limitation_early_config_dependent_failure_does_not_replay_cleanly`, demonstrated with plain non-fan-out code) — but it does mean the *in-memory* attempt is always clean (no wasted per-child dispatch work, no poll-order-dependent partial state), the persisted audit trail never lies about child counts, and the failure surfaces immediately with an unambiguous reason.
- **Cancellation**: both methods check `ctx.is_cancelled()` before scheduling and return `HarvestError::Cancelled`. **Unlike the table's activity-fan-out row, cancellation after dispatch does *not* propagate to already-in-flight children** — fanned-out children are awaited (`parent_close_policy = NULL`), and `ParentClosePolicy`'s cascade (`apply_parent_close_cascade`) only ever acts on detached children (`parent_close_policy IS NOT NULL`); an awaited child can outlive a cancelled or terminated parent, identical to today's single-child `spawn_child_workflow_raw`. Use `spawn_child_workflow_detached` with an explicit `ParentClosePolicy` per child if children must be torn down when the parent closes — fan-out has no detached variant today.
- **Shard pinning**: children remain pinned to the parent's shard (cross-shard child fan-out is out of scope, consistent with the sharding contract).
- **Known limitation — fail-fast replay failure selection**: when two or more children fail and all their terminals are already recorded (a full replay, e.g. crash recovery or `WorkflowReplayer`), `spawn_child_workflow_fan_out_raw`/`_collect_raw` select the failure at the *lowest input-slot index* rather than necessarily the one whose `ChildWorkflowFailed` event was recorded earliest in history — `try_join_all` resolves every already-terminal slot's future synchronously on first poll, so a single poll pass surfaces whichever slot is ready-with-`Err` first while iterating in slot order. **This is shared with the pre-existing `execute_activity_fan_out_raw` (issue #359)**, not introduced by child fan-out, and a proper fix needs an event-index threaded through `HistoryMatch::Failed` plus reworked selection in both primitives — out of scope here. Pinned by `tests/child_fanout_tests.rs::child_fan_out_raw_known_limitation_replay_selects_by_slot_order_not_recorded_order`.
- **Known limitation — stale re-park command leaks into the next suspension batch**: when an earlier-slot child is still `ChildInProgress` while a later-slot child has a recorded failure, `try_join_all` polls the earlier slot first during the same sweep that discovers the fail-fast error. `spawn_child_workflow_raw`'s `ChildInProgress` branch synchronously pushes a re-park `StartChildWorkflow` command *before* suspending on its own oneshot, and that push is not undone when `try_join_all` subsequently short-circuits on the later slot's `Err` and drops the still-pending future. If the workflow catches the fail-fast error and pushes a different command (e.g. schedules an activity), both land in the same suspension batch — a stale re-park alongside an unrelated new command — which the worker's dispatch logic (only single-shape batches are recognized) fails with "workflow task suspended with unsupported commands ...; this command set is not implemented yet", even though the user only handled the fan-out error and continued normally. **Shared with `execute_activity_fan_out_raw` (issue #359)** — `ActivityInProgress` has the identical push-then-suspend structure — not introduced by child fan-out. A proper fix needs either a custom combinator that never touches a sibling slot once the fail-fast winner is known, or a side-effect-free way to peek a child's recorded state against the stateful, cursor-based `HistoryMatcher` without consuming its cursor — both cross-cutting changes shared with the activity fan-out primitive, out of scope here. Proven at the `WorkflowOutcome` level (the resulting `WorkflowFailed` needs a DB-backed worker integration test, since the "unsupported commands" check lives in `worker.rs`, not the pure `executor::run_workflow`) by `tests/child_fanout_tests.rs::child_fan_out_raw_known_limitation_stale_repark_command_leaks_into_next_suspension`.

No engine changes were needed to ship this — the worker's `extract_all_started_child_workflows`/`persist_all_started_child_workflows` path already accepted a suspension batch of N `StartChildWorkflow` commands (proven end-to-end by the pre-existing `worker_completes_parent_workflow_with_parallel_child_workflows` test using a hand-rolled `tokio::join!`), and `HistoryMatcher::match_child_workflow`'s out-of-order terminal scan already tolerated child completions arriving in any order. Fan-out for child workflows is purely a `WorkflowContext` convenience layer over those existing primitives, mirroring `fan_out_raw_impl`/`fan_out_collect_raw_impl` exactly.

See `autumn-harvest/examples/fanout_child_workflows.rs` for a complete end-to-end example covering all three shapes (static N, dynamic N from a prior activity, and collect-all with partial failure).

### Deterministic Primitives (issue #384)

First-class, replay-safe alternatives to the non-deterministic APIs the guardrails (HVG001 wall-clock, HVG002 randomness) warn about. One `WorkflowContext` method call per value — no local-activity definition, no magic strings. Each helper captures its value on the **first** live execution, freezes it into a single `SideEffectRecorded` event, and replays the identical value on every subsequent pass and every worker.

| Method | Returns | Captures |
|--------|---------|----------|
| `ctx.system_now()` | `DateTime<Utc>` | wall-clock instant at the call site |
| `ctx.system_time_now()` | `std::time::SystemTime` | same instant as a `SystemTime` |
| `ctx.new_uuid()` | `Uuid` | a fresh UUIDv7 (idempotency keys) |
| `ctx.random_u64()` / `ctx.random_f64()` | `u64` / `f64` | a sampling draw |
| `ctx.random_range(range)` | `T` | a uniform draw from `range` (e.g. `0..100`) |
| `ctx.side_effect(name, f)` | `HarvestResult<T>` | any one-shot value; `name` dedups within an execution |

`now()` (unchanged) returns the fixed `WorkflowStarted` timestamp — the workflow-logical *start* clock. Use `system_now()` when you need the *current* wall clock captured at the call site (e.g. "skip the notification if the event is older than 24h now").

```rust
#[workflow]
async fn notify(ctx: &WorkflowContext, event: Event) -> Result<(), String> {
    // Wall-clock decision — captured once, replayed verbatim.
    let fresh = ctx.system_now().timestamp() - event.occurred_at < 24 * 60 * 60;
    // Idempotency key — UUIDv7 captured once; safe across retries.
    let key = ctx.new_uuid().to_string();
    // Sampling — deterministic across replays.
    let in_rollout = ctx.random_range(0..100) < 10;
    // One-shot environment capture.
    let region: String = ctx
        .side_effect("region", || std::env::var("REGION").unwrap_or_default())
        .map_err(|e| e.to_string())?;
    // ...
    Ok(())
}
```

All six lower onto the single append-only `WorkflowEvent::SideEffectRecorded { kind, name, value }` variant (`kind: SideEffectKind` ∈ `Now`/`Uuid`/`Random`/`Custom`). The `HistoryMatcher` matches them in command order; a divergence (reorder/insert/remove/rename across a code change) is surfaced by `WorkflowReplayer` as `NonDeterminismKind::SideEffectDrift`. `side_effect`/`random_uuid` recorded under the pre-#384 engine (as `MarkerRecorded`) still replay correctly. Randomness is **not** cryptographically secure — for security-grade entropy, use a regular activity. See `autumn-harvest/examples/deterministic_primitives.rs`.

#### Absolute-deadline timer — `ctx.sleep_until` (issue #749)

`ctx.sleep_until(timer_id, deadline)` is the absolute-instant companion to the relative, whole-second `ctx.timer(timer_id, secs)` — durably wait *until* a `DateTime<Utc>`, then resolve. It is the blessed, replay-safe form of the hand-rolled `ctx.timer(id, (deadline - ctx.system_now()).num_seconds() as u64)` pattern, with both foot-guns closed inside the engine: the wall clock is captured **once** via the deterministic `system_now` (frozen into history as a `SideEffectRecorded { kind: Now }` event — an **existing** variant, **no new event variant, no migration**), so every replay recomputes the identical remaining duration and resolves at the same instant on every worker; and a `deadline` at or before the captured "now" clamps the remaining duration to **zero** (fires on the next poll — never a wrapped multi-year sleep from an `as u64` underflow). Any positive sub-second remainder rounds **up** to the engine's whole-second timer granularity (nanosecond-precise, matching `wait_for_signal_timeout`), so the wait never resolves *before* `deadline`. It lowers onto the existing `TimerStarted`/`TimerFired` events and suspension path — a `sleep_until` timer is byte-identical to a `ctx.timer` timer of the same computed duration, so a non-deterministic author-supplied `deadline` surfaces as ordinary timer non-determinism (not a panic), and like `ctx.timer` exactly one timer suspends per call and it cannot share a suspension batch with activity/child-workflow/signal commands. Derive `deadline` from deterministic state (a workflow input, a prior activity output, or `ctx.system_now()`) — never `chrono::Utc::now()` directly; prefer `ctx.timer(id, secs)` for a purely *relative* wait (it skips the redundant `system_now()` capture); and note that in `WorkflowTestEnv` the internal `system_now()` reads the real wall clock rather than the virtual `ctx.now()`. See `autumn-harvest/examples/sleep_until_renewal_reminder.rs`.

#### Business-day timer — `ctx.timer_business_days` (issue #806)

`ctx.timer_business_days(timer_id, n, calendar)` arms a durable timer that resolves after `n` **business days** per a named calendar, stepping over weekends and that calendar's holidays — the one-call answer to "escalate this ticket after 2 business days", which `ctx.timer(id, 2 * 86_400)` gets wrong the moment the window straddles a weekend or a public holiday. Returns the resolved fire instant. A non-suspending sibling `ctx.business_days_from_now(id, n, calendar)` resolves and freezes the same deadline **without** arming a timer.

**No new event variant, no migration.** The resolved deadline is frozen into the **existing** `SideEffectRecorded { kind: Custom, name: "__harvest_business_day:{seq}:{id}" }` variant (issue #384) and the wait rides the **existing** `TimerStarted` event. The freeze and the arm share **one** suspension batch, so arming costs one decision cycle, not two.

**Determinism — the calendar is read once, ever.** The resolution runs on the **first live execution** from an anchor captured inside the freeze (never a fresh `Utc::now()` on replay). Every later replay returns the recorded value verbatim and **never re-runs the resolution**, so an operator adding a holiday after a timer is armed can never move that timer's deadline (the snapshot lookup still happens on every call, but its result is discarded on replay because `side_effect`'s matched arm never invokes the closure) — only timers armed afterwards see the edit.

**Registration** is one builder call: `HarvestBuilder::state(BusinessCalendars::builtin())`. Holidays resolve on the worker from a `BusinessCalendars` snapshot in shared state (typically loaded once at startup via `calendar::load_exclusions_for_calendar`) — no per-call DB read, no global static.

**Semantics.** Weekends (Sat/Sun) are **always** non-business days whatever the calendar is named — the named calendar contributes *holidays* (this deliberately differs from the scheduler's `"weekends-off"` naming convention, #337: the scheduler decides whether to *skip a firing*, this decides *when a deadline lands*). Business days are counted on **UTC dates** and the deadline preserves the anchor's UTC time-of-day (matching the `DATE`-typed exclusion rows; DST-free) — *known limitation:* a deployment whose local business day is offset from UTC is off by one business day near the UTC-midnight boundary, and for `n = 0` the roll-forward can be suppressed entirely (a local Saturday morning east of UTC is still a UTC Friday). `n = 0` **rolls forward**: fires at the anchor when the anchor's UTC date is a business day, else at the next business date at the same time-of-day; it never means "one business day later". A calendar registered with a *declared* horizon (`with_calendar_covering`, and the shipped built-ins, which declare `BusinessCalendars::BUILTIN_COVERAGE_END` = 2026-12-31) **rejects** a resolution needing a later date rather than silently answering weekends-only; plain `with_calendar` declares no horizon and is never rejected on coverage grounds.

**Two error classes.** *Prologue* errors record **zero** commands: invalid `n` (over `MAX_BUSINESS_DAYS` = 3650) and a timer-id collision with a live `ctx.start_timer` handle (checked only on the timer-arming entry point; `business_days_from_now` arms no timer and uses a disjoint namespace). Both are pure functions of the arguments and this execution's own state, so they fire identically on every worker and every replay — fixing the call and redeploying is enough. *Frozen* errors are recorded and replay identically forever, recoverable only by workflow reset: **calendar unavailable on this worker** (no `BusinessCalendars` registered, or the name absent from the snapshot) → `HarvestError::NotFound`, and coverage exhausted → `Config`. Calendar availability is **worker-local deployment state**, so it must be frozen: returning early looks retryable but is not — a workflow that propagates the error is sealed **terminally** (`process_workflow_task` treats an author `Err` as terminal), and one that *catches* it and records anything afterwards **diverges** on replay once the calendar is registered, which issue #603 turns into a non-terminal block that clears only by rolling *back* the fix. Freezing makes both shapes replay-stable at the documented cost that an already-frozen run needs a reset; the strictly better answer is a capability-miss release that re-pends the task without recording (the issue #804 pattern), which needs a new executor/worker outcome channel and is a follow-up. **Register calendars before deploying workflows that name them.** **`BusinessCalendars::builtin()` carries a dated liability**: its declared horizon (2026-12-31) means that once wall-clock time nears it, every `builtin()`-backed resolution freezes a coverage rejection fleet-wide, and extending the arrays later does not recover already-frozen executions — treat the built-ins as demo/test data and register an operator-owned DB-loaded calendar in production.

**Composition.** Sequential composition works (arm, await, arm the next — each with its own timer id; the second window anchors at the wall clock of the decision cycle that dispatches it, which is at or after the first deadline — so a continuation that crosses UTC midnight can land one business day later than a naive "anchored exactly at the first deadline" reading; the test harness's virtual clock does anchor exactly there). Racing a business-day timer against `receive_signal_timeout` in a **single** suspension is not supported — the engine allows one `StartTimer` per suspension batch and rejects the mixed batch loudly. Business-day timers are fire-once, like `ctx.timer`/`ctx.sleep_until`; there is no cancellable variant today. In `WorkflowTestEnv`, `with_business_calendars(..)` + `with_frozen_anchor(..)` fire them deterministically without real sleeping. See `autumn-harvest/examples/business_day_escalation.rs` and `docs/getting-started/03-durable-timers.md`.

### Run metadata — `ctx.info()` (issue #698)

`ctx.info()` is the replay-safe, zero-footprint accessor for a workflow run's own **system** metadata — the identifiers operators and traces already key on, which author code previously could not see. It returns a `WorkflowExecutionInfo` bundling seven fields: `execution_id`, `workflow_id`, `workflow_type`, `start_time`, `history_event_count`, `is_replaying`, and `parent_execution_id`.

```rust
#[workflow]
async fn checkout(ctx: &WorkflowContext, cart: Cart) -> Result<(), String> {
    let info = ctx.info();
    // One-hop correlation: this IS the management-API path key —
    // GET /api/harvest/workflows/{info.execution_id} opens this exact run.
    tracing::info!(execution_id = %info.execution_id, "starting checkout");
    // Mint a run-scoped idempotency key so a charge is never double-applied
    // across worker-level retries of this run:
    let charge_key = format!("charge-{}", info.execution_id);
    // A child workflow can identify the parent that spawned it:
    if let Some(parent) = info.parent_execution_id {
        tracing::info!(%parent, "spawned by parent");
    }
    // ...
    Ok(())
}
```

`execution_id` is the exact identifier the management API path (`/api/harvest/workflows/{exec_id}/...`), the DLQ, reset/pause/cancel, and the ADR-0001 `execution.id` span attribute all use — so a log line carrying `ctx.info().execution_id` opens that run with **zero** directory lookups. `parent_execution_id` is `None` for a top-level run and the spawning parent's id for a child (threaded by the worker from the execution row's `parent_id`). Two sibling `const fn` accessors also exist: `ctx.start_time()` (the **raw** frozen `WorkflowStarted` timestamp — deliberately **not** the advancing virtual clock that `ctx.now()` moves under the test harness) and `ctx.parent_execution_id()`.

**Guarantees:** every field derives from already-recorded state (the `WorkflowStarted` event + the execution row the executor already loads), so `info()` is **replay-deterministic** (byte-identical on every worker and every replay pass) and **leaves zero footprint** — it appends no `harvest_events` and emits no `WorkflowCommand`, the same leave-no-trace property as a query handler. Usable from any `#[workflow]` body with **no feature flag**. **No new `WorkflowEvent` variant, no migration.** One exception to the byte-identical/branch-safe claim: `is_replaying` is **observability-only** — it intentionally differs live-vs-replay (it *is* the replay indicator), so do not branch command-affecting logic on it or include it in an activity input (doing so records different commands live vs replay → non-determinism). See `autumn-harvest/examples/ctx_info.rs`. The activity-side counterpart is `ActivityContext::info()` — see "Activity run metadata and deadline" below (issue #783).

### Cross-type continue-as-new — multi-phase entities (issue #803)

`ctx.continue_as_new(input)` resets history but always resurrects the run as the **same** workflow type. For a long-lived entity that does genuinely different work per lifecycle phase (`trial_subscription` → `paid_subscription` → `churned`), that forces either one ever-branching monolith or breaking the stable `workflow_id` that `signal_with_start` (#244) / `update_with_start` (#479) depend on. Two new methods continue the *same logical entity* as a *different registered type*:

| Method | Form |
|---|---|
| `ctx.continue_as_new_as::<I>(&paid_subscription_info(), input)` | Typed — resolves the name from the target's companion `WorkflowInfo`, no magic string |
| `ctx.continue_as_new_as_type("paid_subscription", json!(input))` | Untyped — for a dynamically-chosen target |

```rust
#[workflow]
async fn trial_subscription(ctx: &WorkflowContext, sub: Subscription) -> Result<Value, String> {
    let converted: bool = ctx.receive_signal("conversion_decision").await.map_err(|e| e.to_string())?;
    if converted {
        ctx.continue_as_new_as::<Subscription>(&paid_subscription_info(), sub).await.map_err(|e| e.to_string())?;
    } else {
        ctx.continue_as_new_as_type("churned", json!(sub)).await.map_err(|e| e.to_string())?;
    }
    unreachable!("continue_as_new_as* suspends the run and never resolves");
}
```

**What carries, what re-resolves.** The successor keeps the entity's `workflow_id`, shard and queue, and its history starts clean:

| Property | Behaviour |
|---|---|
| `workflow_id`, shard, queue | **Kept** |
| Event history | **Reset** (the point of continue-as-new) |
| `execution_timeout` (#243), `sla` (#487) | **Re-resolved from the new type's `WorkflowInfo`**; per-run deadlines re-anchored to the successor's start. `sla` is clamped to `execution_timeout`, mirroring the start path |
| Concurrency key/limit (#247) | **Re-resolved from the new type's policy**, against the new input |
| `owner` / `runbook_url` / `severity` (#372) | **Re-resolved from the new type** — alerts page the team owning *this* phase |
| Workflow-level retry policy (#523) | **Re-resolved from the new type**, then clamped by the fleet-wide `max_workflow_attempts_ceiling` — a type change is not an escape hatch from an operator's retry cap |
| Chain lifetime cap (#617) — `chain_execution_timeout` / `chain_deadline_at` | **Carried verbatim.** Deliberate: changing type must not be an escape hatch from a runaway-loop budget |
| Unconsumed signals, `last_completion_result` (#488), schedule lineage (#534), `context_headers`, `origin`, `memo`, search attributes, `assigned_build_id` (#171), completion callbacks (#605), run-chain back-links (#701) | **Carried** — that carryover code never consults the workflow type |
| `throttle` (#607), `debounce` (#499), `batch` (#518), `max_input_bytes` (#252) | **Not consulted.** These are *admission* policies and continue-as-new is in-flight continuation, not a start — so a cross-type transition does not pass the target type's admission gates, and the payload cap enforced is the predecessor's |

**Single-shard only, enforced.** Rendezvous routing hashes the **pair** `(workflow_name, workflow_id)`, so changing the type re-routes the key — measured at ~75% of ids on a 4-shard router. The successor cannot follow it: the predecessor's seal and the successor's insert are one transaction, and there is no cross-shard transaction to relocate the row with. Left unguarded that would make the successor unreachable by `workflow_id`-addressed signal/cancel/await (#751) — the very addressing this feature exists to preserve — and would hide a live run of the target type on the routed shard, admitting two live runs under one key. So a transition whose target key routes to a different shard is **rejected terminally**, naming both shards. Naming the run's **own** type is exempt — it changes no key, so a run pinned off its hash-derived shard by explicit placement (#697) can still re-resolve its own declared defaults. Single-shard deployments never hit this (one shard, so the key can only resolve to it), and `HarvestPlugin` rejects multi-shard upstream, so the restriction binds only standalone-runner embedders. Relocation or a routing directory is the follow-up that would lift it.

The fleet-wide `max_workflow_execution_timeout` ceiling is applied to the target's declared timeout, so a type change is not an escape hatch from that either. A declared timeout the ceiling does **not** bound and that cannot be resolved to an absolute deadline (an out-of-range duration) **rejects the transition** rather than persisting a run whose `execution_timeout` claims a hard cap the timeout scanner — which only enforces a non-NULL `deadline_at` — can never fire. An out-of-range **`sla`** instead maps to "no SLA" (both fields cleared), matching the #487 start-path rule: the SLA is observational, so degrading it is benign, whereas silently dropping a runaway cap is not.

**Presence decides.** `new_workflow_type: None` (a plain `ctx.continue_as_new`) takes the byte-identical legacy path: every lifecycle column is carried verbatim, including a per-start override the *type* never declared. Only `Some(target)` re-resolves from a `WorkflowInfo`. A target declaring no default therefore **clears** the column rather than silently inheriting the predecessor's.

**Addressing consequence — read before adopting.** Harvest's active-run identity is the **pair** `(workflow_name, workflow_id)`, not `workflow_id` alone (Temporal differs — it keys on `workflowId`). After a transition an external caller must name the **current phase type**; naming the old type does not error, it silently starts a *separate* run of that old type (the transition released its uniqueness slot) which coexists with the live successor. If external callers cannot track the current phase, keep the entity on one type and branch internally.

The **inverse of that coexistence is a hazard, not a convenience**: because the successor must *take* `(target, workflow_id)`, a **live** run someone already started under that pair blocks the transition, and the transitioning execution is failed terminally rather than displacing the bystander. Do not start the next phase out-of-band while the current phase is still running — let the entity transition itself. A *terminal* prior run of the target phase **also blocks it**: harvest's uniqueness counts a terminal run as occupying the key, and releasing it would rewrite that run's recorded outcome — breaking `await`/`/result` for anyone holding its id, and rolling a scheduled carryover cursor backward — so win-back loops (`churned → trial → churned`) need the old run reset/erased first, or a different `workflow_id`. Naming the run's **own** current type is fine — it is a supported request for that type's declared defaults, and the predecessor's own seal frees the slot.

**Schedule overlap controls do not follow a type change.** `schedule_id`/`scheduled_for` are carried (the successor is still the same logical scheduled run), but `max_active_runs` counting and `OverlapPolicy::CancelOther`/`TerminateOther` all select on the *schedule's* `workflow_name` — so a cross-type successor stops counting toward the cap and cannot be cancelled by the next fire. A schedule with `OverlapPolicy::Skip` will fire again while the previous fire's successor is still live under the other type.

**Handler-removal reachability.** `GET /admin/workflow-types/reachability` (#520) reports `safe_to_remove` from *non-terminal executions of that type* only; it cannot see that a live run of a **different** type is about to `continue_as_new_as` into it. Grep for `continue_as_new_as`/`continue_as_new_as_type` targets before deleting a handler — see `docs/runbooks/safe-handler-removal.md`.

**Rollout ordering.** The target must be registered on the worker running the transition — deploy the new phase's handler **fleet-wide first**. An **unregistered target** is treated as a capability miss (issue #804), not a fault: the claiming worker **releases** the task back to `PENDING` so a peer that registers both phases can run the transition, and the predecessor is failed only once the redelivery budget is exhausted (with a `no_capable_worker:` reason). That makes a mid-deploy transition survivable rather than fatal, but it does not remove the ordering requirement — if the target never reaches any live worker, the run still escalates. The other three rejections are fleet-invariant, so no peer could help and they still fail the predecessor terminally on the first claim with an operator message naming the type, creating **no** successor (never a silent no-op, never an undispatchable row): a blank target, a target naming a registered **unified DAG** (a DAG successor would bypass the admission gates `POST /dags/{name}/trigger` enforces), and a target whose `(type, workflow_id)` slot is held by a **live** run.

**Scope.** Root-only, exactly as before (`reject_child_continue_as_new` is unchanged — a child cannot cross-type continue either). Out of scope per the issue: cross-shard relocation, DAG continue-as-new, and re-validating the successor input against the target's `input_schema` (#373).

**Invariants.** The transition rides the **existing** `WorkflowContinuedAsNew` event via one additive optional field `new_workflow_type: Option<String>` (`#[serde(default, skip_serializing_if = "Option::is_none")]`) — **no new `WorkflowEvent` variant, no migration**; pre-#803 histories deserialize to `None` and replay identically. `HistoryMatcher::match_continue_as_new` compares the recorded target type **before** the input, so a code change that redirects an already-recorded transition surfaces as `HarvestError::NonDeterministic` / `NonDeterminismKind::ContinueAsNewMismatch` rather than silently retargeting a live entity. See `autumn-harvest/examples/entity_phase_transition.rs`.

### Patched / Code Evolution (issue #687)

`ctx.patched(id)` / `ctx.deprecate_patch(id)` are the recommended default for evolving in-flight workflow logic — a boolean two-state gate over the same `MarkerRecorded` event `ctx.version()` uses (`patch:{id}`, no new event variant, no migration).

| Deploy | Code | Effect |
|---|---|---|
| **1 — introduce** | `if ctx.patched("billing-v2") { new } else { old }` | New runs record a `patch:billing-v2` marker and take the new branch; pre-patch runs keep replaying the old branch deterministically. |
| **2 — deprecate** | `ctx.deprecate_patch("billing-v2");` + unconditional new code (once all *pre-patch* runs have drained — see the "Patched gates" section of `docs/runbooks/version-gate-retirement.md`; the runbook's `version-usage`/retirement-check CLI tooling only sees `version:` markers, **not** patch gates — use that section's raw SQL drain queries) | Recorded markers become transparent to replay wherever they sit; marker-bearing runs still replay cleanly; new runs record nothing. |
| **3 — remove** | delete the call (once all *marker-bearing* runs have drained) | Nothing remains of the gate. Removing it too early surfaces as `NonDeterminismKind::PatchMarkerMismatch` in `WorkflowReplayer` — except a trailing stale marker as the final unconsumed event, which no later command trips over and therefore surfaces as the generic `EarlyCompletion` instead. |

**Patched vs. version**: reach for `ctx.patched` for the common before/after change; `ctx.version(id, min, max)` remains the explicit escape hatch for gates with **more than two** concurrent versions. Interop: a history recorded by a two-version `ctx.version(id, 1, 2)` gate is observed as patched by `ctx.patched(id)`, so gates migrate in place. **Shared namespace warning**: patch ids and version change-ids share one namespace — `deprecate_patch(id)` interop-consumes `version:{id}` markers, so never call `deprecate_patch(id)` while a `ctx.version(id, ..)` gate for the same id is still in the code (the still-live gate would read `min` and take the wrong branch → ND).

**Signal-with-start caveat**: a fresh execution whose first-task history ends in un-awaited signals at the gate point — canonically **every signal-with-start run**, whose signal is staged before first dispatch (history `[WorkflowStarted, SignalReceived]`) — takes the *old* branch and records **no** marker, forever, per-execution-deterministically. Deliberate, conservative parity with `ctx.version()` (the history is ambiguous with a phase-0 run parked at a first-line `wait_for_signal`). Drain checks before deploy 2 must therefore use the "no marker" inverse query in the runbook — marker-presence queries can never find these runs.

**Per-call-site answer**: `patched()` answers per **call site**, not per run (an in-flight phase-0 run resuming under phase-1 code with two gated sites can get `false` at site 1 and `true` at site 2) — don't split one logical change across multiple gates of the same id.

**Footgun**: after `deprecate_patch(id)`, a residual `patched(id)` call stays deterministic for old histories but returns `false` for **new** executions (and records nothing) — delete the residual call when you deprecate. Exception keeping live and replay consistent: a marker recorded earlier in the **same cycle** (by `patched` or `version`) counts as present, so a `patched(id)` → `deprecate_patch(id)` → `patched(id)` sandwich yields `(true, true)` on both passes.

### Race / Select (issue #600)

`ctx.race()` is the deterministic wait-**first** counterpart to the sanctioned wait-**all** `futures::join!`/fan-out. Racing two activities and cancelling the loser is five lines:

```rust
let winner = ctx.race()
    .activity(&fetch_primary_info(), input.clone())
    .activity(&fetch_fallback_info(), input)
    .run().await?;                       // loser task row → CANCELLED, atomically with the winner marker
let out: Quote = winner.decode()?;       // winner.index tells you which branch won
```

**Supported shapes** (this slice): a homogeneous race of **N activity** branches (`.activity`/`.activity_raw`), a homogeneous race of **N child-workflow** branches (`.child_workflow`/`.child_workflow_raw`), or exactly one `.timer(duration)` paired with exactly one `.signal(name)` (a thin wrapper over `receive_signal_timeout`, issue #476, for an approval-or-deadline race). Mixing kinds across shapes (e.g. an activity racing a timer in the same call) is rejected with `HarvestError::Config` — bound an individual activity with its own timeout, or use `receive_signal_timeout` directly, instead.

**Determinism contract**: the winning branch is recorded via the *existing* `MarkerRecorded` event (mirrors `execute_activity_fan_out`'s count marker — no new `WorkflowEvent` variant). Every later replay of the same history *verifies* the previously recorded winner rather than re-deriving it, so a code change that would flip the outcome is rejected as `HarvestError::NonDeterministic`. If multiple branches are already resolved by the time the race is (re-)evaluated (e.g. two activities both finished before the workflow noticed), the **lowest-indexed** resolved branch wins — a documented, deterministic tie-break.

**Cancellation**: losing branches are durably torn down in the *same* transaction that persists the winner marker — a still-open losing activity's task row is cancelled and a synthetic `ActivityFailed { error: "lost race to a sibling branch" }` is recorded (reusing the existing event variant, so no future replay observes it stuck in-progress); a losing child workflow is cancelled via the same primitive `ctx.request_cancel_external_workflow` uses (issue #492); a losing timer's row is deleted. A cancelled loser never triggers the workflow-level cancellation path or Saga compensation — only the loser itself is cancelled. See `autumn-harvest/examples/race_hedged_call.rs`.

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
| Heartbeating | **Not supported**; `ctx.heartbeat(...)` returns a runtime `Config` error and no heartbeat checkpoint is available | Supported; retry attempts can read the last flushed payload with `ctx.heartbeat_details::<T>()` |
| `schedule_to_start` timeout | **Not supported** | Supported |
| Custom task queue | **Not supported** | Supported |
| Retry policy | Supported | Supported |
| `ctx.info().task_id` | `None` (no `harvest_task_queue` row exists) | `Some(task_id)` — the id the operator `retry-now`/`fail-now` routes address |
| `ctx.deadline()` | `now + min(start_to_close, WorkerConfig::max_local_activity_start_to_close)` — always bounded | `min(started_at + start_to_close, schedule_to_close_at)`, or `None` when both are unset |
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

### Activity run metadata and deadline — `ctx.info()` (issue #783)

`ctx.info()` is the activity-side counterpart of the workflow-side `ctx.info()` (issue #698): a read-only, zero-footprint snapshot of **who owns this attempt** and **how much of its budget is left**. Reading it performs no I/O, appends no event, and sends no heartbeat.

```rust
#[activity(start_to_close = "30s")]
async fn charge_card(ctx: &ActivityContext, amount_cents: i64) -> Result<String, String> {
    let info = ctx.info();
    // One-hop correlation: `execution_id` IS the management-API path segment —
    // GET /api/harvest/workflows/{execution_id} opens the owning run.
    tracing::info!(
        execution_id = %info.execution_id,   // owning run
        workflow = %info.workflow_type,      // e.g. "order_flow"
        workflow_id = %info.workflow_id,     // e.g. "order-42"
        activity = %info.activity_type,
        attempt = info.attempt, max = info.max_attempts,
        task_id = ?info.task_id,             // the retry-now / fail-now id
        "charging card",
    );
    // A run-scoped idempotency key, stable across every retry attempt:
    let key = format!("charge-{}-{}", info.execution_id, info.activity_id);
    // ...
    Ok(format!("charged {amount_cents} cents"))
}
```

`ActivityExecutionInfo` fields: `execution_id`, `workflow_id`, `workflow_type`, `activity_type`, `activity_id`, `attempt`, `max_attempts`, `is_local`, `queue_name`, `task_id`. Each also has a standalone accessor (`ctx.execution_id()`, `ctx.workflow_id()`, …).

**Identity is stable across retries.** `info.identity()` returns the `ActivityIdentity` subset — `execution_id` / `workflow_id` / `workflow_type` / `activity_type` / `activity_id` — that is byte-identical on every attempt of the same logical invocation. Only `attempt` and the deadline advance. Deriving an idempotency key from `identity()` therefore reuses one key across retries; assert on `identity()` (not a hand-written field list) so a test stays honest as fields are added.

#### Deadline-aware checkpointing

| Method | Returns |
|--------|---------|
| `ctx.deadline()` | `Option<DateTime<Utc>>` — the **earliest** of `started_at + start_to_close` and the cross-retry `schedule_to_close` deadline; `None` when unbounded |
| `ctx.time_remaining()` | `Option<Duration>` — time left, **saturating at zero** (never negative); `None` when unbounded |
| `ctx.is_expiring_within(reserve)` | `bool` — `true` when less than `reserve` remains; **always `false` when unbounded** |

Taking the **minimum** of the two clocks matters: `schedule_to_close` enforcement (issue #378) kills a `RUNNING` row, so reporting only the per-attempt budget would over-report the time available and let an activity work right up to a kill that discards everything.

**Prefer `is_expiring_within` over comparing `time_remaining()` yourself.** `Option` orders `None` *below* `Some(_)`, so a naive `ctx.time_remaining() < Some(reserve)` checkpoints **immediately** on an unbounded activity — exactly backwards.

The pattern this enables — yield cleanly instead of being killed mid-item, so a retry re-executes **nothing**:

```rust
#[activity(start_to_close = "30s", heartbeat_timeout = "60s")]
async fn import_rows(ctx: &ActivityContext, req: ImportRequest) -> Result<u32, String> {
    // 1. resume from the checkpoint (empty on the first attempt)
    let mut next = ctx.heartbeat_details::<ImportCheckpoint>()
        .map_err(|e| e.to_string())?.unwrap_or_default().next;
    while next < req.total_rows {
        // 2. is the budget nearly spent?
        if ctx.is_expiring_within(Duration::from_secs(3)) {
            // 3. yield with a RETRYABLE error — the checkpoint survives the requeue.
            //    Wait out one flusher interval first: the batched heartbeat flusher
            //    ticks ~1 s and does NOT drain on cancel, so a checkpoint sent
            //    immediately before returning can be lost.
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            return Err(format!("deadline reserve reached at row {next}"));
        }
        // 4. ... do the work ... then checkpoint
        next += 1;
        ctx.heartbeat(serde_json::to_value(ImportCheckpoint { next }).map_err(|e| e.to_string())?)
            .await.map_err(|e| e.to_string())?;
    }
    Ok(next)
}
```

Size the reserve above the ~1 s flusher interval plus the cost of one unit of work. Local activities cannot heartbeat, so this pattern is regular-activity-only; a local activity's `deadline()` is still reported (from the worker cap) and is useful for bailing out early.

See `autumn-harvest/examples/activity_ctx_info.rs`. **No new `WorkflowEvent` variant, no migration** — every field is already-resolved dispatch state.

### Worker Sessions (issue #606)

A worker session co-locates a **sequence of activities** — not a single unit of work — on one physical worker for the session's lifetime, so they can share machine-local state: a downloaded artifact, a scratch directory, a loaded ML model, warmed GPU memory. Without a session, each activity in a `download -> transcode -> upload` pipeline may land on a different worker and has to re-fetch or round-trip the artifact through external storage at every hop.

```rust
#[workflow]
async fn transcode_pipeline(ctx: &WorkflowContext, job: MediaJob) -> Result<MediaResult, String> {
    let session = ctx
        .create_session(SessionOptions::new("gpu-workers"))
        .await
        .map_err(|e| e.to_string())?;

    let local_input = session.execute_activity(&download_chunk_info(), job.source_url).await.map_err(|e| e.to_string())?;
    let local_output = session.execute_activity(&transcode_chunk_info(), local_input).await.map_err(|e| e.to_string())?;
    let result = session.execute_activity(&upload_chunk_info(), local_output).await.map_err(|e| e.to_string())?;

    session.complete().await.map_err(|e| e.to_string())?;
    Ok(result)
}
```

Adoption cost is exactly **one builder call** (`WorkerConfig::with_max_concurrent_sessions(n)`, default `0` = disabled, zero behavior change) plus wrapping the pipeline in a session scope — `download_chunk`/`transcode_chunk`/`upload_chunk` above are ordinary `#[activity]` functions with unchanged signatures; only the *dispatch calls* (`session.execute_activity` instead of `ctx.execute_activity`) differ.

**Mechanics.** `ctx.create_session(options)` records a deterministic session identity via the existing `MarkerRecorded` mechanism (`session:{seq}`, mirroring `execute_activity_fan_out`'s count marker), then dispatches a reserved internal activity (`__harvest_session_acquire`) bounded by `SessionOptions::acquisition_timeout` (default 30 s). A worker only claims that internal activity when it has a free session slot (`max_concurrent_sessions`); on claim it durably records a `harvest_sessions` `ACTIVE` row and returns its own worker id as the activity's output — that output is what every replay recovers the session's physical binding from, without ever needing live routing state. `Session::execute_activity`/`execute_activity_raw` push a normal `ScheduleActivity` command carrying `session_id`/`session_worker_id`; the worker persists these as a **hard pin** (`sticky_worker_id` + a new claim gate `session_id IS NULL OR sticky_worker_id = $1`) that — unlike ordinary sticky routing (issue #235) — never fails over to a different worker even after the bookkeeping sticky lease expires, since session-local state only exists on the acquiring host. `Session::complete()` dispatches a second reserved internal activity (`__harvest_session_release`) hard-pinned to the host, freeing its in-process slot and marking the row `COMPLETED`.

**No new `WorkflowEvent` variant, no replay-worker-dependence.** Session *identity* replays via the existing `MarkerRecorded` mechanism; the *physical worker binding* is non-replayed runtime routing state resolved fresh from the acquire activity's recorded output on every replay cycle — exactly like ordinary activity placement today. A `WorkflowReplayer` fixture replays the identical history clean regardless of which worker id was originally recorded as the host.

**Broken sessions.** `sessions::enforce_broken_sessions` (folded into `timeout::enforce_timeouts_once`, no separate poll loop) scans `ACTIVE` sessions whose host has no live `harvest_workers` heartbeat, is `Draining`/`Stopped`, or whose lease (`expires_at`) has elapsed, and reclaims them: the session transitions to `BROKEN` and every `PENDING`/`RUNNING` member task fails **non-retryably** with `ActivityFailed { error_type: "SessionBroken" }` (reusing the existing event variant) — a hard-pinned task on a dead host can never fail over, so non-retryable is required or it would re-pend forever. The workflow observes this as a typed `HarvestError::SessionBroken`, distinct from `HarvestError::SessionAcquireTimeout` (no free worker within the acquisition window), so the author can distinguish "never got a host" from "lost the host mid-pipeline" and re-establish a fresh session.

**Decision matrix — worker session vs. local activity vs. plain activity vs. claim-check (issue #524):**

| | Worker session | Local activity | Plain activity | Claim-check (`PayloadStore`) |
|---|---|---|---|---|
| What's shared | Machine-local state across **N activities** (file, cache, GPU memory) | Nothing shared — one inline call, no task-queue round-trip | Nothing shared — each activity may land on any eligible worker | The **payload itself** (large blobs), not compute locality |
| Unit co-located | A sequence of remote activities | A single in-process call | N/A (each activity is independent) | N/A (a storage indirection, not a placement primitive) |
| Where work runs | One pinned worker for the session's life | Inline on the workflow worker task | Any worker with a free slot for that queue | Wherever the activity that reads/writes the reference happens to run |
| Typical use | Multi-step pipeline needing shared local state (media transcode, ML inference, large-artifact ETL) | Fast (< 1 s) pure computation, cache lookups, format conversions | Any independent I/O call | A single oversized payload (input/output) that would blow the in-history payload cap |
| Failure/host-loss | `HarvestError::SessionBroken` (member task fails non-retryably; scanner-detected) | N/A — runs synchronously in the workflow task | Ordinary retry policy; can fail over to any eligible worker | N/A — storage failures surface as an ordinary activity error |
| Adoption cost | One builder call + session scope | `#[activity(local = true)]` | None (the default) | Register a `PayloadStore` + threshold on the builder |

**Use a worker session when** two or more activities in sequence need to share *machine-local* state that is expensive or impossible to reconstruct on a different host (a downloaded multi-GB artifact, a warmed model, GPU-resident data) — that's the co-location problem this primitive solves, and it composes with sticky routing (#235) and per-shard execution since the session is established within the owning shard while the workflow-task coroutine itself may still run anywhere.

**Don't reach for a session when:**
- Only a *single* activity needs local state — a local activity (or just letting the activity itself manage locality, e.g. an idempotent re-download) is simpler and avoids the session-acquire round-trip.
- The problem is payload *size*, not compute locality — claim-check (`PayloadStore`, issue #524) offloads large blobs to external storage so they never round-trip through `harvest_events`, independent of which worker runs each step.
- Activities don't need to share anything — plain activities compose freely across the fleet with no pinning at all.

Out of scope (per issue #606): cross-worker session migration/failover, a built-in file-transfer or GPU-scheduling API, changes to the claim-check design, reserving heterogeneous hardware classes, or fairness/priority among competing session requests.

See `autumn-harvest/examples/worker_session_pipeline.rs` for a complete download-transcode-upload example.

### Durable Mutex — ctx.mutex (issue #691)

`ctx.mutex(key)` gives a `#[workflow]` body durable, named, cross-workflow **mutual exclusion** for a critical region — the answer to "two executions must not read-modify-write the same shared resource at once." A `#[workflow]` cannot reach for a `std::sync::Mutex` (it would not survive a worker restart, and holding a std lock across a durable `.await` breaks replay); `ctx.mutex` coordinates the lock through Postgres so it survives restarts, worker failover, and replay.

```rust
#[workflow]
async fn apply_ledger_op(ctx: &WorkflowContext, op: LedgerOp) -> Result<i64, String> {
    // acquire() durably suspends THIS execution until it holds the key.
    let guard = ctx.mutex(format!("ledger:{}", op.account)).acquire().await
        .map_err(|e| e.to_string())?;
    // ── critical section ── the guard is held across the durable activity, so
    //    no other execution can enter for the same key. The rest of the
    //    workflow (outside this region) still runs concurrently with peers.
    let new_balance = ctx.execute_activity(&apply_delta_info(), op.amount).await?;
    // Released when `guard` drops at scope end, or explicitly via `guard.release()`.
    // `guard.lock_seq()` is the fencing token minted for this grant.
    Ok(new_balance)
}
```

The guard is a `#[must_use]` RAII handle. It releases the lock on **any** of: drop / scope exit, an explicit `guard.release()`, the holder reaching a terminal state (complete/fail/cancel/terminate/timeout — a terminal sweep frees the lock), or the lease TTL expiring (crash backstop). Dropping it pushes exactly one **event-less** `ReleaseMutex` bookkeeping command (zero `harvest_events` footprint). A guard held while the workflow parks on an `.await` is **not** released under the still-parked holder — the lock stays held across the suspension (the timeout-guard contract).

**Per-key concurrency (#247) vs mutex — different problems:**

| | Per-key concurrency (`#[workflow(concurrency(...))]`, #247) | `ctx.mutex(key)` (#691) |
|---|---|---|
| Gates | **Admission** at workflow *start* | A **region** *mid-workflow* |
| Caps | The running *count* of whole workflows sharing a key | Exactly one holder of a key at a time |
| Effect on the rest of the run | `limit = 1` serializes the **entire** workflow (and risks parent/child deadlock) | Only the critical region serializes; the rest of each workflow runs concurrently with peers |
| Reach for it when | You want at most N concurrent runs per tenant/entity | Two runs must not touch the same shared resource inside a region |

**Semantics & contracts:**
- **No new event variant.** A grant records the single existing `MutexGranted` anchor; release is event-less bookkeeping (mirrors `SetCurrentDetails`). Replay recovers the guard from the recorded grant, regardless of which fencing token (`lock_seq`) was minted.
- **FIFO fairness.** Contenders are granted in arrival order (a BIGSERIAL head-of-line waiter queue).
- **Lease-based liveness (crash safety).** A held lock carries a lease the holder renews each decision cycle. If the holder crashes, an expired lease is reclaimed and the next FIFO waiter is granted. **Honest lease contract:** the lease TTL (default 60 s; `WorkerConfig::with_mutex_lease_ttl`) must exceed the worst-case *single held step* — concretely, the **interval between the lease-renewing decision cycles** of the holder. Renewal happens per decision cycle (and on resume from pause), so a lock held across a **single long region that spans no such cycle** — one long-running activity, a child workflow, or an nd-block backoff longer than the TTL — can have its lease expire and be reclaimed by a waiter while the original holder still believes it holds the lock. Hold locks across **short** critical regions; if a region must span a long durable step, raise the TTL above that step's worst-case duration. `lock_seq` fences the lock table (a reclaimed-then-resumed old holder's release is a 0-row no-op, so the table never corrupts), but the engine cannot fence the author's *external* side effects — the standard lease-lock caveat.
- **Shard-local scope.** The lock is scoped to the workflow's own shard (same scope as per-key concurrency, #247): contending workflows must resolve to the **same** shard. Cross-shard global mutual exclusion is out of scope.
- **Non-reentrant.** Re-acquiring a key this execution already holds returns `HarvestError::MutexSelfDeadlock` **synchronously** (before any history match), so a self-deadlock surfaces the same typed error live and on replay rather than hanging.
- **Cross-key deadlock.** Two workflows each acquiring keys A and B in opposite orders can deadlock (holder-A-waits-B vs holder-B-waits-A) until a lease expires. Acquire multiple keys in a consistent **global order** (e.g. sorted by key) to avoid it.
- **Held-duration metric.** `harvest.mutex.held_duration` is measured on **explicit guard drop / `.release()`** (a normal region exit). A lock held all the way to a terminal state is freed by the **terminal sweep**, which does not measure held-duration — so a lock never explicitly released does not contribute to this histogram.
- **No mixed suspension batch.** `acquire()` is its own suspension shape; `tokio::join!(ctx.mutex(k).acquire(), <other await>)` (racing an acquire against another durable await in one decision cycle) is **rejected** — the worker fails the mixed suspension batch loud rather than silently corrupting. Acquire the lock in its own suspension, then do the other work.
- **`ctx.race()` interop (fixed in #1126).** Calling `ctx.mutex(k).acquire()` — or **any** follow-on durable command (a plain activity, a timer, ...) — in the **same decision cycle** immediately after `ctx.race()` resolves cleanly, even when a losing branch is still in-flight at resolution. Previously such a follow-on nd-blocked: a losing activity branch's `ActivityStarted` was left unconsumed at the `HistoryMatcher` cursor (its synthetic `ActivityFailed` is appended only at persist time, invisible to the resolving cycle), so the next positional `match_*` diverged. `settle_race` now consumes the loser branches' in-flight progress-frontier events in the resolving cycle (matcher-side only — no event mutation, no new `WorkflowEvent` variant), so no interposing state-consulting call is needed and no rewrite of "acquire in a prior cycle" is required.

Author-facing primitive only: exclusive-only, no reentrancy/upgrade/downgrade, and no operator force-release route (all out of scope per the issue). See `autumn-harvest/examples/mutex_ledger.rs` for a complete two-workflow serialized-ledger example.

### Latest-Wins Concurrency — `on_conflict = "cancel_running"` (issue #811)

Per-key concurrency (#247) has one overflow behaviour: **defer**. A new run over the cap is enqueued and waits at the claim gate. For an idempotent, replace-the-work-in-flight job — reindex a document, recompute a report, re-run a per-tenant sync — deferring is exactly backwards: the in-flight run is already stale the moment a newer one is requested, and the queue fills with runs whose output will be discarded.

`on_conflict = "cancel_running"` makes the key **latest-wins**: admitting a new run cancels the oldest in-flight run(s) for the same key, so the newest request is the one that survives.

```rust
#[workflow(concurrency(key = "input.doc_id", limit = 1, on_conflict = "cancel_running"))]
async fn doc_index(ctx: &WorkflowContext, req: IndexRequest) -> Result<(), String> {
    // Only ever one indexing run per document; a newer request supersedes
    // whatever is in flight, and this run itself is superseded if a newer
    // request arrives while it runs.
    ctx.execute_activity(&reindex_info(), req).await?;
    Ok(())
}
```

| | `on_conflict = "defer"` (default) | `on_conflict = "cancel_running"` |
|---|---|---|
| Over the cap | The task waits at the claim gate for a slot | The **oldest** in-flight run(s) are cancelled |
| Which run's output survives | Every run eventually runs; all outputs land | Only the newest admitted run's |
| Fits | Work where every request must be processed (charges, emails, exports) | Idempotent recompute where only the latest result matters (indexing, dashboards, per-tenant sync) |
| Cost of a burst | Queue depth grows | Cancellations, not backlog |

**Semantics.**

- **`limit = 1`** — admitting a run cancels the single incumbent for the key.
- **`limit = N > 1`** — cancels the **oldest** runs until the post-admission population is `<= N`. "Oldest" is `started_at`, tie-broken by execution id, so the order is deterministic.
- **Deterministic tie-break** — the **later-admitted** run always wins. "Later" is admission order (the shard-local advisory lock the supersede pass takes), not wall-clock, so two concurrent starts resolve deterministically.
- **Ordinary cooperative cancellation** — a superseded run reaches `CANCELLED` via the same path an operator cancel takes: its `ctx.is_cancelled()` / `check_cancellation()` observe it, Saga compensation fires, and its `ParentClosePolicy` cascade runs normally. It is **not** a force-terminate. **No new `WorkflowEvent` variant and no migration** — a `WorkflowCancelled` event is recorded exactly as today.
- **Scoped to `(workflow_name, concurrency_key)`** — a *different* workflow type that merely resolved the same key string, and never opted in, is never cancelled.
- **Bounded** — one admission sheds at most `SUPERSEDE_SCAN_LIMIT` (32) runs. Latest-wins is a per-key fair-share control, not a bulk-cancel tool; excess beyond that is shed by the *next* admission for the same key, so the population still converges without any single start paying an unbounded cost.
- **Shard-local**, exactly like the #247 cap it extends — see `docs/sharding.md`. In a multi-shard deployment the guarantee is "at most `limit` per shard"; pin the key with an explicit `residency_key` if you need it globally.

**Where it applies.**

| Start path | Resolves the declared strategy? | Why |
|---|---|---|
| Plain HTTP start, `signal-with-start`, `update-with-start`, operator re-run, trigger-now, scheduler tick, completion-trigger (inline + cross-shard relay), typed client stub, transactional start | **Yes** | Genuine admissions — a new run enters the key's population |
| Deferred debounce / throttle / event-batch fire | **Yes** (carried on `DebounceStartOptions`, resolved at admission and replayed at fire) | Same admission, just deferred |
| Child spawn, detached child spawn, continue-as-new, reset fork | **No** (`StartWorkflowParams` is never constructed) | In-flight continuation, not an admission |
| Workflow-level retry (#523) | **No** — explicit `Defer` | Letting a retry supersede would invert latest-wins: a retry of an *older* run would cancel the newer one that replaced it |
| Schedule **backfill**, outbox relay, Vantage manual trigger, plugin bootstrap | **No** — explicit `Defer` | These pass `concurrency_key: None`, so `on_conflict` is inert. Matches #247, which never applied per-key concurrency on those paths — a backfill deliberately materialises historical slots and must not cancel live work |

This mirrors how the #618 admission gates and #607 throttles treat the same paths.

**Interaction notes.**

- **`ctx.mutex` (#691)** — combining `cancel_running` with the durable mutex on the same terminal path can produce a Postgres-detected lock-ordering cycle (`40P01`) on the *start*. It aborts atomically and is safe to retry; it is a liveness hazard, not a correctness one. See `docs/sharding.md` for the full note.
- **Activity concurrency groups never latest-wins** — an activity task can carry a `concurrency_key` too (#247), but it is gated only at claim time and always defers; the supersede sheds workflow *executions* only.
- **Nested admissions can leave a key transiently over its limit** — a superseded run's cancellation runs its terminal chokepoint, which can start a completion-trigger target inside the same transaction. That nested admission counts the still-uncommitted outer admission toward the key's population but structurally cannot cancel it (the caller is about to report that run as created). If the remaining overflow is entirely such protected in-flight admissions, the key stays over its limit until the next ordinary admission — which sees them unprotected and sheds them — and a `tracing::warn!` names the key and the gap. Same bounded-shed philosophy as `SUPERSEDE_SCAN_LIMIT`.
- **Population** — the superseded set is *non-terminal runs* (`RUNNING`/`PAUSED` execution rows), which is a superset of what the #247 claim gate counts (`RUNNING` task rows with a live worker). A paused run, or one still deferred at the claim gate, occupies no dispatch slot yet is still superseded — that is the intended "at most `limit` non-terminal runs per key" reading.

**Observability.** Each supersede increments `harvest.concurrency.superseded{workflow}` (labelled by the *superseded* run's type; the concurrency key is deliberately not a label — unbounded tenant input). A superseded run also increments the ordinary `harvest.workflow.terminal{outcome="cancelled"}`; the new counter isolates the supersede subset. `GET /admin/concurrency` reports each key's live counters plus a `workflows` array naming the type(s) on it and the effective `on_conflict` each declares. A `task_type = "activity"` row always reports `defer`, even when its owning workflow declares `cancel_running`: latest-wins is a workflow-*start* admission control and sheds workflow executions, so an activity concurrency group is gated purely at claim time and never superseded.

**Source-visible change.** Five `pub` functions in `execution.rs` — `start_or_load_workflow_execution_collect`, `start_or_load_workflow_execution_idempotent`, `signal_with_start_workflow_execution_with_metrics`, `rerun_workflow_execution`, `update_with_start_workflow_execution_with_metrics` — changed their 4th tuple element from `Vec<(String, String)>` to `Vec<StartCancelledRun>` (a struct carrying `workflow_name`/`queue_name` plus a `superseded: bool` discriminator). A downstream caller that binds or iterates that element must adapt. The wrapper functions most callers use (`start_or_load_workflow_execution`, `..._with_metrics`) are unchanged.

**Out of scope** (per the issue): a hard `Terminate`/force-fail supersede, cross-shard latest-wins, trigger-layer debounce (that is #499), "keep oldest, reject newest", and per-activity latest-wins.

### Workflow Input/Output JSON Schema (issue #373)

Operators and non-Rust callers can discover the expected JSON shape of each workflow's input and output through the management API without reading Rust source. Schema publishing is **opt-in** — workflows that don't attach a schema continue to work exactly as today.

#### Opt-in via manual schema function

```rust
fn onboard_input_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "user_id": {"type": "integer"},
            "email":   {"type": "string"}
        },
        "required": ["user_id", "email"]
    })
}

// In the builder — chain after the companion function:
.workflows(vec![
    onboarding_info()
        .with_description("Handles new-user onboarding from signup to first action")
        .with_input_schema_fn(onboard_input_schema),
])
```

#### Opt-in via `schema` feature + `schemars`

Enable the `schema` Cargo feature and derive `JsonSchema` on your types:

```toml
# In your Cargo.toml:
autumn-harvest = { version = "0.3", features = ["schema"] }
schemars = "0.8"
```

```rust
#[derive(serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct OnboardInput { pub user_id: i64, pub email: String }

// In the builder:
.workflows(vec![
    onboarding_info().with_schemas::<OnboardInput, (), String>(),
])
```

#### `#[workflow(description = "…")]`

Attach a one-paragraph description directly in the macro attribute:

```rust
#[workflow(description = "Handles new-user onboarding from signup to first action")]
async fn onboarding(ctx: &WorkflowContext, input: OnboardInput) -> Result<(), String> { … }
```

#### Discovery endpoints

```
GET /api/harvest/workflows/registered
```
Returns a sorted JSON array of all registered workflow types:
```json
[
  { "name": "onboarding", "description": "Handles new-user onboarding…",
    "input_schema": { "type": "object", … }, "output_schema": null, "error_schema": null },
  { "name": "no_schema_wf", "input_schema": null, "output_schema": null, "error_schema": null }
]
```

```
GET /api/harvest/workflows/registered/{name}/schema
```
Returns the record for a single workflow type, or `404` if the name is unknown.

#### Server-side input validation

When a workflow has a published `input_schema`, `POST /api/harvest/workflows/{name}/start` validates the input **before** the workflow enters the task queue. A bad input returns:
```json
HTTP 400
{
  "error": "input validation failed",
  "violations": [
    { "message": "missing required field 'email'", "field_path": "/email" },
    { "message": "expected type 'integer', got 'string'", "field_path": "/user_id" }
  ]
}
```
`field_path` is a JSON Pointer (RFC 6901). Workflows without a schema accept any input, unchanged.

#### curl examples

```bash
# Correctly shaped input — starts the workflow:
curl -X POST /api/harvest/workflows/onboarding/start \
  -H 'Content-Type: application/json' \
  -d '{"input": {"user_id": 1, "email": "alice@example.com"}}'

# Deliberately wrong shape — rejected at the API boundary (400):
curl -X POST /api/harvest/workflows/onboarding/start \
  -H 'Content-Type: application/json' \
  -d '{"input": {"user_id": "not_an_int"}}'
```

See `autumn-harvest/examples/schema_workflow.rs` for a full working demonstration.

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
| `harvest_workers` | `Text` | Live worker process registrations and heartbeat state (`build_id`, `deployment_name` added in issue #171) |
| `harvest_build_policies` | `Uuid` | Per-queue active build policy: new starts get `assigned_build_id = policy.build_id` |
| `harvest_build_compat` | `Uuid` | Compatibility declarations: workers running build B may process executions assigned build A |
| `harvest_payload_refs` | (`blob_key`,`exec_id`) | Per-execution references to offloaded payload blobs (issue #524). `ON DELETE CASCADE` ties the refcount to execution lifetime so retention GCs a blob exactly when its last referencing execution is collected. Shard-local. |
| `harvest_debounce` | `Uuid` | Pending debounce records — one row per `(workflow_name, debounce_key)`. Upserted on each qualifying start (trailing-edge deadline update, `pending_count` increment, `last_input` overwrite). Deleted by the scanner after the corresponding execution is started. `UNIQUE (workflow_name, debounce_key)` is the collapse constraint; `effective_fire_at` index drives the scanner probe. Scoping: shard-local. |
| `harvest_completion_deliveries` | `Uuid` (= `delivery_id`, stable across redeliveries) | Durable completion-callback delivery tasks (issue #605), one row per matched `(execution, callback_index)`. `payload jsonb` is a frozen envelope snapshot immune to later row mutation/retention; `retry_policy jsonb` is frozen at enqueue time. `state` ∈ `PENDING`/`INFLIGHT`/`DELIVERED`/`FAILED`. `UNIQUE (workflow_exec_id, callback_index)` is the anti-double-enqueue key; partial index on `(shard_id, next_attempt_at) WHERE state IN ('PENDING','INFLIGHT')` drives the scanner probe. Scoping: shard-local. |
| `harvest_sessions` | `Uuid` | Worker sessions (issue #606): one row per `ctx.create_session(...)` call, recording `host_worker_id`, `queue_name`, `expires_at` (lease), and `state` ∈ `ACTIVE`/`BROKEN`/`COMPLETED`. `harvest_task_queue.session_id` hard-pins member activity rows to `host_worker_id` via the claim gate `session_id IS NULL OR sticky_worker_id = $1`, which — unlike ordinary sticky routing — never fails over even after the row's bookkeeping sticky lease elapses. `harvest_workers.max_concurrent_sessions`/`in_use_sessions` advertise/track fleet-wide session capacity. Scoping: shard-local (a session's host is resolved on the workflow's own shard). |
| `harvest_start_throttle` | `Uuid` | Pending throttled-start records (issue #607) — **one row per DEFERRED start** (contrast `harvest_debounce`'s one-row-per-key collapse: no unique constraint here, since preserving every start is the point). Inserted when the per-key token bucket (in `harvest_rate_limit_buckets`, keyed `"start-throttle:{workflow}:{key}"`) is empty, before any `WorkflowStarted` event exists. Deleted by the scanner once a token is reserved and the execution is started (or dropped if past its `expires_at`/`schedule_to_start` deadline, or if an id-reuse policy resolves to a no-op — token refunded in that case). `deferred_at` index drives oldest-first drain order; `bucket_key` index backs the admission-time FIFO-backlog-existence probe. Scoping: shard-local. |

`harvest_workflow_executions` is the hub — six tables join back to it via `workflow_exec_id`. `harvest_build_policies` and `harvest_build_compat` are keyed by `queue_name` and `(build_id, compatible_with)` respectively. `execution_timeout` and `deadline_at` (issue #243) are nullable columns on `harvest_workflow_executions`: `deadline_at = started_at + execution_timeout` is set at start time so the timeout scanner uses an indexed O(log n) scan rather than per-row arithmetic.

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

# Replayer harness tests (WorkflowReplayer — no DB required)
cargo test -p autumn-harvest --test replayer_tests --features testing --no-default-features

# Replay throughput benchmark (issue #135 budget: 10k events < 200ms)
cargo bench -p autumn-harvest --features testing --no-default-features --bench replay_bench

# Macro tests
cargo test -p autumn-harvest-macros
```

### Testing workflow code changes with WorkflowReplayer

`autumn_harvest::testing::WorkflowReplayer` (gated by the `testing` feature) lets
you assert that a `#[workflow]` function is replay-safe against recorded histories
before deploying a code change.  This catches non-determinism regressions in CI
rather than via the DLQ in production.

```rust
// In your test binary (Cargo.toml: autumn-harvest = { features = ["testing"] })
let report = WorkflowReplayer::new()
    .register_fn("onboarding", onboarding_handler)
    .replay_from_json(&std::fs::read_to_string("fixtures/onboarding_history.json").unwrap())
    .await
    .expect("fixture must parse");

assert!(
    matches!(report.status, ReplayStatus::ReplaySucceeded),
    "replay regression:\n{report}"
);
```

The replayer never executes activities or writes to the database — it runs the
workflow function in pure replay mode and compares commands against the recorded
history.  A `ReplayReport` with `ReplaySucceeded` means the workflow code can
safely resume all in-flight executions that produced that history.

Key types: `WorkflowReplayer`, `ReplayReport`, `ReplayStatus`, `NonDeterminismKind`,
`HistorySnapshot` (the JSON round-trip format).  See `src/testing.rs` and
`tests/replayer_tests.rs` for examples.

---

## Design Decisions (Phase 2)

**DD-1: Oneshot suspension model**
Coroutine stays in memory; durability comes from the event history. When an activity is scheduled during live execution, the workflow function suspends via a oneshot channel. The executor re-invokes the workflow from the top on each replay cycle, replaying recorded events until it reaches the suspension point.

**DD-2: Separate DB pools with shared ceiling**
Worker pool and web pool are independently sized but share a total connection ceiling (`PoolConfig`). This prevents a burst of worker activity from starving HTTP request handling. `pool.rs` enforces minimum guarantees (at least 1 connection per pool) and distributes remainder to the web pool.

**DD-3: Workflow versioning via ctx.version()**
`WorkflowContext::version()` emits a `VersionMarker` event on first live call and replays the recorded version on subsequent runs. This allows workflow code to branch on version (`if ctx.version() >= 2 { ... }`) to handle non-determinism across deploys without breaking replay of in-flight executions.

**DD-4: Basic in-process LRU cache**
`WorkflowCache` is a bounded LRU cache for workflow state, keyed by `ExecutionId`. It is wired into the worker hot path (Phase 3.11 / issue #235): on a cache hit the worker loads only delta events since the last suspension (`store::load_history_since`) rather than the full history. Sticky cross-worker routing (ensuring follow-up tasks prefer the owning worker) is enabled via `WorkerConfig::with_sticky_routing`. See `docs/sticky-routing.md`.

---

## Phase 4 Scope (next)

- **Worker fleet observability** (implemented, issue #100): `harvest_workers` table, per-worker heartbeat upsert, `Active → Draining → Stopped` lifecycle, `GET /workers`, `GET /workers/{id}`, `GET /workers/health` management routes, cross-shard aggregation via `iter_shards()`.
- **Cancellation semantics** (implemented): explicit workflow/activity cancellation and propagation via `cancel_workflow_execution`, `WorkflowContext::is_cancelled`, `check_cancellation`, cooperative heartbeat cancellation, and grace-period hard-abort. Interaction with `Saga` documented in `docs/saga.md` (issue #238). Parent-close cascade boundary owned by issue #347: when a parent reaches a terminal state, `apply_parent_close_cascade` propagates the configured `ParentClosePolicy` to all running detached children — `RequestCancel` delivers a cancellation (CANCELLED state) and `Terminate` force-fails with a `"ParentClosed"` error (FAILED state); `Abandon` is a no-op. Cascade runs after the parent's terminal transaction commits and is wired into `cancel_workflow_execution`, `persist_workflow_completion`, and `persist_workflow_failure`.
- **Saga primitives** (implemented): `Saga::new`, `Saga::step`, `Saga::compensate_all`, LIFO unwind, `HarvestError::SagaCompensationFailed`. Cancellation + idempotency semantics documented and test-locked in `tests/saga_tests.rs` (issue #238).
- **Cross-worker routing** (implemented, issue #235): sticky execution affinity via `StickyRoutingConfig` + warm-cache delta loading. Shard-aware placement follow-up TBD.
- **Schedule jitter** (implemented, issue #240): `WorkflowSchedule::with_jitter(Duration)` spreads co-scheduled cron/interval fires over a configurable window using a deterministic seahash offset (`compute_jitter_offset`). `DagInfo.jitter` threads through `as_workflow_schedule`. `GET /admin/schedules` surfaces `jitter_secs` and `effective_fire_time`. Zero-jitter default preserves existing behaviour. Migration: `jitter_secs BIGINT NOT NULL DEFAULT 0` on `harvest_schedules`.
- **Schedule overlap policy** (implemented, issue #241): `OverlapPolicy` enum (`Skip`, `BufferOne`, `BufferAll`, `CancelOther`, `TerminateOther`) controls what happens when a new firing collides with a still-running execution. `WorkflowSchedule::with_overlap_policy(OverlapPolicy)` and `with_buffer_all_max(u32)` builder methods configured to match schedulers capability. Effective start-times and execution details surfaced in `GET /admin/schedules`. Migration: `overlap_policy VARCHAR(50) NOT NULL DEFAULT 'skip'`, `buffer_all_max INTEGER NOT NULL DEFAULT 0` on `harvest_schedules`.
- **Calendar-aware schedules and backfills** (implemented, issue #337): `Calendar` definition (durable exclusions, dynamic weekends, default configs); calendar association with schedules; skip/overlap policy interactions; preview generator (`GET /admin/schedules/preview` returning list of effective, original, and skipped fire times); sharded backfill runner (`POST /admin/schedules/{id}/backfill` creating independent executions pinned to shard-local boundaries). Migration: `calendar_name VARCHAR(255) NULL` on `harvest_schedules`.
- **Pre-retention history archival hook** (implemented, issue #345): `HistoryArchiver` trait with custom async `archive` handler, `RetentionConfig.archiver` registered on `HarvestBuilder`, diesel `RetentionMonitor` with `METRIC_RETENTION_DELETED`, row skip on failure with `SkipFreeze` cursor safety, and connection leases in multi-worker environments using background drop lease-releasing guards.
