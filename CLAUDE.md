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
      testing.rs         <- Phase 3.5 (testing feature): WorkflowReplayer harness
      build_routing.rs   <- Phase 3.7: worker build-id routing (issue #171)
      handle_typed.rs    <- Phase 3.14: type-safe client stubs and handles (issue #341)
    migrations/
      20260409000000_harvest_initial/
      20260509000000_harvest_build_routing/
      20260518000001_harvest_workflow_execution_timeout/
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

### Phase Status

- **Phase 1** (complete): types, error, event, policy, context stubs, models, macros, builder
- **Phase 2** (complete): event store, replay engine, workflow context, activity context, task queue (SKIP LOCKED), LISTEN/NOTIFY, worker runtime, heartbeating, timeout enforcement, workflow versioning (ctx.version), LRU workflow cache, dead letter queue, separate worker pool with shared ceiling, testcontainers integration tests
- **Phase 3** (implemented): DAG scheduler/runtime, `DagBuilder`, `#[dag]` macro, trigger rules, signals/queries, management HTTP API, Autumn adapter crate with `HarvestExt` lifecycle integration
- **Phase 3.5** (implemented): Local activities (`#[activity(local = true)]`, `ctx.execute_local_activity_raw`, `WorkflowCommand::RunLocalActivity`, three new `WorkflowEvent` variants, builder cap validation) — see issue #98
- **Phase 3.6** (implemented): Update primitive (`UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed` event variants, `UpdateId` type, `UpdateRegistry`, `WorkflowContext::register_update_handler`, `validate_update`, `execute_admitted_update`, `HistoryMatcher::match_update`, `drain_admitted_updates`) — see issue #140. Declarative `#[query(workflow = "…")]` / `#[update(workflow = "…", validator = …)]` macros with `queries![]/updates![]` bang macros and `HarvestBuilder::queries()/updates()` builder methods now implemented — see issue #346
- **Phase 3.7** (implemented): Worker build-id routing (`BuildId`, `DeploymentName` newtypes; `build_routing.rs` with `BuildCompatibilitySet`, `BuildPolicy`, `BuildReachability`; `harvest_build_policies` + `harvest_build_compat` tables; `required_build_id` on task queue; `assigned_build_id` on executions; `build_id`/`deployment_name` on workers; SKIP LOCKED claim filter; `WorkerConfig::with_build_id`, `with_deployment_name`; build policy wired into `start_or_load_workflow_execution`; cross-shard reachability via `all_build_reachability_sharded`) — see issue #171 and `docs/runbooks/safe-deploy.md` for the operator deploy playbook
- **Phase 3.8** (implemented): Starter production alert pack and runbooks (`docs/alerts/starter-pack-v0.1.0.json`, `docs/alerts/README.md`, `docs/runbooks/harvest-alerts.md`, `docs/runbooks/synthetic-incident-drills.md`) compose ADR-0001/#138 metrics with preflight, worker health, shard health, schedules, DLQ, retention, workflow stack, and build-routing signals. Thresholds are starter defaults, not universal SLOs.
- **Phase 3.9** (implemented): Unified DAG execution (`unified-dag-execution` feature, on by default) — see issue #256. `#[dag]` lowers graph definitions onto the standard workflow execution path: the macro emits a `WorkflowHandlerFn` that walks `DagDefinition` level by level and dispatches activities through `ctx.execute_activity_raw`. `HarvestBuilder::dags()` auto-registers `WorkflowInfo` (and `WorkflowSchedule` when a schedule attribute is present) for each unified DAG. `POST /dags/{name}/trigger` routes through `trigger_unified_dag` → `start_or_load_workflow_execution` when the dag is in `registry.workflows`. `compile_dag_catalog` skips unified DAGs so the classic DAG executor never claims them. Classic DAGs (explicit `workflow_handler: None`) still work unchanged. `harvest_dag_runs` remains write-only for bridge observability during this transition.
- **Phase 3.10** (implemented): Read-only Query handlers (`query.rs`, `QueryRegistry`, `WorkflowContext::register_query_handler<Req,Resp>`, `execute_query_with_args`, `list_query_names`; `WorkerConfig::query_timeout` default 5 s; `telemetry::METRIC_QUERY_DURATION`; `#[query]` macro; management routes `POST /workflows/{id}/query/{name}` and `GET /workflows/{id}/queries`) — see issue #234 and `examples/progress_query.rs`.
- **Phase 3.11** (implemented): Sticky cross-worker routing and warm-cache delta loading (`StickyRoutingConfig`, `WorkerConfig::with_sticky_routing`; `WorkflowCache` wired into worker hot path; `store::load_history_since` for delta event queries; `METRIC_WORKFLOW_CACHE_HIT` / `METRIC_WORKFLOW_CACHE_MISS` constants + `MetricsRecorder` methods; `CachedWorkflowState` redesigned with `events: Vec<WorkflowEvent>` + `next_event_id: i32` for actual delta-load support; sticky routing off by default) — see issue #235 and `docs/sticky-routing.md` for the operator guide
- **Phase 3.12** (implemented): Workflow execution timeouts for SLA enforcement and runaway protection — see issue #243. `WorkflowExecutionTimedOut` event variant (append-only, `deadline` + `timed_out_at` fields); `TimeoutType::WorkflowExecution`; `#[workflow(execution_timeout = "30m")]` attribute parsed by the macro and stored as `WorkflowInfo::execution_timeout: Option<Duration>`; `max_workflow_execution_timeout` ceiling on `HarvestBuilder` (server-side cap applied at workflow start); `deadline_at TIMESTAMPTZ NULL` column on `harvest_workflow_executions` with a partial index on `(deadline_at) WHERE state = 'RUNNING' AND deadline_at IS NOT NULL`; `timeout::enforce_workflow_execution_timeouts` Diesel DSL scanner that appends `WorkflowExecutionTimedOut`, transitions the execution to `TIMED_OUT`, cancels outstanding task queue rows, and notifies parent child-workflows; `METRIC_WORKFLOW_TIMEOUT` counter (`harvest.workflow.timeout{workflow, queue}`) emitted on each enforcement; `WorkflowSchedule::execution_timeout` field + `with_execution_timeout` builder method for schedule-level default deadlines. Migration: `20260518000001_harvest_workflow_execution_timeout`.
- **Phase 3.13** (implemented): Per-key concurrency limits for tenant fair-share scheduling (`ConcurrencyPolicy` newtype; `concurrency.rs` with `resolve_concurrency_key`; `WorkflowInfo.concurrency` field; `#[workflow(concurrency(key = "input.tenant_id", limit = 10))]` macro attribute; `StartWorkflowParams.concurrency_key` + `.concurrency_limit`; plugin API resolves key at workflow start; continue-as-new propagates key; `GET /admin/concurrency` management endpoint; `harvest.concurrency.in_flight`/`harvest.concurrency.deferred` metrics; sharding interaction documented in `docs/sharding.md`) — see issue #247. No new migration: `concurrency_key`/`concurrency_cap` columns and the claim-time advisory-lock enforcement were already present from the concurrency_key migration.
- **Phase 3.14** (implemented): Type-safe workflow client stubs and handles — see issue #341. Generates PascalCase stubs (e.g. `SubscriptionFlowStub` for `fn subscription_flow(...)`) exposing typed `start`, `start_with_options`, and `signal_with_start`. Stub methods return `TypedWorkflowHandle<T>` wrapping the untyped `WorkflowHandle`, with `.result().await -> HarvestResult<T>` and `.result_snapshot().await -> HarvestResult<TypedWorkflowResult<T>>` to safely deserialize outputs. Sibling macros `#[query]`, `#[update]`, and `#[signal]` automatically generate sibling methods (`query_[query_name]`, `update_[update_name]`, `signal_[signal_name]`) on the same stub type.
- **Phase 3.15** (implemented): HA-safe scheduler ticks under multi-replica deployments — see issue #350. `tick_workflow_schedules` now atomically claims each due `harvest_schedules` row before firing it using `UPDATE ... SET fire_claim_token = gen_random_uuid(), fire_claimed_until = NOW() + INTERVAL '30 seconds' WHERE (fire_claim_token IS NULL OR fire_claimed_until < NOW())`. Only one replica holds the claim per slot at a time. Crash-recovery window: if the claiming replica crashes before advancing `next_run_at`, the claim expires after 30 s and a healthy peer retries. The contract does not depend on `WorkflowIdReusePolicy`. New metric `harvest.schedule.fire_attempts{outcome="claimed|lost_race"}` emitted by `tick_workflow_schedules` so operators can verify exclusivity and detect HA misconfiguration. New `record_schedule_fire_attempt` method on `MetricsRecorder`. Migration `20260530000000_harvest_schedule_ha_claim` adds nullable `fire_claim_token UUID` + `fire_claimed_until TIMESTAMPTZ` columns with a partial expiry index. Runbook at `docs/runbooks/ha-deployment.md`. Alert `harvest_schedule_ha_domination` in `docs/alerts/starter-pack-v0.1.0.json`. Integration tests in `tests/scheduler_ha_tests.rs` verify the concurrent-tick guarantee, crash-recovery path, and metric emission.
- **Phase 3.16** (implemented): DAG retry-from-failed-node operator surface — see issue #366. New management route `POST /api/harvest/dags/{dag_name}/runs/{run_exec_id}/retry` (handler `retry_dag_run` in `api.rs`) and pure resolver `autumn-harvest-plugin/src/dag_retry.rs` (`resolve_retry_plan`, `downstream_closure`, `node_outcome`). The endpoint resolves `(dag_name, run_exec_id, from_nodes)` → `reset_to_event_id = earliest_reexecute_schedule - 1` by walking the registered `DagDefinition` (node name == activity name) and the recorded history, then delegates to the existing #148 reset internals. **No new core primitive, no new `WorkflowEvent` variant, no migration.** The only core change is an opt-in `WorkflowResetRequest.allow_terminal_source` flag (`#[serde(default)]` false) so a terminal *failed* DAG run can be forked; `validate_source_execution` accepts `FAILED`/`CANCELLED`/`TIMED_OUT` only when set. Reset `reason` is augmented with `dag_retry: nodes=[...]` for the audit trail (#158); audit op `OP_DAG_RETRY = "dag.retry"`. Semantics are level-granular (operator choice): retrying any node auto-widens to its full execution level + downstream closure, so the cut lands on the clean boundary before the level and the failed node's same-level siblings re-run with it (no "name the succeeded sibling to widen" dead-end). A `409` is returned only when the fork point lands inside an unresolved *upstream* side effect (the #148 validator rejects it). Ambiguous requests (a node name that maps to >1 task because the DAG reuses the activity) are rejected `400`. `WorkflowResetRequest.allow_terminal_source` is `#[serde(skip)]` so it cannot be enabled from the public reset endpoint body. Source-state gating: `COMPLETED` → `409`, `RUNNING`/`SUSPENDED` → `409`, classic DAGs → `400`. CLI `dag retry` subcommand. Runbook `docs/runbooks/dag-retry-from-failed-node.md`. Tests: resolver unit tests in `dag_retry.rs`, HTTP+worker integration tests in `autumn-harvest-plugin/tests/dag_retry_integration.rs`, CLI mapping/coverage tests.
- **Phase 3.17** (implemented): Poison-pill task quarantine — see issue #367. A poison-pill task crashes the worker *process* (panic, OOM, segfault, hard exit) rather than returning a clean `Err`, leaving its row stuck in `RUNNING`; SKIP LOCKED re-claim then cascades the crash across the fleet. New `poison_pill.rs` module: pure `quarantine_decision(strikes_after_increment, threshold) -> ReclaimAction` (no DB dependency, unit-tested without `db`); `orphaned_running_tasks_query()` selects `RUNNING` rows whose `worker_id` has no live `harvest_workers` heartbeat (authoritative liveness signal — reclaim does **not** depend on per-task `start_to_close`/`heartbeat_timeout`, so an un-timed orphan is recovered rather than stuck forever); `reclaim_orphaned_tasks` increments `crash_strikes` and either re-queues (under threshold) or quarantines to the DLQ (at/over threshold); `spawn_poison_pill_reclaimer` runs the sweep on the worker's `poll_interval`, wired into `WorkerMonitoringHandles` alongside the timeout checker. Quarantine moves the task to `harvest_dead_letters` with a typed `DeadLetterReason::PoisonPill { crash_strikes, last_worker_id }` (the reason discriminator distinguishes it from clean retry exhaustion), marks the queue row `FAILED`, and fails the owning workflow terminally via the existing `WorkflowFailed` event path (**no new `WorkflowEvent` variant**), waking any blocked parent. `WorkerConfig::poison_pill_threshold` (default **3**; `with_poison_pill_threshold`; `0` disables quarantine = legacy requeue-forever loop). New metric `harvest.task.quarantined{queue, reason}` (`METRIC_TASK_QUARANTINED`, `record_task_quarantined` on `MetricsRecorder`, bridged in `metrics_rs_adapter`). Migration `20260601000001_harvest_poison_pill_strikes` adds `crash_strikes INT NOT NULL DEFAULT 0` to `harvest_task_queue` plus a partial index on `(worker_id) WHERE state = 'RUNNING' AND worker_id IS NOT NULL`. Shard-local: detection and quarantine run against the connection's own database. Integration tests in `tests/poison_pill_tests.rs`.
- **Phase 3.18** (implemented): Per-activity circuit breaker for fast-fail dispatch during downstream outages — see issue #369. Opt-in `CircuitBreakerPolicy { failure_threshold, window, cooldown }` (in `policy.rs`) attached to `ActivityInfo.circuit_breaker` via the `#[activity(circuit_breaker = ...)]` attribute. New `circuit_breaker.rs` module: pure `CircuitBreakerRegistry` (closed/open/half-open state machine, rolling-window failure counting, single half-open probe, `force_open`/`force_close`, `snapshot`/`list`) — unit-tested without `db`. `on_result` takes an `AttemptOutcome` (`Success`/`RetryableFailure`/`NonRetryableFailure`): only `RetryableFailure` trips the breaker (classification mirrors the retry decision via the shared `failure_is_non_retryable` helper, honouring both the typed `non_retryable` flag and the retry policy's `non_retryable_errors`, incl. legacy `Err(String)`), so a burst of permanent per-request errors can't open the circuit. It also takes a `DispatchToken` carrying a monotonic **generation** (bumped on every state-resetting transition — trip/close/force-open/force-close); `on_result` fences any result whose token predates the current generation, which subsumes both the half-open straggler case and the "pre-force-close failure re-trips the operator's reset" case. Cancellation-driven results (workflow/task cancelled mid-flight) are excluded from breaker accounting entirely. Activity **timeouts** (start-to-close/heartbeat) feed the breaker out-of-band via `on_external_failure` wired into `timeout::enforce_activity_timeout` (token-less, since the dispatching worker may be gone), so a hanging downstream trips the breaker too. Circuit breakers are rejected on local activities (macro compile error + defensive registry filter) since local activities bypass the dispatch path. The worker consults the breaker in `process_activity_task` before running the handler: when open it short-circuits with a non-retryable `ActivityFailure::circuit_open` (error type `"CircuitOpen"`, `ERROR_TYPE_CIRCUIT_OPEN` in `failure.rs`) recorded as an ordinary `ActivityFailed` event — **no new `WorkflowEvent` variant, no migration** — so the append-only contract and deterministic replay are unchanged. `DispatchDecision::ShortCircuit.retry_after` is `Option<Duration>` (`None` = operator-forced open / in-flight probe, so callers don't busy-loop on a stale hint). The typed failure is **consumable from workflow code**: `error_type`/`details` are threaded through `HistoryMatch::Failed` and `HarvestError::ActivityFailed` (which now carries `error_type` + `details`), with accessors `HarvestError::activity_error_type()`/`activity_details()`/`is_circuit_open()` and the `HarvestError::activity_failed(name, attempt, payload)` decoding constructor — deterministic on replay. **Rate-limit interaction:** for an activity with both `rate_limit_*` and `circuit_breaker`, rate limiting is enforced at **dispatch** rather than at claim. `queue::claim_task` skips the rate-limit gate **and** token debit for every activity with a breaker (the static set `CircuitBreakerRegistry::tracked_activity_names()` passed into the claim query), so a `CircuitOpen` short-circuit is always claimable and propagates at full speed during an outage without burning tokens. A genuine call — admitted by the authoritative `on_dispatch` in `process_activity_task` — atomically reserves one token via `queue::try_consume_rate_limit_token`; if the bucket is empty the task is rescheduled (one refill interval ahead, clamped) instead of running, so a real call can never run below zero tokens. Enforcing at dispatch (gated on the real breaker decision) avoids the claim-vs-dispatch staleness window since the breaker state is in-process and can change between the two. Plain rate-limited activities without a breaker are unchanged (gate + debit at claim). State is in-process and per-shard, shared (`HandlerRegistry::circuit_breakers()`) between the worker and the management API. Management routes `GET /admin/circuits`, `GET /admin/circuits/{activity_name}`, `POST /admin/circuits/{activity_name}/force-{open,close}` (audit ops `OP_CIRCUIT_FORCE_OPEN`/`OP_CIRCUIT_FORCE_CLOSE`). New metrics `harvest.activity.circuit.tripped` / `harvest.activity.circuit.closed` (`activity.name` label; `record_circuit_tripped`/`record_circuit_closed` on `MetricsRecorder`, bridged in `metrics_rs_adapter`). Runbook `docs/runbooks/activity-circuit-breaker.md` with the breaker-vs-retry/jitter/rate-limit decision matrix. Tests: `circuit_breaker.rs` unit tests, `tests/circuit_breaker_wiring_tests.rs`, `context::tests::context_replays_circuit_open_failure_with_typed_metadata`, and the `circuit_breaker_short_circuits_after_tripping` e2e in `tests/integration_e2e.rs`.
- **Phase 3.19** (implemented): Published workflow input/output JSON Schema for self-service triggering — see issue #373. `WorkflowInfo` gains four new optional fields: `description: Option<&'static str>`, `input_schema: Option<fn() -> serde_json::Value>`, `output_schema: Option<fn() -> serde_json::Value>`, `error_schema: Option<fn() -> serde_json::Value>`. Three fluent builder methods: `with_description`, `with_input_schema_fn`, `with_output_schema_fn`, `with_error_schema_fn` (all `#[must_use]`). Under the new `schema` Cargo feature, `with_schemas::<I, O, E>()` derives all three schemas automatically from types that implement `schemars::JsonSchema`. Two new management API routes: `GET /workflows/registered` (sorted list of all registered workflow types with optional schemas) and `GET /workflows/registered/{name}/schema` (404 for unknown names). `POST /workflows/{name}/start` validates input against the published schema when one is set; on failure returns `400` with a structured JSON body `{"error": "input validation failed", "violations": [{"message": "…", "field_path": "…"}]}` where `field_path` is a JSON Pointer (RFC 6901). `WorkflowInfo::validate_input` is the pure validation method — returns `Ok(())` or `Err(Vec<SchemaViolation>)`. `validate_against_schema` is the standalone recursive validator. `#[workflow(description = "…")]` attribute wires description through the companion function. Opt-in: workflows without a schema compile and run identically to today — no breakage. **No new `WorkflowEvent` variants, no migrations, no shard-routing changes, no replay-determinism impact.** `RegisteredWorkflowRecord` is the serialisable discovery record. Example: `autumn-harvest/examples/schema_workflow.rs` (requires `--features schema`). New types in `info.rs`: `SchemaViolation`, `RegisteredWorkflowRecord`, `validate_against_schema`.
- **Phase 4** (next): production hardening -- sharding, observability, metrics, dashboard (Vantage UI — Workers tab shipped in issue #142; DLQ, schedules, and DAG visualization pages remain); Step 5 of issue #256 (remove classic DAG executor, drop `harvest_dag_runs`). Note: the cancellation primitive and `Saga` both ship; their interaction semantics, idempotency contract, and replay-determinism contract are documented in `docs/saga.md` and locked in by three integration tests in `tests/saga_tests.rs` (issue #238).

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

`/api/harvest/health` is liveness-style by default. To make it fail with `503` when writable shard readiness is not `ready`, configure `[harvest.readiness] require_shard_readiness = true` or set `AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS=true`.

Current implementation scope: `ExecutionId`/`ShardId` encoding, `ShardRouter`, `ShardedDbPool`, shard-aware start/read paths in the plugin, `WorkerConfig.shard_assignments`, and shard health readiness for worker/scheduler rollout coverage. Per-shard worker poll loops and per-shard scheduler tick loops with DAG→shard pinning remain as follow-up work.

---

## Module Guide

| Module | Phase | Purpose |
|--------|-------|---------|
| `types.rs` | 1 | Newtypes: `WorkflowId` (String), `ExecutionId` (Uuid v4), `ActivityExecId` (Uuid v4), `TimerId` (String), `WorkerId` (String) |
| `error.rs` | 1 | `HarvestError` (thiserror), `HarvestResult<T>`, `TimeoutType` enum |
| `policy.rs` | 1 | `RetryPolicy`, `TriggerRule`, `Schedule`, `TaskStatus`, `compute_retry_delay` |
| `event.rs` | 1 | `WorkflowEvent` enum (28 variants, adjacently-tagged serde), `type_name()`. Variants added in issue #140: `UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed` |
| `context.rs` | 1+2 | `WorkflowContext` (replay, suspension, version gate, timers), `ActivityContext` (heartbeat channel, cancellation) |
| `info.rs` | 1 | `WorkflowInfo`, `ActivityInfo`, `WorkflowHandlerFn`, `ActivityHandlerFn` type aliases |
| `builder.rs` | 1 | `HarvestBuilder` (fluent), `WorkerConfig` (queues, concurrency, timeouts) |
| `prelude.rs` | 1 | Core glob re-export surface including macros |
| `schema.rs` | 1 | Diesel `table!` macros -- 11 tables (includes `harvest_build_policies`, `harvest_build_compat`) |
| `models.rs` | 1 | `Queryable`/`Selectable` read structs and `Insertable` `New*` write structs for all 11 tables |
| `store.rs` | 2 | Event store and read helpers: `append_events`, `load_history`, `load_history_since` (delta load for cache-hit path, issue #235), `events_to_rows` with sequential event IDs, `load_workflow_children` for parent -> child operator queries |
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
| `update.rs` | 3.6 | Update primitive: `UpdateRegistry` (type-erased validators + async handlers), `BoxUpdateHandler`, `BoxUpdateValidator`. `WorkflowContext` methods: `register_update_handler`, `register_update_handler_no_validator`, `validate_update`, `execute_admitted_update`. `HistoryMatcher` methods: `match_update(update_id)`, `drain_admitted_updates()`. Error variants: `HarvestError::UpdateRejected`, `HarvestError::UpdateHandlerNotFound` |
| `query.rs` | 3.10 | Query registry: `QueryRegistry`, `QueryHandler`. `WorkflowContext` methods: `register_query` (no-arg), `register_query_handler<Req,Resp>` (typed), `execute_query_with_args`, `list_query_names`. Error variants: `QueryHandlerNotFound`, `WorkflowNotRunning`, `QueryHandlerPanicked`, `QueryTimedOut`. `WorkerConfig::query_timeout` (default 5 s). `telemetry::METRIC_QUERY_DURATION` constant. No `WorkflowEvent` variants — queries leave zero footprint in `harvest_events`. |
| `build_routing.rs` | 3.7 | Worker build-id routing: `BuildCompatibilitySet` (in-memory eligibility checker), `BuildPolicy`, `BuildCompatEntry`, `BuildReachability`. DB functions: `set_build_policy`, `get_build_policy`, `list_build_policies`, `declare_compat`, `revoke_compat`, `load_compat_set`, `build_reachability`, `all_build_reachability`, `all_build_reachability_sharded` (cross-shard fan-out), `merge_reachability`. New newtypes in `types.rs`: `BuildId`, `DeploymentName`. See `docs/runbooks/safe-deploy.md` for the operator deploy playbook. |
| `telemetry.rs` | 4 | OpenTelemetry surface: `TraceContextCarrier`, `TraceContextPropagator`, `MetricsRecorder`, `TelemetryConfig` — no-op by default, opt-in via `HarvestBuilder::telemetry`. Implements all 8 ADR-0001 span kinds (issue #136); see `docs/adr/0001-otel-trace-contract.md` for the full attribute schema and propagation rules. Metric catalogue (ADR-0001 §7): `harvest.workflow.started` (counter, `worker.rs`), `harvest.workflow.duration` (histogram, `worker.rs`), `harvest.activity.duration` (histogram, `worker.rs`), `harvest.timer.started` (counter, `worker.rs`), `harvest.queue.depth` (gauge, `worker.rs` sampler), `harvest.dlq.entries` (gauge, `worker.rs` sampler), `harvest.schedule.runs` (counter, `scheduler.rs`), `harvest.schedule.skipped` (counter, `scheduler.rs`), `harvest.retention.deleted` (counter, `retention.rs`), `harvest.workflow.cache_hit` (counter, `worker.rs`, issue #235), `harvest.workflow.cache_miss` (counter, `worker.rs`, issue #235), `harvest.workflow.timeout` (counter, `timeout.rs`, issue #243), `harvest.schedule.fire_attempts` (counter, `scheduler.rs`, issue #350, labels: `schedule`, `outcome`), `harvest.task.quarantined` (counter, `poison_pill.rs`, issue #367, labels: `queue`, `reason`), `harvest.activity.circuit.tripped` (counter, `worker.rs`, issue #369, label: `activity.name`), `harvest.activity.circuit.closed` (counter, `circuit_breaker.rs`, issue #369, label: `activity.name`). Cardinality rule: `execution.id` is span-only; `MetricsRecorder` API enforces this by construction. |
| `concurrency.rs` | 3.13 | Per-key concurrency limits (issue #247): `ConcurrencyPolicy { key_expr, limit }` attached to `WorkflowInfo`; `resolve_concurrency_key(expr, input)` resolves a dot-notation field path against the JSON input at workflow-start time. Limits enforced within a shard via the existing `concurrency_key`/`concurrency_cap` claim-query path. See `docs/sharding.md` for the cross-shard scope contract. |
| `metrics_rs_adapter.rs` | 4 | `metrics-rs` feature flag adapter: `MetricsRsRecorder` bridges `MetricsRecorder` → `metrics` crate global registry. See `docs/telemetry.md` for recipe. |
| `poison_pill.rs` | 3.17 | Poison-pill task quarantine (issue #367): pure `quarantine_decision`/`ReclaimAction` (no DB dep), `orphaned_running_tasks_query` (worker-liveness reclaim, independent of per-task timeouts), `reclaim_orphaned_tasks` (increment `crash_strikes`, requeue-or-quarantine), `spawn_poison_pill_reclaimer`. Quarantine → `harvest_dead_letters` with `DeadLetterReason::PoisonPill` + terminal `WorkflowFailed` (no new event variant). `WorkerConfig::poison_pill_threshold` (default 3, 0 disables). Shard-local. |
| `circuit_breaker.rs` | 3.18 | Per-activity circuit breaker (issue #369): `CircuitBreakerRegistry` (closed/open/half-open, rolling-window failure count, single half-open probe, `on_dispatch`/`on_result`, `force_open`/`force_close`, `snapshot`/`list`), `CircuitPhase`, `DispatchDecision`, `CircuitTransition`, `CircuitSnapshot`. Pure/in-process, per-shard; consulted by the worker before dispatch and shared with the management API via `HandlerRegistry::circuit_breakers()`. No new event variant, no migration. |
| `migrations/` | 1 | SQL -- run with `diesel migration run` |

### Macro Modules (`autumn-harvest-macros`)

| File | Purpose |
|------|---------|
| `lib.rs` | Entry points: `#[workflow]`, `#[activity]`, `#[query]`, `workflows![]`, `activities![]` |
| `workflow.rs` | `workflow_macro` — emits user fn + companion `WorkflowInfo` fn |
| `activity.rs` | `activity_macro` — parses `retry`, `start_to_close`, `heartbeat_timeout`, `schedule_to_start`, `queue` attrs; emits user fn + companion `ActivityInfo` fn |
| `collect.rs` | `workflows_macro` / `activities_macro` — expand to `vec![companion_calls...]` |
| `query.rs` | `query_macro` — pass-through attribute that validates the annotated item is a function; used for documentation and future typed query discovery |

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

HTTP route:
- `POST /api/harvest/workflows/{workflow_name}/signal-with-start` with body
  `{ workflow_id, start_input, signal_name, signal_payload, id_reuse_policy?, idempotency_key?, queue?, memo?, search_attrs?, execution_timeout_secs? }`
  → `201 Created` (fresh start) or `200 OK` (attached) with response
  `{ execution_id, workflow_name, workflow_id, state, started_fresh, signal_delivered }`.
- `409 Conflict` when `id_reuse_policy = reject_duplicate` rejects an
  existing execution.

See `examples/signal_with_start_webhook.rs` for a worked Stripe webhook
example.

### Typed Dispatch

Use the companion functions generated by `#[workflow]` and `#[activity]` instead of raw string names when dispatching from within a workflow. The companion name is `{fn_name}_info()` (the public alias for the hidden `__autumn_{workflow|activity}_info_{name}()` function).

| Raw (string-based)                                            | Typed (info-based)                                           |
|---------------------------------------------------------------|--------------------------------------------------------------|
| `ctx.execute_activity_raw("send_email", json!(...), "q")`    | `ctx.execute_activity(&send_email_info(), input).await?`     |
| `ctx.execute_activity_raw_with_opts("send_email", ..., "q")` | `ctx.execute_activity_with_opts(&send_email_info(), input, queue_override, retry, timeout).await?` |
| `ctx.execute_local_activity_raw("checksum", ...)`            | `ctx.execute_local_activity(&checksum_info(), input).await?` |
| `ctx.spawn_child_workflow_raw("child", json!(...))`           | `ctx.spawn_child_workflow(&child_info(), input).await?`      |
| `ctx.wait_for_signal("approved")`                             | `ctx.receive_signal::<Approval>("approved").await?`          |

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

Errors: `QueryHandlerNotFound` (404), `WorkflowNotRunning` (409), `QueryHandlerPanicked` (503), `QueryTimedOut` (408).

Configure the per-query timeout via `WorkerConfig::default().with_query_timeout(Duration::from_secs(10))` (default 5 s). Queries are replay-safe: they never emit `WorkflowCommand`s and leave zero footprint in `harvest_events`.

### Fan-out / Parallel Activities

`WorkflowContext` exposes first-class fan-out for dispatching N activities in parallel and collecting results in input order.  Two semantics are available:

| Method | Semantics |
|--------|-----------|
| `execute_activity_fan_out(info, inputs)` | Fail-fast: returns `Ok(Vec<O>)` or the **first** `Err` |
| `execute_activity_fan_out_collect(info, inputs)` | Collect-all: returns `Ok(Vec<Result<O, String>>)` — per-slot errors |
| `execute_activity_fan_out_raw(activities)` | Raw fail-fast: `Vec<(String, Value, String)>` input |
| `execute_activity_fan_out_collect_raw(activities)` | Raw collect-all |

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
```

**Determinism rule — the input collection MUST be derived from already-recorded state** (workflow input, prior activity outputs, signals).  Never derive the collection from non-deterministic sources such as the system clock, `rand`, or an in-process counter.  If the collection is derived from a prior activity output, that output is in history and is therefore deterministic.

**Replay mechanics**: a `MarkerRecorded { name: "fan_out:{n}" }` event is appended before the activity events on the first live run.  On replay the recorded count is compared to the current collection length; if they differ, `HarvestError::NonDeterministic` is returned immediately rather than silently corrupting results.

**Cancellation**: both methods check `ctx.is_cancelled()` before dispatching and return `HarvestError::Cancelled` if the workflow has been cancelled.

See `autumn-harvest/examples/fanout_batch.rs` for a complete end-to-end example covering all three shapes (static N, dynamic N from a prior activity, and collect-all with partial failure).

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
