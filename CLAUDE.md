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
- **Phase 3.22** (implemented): DLQ root-cause aggregation API for fast incident triage — see issue #385. New read-only management route `GET /api/harvest/dead-letters/aggregate` (admin auth, parity with the DLQ list endpoint, placed under the existing `/dead-letters` route family; handler `aggregate_dead_letters` in `api.rs`, fanning out across shards with `iter_shards()`). Groups dead-letter rows along a named set of dimensions and returns per-group counts plus representative `dead_letter_id`s. Repeatable `group_by=` supports `workflow_name`, `activity_name`, `queue_name`, `task_type`, `time_bucket` (companion `time_bucket=hour|day`), and `failure_signature`; repeats build a hierarchical key. Filters (`workflow_name`/`activity_name`/`queue_name`/`since`/`until`/`min_attempts`) mirror the list endpoint and apply before grouping; `since`/`until` accept RFC 3339 or relative durations (`24h`). `limit_groups` (default 50, max 500) rolls the long tail into a single `{"_other": true}` group so counts reconcile to `filtered_total`; `samples_per_group` (default 3, max 10) caps sample IDs. **`failure_signature` is the compute-on-read normalized-substring option (zero schema change)**: `dlq::failure_signature` takes the first line of `error` and normalizes UUIDs/hex/decimal runs to `<UUID>`/`<HEX>`/`<NUM>` placeholders, truncated to 200 chars — a pure, deterministic, shard-stable function. Invalid params return `400` with a JSON error body (never `500`, never a silent empty match). New core types/functions in `autumn-harvest/src/dlq.rs`: `failure_signature`, `DlqGroupDimension`, `TimeBucketGranularity`, `DlqAggregateParams` (`from_query_pairs`), `DlqRawGroup`, `DlqAggregatePartial`, `DlqGroup`, `DlqAggregateResponse`, `aggregate_dead_letters` (per-shard, `db`-gated), `merge_dlq_aggregates` (pure cross-shard merge). CLI: `harvest dlq aggregate --group-by … [--json]` (table by default). Runbook: "DLQ flood — first 60 seconds" section in `docs/runbooks/harvest-alerts.md`. Contract: `GET /dead-letters/aggregate` registered in `management_api_routes()`/`management_api_response_fields()` and `docs/api-contract.json`. **No new `WorkflowEvent` variant, no migration.** Pure unit tests in `dlq.rs`; HTTP+shard integration tests in `autumn-harvest-plugin/tests/dlq_aggregate_integration.rs`; CLI mapping/render tests in `lib.rs`. Vantage UI: the DLQ inspection page (#226) gains a **Summary toggle** (`?view=summary`) that runs the same per-shard aggregation in-process, with a `group_by` selector (default `workflow_name,failure_signature`), cross-shard merged counts/samples, and click-through into the filtered list view (`render_dead_letters_summary_view` in `ui.rs`); covered by `ui_integration.rs` tests.
- **Phase 3.21** (implemented): Workflow terminal-outcome counter for success-rate SLOs and alerting — see issue #519. New counter `harvest.workflow.terminal` (`METRIC_WORKFLOW_TERMINAL`) incremented **exactly once** per terminal workflow outcome. Labels: `outcome` (6 bounded values: `completed`/`failed`/`cancelled`/`timed_out`/`terminated`/`continued_as_new`), `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue`. `execution.id` is never a label (ADR-0001 §7 cardinality rule). Emission points: `worker.rs` (`process_workflow_task` for Completed/Failed/ContinuedAsNew and `fail_workflow_for_history_cap`), `timeout.rs` (`enforce_workflow_execution_timeouts` for TimedOut), `execution.rs` (`cancel_workflow_execution` for Cancelled, `terminate_workflow_execution` for Terminated). `WorkflowStatus` enum extended with `Cancelled`, `TimedOut`, `Terminated` variants. `record_workflow_terminal` added to `MetricsRecorder` trait as a no-op default (additive, no breaking change). Bridged in `metrics_rs_adapter` (`MetricsRsRecorder`). `BatchExecutorConfig` gains `metrics: Arc<dyn MetricsRecorder>` field (default `NoOpMetrics`). Suspended executor cycles never increment the counter. Workflow-failure-rate alert added to `docs/alerts/starter-pack-v0.1.0.json`. ADR-0001 §7 metric catalogue updated. **No new `WorkflowEvent` variant, no migration.**
- **Phase 3.20** (implemented): Per-activity cross-retry wall-clock deadline (`schedule_to_close`) for SLA enforcement — see issue #378. `ActivityInfo.default_schedule_to_close: Option<Duration>` (`None` = unbounded, no regression for existing activities). `#[activity(schedule_to_close = "5m", start_to_close = "30s", retry = ...)]` parses cleanly (compile-time error if used on `local = true` activities). Migration `20260606000001_harvest_activity_schedule_to_close` adds `schedule_to_close_at TIMESTAMPTZ NULL` to `harvest_task_queue` with a partial index on `(schedule_to_close_at) WHERE state IN ('RUNNING', 'PENDING') AND schedule_to_close_at IS NOT NULL`. Worker sets `EnqueueParams::schedule_to_close_at = Some(Utc::now() + schedule_to_close)` at schedule time. Retry path: before calling `requeue_for_retry`, `schedule_to_close_deadline_exceeded(task, delay)` checks if `now + retry_delay >= deadline`; if so, `record_schedule_to_close_activity_timeout` appends `ActivityTimedOut { timeout_type: ScheduleToClose }` and fails the task instead of requeuing. Scanner: `TimeoutReason::ScheduleToClose` added to `find_timed_out_tasks`; `expected_task_states_for_timeout` returns `&["RUNNING", "PENDING"]` for this reason so both in-flight and queued-past-deadline tasks are caught. `TimeoutType::ScheduleToClose` already existed in `error.rs` — no new `WorkflowEvent` variant needed. Decision matrix documented in `docs/getting-started/07-reliability-knobs.md`. Integration tests in `tests/schedule_to_close_tests.rs`.
- **Phase 3.23** (implemented): Operator pause/resume primitive for individual workflow executions — see issue #383. Two new append-only `WorkflowEvent` variants (`WorkflowExecutionPaused { paused_at, reason, actor }`, `WorkflowExecutionResumed { resumed_at, actor }`) added at the end of the enum; both are **non-terminal** and **transparent to replay** — `HistoryMatcher::new` pre-marks their indices consumed so every scan loop skips them, leaving the reconstructed command sequence unchanged (`tests/pause_tests.rs` verifies pause→timer→resume→fire replays deterministically). Pause is enforced at the **executor/claim layer**, not the workflow handler: `queue::claim_task` skips workflow tasks whose execution is `PAUSED`, so a parked task woken by a timer fire, signal arrival, or activity completion is deferred (stays PENDING) until resume — no workflow-author cooperation required, in-flight activities still run to completion. For the claimed-then-paused race, `worker::process_workflow_task` enforces pause **authoritatively at persist time**: it opens the persistence transaction with a `FOR UPDATE` row lock on the execution (mirroring `pause_workflow_execution`'s own lock, so the two serialize) and re-checks `PAUSED` *under that lock*. If the pause committed first, the pending decision is discarded (no events appended, no tasks enqueued) and the task is re-parked inside the same transaction so resume re-derives the same commands deterministically on replay; otherwise the pause blocks until the in-flight decision commits ("already-dispatched work runs to completion"). A non-locking re-check earlier in `process_workflow_task` remains as a fast-path optimization (bail before computing metrics/history-cap/cache) but is no longer the guarantee. Schedule-failure counters are deferred to after that transaction commits so a best-effort counter query can never roll back the persisted decision. `PAUSED` is a **non-terminal active state** everywhere active runs are enumerated: it is in `KNOWN_WORKFLOW_STATES` (so `GET /workflows?state=PAUSED` and batch filters work), counted in scheduler `max_active_runs` guards and selected by `CancelOther`/`TerminateOther`, and included in the default batch `Cancel`/`Signal` target set. `execution::pause_workflow_execution` (RUNNING→PAUSED, idempotent, 404/409) and `resume_workflow_execution` (PAUSED→RUNNING, wakes the parked task) plus the pure `pause_timeout_exceeded` helper and `auto_resume_expired_pauses` scanner (`WorkerConfig::max_workflow_pause_duration`, default **24h**; force-resumes with `actor = "auto-resume(timeout)"` via `spawn_pause_auto_resumer`). Cancellation beats pause: `cancel_workflow_execution` accepts `PAUSED` and clears the pending pause record. **Pause suspends the SLA clock** (interaction with #243): `enforce_workflow_execution_timeouts` scans `state = 'RUNNING'` only, so a paused execution never times out mid-pause, and `resume_workflow_execution` pushes `deadline_at` forward by the (clamped, non-negative) pause duration so paused wall-clock is not charged against the workflow's `execution_timeout`. Overlap/`max_active_runs` counters treat `PAUSED` as active everywhere — including the backfill (`query_running_count`) and Vantage manual-trigger counters, which count `state IN ('RUNNING','PAUSED')` to match the scheduler. Updates submitted while paused are rejected with the new `HarvestError::WorkflowPaused` (409) in `store::admit_update_event`; queries still serve. Management routes `POST /api/harvest/workflows/{id}/pause` and `/resume` (audit ops `OP_WORKFLOW_PAUSE`/`OP_WORKFLOW_RESUME`); Vantage UI Pause/Resume buttons (disabled when terminal). Metrics `harvest.workflow.paused` (counter) + `harvest.workflow.pause_duration` (histogram), ADR-0001 §7 catalogue updated. Migration `20260607000002_harvest_workflow_pause` adds nullable `paused_at`/`pause_reason`/`pause_actor` to `harvest_workflow_executions` with a partial index on `(paused_at) WHERE state = 'PAUSED'`. `ctx.is_paused()` is intentionally **not** exposed — pause is operator-only.
- **Phase 3.22** (implemented): Deterministic side-effect primitives on `WorkflowContext` — see issue #384. New public methods `system_now() -> DateTime<Utc>` and `system_time_now() -> SystemTime` (per-call wall-clock captured at first execution; distinct from the pre-existing `now()` which returns the fixed `WorkflowStarted` start-time logical clock and is **unchanged** for backward + in-flight safety), `new_uuid() -> Uuid` (UUIDv7 idempotency keys), `random_u64()`/`random_f64()`/`random_range(range)` (sampling draws), plus the pre-existing `side_effect(name, f)` and `random_uuid(id)`. All of them lower onto a **single new** append-only `WorkflowEvent::SideEffectRecorded { kind: SideEffectKind, name: Option<String>, value }` variant (added at the end of the enum; `SideEffectKind` is a bounded enum `Now`/`Uuid`/`Random`/`Custom` with `as_str()` for OTel labels), so the event-schema cost is paid once forever. New internal `WorkflowCommand::RecordSideEffect` (bookkeeping, persisted by the worker exactly like `RecordMarker`). `HistoryMatcher::match_side_effect_event(kind, name)` matches them in command (cursor) order and surfaces drift as `HistoryMatch::Diverged`; `match_side_effect(id)` delegates to it and remains **backward-compatible** with pre-#384 executions that recorded `side_effect` as `MarkerRecorded { name: "side_effect:{id}" }`. The infallible built-ins (`system_now`/`new_uuid`/`random_*`) return plain values, so on a replay divergence they record a deferred non-determinism error (`WorkflowContext::take_deferred_nd_error`) that the executor converts to `WorkflowFailed`; `WorkflowReplayer` classifies it as the new `NonDeterminismKind::SideEffectDrift`. Guardrails HVG001/HVG002 now recommend these primitives by name. Deps: `uuid` gains the `v7` feature, new `rand` workspace dep. **No DB migration** (the variant is opaque JSON in `harvest_events`), no change to the adjacently-tagged event JSON contract, no macro-path change. Example: `autumn-harvest/examples/deterministic_primitives.rs`. Tests: unit tests in `event.rs`/`context.rs`/`replay.rs`, replayer drift tests in `tests/replayer_tests.rs`.
- **Phase 3.24** (implemented): Signal-or-deadline waits for human-in-the-loop flows — see issue #476. `WorkflowContext::receive_signal_timeout::<O>(signal_name, timeout) -> HarvestResult<Option<O>>` and untyped sibling `wait_for_signal_timeout(signal_name, timeout) -> HarvestResult<Option<Value>>` resolve to `Some(payload)` when the signal arrives before the deadline and `None` when the durable timer fires first. **No new `WorkflowEvent` variant, no migration** — the race composes the existing `TimerStarted`/`TimerFired` + `SignalReceived` events; the winner is decided by recorded-history order via the new `HistoryMatcher::match_signal_or_timer` (returns the new `SignalOrTimerMatch` enum, exported from `lib.rs`). Timer-win never consumes a late signal (it stays observable for a later `receive_signal*`); signal-win transparently consumes the stray `TimerFired` from the still-armed durable timer. Deterministic race timer IDs `__signal_timeout:{seq}:{signal_name}` from a per-context `signal_timeout_seq` counter; live path suspends with the already-supported `StartTimer` + `WaitForSignal` mixed batch (timer row deduped by `timer_id` on re-park). `WorkflowTestEnv` exercises both branches deterministically without sleeping (`queue_signal` → signal branch; omitted → auto-fired timer). Workflow-author-side only: no client/typed-stub change, no HTTP route. Example `examples/approval_with_timeout.rs`. Tests: matcher unit tests in `replay.rs`, context tests in `context.rs`, harness tests in `tests/workflow_test_env_tests.rs`, replayer fixtures (incl. 1,000 randomized-ordering replays) in `tests/replayer_tests.rs`.
- **Phase 3.25** (implemented): Atomic start-or-attach + update (`update_with_start`) for entity-workflow patterns — see issue #479. `update_with_start_workflow_execution` applies the same start-or-attach reuse-policy matrix as `signal_with_start_workflow_execution` (issue #244) but admits exactly one update in the same shard-local transaction. **No new `WorkflowEvent` variant, no migration** — reuses the existing `UpdateAdmitted`/`UpdateCompleted`/`UpdateFailed` variants. `UpdateWithStartParams<'a>` mirrors `SignalWithStartParams` with update-specific fields (`update_id: UpdateId`, `update_name: String`, `update_args: Value`); `UpdateWithStartOutcome` reports `exec_id`, `workflow_name`, `workflow_id`, `state`, `started_fresh: bool`, `update_id: UpdateId`, `update_admitted: bool`. Idempotency deduplication is scoped to `(workflow_name, workflow_id)` via the `idempotency_key` field — a retry carrying the same key returns the cached outcome without re-admitting. PAUSED executions are rejected with `HarvestError::WorkflowPaused` (409); the entire transaction rolls back so no orphan `WorkflowStarted` event is appended on rejection. Validator runs at admission time inside the outer transaction; a rejected validator also rolls back the start. `TypedUpdateWithStartOptions` in `handle_typed.rs` provides the options struct for the generated typed stub method. `#[update(workflow = "name")]` macro generates `update_with_start_{fn_name}` on the stub type (sibling to the existing `signal_with_start` method). HTTP route `POST /api/harvest/workflows/{workflow_name}/update-with-start` → `201 Created` (fresh start) or `200 OK` (attached); `409 Conflict` on `RejectDuplicate`; `422` on validator rejection. Audit op `OP_WORKFLOW_UPDATE_WITH_START = "workflow.update_with_start"`. Example `examples/update_with_start_cart.rs` (cart entity-workflow: first `add_item` creates the cart; subsequent calls attach). Tests: `tests/update_with_start_tests.rs` (struct-level + DB integration). Outcome matrix (same as signal-with-start except PAUSED always rejects): Prior `RUNNING`/`SUSPENDED` + `AllowDuplicate` → admit to existing; + `RejectDuplicate` → `Err(AlreadyExists)`; + `TerminateIfRunning` → cancel + start fresh + admit. Terminal prior (`COMPLETED`/`FAILED`/`CANCELLED`) + `AllowDuplicate` or `AllowDuplicateFailedOnly` → start fresh + admit. Prior `TERMINATED` → start fresh + admit for all policies (sealed state released from uniqueness index). Prior `PAUSED` → `Err(WorkflowPaused)` for all policies.
- **Phase 3.26** (implemented): Bounded schedule catchup window — see issue #484. New `CatchupPolicy` enum (`SkipAll`, `MostRecent`, `Window(Duration)`, `Unbounded`) in `policy.rs` controls post-downtime catchup behavior. `from_db(mode, window_secs, catchup_bool)` resolves the effective policy, with `NULL`/unknown modes falling back to the legacy `catchup` bool (zero-backfill migration, identical behavior for existing rows). `WorkflowSchedule` gains `catchup_policy: Option<CatchupPolicy>`, plus builder methods `with_catchup_policy` and `with_catchup_window`. New `catchup_run_plan` function in `scheduler.rs` replaces the direct `due_run_plan` call: `MostRecent` keeps only the last slot, `Window(w)` keeps slots within `now - w`, both record a `dropped` count. Drops are emitted on the `harvest.schedule.skipped` counter with reason `catchup_window_exceeded` and written to a single `record_decision_graceful` audit row. `last_catchup_dropped` / `last_catchup_at` columns on `harvest_schedules` persist the drop count from the most recent recovery tick. `GET /admin/schedules` surfaces four new fields: `catchup_policy_effective`, `catchup_window_secs`, `catchup_dropped_last_recovery`, `last_catchup_at`. Migration `20260613000001_harvest_schedule_catchup_window` adds the four columns with DB-level defaults. **No new `WorkflowEvent` variant, no migration backfill required.** Tests: pure unit tests in `policy.rs` and `scheduler.rs`; DB integration tests in `tests/scheduler_catchup_tests.rs`.
- **Phase 3.27** (implemented): External workflow cancel primitive — see issue #492. `ctx.request_cancel_external_workflow(target: ExecutionId) -> HarvestResult<()>` lets a running workflow durably cancel an arbitrary sibling workflow by `ExecutionId`. Three new append-only `WorkflowEvent` variants at the end of the enum: `ExternalCancelRequested { cancel_id: ExternalCancelId, target }`, `ExternalCancelDelivered { cancel_id }`, `ExternalCancelFailed { cancel_id, reason_code: String }`. New `ExternalCancelId(Uuid)` newtype (exact clone of `ExternalSignalId`, re-exported from `lib.rs`). New `HarvestError::ExternalCancelFailed { cancel_id, target, reason_code }`. **Key semantic differences from `signal_external_workflow`:** (1) already-terminal target = **no-op success** (`ExternalCancelDelivered`), NOT a failure — the goal (target not running) is already met; (2) only `target_unknown` after the grace window resolves as `ExternalCancelFailed`; (3) **self-cancel** (`target == own ExecutionId`) is rejected immediately with `reason_code = "self_cancel"`; (4) no payload field. `HistoryMatcher::match_external_cancel(target)` added with `StashedExternalCancel` stash and `HistoryMatch::ExternalCancelInProgress`/`ExternalCancelFailed` variants; all ~8 existing scan loops updated to stash cancel events transparently. `WorkflowCommand::RequestCancelExternalWorkflow` dispatched through the worker's generalized `SignalBatchItem::Cancel(CancelExternalWorkflowRun)` variant: same-shard delivery calls `execution::cancel_workflow_execution` (already-terminal → Delivered); cross-shard leaves for `enforce_external_cancels_outbox` in `timeout.rs` (mirrors `enforce_external_signals_outbox`). `record_external_cancel_sent` no-op metric on `MetricsRecorder`. **No DB migration** (events are opaque JSON in `harvest_events`). `WorkflowContext::new_test()` harness covers the cancel path via `testing.rs`. Integration tests in `tests/cross_workflow_cancel_tests.rs` (same-shard live cancel, already-terminal no-op, grace-window unknown, cross-shard outbox). Example: `examples/cancel_external_workflow.rs` (fraud-review supervisor aborts in-flight fulfillment). `ExternalCancelId` and `ExternalSignalId` both re-exported from `autumn_harvest::types` via `lib.rs`. Event enum now has 41 variants. **Shared external-primitive hardening (applies to both `signal_external_workflow` and the cancel primitive):** the inline persist path (`persist_external_signal_inline`) wraps each batch's request + delivery + terminal appends in a single transaction so a concurrent outbox sweep never observes a half-written `*Requested` event without its terminal (no double-append at a stale `next_event_id`); and the outbox scanners (`enforce_external_signals_outbox`/`enforce_external_cancels_outbox`) attempt delivery *before* the grace window converts a result to `target_unknown` — only a `NotFound` delivery attempt past the grace window becomes a permanent failure, so a target that starts slightly late (or is first seen after worker downtime) is still reached. Because the inline/outbox cancel now runs the target cancellation inside that outer transaction, it uses `execution::cancel_workflow_execution_collect` (the no-spawn variant) and spawns the target's completion-trigger / parent-close-cascade follow-up starts (and records the terminal metric) **only after the outer transaction commits**, so a rollback never leaves trigger workflows started for a cancellation that did not become durable. Finally, the shared `persist_signal_wait_park` re-checks recorded history for a resolved external terminal after parking (mirroring its pending-signal self-wake), so a caller whose cross-shard/`NotFound` request the outbox resolves during the park gap is woken rather than left parked until an unrelated event.
- **Phase 3.28** (implemented): Soft workflow SLA breach signal for slow-but-healthy runs — see issue #487. A non-fatal companion to `execution_timeout` (#243): a workflow author declares an expected duration (`#[workflow(sla = "2h")]` → `WorkflowInfo::sla: Option<Duration>`) and a scanner emits the `harvest.workflow.sla_breached{workflow, queue}` counter (`METRIC_WORKFLOW_SLA_BREACHED`) **exactly once** when the run exceeds it — **without ever altering the run's lifecycle**. A breaching run that later succeeds still reaches `COMPLETED` normally. **No new `WorkflowEvent` variant, zero `harvest_events` footprint, replay-neutral** (like query handlers). Start-time override via `StartWorkflowParams.sla` (HTTP `sla_secs` on `POST /workflows/{name}/start`), falling back to the `WorkflowInfo` default. The declared default is resolved on **every** start path — plain start, signal-with-start, update-with-start, batch start, schedule tick/backfill, manual UI trigger, outbox, webhook delivery, completion-trigger (via `GLOBAL_WORKFLOW_METADATA`/`WorkflowMetadata.sla` and `DeferredTriggerStart.sla`), and spawned child workflows (`worker.rs` resolves the child's `WorkflowInfo.sla` and stamps `sla_deadline_at` inline). DAG start paths carry no SLA (the `#[dag]` macro has no `sla` attribute). Persisted on `harvest_workflow_executions` as four nullable/defaulted columns: `sla INTERVAL`, `sla_deadline_at TIMESTAMPTZ` (= `started_at + effective_sla`, NULL when no SLA), `sla_breached BOOLEAN NOT NULL DEFAULT FALSE`, `sla_breached_at TIMESTAMPTZ`, with a partial index on `(sla_deadline_at) WHERE sla_deadline_at IS NOT NULL AND sla_breached = FALSE AND state <> 'PAUSED' AND (completed_at IS NULL OR completed_at > sla_deadline_at)` (the `completed_at` clause keeps on-time terminal rows out of the index so it doesn't accumulate every SLA-bearing completed run). `timeout::enforce_workflow_sla_breaches` is an observation-only atomic Diesel `UPDATE ... SET sla_breached = true, sla_breached_at = NOW() WHERE state <> 'PAUSED' AND sla_deadline_at IS NOT NULL AND sla_breached = false AND sla_deadline_at < COALESCE(completed_at, NOW()) RETURNING (id, workflow_name, queue_name)`; the `sla_breached = false` guard makes it **exactly-once** across repeated scans, restarts, and multi-replica deployments. **The scanner is terminal-inclusive**: it compares `sla_deadline_at` against `COALESCE(completed_at, NOW())`, so RUNNING rows are judged against the current time and already-terminal rows (COMPLETED/FAILED/CANCELLED/TIMED_OUT/TERMINATED/CONTINUED_AS_NEW) against their actual `completed_at`. This means (a) a run that finished *before* its deadline is never a false-positive breach, and (b) a run that crossed its deadline and then went terminal within one scan interval is still caught after the fact — covering completion, failure, cancel, terminate, timeout, and continue-as-new uniformly with no per-terminal-path marker (and avoiding the metrics-availability problem, since the scanner always owns its recorder). `PAUSED` is excluded so a parked run never breaches mid-pause; `SUSPENDED` is not a persisted state (the state CHECK constraint forbids it). It is folded into `enforce_timeouts_once` **before** `enforce_workflow_execution_timeouts`, reusing the existing shard/pool/poll-interval/telemetry wiring, and never touches `state`, `harvest_events`, or the task queue. A non-positive SLA budget (`<= 0`) is treated as "no SLA" at start; an out-of-range/overflowing duration maps to "no SLA" rather than an immediate breach. **Clamp rule:** when `sla > execution_timeout`, `sla` is clamped down to `execution_timeout` at start (the hard timeout would kill the run first, so a later soft signal could never fire); the clamp is resolved at every registry-aware start path (the plain HTTP start plus signal-with-start, update-with-start, batch, manual-trigger, backfill, and the Vantage UI trigger, via `clamp_info_default_sla`). Continue-as-new (`worker.rs`) and reset (`reset.rs`) re-anchor a fresh `sla_deadline_at` per run; pause suspends the SLA clock — resume pushes `sla_deadline_at` forward by the pause span (mirroring `deadline_at`), but **only for a deadline still ahead when the pause began**: a deadline already elapsed before the pause stays in the past so the breach is still observed by the scanner after resume rather than being pushed into the future. Observable via management API (`sla_deadline_at`, `sla_breached`, `sla_breached_at` on the execution record) and filterable with `GET /workflows?sla_breached=true` (`WorkflowFilters.sla_breached`, applied on both the standard and stalled-workflow loaders). `record_workflow_sla_breach` added to `MetricsRecorder` as a no-op default (additive), bridged in `metrics_rs_adapter`. Migration `20260613000000_harvest_workflow_sla`. Out of scope (per issue): hard termination, stalled-run detection (#486), per-activity SLAs, auto-remediation, dedicated Vantage UI page.
- **Phase 3.29** (implemented): Last-completion-result carryover for incremental scheduled jobs — see issue #488. Two new `WorkflowContext` accessors: `last_completion_result::<T>() -> HarvestResult<Option<T>>` (deserialized output of the most recent prior COMPLETED run of the same schedule) and `last_error() -> Option<String>` (error of the most recent terminal run if it ended FAILED/TIMED_OUT, `None` when it recovered). Both values are **resolved once at workflow-start time inside the start transaction** and **frozen into two additive `#[serde(default, skip_serializing_if = "Option::is_none")]` fields on the existing `WorkflowStarted` event** — pre-upgrade JSON deserializes to `None`, no new `WorkflowEvent` variant, append-only invariant preserved. `schedule_id UUID NULL` + `scheduled_for TIMESTAMPTZ NULL` columns added to `harvest_workflow_executions` with a partial covering index on `(schedule_id, scheduled_for DESC) WHERE schedule_id IS NOT NULL AND completed_at IS NOT NULL` (migration `20260616000001_harvest_workflow_schedule_id`). `scheduled_for` is the **logical schedule slot** the run fires for; carryover is selected by **previous logical slot** (`scheduled_for < current slot`, ordered `scheduled_for DESC`), NOT completion time, so overlapping / catch-up / backfilled fires that finish out of slot order can't roll an incremental cursor backward (and a backfill of an *older* slot sees the cursor as of its own slot, never a future fire's output); when the current run carries no slot, no carryover is resolved. Scheduler dispatch (incl. the backfill runner) sets `schedule_id: Some(schedule.id)` and `scheduled_for: Some(slot)` (the same slot encoded in the `sched:` workflow_id) so backfilled runs share the schedule's carryover lineage; all other `StartWorkflowParams` sites set both to `None`. `OverlapPolicy::Skip` never reaches the start path, so skipped fires cannot advance carryover. Manual starts see `None` for both accessors. `continue_as_new` preserves carryover across the fork by **copying the predecessor's frozen `last_completion_result`/`last_error` forward** (threaded through `WorkflowTaskPersistence`, sourced from the already-decoded replay history so it is codec-safe) and keeps the predecessor's `scheduled_for` — the continuation is the same logical run and must not re-resolve. **Reset forks set `schedule_id: None`** (operator interventions are deliberately excluded from scheduled carryover so resetting an old slot cannot roll a later run's cursor backward). The migration backfills `schedule_id` and `scheduled_for` for already-fired scheduled rows by parsing the `sched:{uuid}:{name}:{slot}` workflow_id (regex-guarded `::uuid` / `to_timestamp(...::double precision)` casts) so the first post-upgrade fire still sees prior output in the right order. Two shard-local Diesel queries in `resolve_carryover` (slot-ordered, both bounded `scheduled_for < current slot`) run inside the same connection transaction as the start: one for `COMPLETED` output (`.eq("COMPLETED")`), one for the most-recent terminal across **all** terminal states (`.eq_any(["COMPLETED","FAILED","TIMED_OUT","CANCELLED","TERMINATED"])`, error surfaced only for FAILED/TIMED_OUT so a later cancellation masks an older failure) — so after `completed → failed` the result reflects the old completed output while error reflects the recent failure. The frozen `last_completion_result` is added to the payload-codec key set (`PayloadCodecs::transform_event_data`) and the redacted-history allowlist (`history_export::is_payload_field`) so a configured codec encrypts/redacts the carried-over output copy. `WorkflowTestEnv::with_last_completion_result<T>()` and `with_last_error(String)` fluent builder methods for no-DB unit testing. `WorkflowDetailsResponse` in the plugin API exposes `last_completion_result` and `last_error` convenience fields surfaced from `history[0]`. Example `autumn-harvest/examples/incremental_etl_schedule.rs`. Docs `docs/getting-started/08-dags-and-schedules.md` — "Incremental scheduled jobs" section. Tests: 4 unit tests in `tests/workflow_test_env_tests.rs` (`test_last_completion_result_none_on_first_run`, `test_last_completion_result_seeded_value`, `test_last_error_seeded_value`, `test_last_completion_result_replays_deterministically`); integration test file `tests/scheduler_carryover_tests.rs` (testcontainers, 4 scenarios).
- **Phase 3.30** (implemented): Targeted PII erasure for completed workflow executions — see issue #495. New `erase.rs` module: `tombstone_payload_fields(event_value)` replaces payload-bearing fields (`input`, `output`, `payload`, `details`, `value`, `last_completion_result`) inside each event's `data` object with a tombstone marker `{"_harvest_erased": true}`; `is_terminal_state(state)` gates erasure to terminal executions (COMPLETED/FAILED/CANCELLED/TIMED_OUT/CONTINUED_AS_NEW/TERMINATED); `erase_workflow_payloads(conn, exec_id, reason)` runs a single shard-local transaction scrubbing all event rows, the execution row (`input`/`output`/`memo`/`search_attrs`/`context_headers`), and all signal payloads, then recursively cascades to terminal child executions while skipping and reporting non-terminal children. **Append-only invariant preserved**: event rows are never deleted or reordered — only the `data` field contents within already-stored JSON are mutated (the same sanctioned exception as heartbeat checkpoints). **Terminal-only gate** (409 via `HarvestError::Config` → `conflict_from`) protects replay determinism. **Idempotent**: re-running yields `fields_tombstoned == 0`, no error. **`error` column deliberately kept**: consistent with `history_export` which does not redact errors (operational, non-payload). Management route `POST /workflows/{id}/erase-payloads` (admin-guarded, audit op `OP_WORKFLOW_ERASE_PAYLOADS = "workflow.erase_payloads"`). CLI subcommand `harvest workflow erase-payloads <id> [--reason <text>]`. Route registered in `management_api_routes()`, `management_api_response_fields()`, `CLASSIFIED_ROUTES` (Mutating), `ALL_MUTATION_ROUTES`, and `docs/api-contract.json`. Integration tests in `autumn-harvest-plugin/tests/erase_payloads_integration.rs` (6 scenarios: tombstone coverage, 409 on non-terminal, idempotency, audit row, adjacent-workflow isolation, 404 on unknown). Unit tests in `erase.rs` (14). CLI mapping tests in `lib.rs` (3). **No new `WorkflowEvent` variant, no migration.**
- **Phase 3.31** (implemented): Debounced workflow starts — collapse trigger bursts into one run (issue #499). Webhook providers retry 5–10× and fan-out systems emit dozens of row-change notifications for one logical event. Without debouncing, every trigger starts a redundant execution; hand-rolling a timer + buffer + dedupe scheme costs ~40–60 LOC. `DebouncePolicy { key_expr, window, max_wait }` is a `Copy` struct (mirrors `ConcurrencyPolicy`) attached to `WorkflowInfo` via `#[workflow(debounce(key = "input.tenant_id", window = "30s", max_wait = "5m"))]` or `.with_debounce(DebouncePolicy { ... })`. Key semantics: **trailing-edge** (each qualifying start (re)sets the fire deadline to `now + window`), **burst collapse** (K triggers → 1 execution), **last-input-wins** (most recent request's input is used), **max-wait cap** (configurable `max_wait`; default 1h via `WorkerConfig::default_debounce_max_wait`, overridable per worker with `.with_default_debounce_max_wait(d)`). Implementation: `harvest_debounce` table (one row per `(workflow_name, debounce_key)`) upserted via `ON CONFLICT DO UPDATE SET effective_fire_at = LEAST(now+window, max_fire_at), pending_count += 1, last_input = excluded.last_input`. Background scanner `fire_due_debounced_starts` (wired into `enforce_timeouts_once` — no new spawn) claims the due batch `FOR UPDATE SKIP LOCKED`, calls `start_or_load_workflow_execution`, and deletes each row **all inside one transaction** so the row lock is held from claim through delete (a `FOR UPDATE` in autocommit mode releases immediately, which would let two workers double-fire a key and let a concurrent `admit` overwrite a row the scanner then deletes — breaking trailing-edge/last-input-wins). An `AlreadyExists` start (e.g. `reject_duplicate` collision) deletes the row instead of looping forever. The debounce gate resolves the same effective start parameters the normal path does (`WorkflowInfo` `owner`/`runbook_url`/`severity`, effective `sla`/`execution_timeout` defaults, the server-side execution-timeout ceiling, and the workflow-input byte cap) and persists them in `start_options` so a debounced run is not a second-class start; `start_at`/`delay` combined with debounce is rejected `400` (contradictory). Debounced start returns `202 Accepted { debounced, workflow_name, workflow_id, debounce_key, fire_at, pending_count }` (the generated `workflow_id` lets the caller correlate the eventual run) and writes a succeeded admission audit row. Key scoping: **shard-local** (consistent with per-key concurrency, #247); cross-shard global debounce out of scope. **No new `WorkflowEvent` variant, no replay impact** (pre-start admission gate). Typed-client interaction: the typed client stub's immediate-start API (`Stub::start`/`start_with_options`) cannot express a deferred debounced start — no `exec_id`-keyed `WorkflowHandle` exists until the scanner fires — so for a workflow whose debounce key resolves it returns an explicit `HarvestError::Config` directing the caller to the HTTP start route, rather than silently bypassing the policy. Debounce admission applies to the HTTP start route. Management endpoint `GET /admin/debounce` (read-only, registered in `management_api_routes()` and `docs/api-contract.json`) lists all pending records with `debounce_key`, `effective_fire_at`, `max_fire_at`, `pending_count`, `shard_id`. Metrics: `harvest.workflow.debounced` (counter, on each admission), `harvest.workflow.debounce_fired` (counter, after scanner fires). Module: `debounce.rs`. Migration: `20260618000001_harvest_debounce`. Tests: 14 pure unit tests (no DB) in `debounce.rs`; 8 DB integration tests in `tests/debounce_tests.rs` (burst collapse, trailing-edge, last-input-wins, independent keys, max-wait cap, backward compat, operator visibility, metric emission); 5 example tests in `examples/debounce_webhook.rs`. See `examples/debounce_webhook.rs` for a complete Stripe webhook example.
- **Phase 3.32** (implemented): Single-execution force-terminate endpoint for wedged workflows — see issue #504. Completes the graceful-vs-forceful operator split: `cancel` (cooperative) and now `terminate` (forceful) are both single-execution, shard-aware, audited routes. New management route `POST /api/harvest/workflows/{id}/terminate` (admin-guarded, body `{ "reason": "" }`, handler `terminate_workflow` in `api.rs`) delegates to the existing `terminate_workflow_execution` core primitive. **cancel vs terminate:** `cancel` appends `WorkflowCancelled` and seals `CANCELLED`, and the workflow body must observe it cooperatively (`is_cancelled`/`check_cancellation`); `terminate` seals a live run (`RUNNING`/`SUSPENDED`/`PAUSED`) in the **`TERMINATED`** state unilaterally — no body cooperation required — surfacing to result-awaiting callers as `HarvestError::Terminated` (distinct from a `CANCELLED` cancel and from a `FAILED`). Terminate is forceful at the **durable/orchestration** layer only: it stops further durable progress, fails outstanding task-queue rows, and runs `apply_parent_close_cascade`; interrupting an already-executing hot workflow task (a CPU-spinning body) is the domain of the workflow-task timeout (#494), **not** terminate. **Behavior change (consistency fix):** `terminate_workflow_execution` now seals `TERMINATED` everywhere (single endpoint **and** batch API #102 `BatchAction::Terminate` **and** scheduler `TerminateOther`), resolving the prior latent inconsistency where it wrote `CANCELLED` state while already recording the `harvest.workflow.terminal{outcome="terminated"}` metric. The batch terminate target filter now excludes both `CANCELLED` and `TERMINATED`. **Idempotency:** terminate is a non-mutating no-op against **any** already-terminal state (COMPLETED/FAILED/CANCELLED/TIMED_OUT/CONTINUED_AS_NEW/TERMINATED) — returns `newly_terminated: false`, appends no second terminal transition (guarded by `erase::is_terminal_state`). Returns `202 Accepted { ok, execution_id, state, reason, newly_terminated, failed_task_count }`; `404` for unknown id. Audit op `OP_WORKFLOW_TERMINATE = "workflow.terminate"` via the existing `TARGET_WORKFLOW` pattern (registered in `CLASSIFIED_ROUTES`, `ALL_MUTATION_ROUTES`, `AUDITED_OPERATIONS`, `management_api_routes()`/`request_fields()`/`response_fields()`, and `docs/api-contract.json`). Client surface completed: `WorkflowHandle::cancel`/`terminate` and `TypedWorkflowHandle::cancel`/`terminate` (neither `cancel` nor `terminate` existed on the handles before). **Completion-trigger classification:** terminate fires `TerminalState::Terminated` completion triggers (new variant, added in `completion_trigger.rs`), **not** `Cancelled` — a force-kill is distinct from a cooperative cancel downstream, so a trigger registered for `["Cancelled"]` no longer fires on a terminate (operators opt into terminate cascades with `["Terminated"]`); the request body is optional (`required: false`, defaulted reason). `TERMINATED` is already terminal in `erase::is_terminal_state`, already excluded from the active-uniqueness partial index, and already excluded by `try_load_by_key`, so sealing it is semantically consistent with the existing reset-fork sealing. **No new `WorkflowEvent` variant** (reuses `WorkflowCancelled`), **no migration**, no shard-model change.
- **Phase 3.33** (implemented): Weighted task-queue draining for multi-queue worker fairness — see issue #515. A single Harvest worker can bind multiple task queues, but a high-volume bulk queue could monopolize concurrency slots and starve a latency-sensitive queue. **New `WorkerConfig` field `queue_weights: HashMap<String, u32>`** with builder method `with_queue_weights(impl IntoIterator<Item=(S,u32)>)` (mirrors `with_labels`; `#[must_use]`; absent from map defaults to weight `1`). **Weight `0`** = fallthrough-only (queue placed last; drained only when all positive-weight queues are empty). **Default path unchanged** — when `queue_weights` is empty, the existing single `ANY($queues)` `SKIP LOCKED` query runs byte-for-byte identically; no behavior change for existing workers. **Weighted path** — when weights are set, uses Efraimidis-Spirakis key-based weighted sampling without replacement (`U^(1/w)` key per positive-weight queue; sorted descending) to produce a full permutation every poll; zero-weight queues appended in stable order; `claim_task` called per-queue in permutation order; first `Some` result is dispatched. First-position frequency is proportional to weight under saturation; full permutation guarantees no starvation by construction. **No-starvation guarantee**: every non-zero-weight queue appears exactly once in the permutation and makes forward progress whenever it has available work. **Per-queue dispatch counter** `harvest.queue.dispatched{queue}` (`METRIC_QUEUE_DISPATCHED`, `record_task_dispatched` on `MetricsRecorder`, bridged in `metrics_rs_adapter`) lets operators confirm the live dispatch split matches configured weights. Emitted on every dispatched task (both weighted and default paths). **Composition with within-queue priority (#249)**: weights decide *which queue* to claim from; the existing `ORDER BY priority DESC, scheduled_at ASC` SQL decides *which row* within the chosen queue — entirely unchanged. New pure module `queue_fairness.rs` (`effective_queue_weights`, `weighted_queue_order`). `WorkerRuntimeConfig` gains `queue_weights` field mapped from `WorkerConfig` via `From` impl. **Zero** new `WorkflowEvent` variants, **zero** migrations, **zero** shard-semantics changes. Docs: `docs/getting-started/09-worker-routing.md` "Multi-queue Worker Fairness" section; `harvest.queue.dispatched` added to `docs/telemetry.md` metric catalogue. Tests: 12 unit tests in `queue_fairness.rs` (permutation property, zero-weight-last, 3:1 distribution ±10% tolerance, equal-weights uniformity, edge cases); integration tests in `tests/queue_fairness_tests.rs` (3:1 distribution, no-starvation drain-to-completion, per-queue dispatch counter).
- **Phase 3.34** (implemented): Activity backoff skip + observability — see issue #516. Two gaps closed: (1) **`next_retry_at` in stack response** — `GET /workflows/{id}/stack` now populates `next_retry_at` on `PendingActivity` for tasks in `PENDING` state with `scheduled_at > now` (backing off); `null` for `RUNNING`/eligible/local activities. (2) **Force-retry endpoint** — `POST /api/harvest/workflows/{id}/activities/{activity_exec_id}/retry-now` (admin-guarded) advances a backing-off `PENDING` task's `scheduled_at` to `NOW()` and wakes an idle worker via `pg_notify`. Core: `queue::force_retry_activity_now(conn, workflow_exec_id, task_id) -> HarvestResult<RetryActivityOutcome>`. Logic: load by PK + `workflow_exec_id` filter (wrong workflow → `NotFound`/404); non-PENDING → `Config`/409 via `conflict_from`; already eligible → `advanced: false` (idempotent no-op); else `UPDATE scheduled_at = NOW() WHERE state = 'PENDING'` (race-safe) + `notify_task_enqueued`. Only `scheduled_at` touched; `attempt`/`max_attempts`/`error`/`crash_strikes` untouched. **No new `WorkflowEvent` variant, no migration.** CLI: `harvest workflow retry-activity <workflow_id> <activity_exec_id>`. Audit: `OP_ACTIVITY_RETRY_NOW`/`TARGET_ACTIVITY` in `audit.rs`, registered in `CLASSIFIED_ROUTES`, `ALL_MUTATION_ROUTES`, `AUDITED_OPERATIONS`. Response: `202 Accepted { ok, execution_id, activity_exec_id, queue, next_retry_at, advanced }`. Tests: 7 core integration tests in `autumn-harvest/tests/retry_now_tests.rs` (testcontainers); 2 lib unit tests + 1 `request_mapping.rs` integration test for CLI.
- **Phase 3.35** (implemented): Workflow-type reachability check to gate safe handler removal — see issue #520. Read-only pre-flight answer for the **default, non-build-routed** deployment to the question "is it safe to delete or rename this `#[workflow]` handler?" A non-terminal execution's `workflow_name` directly names the handler its next replay requires, so removing that handler would wedge in-flight runs into permanent `HandlerNotFound` replay failure (surfacing only later as a timeout/DLQ entry). New management route `GET /api/harvest/workflows/../admin/workflow-types/reachability` (admin-guarded, `read_only`) returns, per workflow type that is *either* registered *or* has ≥1 non-terminal execution on any shard: `workflow_type`, `registered` (bool), `non_terminal_count`, `oldest_non_terminal_age_secs`, `verdict`, and a per-shard `shard_breakdown`. Verdicts (additive-only `ReachabilityVerdict` enum): `safe_to_remove` (zero non-terminal executions), `in_use` (≥1 non-terminal **and** registered), `orphaned` (≥1 non-terminal **and** NOT registered — already-wedged runs surfaced before DLQ/timeout). Optional `?workflow_type=` narrows to one type (still returns the full object; `non_terminal_count = 0` + `safe_to_remove` if none). Cross-shard fan-out via `iter_shards()` (mirrors `all_build_reachability_sharded`); an unreachable shard is **reported** in `shards` (never silently dropped) and sets report `status = partial`/`unavailable` so a `safe_to_remove` verdict is authoritative only when `status = complete` — a partial answer is never mistaken for safe. Core: one read-only query helper `execution::non_terminal_counts_by_workflow_name(conn, Option<&str>) -> Vec<WorkflowTypeNonTerminalCount>` (`GROUP BY workflow_name` over `harvest_workflow_executions` filtered to the exact complement of `erase::is_terminal_state` — `RUNNING`/`SUSPENDED`/`PAUSED`). Plugin module `workflow_reachability.rs` (pure `build_report_from_observations` + verdict logic, unit-tested without DB). CLI: `harvest workflow-types reachability [--type <name>] [--json]` — table by default, exit code **2** when any `orphaned` verdict is present **or** the report is incomplete (fail-closed CI/deploy gate), exit 0 otherwise. Contrasted with **build-id** `build_reachability` (#171) and **`ctx.version()`** gate-retirement in runbook `docs/runbooks/safe-handler-removal.md` (three distinct reachability questions). Contract: route registered in `management_api_routes()`/`management_api_response_fields()` and `docs/api-contract.json`. **Read-only and side-effect-free: no task claims, no state mutation, no `WorkflowEvent` appended, no migration, no macro impact.** Out of scope (per issue): activity-type reachability (needs static call-graph analysis), Vantage UI surface. Tests: 9 pure unit tests in `workflow_reachability.rs`, 8 CLI unit tests in `lib.rs`, HTTP+shard integration tests in `autumn-harvest-plugin/tests/workflow_reachability_integration.rs` (verdicts, filter, cross-shard partial, admin auth).
- **Phase 3.36** (implemented): Idempotency keys for standalone & external workflow signals — see issue #521. Closes the exactly-once gap on the three signal paths that lacked it (signal-with-start #244 already had it): the standalone HTTP route `POST /workflows/{id}/signal/{signal_name}`, core `signal::send_signal`, and in-workflow `ctx.signal_external_workflow`. **No new `WorkflowEvent` variant, no migration** — the `harvest_signals.idempotency_key` column and partial unique index `uq_harvest_signals_idem (workflow_exec_id, idempotency_key) WHERE idempotency_key IS NOT NULL` already exist from migration `20260518000000_harvest_signal_idempotency`. Core: new `signal::send_signal_idempotent(conn, exec_id, name, payload, key: Option<&str>) -> HarvestResult<bool>` mirrors `execution::stage_signal_with_idempotency` — `.on_conflict_do_nothing()` insert, returns `true` = freshly queued (workflow woken) / `false` = deduped (no re-wake); the legacy `send_signal` is now a thin wrapper passing `None` (a `NULL` key is excluded from the partial index, so every insert succeeds → byte-for-byte legacy at-least-once behavior). External signal: **additive** `idempotency_key: Option<String>` field on the existing `WorkflowEvent::ExternalSignalRequested` variant (`#[serde(default, skip_serializing_if = "Option::is_none")]`, so pre-#521 JSON deserializes to `None` — append-only invariant preserved, **still 41 variants**). New `WorkflowContext::signal_external_workflow_with_idempotency<P>(target, signal_name, payload, key: impl Into<Option<String>>)`; the plain `signal_external_workflow` delegates with `None`. The `#[signal]` macro also generates an idempotent typed-stub sibling `signal_[name]_idempotent` (trailing `idempotency_key: impl Into<Option<String>>`, returns `HarvestResult<bool>`) alongside the plain `signal_[name]` method, sharing the payload-cap prologue. The key threads through `WorkflowCommand::SignalExternalWorkflow.idempotency_key`, `worker::SignalExternalWorkflowRun.idempotency_key`, `replay::StashedExternalSignal` + `HistoryMatch::ExternalSignalInProgress`, and is **reused verbatim from recorded history on crash-recovery re-dispatch** (so a code change to the key expression cannot diverge an in-flight delivery the outbox later resolves). Both the same-shard inline path (`persist_external_signal_inline`) and the cross-shard outbox scanner (`timeout::enforce_external_signals_outbox`) deliver via `send_signal_idempotent` and treat a deduped insert (`Ok(false)`) as `ExternalSignalDelivered` (the signal already landed once = success). HTTP route: the exactly-once key is supplied **out-of-band** (the body stays the raw payload) via the `Idempotency-Key` header (wins) or `?idempotency_key=` query param; response is `202 { ok, signal_delivered }` (`signal_delivered = false` on a deduped retry). Dedupe scope is **shard-local**, keyed on `(workflow_exec_id, idempotency_key)`, matching signal-with-start. Contract: `signal_delivered` added to `management_api_response_fields()` and `docs/api-contract.json` (header + query params, `202` status). Docs: `docs/getting-started/04-signals.md` (external-method + standalone-HTTP idempotency sections). Tests: 5 DB integration tests in `tests/signal_tests.rs` (same-key→1 row, distinct keys→N rows, unkeyed→N rows, per-execution scoping, plain `send_signal` unchanged); 3 context unit tests (`signal_external_workflow_with_idempotency_threads_key_into_command`, `..._without_key_has_none_on_command`, `..._crash_recovery_reuses_recorded_key`); event round-trip + pre-#521 back-compat unit tests in `event.rs`; 4 HTTP integration tests in `autumn-harvest-plugin/tests/signal_with_start_integration.rs` (header dedupe, query-param dedupe, header-wins-over-query, unkeyed at-least-once).
- **Phase 3.37** (implemented): Large-payload offloading to external storage via claim-check — see issue #524. New async `PayloadStore` trait (content-addressed `put(bytes)->key` / `get(key)->bytes` / `delete(key)`, boxed-pin futures mirroring `HistoryArchiver`); harvest core ships **no** cloud client — the embedder supplies the backend, preserving the Postgres-only boundary. Registered via `HarvestBuilder::payload_store(impl PayloadStore)` + `payload_offload_threshold(bytes)` (default 256 KiB); builds a `PayloadOffloader { store, threshold, store_id, metrics }` carried on `HandlerRegistry` and threaded into retention. **Offload composes after `PayloadCodec::encode`** (encrypt-then-offload) and inflate runs **before** decode: any payload-bearing field (`input`/`output`/`payload`/`details`/`value`/`last_completion_result`) larger than the threshold is replaced **inline** with a small self-describing **reference envelope** `{"_harvest_offload_envelope":1,"store_id","key","len","checksum"}` (sha256 content checksum, verified on read) — **no new `WorkflowEvent` variant**, the envelope rides in the existing payload JSON exactly as the codec envelope does (distinct `_harvest_offload_envelope` discriminator, no collision). Store-layer seam: `store::append_events_offloaded` / `load_history_inflated` / `load_history_since_inflated` — when no store is registered they **delegate verbatim** to the inline path (byte-for-byte zero behavior change, AC-by-construction). Wired write paths: activity input (`persist_scheduled_activities`) + output (`finalize_activity_completion`), workflow output (`persist_workflow_completion`), child input, continue-as-new. Replay/read paths inflate on both the full and delta (`load_history_since`) hot-path loads. **#252 cap interaction**: `WorkflowContext` gains `payload_offload_threshold: Option<u64>`; the cap is skipped only when a payload will be offloaded (`observed > threshold`), so an offloadable blob never trips the cap while an oversized payload with **no** store still fails fast. **continue-as-new carry-forward** (AC #8): the predecessor's `last_completion_result` is copied forward as its **raw envelope** (no re-inflate, no re-upload) and a new ref row is recorded for the successor. **Orphan-safe GC** (AC #7): migration `20260627000001_harvest_payload_refs` (one row per `(blob_key, workflow_exec_id)`, `ON DELETE CASCADE` makes the live `COUNT(*)` the authoritative refcount); the retention sweep collects a candidate's refs, deletes the execution (refs cascade), then deletes each blob **only when no surviving execution references it** (`blob_key_still_referenced`) — a content-addressed blob shared by a continue-as-new fork survives until the last referencing execution is collected; a failed blob delete only leaks storage (never a dangling reference). Telemetry: `harvest.payload.offloaded` (counter+bytes on write, labels `payload.field`/`store.id`) and `harvest.payload.offload_fetch_duration` (histogram on read, label `store.id`); `execution.id` is never a metric label. `WorkflowReplayer::with_payload_offloader` inflates offloaded fixtures so replay reconstructs byte-identical payloads. Module `payload_store.rs`; deps: `sha2`. Example `examples/payload_offload.rs`. Tests: 9 pure unit tests in `payload_store.rs`; `tests/payload_offload_replay_tests.rs` (replay fidelity: ~2 MB output stored as <4 KB envelope, 100% byte fidelity); `tests/payload_cap_tests.rs` cap-skip cases; `tests/payload_offload_db_tests.rs` (Postgres round-trip + shared-blob GC). **Remaining (follow-up):** the HTTP start-path `WorkflowStarted` input, standalone signal-send payloads, and query/update result payloads are not yet routed through the offloader (the engine activity/child/continue-as-new/output boundaries are).
- **Phase 3.38** (implemented): Workflow result HTTP endpoint for external callers — see issue #527. Read-only route `GET /api/harvest/workflows/{id}/result` returns a compact `WorkflowResult` object without the full event history so non-Rust callers and CI scripts can await a workflow's typed terminal output. **Envelope shape** (`WorkflowResult`, snake_case): `state` ∈ `running`/`completed`/`failed`/`cancelled`/`timed_out`/`terminated`/`continued_as_new`; `output: Optional<Value>` (for `completed`/`continued_as_new`); `error: Optional<String>` (for `failed`/`cancelled`/`timed_out`/`terminated`); `completed_at: Optional<DateTime<Utc>>` (all terminal states). **Status mapping**: `200 OK` for any terminal state (including failures); `204 No Content` + `Retry-After: 1` when still running after the `?wait=` window; `404` for an unknown execution id; `400` for a malformed wait duration. **`?wait=` long-poll**: up to the server ceiling (default **30 s**, configurable via `HarvestApiState::set_workflow_result_max_wait`). Values above the ceiling are **silently clamped** — not rejected with 400. Uses LISTEN/NOTIFY (`WorkflowHandle::result_snapshot_with_wait`) so the server-side wait is efficient — no busy-polling. **`ContinuedAsNew` resolution**: when the requested execution is in state `CONTINUED_AS_NEW`, the endpoint **follows the successor chain** (reading `WorkflowContinuedAsNew { new_exec_id }` from history) until a non-`CONTINUED_AS_NEW` terminal is found; the final successor's output and state are returned rather than the sentinel. The successor always lives on the same shard. **Shard-awareness**: uses `db_conn_for_execution` (shard routing from `ExecutionId`) — works transparently in multi-shard deployments. Route classification: `RouteClass::ReadOnly` in `CLASSIFIED_ROUTES`. No new `WorkflowEvent` variant, no migration. Example: `autumn-harvest/examples/await_workflow_result_http.rs`. Integration tests: `autumn-harvest-plugin/tests/workflow_result_integration.rs` (404, COMPLETED→200, RUNNING→204, ContinuedAsNew chain following).
- **Phase 3.39** (implemented): Replay-safe custom metrics API for workflow business KPIs — see issue #532. `ctx.metrics().counter(name, value, &labels)` / `.gauge(...)` / `.histogram(...)` on both `WorkflowContext` and `ActivityContext` lets workflow and activity authors emit business KPIs (`harvest.user.*`) into the same `MetricsRecorder` pipeline as engine metrics. Three new default no-op methods on `MetricsRecorder`: `record_user_counter`, `record_user_gauge`, `record_user_histogram` (additive, no breaking change). `UserMetrics<'a>` handle holds `&'a dyn MetricsRecorder` + `suppressed: bool`; suppressed when `WorkflowContext::is_replaying()` is true so workflow metrics are emitted exactly once on the live execution frontier and zero times during replay — **no new `WorkflowEvent` variant**, no migration, byte-identical history. Activity metrics are never suppressed (`suppressed: false`; each retry = separate execution). Validation (`validate_user_metric`): rejects names starting with `harvest.` (reserved engine namespace), names over 200 chars, empty names, forbidden high-cardinality label keys (`execution.id`, `activity.id`, `workflow.id`, `harvest.execution.id`, `harvest.activity.id`, `idempotency_key`, `run_id`), over 16 labels; violations log a `tracing::warn!` and drop the call (not a hard error). `harvest.user.*` prefix applied automatically by `UserMetrics`. `is_enabled()` short-circuit for zero overhead when telemetry is off. `Arc<dyn MetricsRecorder>` threaded into both context types via new `metrics` field (default `Arc::new(NoOpMetrics)`) + `with_metrics` builder; wired from `registry.telemetry().metrics` at the worker activity dispatch and local-activity dispatch sites, and from the `run_workflow_*` family in `executor.rs`. `WorkflowReplayer::with_metrics` builder (feature `testing`) lets replay-safety tests inject a counting recorder. `metrics-rs` adapter bridged via `metrics::with_recorder` + `Key::from_parts` for dynamic names/labels. Public re-exports: `UserMetrics`, `UserMetricError`, `USER_METRIC_PREFIX` from `lib.rs`. Docs: `docs/telemetry.md` "Custom workflow/activity metrics" section with worked examples for workflow + activity, replay-suppression callout, `harvest.user.*` namespacing, and the low-cardinality label rule. Tests: 14 unit tests in `telemetry.rs` (validation, suppression, no-op, cardinality-by-construction), `context.rs` context tests, `metrics_rs_adapter.rs` bridge test.
- **Phase 3.40** (implemented): Per-schedule run history with terminal outcomes for triage — see issue #534. New read-only management route `GET /api/harvest/admin/schedules/{id}/runs` (admin-guarded, handler `list_schedule_runs_handler` in `api.rs`) lists the executions a schedule launched, newest-first, each row carrying `execution_id`, `nominal_fire_time` (the `scheduled_for` slot), `started_at`, `completed_at`, terminal `state`, and dispatch `origin`. **Most of the linkage already existed from #488** (`schedule_id`/`scheduled_for` on `harvest_workflow_executions`); the only genuinely new column is **`origin TEXT NULL`** (migration `20260628000001_harvest_execution_origin`, plus a partial index `idx_harvest_wfx_schedule_runs ON (schedule_id, started_at DESC, id DESC) WHERE schedule_id IS NOT NULL`). New origin constants in `execution.rs`: `ORIGIN_SCHEDULED`/`ORIGIN_BACKFILL`/`ORIGIN_MANUAL_TRIGGER`; `StartWorkflowParams.origin: Option<&'a str>` threaded into the execution row insert. **The three scheduled-start paths set it**: scheduler tick (workflow + DAG) → `scheduled`; backfill (`schedule_backfill` workflow + DAG loops) → `backfill`; `trigger_schedule_now` → `manual_trigger` (and it now sets `schedule_id = Some(..)` so manual fires are *attributed*, while keeping `scheduled_for = None` so `resolve_carryover` still short-circuits — origin never affects #488 carryover). Continue-as-new preserves the predecessor's origin; all non-scheduled paths (manual start, signal-/update-with-start, children, reset forks, outbox, completion-triggers, debounce) set `origin = None`. Core read helpers in `execution.rs`: `ScheduleRunRow`, `ScheduleRunQuery` (state/origin filter + `since`/`until` + keyset cursor `(started_at, id)` + limit), `list_schedule_runs` (newest-first, fetches `limit+1` to detect a further page), `ScheduleRunStateCount`, `schedule_run_state_summary` (counts **`origin = 'scheduled'` only** so a backfill storm or manual fire never inflates the cadence failure ratio). New plugin module `schedule_runs.rs` holds the pure, unit-tested cross-shard merge (`build_runs_response`: keyset merge across `iter_shards()`, truncate to `limit`, derive `next_cursor`, sum per-shard summaries, `status` = `complete`/`partial`/`unavailable` with unreachable shards named — never a hard 500) plus `ScheduleRunsParams::from_query_pairs` (repeatable/CSV `state`+`origin`, RFC3339-or-relative `since`/`until`, `limit` default 100/max 1000, cursor). Response: `{ schedule_id, status, runs[], summary{succeeded,failed,timed_out,cancelled,terminated,continued_as_new,running,paused,other,total}, limit, next_cursor, shards[] }`. CLI: `harvest schedule runs <id> [--state .. --origin .. --since .. --until .. --limit .. --cursor ..]`. Route registered in `management_api_routes()`/`management_api_response_fields()` and `docs/api-contract.json` (read-only). **No new `WorkflowEvent` variant, no replay-determinism impact, no shard-encoding change.** Backward compatible: pre-migration executions have `schedule_id = NULL` and are un-attributable (documented); historical scheduled rows are backfilled to `origin = 'scheduled'`. Runbook `docs/runbooks/schedule-run-history.md` ("flaky cron → list failed runs → pause or backfill"). Tests: 16 pure unit tests in `schedule_runs.rs` (merge ordering, limit/cursor, summary summation, partial-shard, param parsing), core DB integration `tests/schedule_runs_tests.rs` (origin persistence per dispatch source, ordering/filter/keyset, scheduled-only summary), plugin HTTP integration `autumn-harvest-plugin/tests/schedule_runs_integration.rs` (success-metric origin separation, state filter, pagination, cross-shard merge, one-shard-down → partial), CLI mapping tests in `request_mapping.rs`.
- **Phase 3.41** (implemented): Finite/bounded schedules — reconciled with prior art, gaps closed — see issue #543. **Issue #543 asks for the same capability issue #478 ("Bounded schedule runs") already shipped**: `WorkflowSchedule.end_at`/`max_runs` + `harvest_schedules.runs_started`/`exhausted_at`/`exhausted_reason`, with exactly-once budget decrement under the #350 HA `fire_claim_token` claim, skipped/suppressed firings never consuming budget, and `GET /admin/schedules`/`{id}` already surfacing `end_at`/`remaining_runs`/`exhausted_reason`. The two issues diverge only in naming/mechanism: #543's literal AC text asks for a `limited_actions` field and for exhaustion to reuse the `auto_paused_at` (#360) pause mechanism with reason strings `"schedule_ended"`/`"schedule_exhausted"`; #478 instead introduced its own terminal `exhausted_at`/`exhausted_reason` pair (`"end_at_reached"`/`"max_runs_exhausted"`), deliberately **not** reusing `auto_paused_at` — reusing it would collide with `maybe_reset_schedule_failure_counter`'s blanket clear-on-next-success, which would silently un-exhaust a budget-exhausted schedule the moment any unrelated run completed. Given #478's design is already mature, HA-tested (`tests/scheduler_bounded_runs_tests.rs`, 809 lines) and live in the admin API/UI/CLI, issue #543 was resolved by mapping its ACs onto the existing #478 implementation rather than renaming shipped columns. Two gaps were found and closed: (1) `WorkflowSchedule::with_limited_actions(u32)` added in `policy.rs` as a documented alias for `with_max_runs` so callers can literally spell the "N actions remaining" intent from #543's AC text without a breaking rename. (2) **Real bug**: `GET /admin/schedules/{id}/preview` (issue #348) ignored `end_at`/`max_runs`/`exhausted_at` entirely — it would project fire times past a schedule's cutoff or after its budget was spent, and would show a full preview for an already-exhausted schedule instead of the empty-with-reason response already used for `is_paused`. Fixed in `autumn-harvest-plugin/src/api.rs`: new pure `truncate_preview_entries_by_bounds` (drops entries at/after `end_at`; stops after the first `remaining_budget` actually-firing entries; calendar-suppressed entries pass through without consuming budget, matching the live scheduler's contract) wired into `preview_schedule_firings_handler`, plus an `exhausted_at`-short-circuit (mirroring the `is_paused` short-circuit) and `end_at`/`remaining_runs`/`exhausted_reason` added to every preview response branch via the new `empty_preview_response` helper. **No new `WorkflowEvent` variant, no migration** (all fields already existed from #478's `20260610000001_harvest_schedule_bounded_runs`). Tests: 5 unit tests for `truncate_preview_entries_by_bounds` and 5 for `with_limited_actions` (no DB required); DB-backed HTTP coverage of the preview fix was not added because this sandbox has no Docker/testcontainers to execute it — the wiring was verified by direct code reading instead. AC-to-evidence mapping and the full reconciliation rationale are recorded in the PR/session notes for issue #543.
- **Phase 3.42** (implemented): Push-based signal handlers — `register_signal_handler` (issue #546). Completes the query/update/signal handler-registration trio: `WorkflowContext::register_signal_handler<Req>` (typed, `Req: Deserialize`) and `register_signal_handler_raw` (untyped `serde_json::Value`) let a workflow author react to a signal arriving at *any* point in the run, mirroring `register_update_handler`'s ergonomics but fire-and-forget (no validator, no completion event — that's the `update` primitive's job). New module `signal_handler.rs`: `SignalHandlerRegistry` (type-erased, synchronous, idempotent first-registration-wins, mirrors `UpdateRegistry`/`QueryRegistry`), `BoxSignalHandler`, `invoke_signal_handler` (panic-safe, logs and drops on a panicking handler rather than crashing the workflow task). New `HistoryMatcher::drain_signal_events(name)` in `replay.rs`: a full-history scan mirroring `drain_admitted_updates` — independent of the main replay cursor so a handler registered at the top of the workflow body (before any other cursor-based scan has run that cycle) still sees every already-recorded matching signal, including ones delivered before the handler existed (buffered exactly like `wait_for_signal`'s existing `pending_signals` queue, so no signal is silently dropped). **No new `WorkflowEvent` variant** — `SignalReceived` (issue #140's existing variant) is reused; the append-only invariant and adjacently-tagged JSON contract are untouched, and there is no migration. Draining marks claimed event indices consumed so the push and pull (`wait_for_signal`/`receive_signal`) consumption styles never double-deliver a single `SignalReceived` event — whichever style claims an event first (in code-execution order) receives it; the other sees nothing for that occurrence. Handler dispatch is replay-deterministic by construction: the same recorded history always drains to the same handler calls in the same order, and re-registering the same name within one workflow-task cycle is a no-op (both for registry storage and for dispatch, since a fully-drained match returns nothing on a second call). `ctx.list_signal_handler_names()` returns the sorted registered names (parity with `list_query_names`). Out of scope (per issue): `#[signal]` macro sugar for handler-side registration (the macro remains client-stub-only this slice), signal validators/rejection, and any change to signal delivery, idempotency-key dedupe (#521), or the `harvest_signals` table/wire format. Docs: CLAUDE.md "Signal Handlers" subsection with a pull-vs-push decision table. Example: `autumn-harvest/examples/signal_handlers_subscription.rs` (a subscription workflow reacting to `cancel`/`pause`/`upgrade` signals mid-run). Tests: unit tests in `signal_handler.rs` (registry) and `replay.rs` (`drain_signal_events`, incl. push/pull no-double-delivery in both directions), `context.rs` (registration + typed/untyped dispatch, buffered-before-handler-exists, deserialization-failure-does-not-panic), `executor.rs` (end-to-end `run_workflow` dispatch), and `WorkflowReplayer` fixtures in `tests/replayer_tests.rs` (incl. reordered signal-arrival fixtures, per the issue's success metric). **Post-ship hardening from code review:** `HistoryMatcher::new` now builds a one-time `signal_events_by_name` index plus a `race_reserved_signal_events` set (indices that fall inside a still-open `receive_signal_timeout`/`wait_for_signal_timeout` race window for their signal name, detected via the `__signal_timeout:{seq}:{name}` timer-id convention); `drain_signal_events` looks up candidates through the index (O(matches) instead of a full history rescan per registered handler) and skips reserved indices, closing a real bug where a push handler could silently steal the winning signal from an in-flight signal-or-deadline race and flip its outcome from `SignalWon` to `TimerWon`. `SignalHandlerRegistry::register` now returns the resolved (first-wins) handler directly so dispatch never needs a second registry lock/lookup; the unused `contains` method was removed. The triplicated `catch_unwind` panic-message extraction in `query.rs`/`context.rs`/`signal_handler.rs` was consolidated into `error::panic_message`. Doc comments and the example now call out that dispatch is per-cycle (not once-ever, since there is no persisted completion marker), that `catch_unwind` does not un-poison a mutex a handler panicked while holding, and that registering from inside an `#[update]`/query handler's throwaway context is a silent no-op. **Follow-up architectural fix (PR #890 review, deeper pass):** the original `drain_signal_events` was a full-history scan independent of the main replay cursor, which meant a handler could fire on a signal recorded *after* an activity/timer the workflow hadn't replayed yet this cycle — a real ordering bug (confirmed reproducible: `WorkflowStarted, ActivityScheduled, SignalReceived("cancel"), ActivityCompleted` could flip state before the recorded activity replayed). Fixed by threading dispatch through the cursor-bound scan every other `match_*` method already uses: `signal_events_by_name` was removed from `HistoryMatcher`, `drain_signal_events` was replaced by `pub(crate) claim_pending_signal(name) -> Vec<(usize, Value)>` (partitions only `pending_signals`, never an independent history lookup), `prepare_match` became `pub(crate)`, and `WorkflowContext::match_history` grew a post-hook (`pump_signal_handlers`) that runs after every cursor-advancing call, claims from every currently-registered handler name, sorts the combined claims by event index, and dispatches in that order — fixing cross-handler-name ordering (a second review finding: registering "cancel" before "pause" no longer fires "cancel" first when history recorded "pause" first) whenever a real command separates registration from the signals. `register_and_dispatch_signal_handler` still triggers an eager pump inline at registration time (preserving same-cycle visibility for the common single-handler case); the one residual, documented limitation is that two handlers registered back-to-back with *zero* intervening command, where both names already have a recorded signal at that exact point, still dispatch in registration order for that specific pairing (unavoidable without giving up same-cycle visibility, since there is no signal that "no more handlers are about to register"). Regression tests: `claim_pending_signal_does_not_advance_past_an_unconsumed_activity` (replay.rs), `register_signal_handler_does_not_fire_before_an_unconsumed_activity_is_matched` and `signal_handlers_dispatch_in_history_order_across_names_not_registration_order` (context.rs), plus the full `drain_signal_events_*` test suite rewritten against `claim_pending_signal`.
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

`/api/harvest/health` is liveness-style by default. To make it fail with `503` when writable shard readiness is not `ready`, configure `[harvest.readiness] require_shard_readiness = true` or set `AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS=true`.

Current implementation scope: `ExecutionId`/`ShardId` encoding, `ShardRouter`, `ShardedDbPool`, shard-aware start/read paths in the plugin, `WorkerConfig.shard_assignments`, and shard health readiness for worker/scheduler rollout coverage. Per-shard worker poll loops and per-shard scheduler tick loops with DAG→shard pinning remain as follow-up work.

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
| `erase.rs` | 3.30 | Targeted PII erasure (issue #495): `ERASURE_TOMBSTONE_KEY`, `erasure_tombstone()`, `tombstone_payload_fields(event_value)` (pure, no-DB), `is_terminal_state(state)`, `EraseOutcome`/`SkippedChild`/`EraseFailure`; DB-gated `erase_workflow_payloads(conn, exec_id, reason)`. **Sanctioned in-place mutation exception** to the append-only invariant (alongside heartbeat checkpoints): only `data` field contents are mutated, never event structure. Terminal-only, irreversible, idempotent, cascades to terminal children on the same shard. |
| `payload_store.rs` | 3.37 | Large-payload claim-check offloading (issue #524): `PayloadStore` async trait (embedder-supplied backend, no cloud client in core), `PayloadOffloader` (`offload_event_value`/`inflate_event_value`/`extract_offload_ref`/`refs_in_event_value`), reference-envelope + sha256 checksum helpers. Composes after `PayloadCodec`; no new `WorkflowEvent` variant. Store seam in `store.rs` (`append_events_offloaded`/`load_history_inflated`); GC via `harvest_payload_refs` (migration `20260627000001`). |
| `update.rs` | 3.6 | Update primitive: `UpdateRegistry` (type-erased validators + async handlers), `BoxUpdateHandler`, `BoxUpdateValidator`. `WorkflowContext` methods: `register_update_handler`, `register_update_handler_no_validator`, `validate_update`, `execute_admitted_update`. `HistoryMatcher` methods: `match_update(update_id)`, `drain_admitted_updates()`. Error variants: `HarvestError::UpdateRejected`, `HarvestError::UpdateHandlerNotFound` |
| `query.rs` | 3.10 | Query registry: `QueryRegistry`, `QueryHandler`. `WorkflowContext` methods: `register_query` (no-arg), `register_query_handler<Req,Resp>` (typed), `execute_query_with_args`, `list_query_names`. Error variants: `QueryHandlerNotFound`, `WorkflowNotRunning`, `QueryHandlerPanicked`, `QueryTimedOut`. `WorkerConfig::query_timeout` (default 5 s). `telemetry::METRIC_QUERY_DURATION` constant. No `WorkflowEvent` variants — queries leave zero footprint in `harvest_events`. |
| `signal_handler.rs` | 3.42 | Push-based signal handler registry (issue #546): `SignalHandlerRegistry` (type-erased, synchronous, fire-and-forget, first-registration-wins), `BoxSignalHandler`, `invoke_signal_handler` (panic-safe invocation). `WorkflowContext` methods: `register_signal_handler<Req>` (typed), `register_signal_handler_raw` (untyped), `list_signal_handler_names`. Dispatch runs through `WorkflowContext::pump_signal_handlers` (triggered by `match_history`'s post-hook) and `HistoryMatcher::claim_pending_signal(name)`, a cursor-bound claim against `pending_signals` (populated by the same `prepare_match`/`drain_early_signals` sweep every other `match_*` call uses) — never a full-history scan — so a handler cannot fire ahead of an unconsumed activity/timer/etc. in history. Claims across all registered names are sorted by event index before dispatch. Marks claimed events consumed so `match_signal`/`wait_for_signal` never double-delivers the same event. No new `WorkflowEvent` variant — `SignalReceived` is reused. |
| `build_routing.rs` | 3.7 | Worker build-id routing: `BuildCompatibilitySet` (in-memory eligibility checker), `BuildPolicy`, `BuildCompatEntry`, `BuildReachability`. DB functions: `set_build_policy`, `get_build_policy`, `list_build_policies`, `declare_compat`, `revoke_compat`, `load_compat_set`, `build_reachability`, `all_build_reachability`, `all_build_reachability_sharded` (cross-shard fan-out), `merge_reachability`. New newtypes in `types.rs`: `BuildId`, `DeploymentName`. See `docs/runbooks/safe-deploy.md` for the operator deploy playbook. |
| `telemetry.rs` | 4 | OpenTelemetry surface: `TraceContextCarrier`, `TraceContextPropagator`, `MetricsRecorder`, `TelemetryConfig` — no-op by default, opt-in via `HarvestBuilder::telemetry`. Implements all 8 ADR-0001 span kinds (issue #136); see `docs/adr/0001-otel-trace-contract.md` for the full attribute schema and propagation rules. Metric catalogue (ADR-0001 §7): `harvest.workflow.started` (counter, `worker.rs`), `harvest.workflow.duration` (histogram, `worker.rs`), `harvest.workflow.terminal` (counter, `worker.rs`/`timeout.rs`/`execution.rs`, issue #519, labels: `workflow.name`, `queue`, `outcome` — 6 bounded values: completed/failed/cancelled/timed_out/terminated/continued_as_new), **Activity-outcome trio** (issue #528): `harvest.activity.duration` (histogram, `worker.rs`, labels: `activity`, `queue`, `status`), `harvest.activity.failed` (counter, `worker.rs`, richer labels: `activity`, `workflow.type`, `error.type`, `non_retryable` — per-attempt terminal/non-retryable failure signal), `harvest.activity.attempts` (counter, `worker.rs`, labels: `activity`, `queue`, `outcome` — 2 values: `completed`/`failed`; fires for **both** outcomes so success-rate = `attempts{outcome=completed}/attempts` within a single family), `harvest.activity.retries` (counter, `worker.rs` `handle_activity_result`, labels: `activity`, `queue`; fires once per retry actually enqueued after the `schedule_to_close` deadline check — retry-storm signal). `harvest.timer.started` (counter, `worker.rs`), `harvest.queue.depth` (gauge, `worker.rs` sampler), `harvest.queue.schedule_to_start` (histogram, `worker.rs` `dispatch_task` — recorded after the concurrency permit is acquired so it captures worker-local backpressure, issue #501, label: `queue`; wall-clock seconds from task eligibility to execution start, discounting the immediate-enqueue skew allowance via `queue::schedule_to_start_secs` — the canonical worker-capacity SLI), `harvest.queue.oldest_pending_age` (gauge, `worker.rs` sampler, issue #501, label: `queue`; age of oldest *claimable* eligible task — excludes PAUSED executions mirroring `claim_task`, skew-discounted; resets to 0 when queue drains), `harvest.dlq.entries` (gauge, `worker.rs` sampler), `harvest.worker.slots_in_use` / `harvest.worker.slots_available` (gauges, `worker.rs` `spawn_worker_slot_sampler`, issue #531, label: `slot_type` — `workflow`/`activity`; pure in-memory read of the two dispatch `Semaphore`s' `available_permits()` against the configured max, invariant `slots_in_use + slots_available == configured_max` per slot type within one sampler interval; `execution.id` is never a label), `harvest.schedule.runs` (counter, `scheduler.rs`), `harvest.schedule.skipped` (counter, `scheduler.rs`), `harvest.retention.deleted` (counter, `retention.rs`), `harvest.workflow.cache_hit` (counter, `worker.rs`, issue #235), `harvest.workflow.cache_miss` (counter, `worker.rs`, issue #235), `harvest.workflow.timeout` (counter, `timeout.rs`, issue #243), `harvest.workflow.sla_breached` (counter, `timeout.rs`, issue #487, labels: `workflow`, `queue`; observation-only, emitted exactly once per run on soft-SLA breach), `harvest.schedule.fire_attempts` (counter, `scheduler.rs`, issue #350, labels: `schedule`, `outcome`), `harvest.task.quarantined` (counter, `poison_pill.rs`, issue #367, labels: `queue`, `reason`), `harvest.activity.circuit.tripped` (counter, `worker.rs`, issue #369, label: `activity.name`), `harvest.activity.circuit.closed` (counter, `circuit_breaker.rs`, issue #369, label: `activity.name`), `harvest.workflow.debounced` (counter, `api.rs` plugin, issue #499, label: `workflow` — the debounce key is deliberately *not* a label, as it is derived from user/tenant input and would be unbounded), `harvest.workflow.debounce_fired` (counter, `debounce.rs` scanner, issue #499, labels: `workflow`, `queue`), `harvest.payload.offloaded` (counter+bytes-offloaded measure, `payload_store.rs`, issue #524, labels: `payload.field`, `store.id`; incremented once per offloaded field on write), `harvest.payload.offload_fetch_duration` (histogram, `payload_store.rs`, issue #524, label: `store.id`; records inflate latency on read). Cardinality rule: `execution.id` is span-only; `MetricsRecorder` API enforces this by construction. **Custom user metrics (issue #532):** three additive default no-op trait methods `record_user_counter`/`record_user_gauge`/`record_user_histogram`; `USER_METRIC_PREFIX = "harvest.user."` constant; `UserMetricError` enum (thiserror); `validate_user_metric(name, labels)` pure validation (reserved prefix, length, forbidden label keys, label cap); `UserMetrics<'a>` handle with suppression gate + `is_enabled()` short-circuit + validation + prefix; all re-exported from `lib.rs`. |
| `concurrency.rs` | 3.13 | Per-key concurrency limits (issue #247): `ConcurrencyPolicy { key_expr, limit }` attached to `WorkflowInfo`; `resolve_concurrency_key(expr, input)` resolves a dot-notation field path against the JSON input at workflow-start time. Limits enforced within a shard via the existing `concurrency_key`/`concurrency_cap` claim-query path. See `docs/sharding.md` for the cross-shard scope contract. |
| `debounce.rs` | 3.31 | Debounced workflow starts (issue #499): `DebouncePolicy { key_expr, window, max_wait }` (`Copy`, mirrors `ConcurrencyPolicy`); `resolve_debounce_key(expr, input)` delegates to `concurrency::resolve_concurrency_key`; `compute_fire_deadline(now, window, first_seen, max_wait)` pure deadline logic (no DB); DB-gated `admit_debounced_start` (upsert, trailing-edge, last-input-wins), `fire_due_debounced_starts` (scanner: claim, start, delete in one txn), `list_pending_debounce` (management API). Key scoping: shard-local. **Semantics**: trailing-edge, burst collapse, last-input-wins, max-wait cap (default 1h, overridable via `WorkerConfig::default_debounce_max_wait`). See `examples/debounce_webhook.rs`. |
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
| `ctx.wait_for_signal_timeout("approved", timeout)`            | `ctx.receive_signal_timeout::<Approval>("approved", timeout).await?` |

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

**Mechanics.** `WorkflowContext::register_signal_handler` (typed, `Req: Deserialize`) and `register_signal_handler_raw` (untyped `serde_json::Value`) store the handler in an in-memory `SignalHandlerRegistry` (`signal_handler.rs`) and trigger `WorkflowContext::pump_signal_handlers` (via `match_history`'s post-hook), which dispatches every registered handler whose target signal is currently **claimable**. "Claimable" is deliberately narrow: `HistoryMatcher::claim_pending_signal` only inspects `pending_signals`, the same stash `prepare_match`'s cursor-bound `drain_early_signals` sweep populates for every other `match_*` call (`match_activity`, `match_timer`, ...) — it can never reach ahead of wherever the workflow's own code-driven cursor progression has carried the matcher so far. Claims from every currently-registered handler name are collected and sorted by event index *before* any dispatch, so two differently-named handlers fire in true historical order relative to each other when a real command (an activity, timer, child workflow, etc.) separates their registration from the signals. **No new `WorkflowEvent` variant** — `SignalReceived` is reused, so the append-only invariant and adjacently-tagged JSON contract are untouched. Handlers are **fire-and-forget** and **synchronous**: no validator, no completion event, no suspension shape to reason about — a mutation of captured state is visible to the rest of the workflow body immediately, within the same cycle. A panicking handler is caught at the dispatch boundary and logged rather than propagating past that call; a payload that fails to deserialize under the typed variant is logged and dropped.

**History-order guarantee (post-ship hardening).** An earlier implementation drained *every* recorded `SignalReceived` event for a name at registration time regardless of cursor position — a handler registered at the top of a workflow function could fire on a signal recorded *after* an activity or timer the workflow hadn't reached yet in this replay cycle, silently reordering observable side effects relative to history (confirmed via code review on PR #890: `WorkflowStarted, ActivityScheduled, SignalReceived("cancel"), ActivityCompleted` could flip a cancel handler's state before the recorded activity replayed, causing strict-replay failures or a live worker completing from a state that never produced the recorded activity events). The fix threads dispatch through the same cursor-bound scan every other `match_*` call already uses (`claim_pending_signal`/`prepare_match`), so a handler cannot see a signal until the workflow's own code has actually driven the cursor past whatever precedes it in history. **Residual limitation:** if two differently-named handlers are registered on consecutive lines with *no* intervening command at all, and both names already have a recorded signal at that exact point, each registration's own eager dispatch can only sort claims among handlers that already exist — the second handler's earlier-recorded signal cannot retroactively be reordered ahead of the first handler's already-dispatched one, since there is no signal that "no more handlers are about to register" short of the workflow body reaching a real command. This is unavoidable without giving up same-cycle dispatch visibility for the (far more common) single-handler case. Once any real command separates registration from the signals, ordering across handler names is correct.

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
| `harvest_payload_refs` | (`blob_key`,`exec_id`) | Per-execution references to offloaded payload blobs (issue #524). `ON DELETE CASCADE` ties the refcount to execution lifetime so retention GCs a blob exactly when its last referencing execution is collected. Shard-local. |
| `harvest_debounce` | `Uuid` | Pending debounce records — one row per `(workflow_name, debounce_key)`. Upserted on each qualifying start (trailing-edge deadline update, `pending_count` increment, `last_input` overwrite). Deleted by the scanner after the corresponding execution is started. `UNIQUE (workflow_name, debounce_key)` is the collapse constraint; `effective_fire_at` index drives the scanner probe. Scoping: shard-local. |

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
