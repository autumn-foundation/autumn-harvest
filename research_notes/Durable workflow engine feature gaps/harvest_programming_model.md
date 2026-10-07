# Autumn Harvest: programming model and runtime feature inventory (HEAD 301ea95, v0.7.0)

Scope: developer-facing workflow features, checked against source at HEAD 301ea95 (workspace version `0.7.0`, `Cargo.toml:30`). Every citation is a repository path plus line number. Paths are relative to `/home/user/autumn-harvest`. "Present" means implemented in code. "Documented" without code is called out. Status labels: **Present / Partial / Absent / Unclear**.

Note on the repo's own comparison docs: `docs/comparison.md` says competitor facts are "accurate as of 2026-07-14" (`docs/comparison.md:17`). Some of its Harvest cells are now stale. It says "Postgres only" (`docs/comparison.md:46`) and calls a TypeScript client "planned" (`docs/comparison.md:48,89`). At HEAD a Redis dispatch crate, an embedded SQLite crate and a TypeScript management-API client all exist (see sections 13 and 14). `docs/migrating-from-temporal.md:63-204` is the most accurate concept map and matches the code checked here.

## Q0. At-a-glance scorecard

### Takeaway
Harvest is a Rust-only, Temporal-shaped engine with an unusually wide surface. It covers almost every Temporal primitive and adds Inngest-style flow control (debounce, throttle, event batching, per-key concurrency), DBOS-style transactional steps, and many operator brakes. The clear **Absent** items are: non-Rust workflow/worker SDKs, Nexus-style cross-service RPC, Restate virtual objects and Azure durable entities, cancellation scopes, an in-workflow `waitForEvent`/pub-sub primitive, gRPC, and any LLM/tool-calling library in the engine itself.

### Cited Findings
Scorecard (detail and citations are in Q1–Q16):

| Feature | Status | Key evidence |
|---|---|---|
| Code-first workflows/activities via macros | Present | `autumn-harvest-macros/src/{workflow,activity}.rs`; `docs/architecture.md:466-485` |
| DAG workflows (`#[dag]`) | Present | `autumn-harvest-macros/src/dag.rs:37-98`; `autumn-harvest/src/dag.rs:1-5` |
| Local activities | Present | `autumn-harvest-macros/src/activity.rs:106`; `context.rs:8298` |
| Side effects, deterministic time/UUID/random | Present | `context.rs:5074,5304,5341,5355,5397,5491` |
| Durable timers, sleep-until, cancellable/resettable timers, business-day timers | Present | `context.rs:6411,6576,6923,6724,1306,1315` |
| Cancellation scopes | Absent | no `CancellationScope`/shield API in `context.rs` (grep) |
| Workflow cancel/terminate, propagation to children and activities | Present (cooperative) | `context.rs:4907-4960,15600-15650`; `types.rs:1234-1248` |
| Activity timeouts (s2c, s2s, start-to-close, heartbeat) | Present | `autumn-harvest-macros/src/activity.rs:74-86`; `timeout.rs:1-11` |
| Workflow run/execution/chain/task timeouts | Present | `execution.rs:54,80`; `worker.rs:228` |
| Signals (pull + push handlers), signal-with-start | Present | `context.rs:8825-9372,13474,13513`; `autumn-harvest-plugin/src/api.rs:4847` |
| Queries | Present | `context.rs:12884,12927`; `query.rs:1-12` |
| Updates with validators, update-with-start | Present | `context.rs:13241,13292`; `api.rs:4851,4922` |
| Child workflows, parent close policy, fan-out, detached children | Present | `context.rs:8371,8171,11082`; `types.rs:1234` |
| Continue-as-new (incl. cross-type) | Present | `context.rs:12562,12701,12735` |
| Reset / rewind, rerun, workflow-level retry | Present | `reset.rs:80-90,792`; `execution.rs:169`; `/workflows/{id}/rerun` |
| Saga helper | Present | `saga.rs:119,179,276` |
| Async activity completion (task token) | Present | `context.rs:12398-12422`; `external_task.rs:1-7` |
| Durable promises / awakeables as first-class objects | Partial (via external activity, signals, `await_condition`) | `context.rs:9019,12422` |
| Durable mutex | Present (shard-local) | `context.rs:7125`; `mutex.rs:1-25` |
| Semaphore primitive | Partial (per-key concurrency limit only) | `concurrency.rs:1-25` |
| Versioning: `version`, `patched`, `deprecate_patch` | Present | `context.rs:5512,5652,5712` |
| Worker build-ID routing + percent ramp | Present | `build_routing.rs:1-6`; `docs/migrating-from-temporal.md:165-166` |
| Replay testing / replay verifier / canary | Present | `testing.rs:410,1412,3739,1773` |
| Schedules: cron, interval, TZ, overlap, catchup, backfill, pause, jitter, calendars | Present (no `AllowAll` overlap) | `policy.rs:731,815-831,984-995`; `calendar.rs:1-13` |
| Delayed start | Present | `execution.rs:131-135` |
| Rate limits, per-key concurrency, debounce, throttle, event batching, priority, queue weights, quotas | Present | sections Q7 |
| Start on event (webhook, Kafka, SQS, completion trigger) | Present | `webhook_trigger.rs`; `docs/getting-started/13-broker-connectors.md`; `docs/completion-triggers.md` |
| In-workflow `waitForEvent` / general pub-sub | Absent | no matches for `wait_for_event` in src (grep) |
| Streaming progress | Present (ephemeral, SSE) | `context.rs:13640-13675`; `docs/streaming-progress.md:111-129` |
| Payload offload (claim-check) | Present (embedder supplies the store) | `payload_store.rs:1-25` |
| Payload codecs / AES-GCM encryption / key rotation | Present | `payload_codec.rs:1-10`; `aead_codec.rs:1-22` |
| Search attributes, memo, list/filter | Present | `context.rs:5002`; `execution.rs:55-56`; `docs/search-attributes.md:165-190` |
| Nexus / cross-namespace RPC | Absent | `docs/migrating-from-temporal.md:185-191` |
| Virtual objects / durable entities | Absent | no source matches (grep) |
| Batch signal/cancel/terminate/reset/start | Present | `batch.rs:1-16`; `/workflows/batch_reset`, `/workflows/batch_start` |
| Non-Rust SDK (author/worker) | Absent; WASM activities are an R&D spike | `docs/comparison.md:255-261`; `docs/rnd/wasm-activities-spike.md:3` |
| Non-Rust client | Partial (TypeScript management-API client only) | `clients/typescript/package.json:2-4` |
| REST API / OpenAPI | Present (174 operations) | `docs/openapi.json` |
| gRPC API | Absent | no `tonic`/`grpc` in src (grep) |
| Postgres backend | Present (full) | `docs/comparison.md:67` |
| Redis | Partial (dispatch channel only; state stays in Postgres) | `autumn-harvest-redis/src/lib.rs:1-27` |
| SQLite | Partial (single-writer subset) | `docs/sqlite-backend.md:415-433` |
| Test env with mocks and auto-firing timers | Present | `testing.rs:5240-5265,5469-5868` |
| AI: MCP tool exposure | Present | `docs/mcp-tools.md:1-50` |
| AI: LLM-call / tool-calling library | Absent in engine; example only | `examples/claude-agent-daemon/README.md:1-20` |

### Inferences
- The gap profile differs from Temporal's. Harvest has most Temporal primitives. It lacks polyglot SDKs, Nexus and multi-region active-active. It also lacks the Restate/Azure keyed-state models.
- Harvest exceeds Temporal in flow control (debounce, throttle, batching, quotas), operator brakes (queue/activity pause, admission gates, circuit breakers) and determinism tooling.

### Gaps
- The repository has no machine-readable feature matrix. This scorecard comes from code and docs, not from tests run in this session.

## Q1. Workflow definition model, activities, local activities, side effects, deterministic primitives

### Takeaway
**Present.** Workflows are `async fn`s annotated with `#[workflow]` that take `&WorkflowContext`. Activities are `#[activity]` functions. Determinism is enforced by event-sourced replay plus compile-time guardrails. Deterministic time, UUID, random and side-effect primitives exist.

### Cited Findings
- Macros: `#[workflow]`, `#[activity]`, `#[dag]`, `#[query]`, `#[update]`, `#[signal]`, `#[webhook]` live in `autumn-harvest-macros/src/{workflow,activity,dag,query,update,signal,webhook}.rs` (directory listing).
- The `#[activity]` attribute keys are `retry`, `start_to_close`, `heartbeat_timeout`, `schedule_to_start`, `schedule_to_close`, `queue`, `max_concurrent`, `concurrency_key`, `local`, `max_input_bytes`, `max_result_bytes`, `rate_limit_rps/burst/key`, nested `rate_limit(key, rps, burst)`, `circuit_breaker` and `requires` (capability labels) — `autumn-harvest-macros/src/activity.rs:68-242`.
- The `#[workflow]` attribute keys are `execution_timeout`, `chain_execution_timeout`, `sla`, `concurrency(key, limit, on_conflict)`, `debounce(key, window, max_wait)`, `batch(key, max_size, max_wait)`, `throttle(rate, burst, key, schedule_to_start)`, `quota(key, max_active_executions, max_history_bytes, max_dead_letters)`, `allow_nondeterministic_apis`, `max_input_bytes`, `owner`, `runbook`, `severity`, `description`, `retry`, `mcp`, `activities` and `children` — `autumn-harvest-macros/src/workflow.rs:337-690`; summarised in `docs/architecture.md:471-485`.
- `WorkflowContext` is the deterministic context — `autumn-harvest/src/context.rs:2752`. Activity calls: `execute_activity` `context.rs:8236`, `execute_activity_with_opts` `:8259`, raw variants `:5903,5918`.
- Local activities: `#[activity(local = true)]` (`autumn-harvest-macros/src/activity.rs:106`); `execute_local_activity` `context.rs:8298`, `_with_opts` `:8326`, raw `:6111`. Maps to Temporal Local Activity (issue #98) — `docs/migrating-from-temporal.md:164`.
- Side effects and deterministic values: `side_effect(id, f)` `context.rs:5074`; `system_now` `:5304`; `system_time_now` `:5321`; `new_uuid` `:5341`; `random_u64` `:5355`; `random_f64` `:5373`; `random_range` `:5397`; `random_uuid(id)` `:5491` (issue #384, `docs/migrating-from-temporal.md:156`). `ctx.now()` returns replay-stable workflow time — `context.rs:4124`.
- Deterministic race/select over mixed awaitables: `ctx.race()` builder with `.activity/.child_workflow/.timer/.signal` branches and `.run()` — `context.rs:1661-1780` (issue #600). `futures::join!` is the sanctioned wait-all form — `docs/migrating-from-temporal.md:133`.
- Activity fan-out helpers, including windowed (bounded in-flight) variants: `context.rs:10362-10933`.
- Worker sessions pin a sequence of activities to one worker (Temporal Go `CreateSession` analogue): `create_session` `context.rs:12309`; `sessions.rs:1-18` (issue #606).
- Transactional activities (DBOS-like): `ActivityContext::run_transactional` commits a domain write atomically with `ActivityCompleted` — `context.rs:15778`; `docs/transactional-activities.md:1-12`. Transactional workflow start: `WorkflowHandleClient::start_workflow_transactional` `handle.rs:762`; `docs/transactional-start.md:1-10` (issue #763).
- Determinism guardrails HVG001–HVG011, the `det_check` static analyzer, and ND-blocking that parks divergent runs instead of failing them — `docs/comparison.md:111,214-231`; macro lint `autumn-harvest-macros/src/determinism_lint.rs`.
- Workflow logger, user metrics and headers inside the workflow: `log_info/warn/error` `context.rs:4846-4860`; `metrics()` `:4500`; `header()` `:4509`.
- Resident (cached) workflow state avoids a full replay per decision, for a narrow set of single-await suspensions — `autumn-harvest/src/resident.rs:1-25` (issue #1798).
- Typed client handles: `TypedWorkflowHandle<T>` — `autumn-harvest/src/handle_typed.rs:46`.

### Inferences
- Explicit string IDs are required for timers and side effects (`timer(id, secs)`, `side_effect(id, f)`). This is more verbose than Temporal, which sequences them implicitly.

### Gaps
- I did not verify which HVG rules are hard errors and which are warnings. See `docs/workflow-determinism-guide.md`.

## Q2. Timers, cancellation, timeouts

### Takeaway
**Timers: Present.** This includes cancellable/resettable timers and business-day timers. **Cancellation: Present but cooperative.** Cancellation propagates to children through the parent close policy and to activities through a cancellation token or a heartbeat/durable check. **Cancellation scopes: Absent.** **Timeouts: Present** at both activity and workflow level.

### Cited Findings
- `timer(timer_id, duration_secs: u64)` takes whole seconds — `context.rs:6411`. `sleep_until(timer_id, deadline)` — `:6576`. `start_timer` returns a `TimerHandle` with `cancel()`, `reset()` and `await_fire()` — `:6923,1306,1315,1325` (issue #768). Free functions `cancel_timer`/`reset_timer` — `:7316,7387`.
- Business-day timers: `business_days_from_now` `context.rs:6649`; `timer_business_days` `:6724` (issue #806). Known open bug #1772: business-day timers land on weekends for two calendars — `research_notes/Autumn Harvest resilience gap analysis/codebase_map_known_gaps.md` (open-issues list).
- `await_condition(pred)` and `await_condition_timeout(...)` (timeout in whole seconds plus a timer ID) — `context.rs:9019,9030`; `docs/migrating-from-temporal.md:101-104`.
- Workflow cancellation: `is_cancelled()` reads a `WorkflowCancelled` event (`context.rs:4907-4920`); `cancellation_reason()` `:4927`; `check_cancellation()` `:4960`. HTTP `POST /workflows/{id}/cancel` and `/terminate` (`docs/openapi.json`); `WorkflowHandle::cancel` `handle.rs:1743`, `terminate` `:1773`.
- Activity cancellation is cooperative. `ActivityContext::is_cancelled()` checks the worker token, then a throttled durable check. **It is a no-op for local activities** — `context.rs:15600-15650`. `check_cancellation` `:15634`.
- Cross-workflow cancel: `request_cancel_external_workflow(_by_id)` — `context.rs:9769,9835`.
- Parent close policy: `Abandon`, `RequestCancel` (default; cooperative, "no hard-kill guarantee"), and `Terminate` (force-fail without compensations) — `types.rs:1234-1250`.
- No cancellation-scope API. A grep for `CancellationScope|cancel_scope|shield|non_cancellable` finds only one WASM-internal comment (`wasm_activities.rs:722`). The old plan doc names Temporal cancellation scopes as a gap — `docs/plans/vantage-spec-cancellation-semantics.md:13-15`.
- Activity timeouts: `start_to_close`, `heartbeat_timeout`, `schedule_to_start`, `schedule_to_close` — `autumn-harvest-macros/src/activity.rs:74-86`. The scanner enforces heartbeat, start-to-close and schedule-to-start timeouts — `timeout.rs:1-11`. Remaining-time helpers: `ActivityContext::time_remaining` `context.rs:15237`, `is_expiring_within` `:15260`.
- Heartbeats with details, plus auto-heartbeat: `heartbeat` `context.rs:15356`; `heartbeat_details` `:15303`; `start_auto_heartbeat` `:15461`.
- Workflow timeouts: per-run `execution_timeout` (`execution.rs:54`); `chain_execution_timeout` across a continue-as-new chain (`execution.rs:80`), equivalent to Temporal's execution timeout versus run timeout; operator ceilings (`execution.rs:74,87`); `workflow_task_timeout` on the worker (`worker.rs:228`); soft `sla` (`execution.rs:151`). Replay-stable `ctx.deadline()` — `context.rs:4165`.
- Child-or-deadline helpers: `spawn_child_workflow_timeout` `context.rs:8474`; `execute_child_workflow_timeout` `:8764`.

### Inferences
- Without scopes, an author cannot shield cleanup code from cancellation, or cancel only a sub-tree of awaits. The `TimerHandle` and `race()` APIs cover some of these patterns.

### Gaps
- I did not verify whether a cancelled activity that ignores cancellation is force-stopped after a grace period. The cancellation spec lists that as a criterion (`vantage-spec-cancellation-semantics.md:21`).

## Q3. Signals, queries, updates, with-start

### Takeaway
**Present and broad.** Pull and push signals, typed queries, updates with validators, signal-with-start and update-with-start all exist, both in-process and over HTTP. Signal delivery can be idempotent.

### Cited Findings
- Pull signals: `receive_signal` `context.rs:8825`; `try_receive_signal` `:8888`; `drain_signals` `:8926`; `wait_for_signal` `:9064`; `wait_for_signal_timeout` `:9196`; `receive_signal_timeout` `:9372`.
- Push signal handlers: `register_signal_handler_raw` `context.rs:13474`; typed `register_signal_handler` `:13513`. Handlers are synchronous and fire-and-forget, and are re-dispatched every replay cycle — `signal_handler.rs:1-25` (issue #546).
- External signals with an idempotency key: `signal_external_workflow(_by_id)(_with_idempotency)` — `context.rs:9426-9555`. HTTP `Idempotency-Key` on signal delivery (issues #521/#753) — `docs/migrating-from-temporal.md:87`.
- Queries: `register_query` `context.rs:12884`; typed `register_query_handler` `:12927`; read-only, with no history footprint — `query.rs:1-12`. HTTP `GET /workflows/{id}/query/{query_name}` (openapi). `WorkflowHandle::execute_query_in_process` `handle.rs:2313`.
- Updates: `register_update_handler(name, validator, handler)` `context.rs:13241`; `_no_validator` `:13263`; `validate_update` `:13292`; `execute_admitted_update` `:13333`; registry `update.rs:1-6` (issue #140). HTTP `POST /workflows/{id}/update/{update_name}` plus `GET .../update/{update_id}/result` — `autumn-harvest-plugin/src/api.rs:4922-4924`. In-process `execute_update_in_process` `handle.rs:2478`. `all_handlers_finished()` / `unfinished_update_handler_count()` `context.rs:4382,4395`.
- Signal-with-start (`POST /workflows/{workflow_name}/signal-with-start`) and update-with-start (`.../update-with-start`) — `api.rs:2464,2625,4847-4851` (issues #244, #479).
- Declarative `#[query]`/`#[update]` macros, MCP-exposable via `#[update(..., mcp)]` — `autumn-harvest-macros/src/update.rs:48`.

### Inferences
- Harvest matches Temporal's message-passing surface. Signal deduplication by key goes further than Temporal.

### Gaps
- On the SQLite backend, updates and push signal handlers are unsupported (see Q14).

## Q4. Composition: child workflows, continue-as-new, reset, retries, sagas, external completion, human-in-the-loop

### Takeaway
**Present.** Children (attached, detached, fan-out, cross-shard opt-in), continue-as-new (including cross-type), reset to several reset-point kinds, rerun, workflow-level retry policy, a `Saga` helper and task-token external completion all exist. Human-in-the-loop waits are built from signals or updates plus deadlines, or from external activities. There is no dedicated "durable promise" object.

### Cited Findings
- Children: `spawn_child_workflow` `context.rs:8371`; placed variant `:8393`; detached `:8171`; fan-out `:11082-11379` (issue #601). Placement: `ChildPlacement` (default `ParentShard`) — `shard.rs:3910-3917`. Cross-shard children use an outbox row and pull-based terminal notify, at-least-once with dedupe — `cross_shard_child.rs:1-22` (issue #956).
- Awaiting a non-child workflow's result: `await_external_workflow` — `context.rs:9993`.
- Continue-as-new: `continue_as_new(input)` `context.rs:12562`; cross-type `continue_as_new_as_type` `:12701`, `continue_as_new_as` `:12735` (issue #803); `should_continue_as_new()` history-budget hint `:4593`. Run-chain view `run_chain.rs:1-20` (issue #701). Carry-over between runs: `last_completion_result()` / `last_error()` `context.rs:4426,4448`.
- Reset: `ResetPoint::{EventId, FirstActivityRun, LastWorkflowTask}` — `reset.rs:80-90`. Signal re-apply policy `Drop|Buffer` — `reset.rs:29-40`. `preview_workflow_reset` `:757`; `reset_workflow_execution` `:792`; batch reset `:1074` and `POST /workflows/batch_reset`. Replay-with-reset test helper `testing.rs:1563`. DAG retry-from-failed-node (`/dags/{dag_name}/runs/{run_exec_id}/retry`, openapi).
- Workflow retry: `workflow_retry_policy`, `workflow_attempt`, `retry_of_exec_id` on start params — `execution.rs:164-171`; `#[workflow(retry = ...)]` (issue #523) — `autumn-harvest-macros/src/workflow.rs:679`; `POST /workflows/{id}/rerun` (openapi). `RetryPolicy` fields: `max_attempts`, `initial_interval`, `backoff_coefficient`, `max_interval`, `non_retryable_errors`, `jitter` — `policy.rs:156-215`.
- Saga: `Saga<'ctx>` `saga.rs:119`; `step(forward, compensate)` `:179`; `compensate_all` `:276`; docs `docs/saga.md`; saga metrics (issue #801) `docs/shipped-work.md:87`. A choreography example is in `examples/saga-choreography`.
- Async activity completion: `execute_activity_external(name, input, ...)` suspends without holding a worker slot until `POST /activities/external/{token}/complete|fail` (heartbeat route too) — `context.rs:12398-12422`; `external_task.rs:1-7`; admin listing `/admin/external-handoffs` (openapi).
- Human-in-the-loop: the MCP doc presents "human approval gate" workflows built on signals, updates and timers — `docs/mcp-tools.md:1-10,25-27`. The agent daemon parks on "a durable signal with a deadline" for tool approval — `examples/claude-agent-daemon/README.md:13-16`. DAG approval nodes branch on a signal body — `autumn-harvest/src/dag.rs:951-958`.
- Durable mutex: `ctx.mutex(key).acquire()` returns a `MutexGuard` — `context.rs:7125,1372`. It is **shard-local**, with lease plus fencing token — `mutex.rs:10-25` (issue #691).
- Per-execution pause/resume (stable; experimental in Temporal) — `docs/migrating-from-temporal.md:176`; `/workflows/{id}/pause|resume` (openapi).

### Inferences
- Restate awakeables and Inngest `waitForEvent` map most closely to `execute_activity_external` (token-addressed) and to signals. Neither is correlation-by-event-payload.

### Gaps
- I did not confirm whether the Saga helper runs compensations in parallel or only in reverse order. Module docs say reverse order (`saga.rs:3-5`).

## Q5. Versioning, deploy safety, replay testing

### Takeaway
**Present and deep.** It has `GetVersion`-style `version()`, `patched`/`deprecate_patch`, worker build-ID routing with compatibility sets and percent ramp, workflow-type reachability checks, replay harnesses, canaries and drift gates. Hot code swap of workflow logic through WASM exists only as an **R&D spike** behind a feature flag.

### Cited Findings
- `version(change_id, min, max)` `context.rs:5512`; `patched(id)` `:5652`; `deprecate_patch(id)` `:5712` (issue #687).
- Build-ID routing sends tasks only to workers whose build is compatible with the starting build — `build_routing.rs:1-6` (issue #171). Percent ramp (issue #604) — `docs/migrating-from-temporal.md:166`. Admin routes `/admin/build-routing/{compat,ramp,retire,policies}` (openapi). Version-gate usage and retirement checks: `/admin/version-gates/usage`, `/retirement-check`; `version_usage.rs`, `version_gate_retirement.rs`.
- Workflow-type reachability gates safe handler removal (issue #520) — `docs/comparison.md:122`.
- Replay testing: `WorkflowReplayer` `testing.rs:410` with `replay_from_events` `:1412`, `replay_from_json` `:1627`, `replay_from_db` `:1643`, `replay_bundle` `:960`, `run_canary` `:1773`. `ReplayVerifier` batch `verify_all/verify_dir` `:3739,4139,4157`. CI report exit codes `:2672-2713`. Docs: `docs/replay-verify.md`, `docs/replay-drift-gate.md`, `docs/replay-debugger.md`. Admin replay canary `/admin/workflows/replay-canary` (openapi).
- Hot code swap: "R&D spike, behind the `hot-code-swap` Cargo feature. Not a committed GA feature" — `hot_swap.rs:1-6`; feature `autumn-harvest/Cargo.toml:74`.

### Inferences
- Versioning parity with Temporal is effectively complete. The extra tooling (reachability, ND-parking, drift gate) goes beyond Temporal OSS.

### Gaps
- None material.

## Q6. Schedules, cron and delayed start

### Takeaway
**Present.** Schedules support cron (UTC or IANA timezone with DST rules), interval and manual triggers, overlap policies, catchup policies, backfill, pause/resume, jitter, holiday calendars, bounded runs, in-place update and run history. The only Temporal overlap mode missing is `AllowAll`. Delayed start is supported.

### Cited Findings
- `Schedule::{Cron, Interval, Manual, CronInTimezone}` with DST handling — `policy.rs:731-770`; `chrono-tz` and `croner` dependencies (`autumn-harvest/Cargo.toml:108,119`).
- `OverlapPolicy::{Skip (default), BufferOne, BufferAll, CancelOther, TerminateOther}` — `policy.rs:815-831`. There is no `AllowAll` variant (grep of `policy.rs`). `max_active_runs` exists — `policy.rs:1130`.
- `CatchupPolicy::{SkipAll, MostRecent, Window(Duration), Unbounded}` — `policy.rs:984-995` (issue #484).
- Schedule fields: jitter, calendar plus `SkipPolicy`, `consecutive_failure_limit`, `end_at`, `max_runs`, `retry_policy`, `execution_timeout` — `scheduler.rs:2706-2737`; `policy.rs:1108-1184`.
- Calendars (named excluded-date sets, calendar-aware backfill) — `calendar.rs:1-13,623-635` (issue #337).
- Admin routes: `/admin/schedules/{id}/{pause,resume,trigger,backfill,preview,runs,decisions}` and `PATCH /admin/schedules/{id}` (openapi; issue #771). HA-safe multi-replica ticks (issue #350) — `docs/comparison.md:133`. Known residual: the BufferOne/BufferAll drain has a double-dispatch risk — `codebase_map_known_gaps.md` citing `docs/runbooks/ha-deployment.md:157-161`.
- `#[dag(schedule, catchup, max_active_runs, jitter, ...)]` — `autumn-harvest-macros/src/dag.rs:37-98`.
- Delayed start: `start_at`, `delay`, and the operator ceiling `max_workflow_start_delay` — `execution.rs:131-135`.

### Inferences
- Scheduling depth is at or above Temporal's, except for `AllowAll`. It is clearly above Restate, Inngest and DBOS.

### Gaps
- None material.

## Q7. Concurrency control and flow control

### Takeaway
**Present and wide.** Per-key workflow concurrency has a latest-wins option. Per-activity rate limits use a DB token bucket. Activity concurrency can be per worker or per key. There are debounce, throttle, event batching, 4-level priority, weighted queue fairness, tenant quotas, an adaptive concurrency limit, retry budgets, circuit breakers, admission gates, and queue/activity pause. Mutexes are shard-local. There is no general counting-semaphore primitive inside a workflow. Tenant "fairness keys" are approximated by key-based limits; weighted fairness is per queue only.

### Cited Findings
- Per-key workflow concurrency `ConcurrencyPolicy::new("input.tenant_id", 10)`, enforced fleet-wide in the `SKIP LOCKED` claim — `concurrency.rs:1-20`. `ConcurrencyOnConflict::{Defer (default), CancelRunning}` — `concurrency.rs:50-57` (issue #811). Cross-shard global limits are out of scope — `codebase_map_known_gaps.md` (sharding limits).
- Activity rate limits: `rate_limit_rps/burst/key` and nested `rate_limit(key = "input.tenant_id", rps, burst)` (issue #699) — `autumn-harvest-macros/src/activity.rs:130-201`. Buckets live in the `harvest_rate_limit_buckets` table — `models.rs:19,659`. Admin overrides `/admin/rate-limits/...` (openapi).
- Activity `max_concurrent` and `concurrency_key` — `autumn-harvest-macros/src/activity.rs:94-102`.
- Debounce (trailing edge, `max_wait`) — `debounce.rs:1-25` (issue #499). Throttle (rate, burst, key; defers, never drops) — `throttle.rs:1-25` (issue #607). Event batching `batch(key, max_size, max_wait)` buffers payloads into one run — `event_batch.rs:21-25,119,802`; `autumn-harvest-macros/src/workflow.rs:448-470`; batched redelivery caveat `docs/shipped-work.md:4796-4805`. A deferred start returns 202 with no execution id — `clients/typescript/README.md` ("A debounce, batch or throttle policy can defer the start").
- Priority `Low/Normal/High/Critical` (claim order) — `types.rs:1399-1414`; `execution.rs:123`. Weighted queue selection, no starvation — `queue_fairness.rs:1-30` (issue #515).
- Quotas on active executions, history bytes and DLQ size per tenant key — `quota.rs:12-21` (issue #946). Tenant isolation cells ADR — `docs/adr/0004-tenant-isolation-cells.md:1-12`.
- Adaptive per-activity-type concurrency limit (gradient algorithm) — `adaptive_limit.rs:1-25` (issue #1836). Retry budget token bucket — `retry_budget.rs:1-20` (issue #1793). Circuit breaker `Defer|FailFast` — `circuit_breaker.rs:1-18` (issues #369, #1809).
- Operator brakes: admission gates — `admission_gate.rs:1-25` (#377/#618); queue pause — `queue_pause.rs:1-17` (#619); activity-type pause — `activity_pause.rs:1-16` (#807).
- Task queues: per-activity `queue`, worker routing, sticky routing, capability labels (`requires`) — `autumn-harvest-macros/src/activity.rs:90,242`; `docs/sticky-routing.md`; `docs/getting-started/09-worker-routing.md`.
- Mutex is shard-local — `mutex.rs:10-16`. Grep finds no semaphore API in `context.rs`.

### Inferences
- The flow-control set is closer to Inngest's (debounce, throttle, batch, concurrency keys, priority) than to Temporal's, and adds operator brakes that few competitors have.

### Gaps
- I did not verify whether the activity rate limit is strictly fleet-wide in every dispatch mode, for example under Redis dispatch.

## Q8. Event-driven triggers, webhooks, pub/sub

### Takeaway
**Start-on-event: Present** through inbound webhooks, Kafka/SQS connectors and declarative completion triggers. Outbound completion callbacks and outbound webhooks exist. **In-workflow `waitForEvent` with payload matching, and a general pub/sub bus: Absent.** Signals address a specific workflow.

### Cited Findings
- Inbound webhooks: `#[webhook(path, starts|signals, signal_name, queue, ...)]` — `autumn-harvest-macros/src/webhook.rs:53-73`. Signature verification is delegated to autumn-web `SignedWebhook` — `webhook_trigger.rs:1-25` (issue #344).
- Broker connectors: Kafka (`rdkafka`) and SQS in `autumn-harvest-plugin` behind the `connectors`, `kafka` and `sqs` features. They handle idempotent redelivery, ack ordering, poison isolation and backpressure — `docs/getting-started/13-broker-connectors.md:1-30`; `autumn-harvest-plugin/src/connector/`.
- Completion triggers start a target workflow on a source's terminal state, exactly once after dedupe — `docs/completion-triggers.md:1-30`.
- Completion callbacks are HMAC-signed, SSRF-guarded POSTs on terminal state — `completion_callback.rs:1-15` (issue #605). The outbound webhook feature is in the plugin — `webhook_trigger.rs:15-17`.
- No `wait_for_event`/`waitForEvent` in src (grep: 0 files). `publish_progress` is an ephemeral output stream, not a durable bus (Q9).

### Inferences
- Inngest-style "wait for an event matching expression X" must be emulated: route the event to a known workflow ID with signal-with-start, or a webhook `signals` binding.

### Gaps
- None material.

## Q9. Streaming, large payloads, codecs and encryption

### Takeaway
**Present.** Ephemeral progress streaming over SSE, claim-check payload offload (the embedder supplies the store), pluggable payload codecs, an AES-256-GCM codec with env/file/KMS key providers, and in-place key-rotation re-encryption all exist. Payload size caps are enforced by default.

### Cited Findings
- `ctx.publish_progress(chunk)` is never recorded or replayed, ordered by `seq`, and capped at `PROGRESS_CHUNK_MAX_BYTES` (oversize chunks become a truncation marker) — `context.rs:13640-13675` (issue #791). SSE route `/workflows/{id}/stream` — `docs/streaming-progress.md:111-129`. Chunks can drop under back-pressure — `docs/streaming-progress.md:96`. Operator engine-event SSE tail `/executions/{exec_id}/events/stream` (#324) — `docs/streaming-progress.md:49`. `set_current_details` — `context.rs:13601`.
- Claim-check offload to an embedder-supplied `PayloadStore`. Core ships no S3/GCS client — `payload_store.rs:1-25` (issue #524). Default behaviour is to reject payloads over the cap (issue #252) — `payload_store.rs:4-5`. Per-activity and per-workflow caps: `max_input_bytes`/`max_result_bytes` — `autumn-harvest-macros/src/activity.rs:110-120`; `workflow.rs:647`.
- `PayloadCodec` trait — `payload_codec.rs:1-25`. `AeadCodec` (AES-256-GCM) with `EnvKeyProvider`, `FileKeyProvider` and `KmsKeyProvider` (AWS KMS through the plugin `aws-kms` feature) — `aead_codec.rs:1-22` (issue #1825). Re-encryption sweep (issue #948) — `CLAUDE.md` exception #3; `/admin/codec/rotation` (openapi).
- PII erasure `POST /workflows/{id}/erase-payloads` and legal hold (openapi; `erase.rs`, issue #495).

### Inferences
- The streaming feature fits LLM token streaming. It is best-effort, not durable.

### Gaps
- No built-in object-store adapter. Users must implement `PayloadStore` themselves.

## Q10. Search attributes, memo, visibility and list APIs

### Takeaway
**Present.** Search attributes are JSONB with a GIN index and can be upserted from workflow code. Memo is set at start. List and count endpoints support comparison and set predicates. There is no SQL-like visibility query language and no nested-key filters.

### Cited Findings
- `upsert_search_attrs` — `context.rs:5002`. `memo` and `search_attrs` start params — `execution.rs:55-56`; client builder `with_memo`/`with_search_attrs` — `handle.rs:1454,1461`.
- Comparison table: search attributes are indexed and mutable; memo is unindexed and immutable — `docs/search-attributes.md:8-13`. Constraints: keys of 64 characters or fewer, values must be scalar (no objects or arrays), reserved keys — `:104-113`.
- `GET /workflows?search_attr_filter=key:op:value` with `op ∈ {eq, ne, gt, gte, lt, lte, in, exists}`. Only top-level keys are filterable. Predicates are ANDed (no OR across keys) — `docs/search-attributes.md:165-190` (issue #506). `BatchFilter` is reused by batch operations and the UI — `:132-147`.
- Other list/visibility routes: `/workflows/count`, `/workflows/summaries`, `/workflows/{id}/timeline`, `/tree`, `/children`, `/awaitables`, `/stack`, `/diagnose`, `/triage` (openapi). Open-awaitables projection — `awaitables.rs:1-23` (issue #615).

### Inferences
- The filter model is less expressive than Temporal's List Filter (no OR, no ORDER BY clauses). It needs no Elasticsearch.

### Gaps
- I did not check ordering and pagination parameters for `GET /workflows` in detail.

## Q11. Cross-service calls, RPC services, virtual objects, durable entities

### Takeaway
**Absent.** There is no Nexus-like cross-namespace RPC, no Restate-style services or virtual objects with keyed state, and no Azure durable entities. Harvest has no namespace concept. Keyed serialization can be approximated with a mutex, `concurrency(limit = 1)`, signal-with-start on a deterministic workflow ID, and queries.

### Cited Findings
- "Nexus ... Harvest has no cross-service RPC primitive. Every primitive ... operates within one harvest deployment" — `docs/migrating-from-temporal.md:185-191`.
- Grep for `nexus` in src finds 0 files. Grep for `virtual.?object|durable.?entit` finds no feature code (matches are unrelated uses of "actor" in `poison_pill.rs`).
- The comparison doc names Restate's "Virtual Objects / durable-RPC model" as a reason to choose Restate — `docs/comparison.md:351-358`.
- Building blocks that exist: shard-local mutex `mutex.rs:10-16`; per-key concurrency `concurrency.rs`; signal-with-start `api.rs:4847`; `await_external_workflow` `context.rs:9993`; cross-workflow signal and cancel `context.rs:9426-9835`.

### Inferences
- A comparison report should mark these as genuine model gaps, not just missing sugar.

### Gaps
- No issue number for a planned Nexus or entity feature was found.

## Q12. Batch operations

### Takeaway
**Present.** Durable batch jobs handle signal, cancel and terminate over a filter. There are also batch reset, batch start (hard-capped), and DLQ bulk replay/discard.

### Cited Findings
- `harvest_batch_jobs`: a background executor applies per-target actions, and per-target failures do not abort the batch — `batch.rs:1-16` (issue #102). Routes `/batch-operations`, `/batch-operations/{id}` (openapi).
- `POST /workflows/batch_reset` — `reset.rs:1074`. `POST /workflows/batch_start` with a hard cap — `batch_start.rs:1-7` (issue #357).
- DLQ: `/dead-letters/{replay,discard,aggregate}`, `/dlq/redrive` (openapi).

### Inferences
- This is at parity with or beyond Temporal batch operations.

### Gaps
- I did not verify which `BatchFilter` fields batch terminate supports.

## Q13. SDKs, clients and APIs (polyglot story)

### Takeaway
**Workflow and activity authoring is Rust only (Absent for other languages).** Non-Rust systems are callers only. They can use a REST management API (174 operations, OpenAPI), a generated **TypeScript management-API client** (shipped 0.7.0, not on npm), MCP tools, and published JSON Schemas. WASM "polyglot activities" are an R&D spike. There is no gRPC and no Python client.

### Cited Findings
- "Harvest ships a Rust SDK only ... No bridge or interop layer exists" — `docs/migrating-from-temporal.md:198-200`; `docs/comparison.md:89,255-261`. Planned items cited there: TS activity-worker SDK #959, and TS plus Python management clients #955.
- `clients/` contains only `typescript/`. The package `autumn-harvest-client` v0.7.0 is "generated from docs/openapi.json (issue #1616)" — `clients/typescript/package.json:2-4`. It is distributed as a GitHub release tarball, not on the npm registry — `clients/typescript/README.md:8-18`. No Python client exists (directory listing).
- OpenAPI: `docs/openapi.json` holds 174 `operationId`s (grep count). Workflow input/output JSON Schema routes are `/workflows/registered/{name}/schema` and `/interface` (openapi; issue #373).
- No gRPC: grep for `tonic|grpc` in src finds only "monotonic" false positives.
- WASM activities are "R&D spike, behind the `wasm-activities` Cargo feature. Not a committed GA feature" — `docs/rnd/wasm-activities-spike.md:3`; runtime `wasm_activities.rs:1-12`. Heartbeat into the guest "remains the gap" — `docs/rnd/wasm-activities-spike.md:120`. Signed modules — `wasm_signing.rs` (HEAD commit message).
- Rust CLI `autumn-harvest-cli` (`debug.rs`, `tui.rs`); embedding on plain Axum via `HarvestEmbedding` (0.7.0+) — `docs/embedding.md:1-15`.

### Inferences
- The `comparison.md` row that marks TS as "planned" is stale for the *management client* but still true for a worker SDK.

### Gaps
- No status found for #959 or #955 beyond the comparison doc text.

## Q14. Storage backends

### Takeaway
**Postgres is the full backend**, with optional sharding and cross-region DR. **Redis is Partial.** It provides a Streams dispatch channel that offloads claim reads, and a standalone task queue, while all workflow state stays in Postgres. **SQLite is Partial.** It is an embedded single-writer edge runtime that supports a subset of primitives and rejects the rest loudly.

### Cited Findings
- Postgres: task queue via `SKIP LOCKED`, wakeups via LISTEN/NOTIFY — `docs/comparison.md:67`. Sharding and cross-region DR (operator-driven failover, no automatic promotion) — `docs/comparison.md:155,262-267`.
- Redis: `RedisDispatch` carries task references only, and "Postgres keeps every `harvest_task_queue` row". Workflow state, history, signals, timers, schedules and DAGs are out of scope — `autumn-harvest-redis/src/lib.rs:1-27`. Earlier notes list v1 limits (single node, no Redis Cluster, best-effort priority) — `codebase_map_known_gaps.md` (Redis dispatch section).
- SQLite: single writer enforced by an OS lock, `BEGIN IMMEDIATE`, polling instead of NOTIFY, real-clock timers, histories byte-identical to Postgres — `autumn-harvest-sqlite/src/lib.rs:1-60`. Signals are pull-only; push handlers and drain APIs are unavailable — `docs/sqlite-backend.md:300-333`. Unsupported, run ends FAILED with `UnsupportedFeature`: child workflows, external signals/cancels, local activities, external/task-token activities, updates, search attributes, continue-as-new, worker sessions, cancellable timers — `docs/sqlite-backend.md:415-433`. Also out of scope: schedules, management API, DAGs, retention, sharding — `autumn-harvest-sqlite/README.md:105-116`.

### Inferences
- "Postgres-only" in `comparison.md:46` is now imprecise, but still true in substance: only Postgres supports the full feature set.

### Gaps
- None material.

## Q15. Testing framework

### Takeaway
**Present.** It includes an in-process `WorkflowTestEnv` with activity and child mocks, retry-attempt mocks, pre-queued signals, simulated cancellation, auto-firing timers on a simulated clock, and a post-run replay check. Replay harnesses, a workflow simulator and a deterministic-simulation-testing (DST) harness for the claim protocol are also present. Gaps: signals can only be pre-queued, not injected mid-run, and there is no documented time-skipping test server against a real database.

### Cited Findings
- `WorkflowTestEnv` runs "without Postgres, workers, or Docker. Activities are satisfied by registered closures; timers auto-fire; signals are injected from a pre-queued list; child workflows are stubbed" — `testing.rs:5240-5265`. Methods: `mock_activity` `:5469`, `mock_activity_attempt` `:5492`, `mock_activity_retries` `:5541`, `mock_child_workflow` `:5562`, `queue_signal` `:5577`, `with_mutex_contended` `:5588`, `with_external_await_result/failure` `:5596,5609`, `with_cancellation` `:5636`, `with_business_calendars` `:5666`, `with_last_completion_result` `:5695`, `run` `:5868`. `TestRunOutcome::replay_check` `:5223` and `elapsed()` `:5198`. Simulated `simulated_now` clock — `testing.rs:5339-5342`. `WorkflowContext::with_advancing_timer_clock` — `context.rs:3847`. Activity test contexts `ActivityContext::new_test*` — `context.rs:15988-16111`.
- Simulator — `simulator.rs:1-5`. DST, seed-driven, differential against Postgres — `dst/mod.rs:1-12` (issue #1830). Further test docs: `docs/testing/{simulation,loom,shuttle,property-and-fuzz,chaos,formal-methods}.md`.
- SQLite `*_as_of` drivers give sleep-free deterministic timer tests — `autumn-harvest-sqlite/src/lib.rs:52-55`.

### Inferences
- The unit-test story is comparable to Temporal's `TestWorkflowEnvironment` time-skipping. Query and update injection in `WorkflowTestEnv` was not found.

### Gaps
- I did not confirm whether `WorkflowTestEnv` supports updates or queries. The method list at `testing.rs:5469-5868` shows none.

## Q16. AI and agent-specific features

### Takeaway
**Partial.** `#[workflow(mcp)]` exposes workflows (start, watch, steer through updates) as MCP tools. `publish_progress` supports token streaming. An example daemon runs Claude agent loops as durable workflows. The engine has no LLM-call activity library, tool-calling abstraction, agent-loop primitive, or MCP *client*.

### Cited Findings
- MCP tool exposure: `#[workflow(mcp, description = ...)]`, `#[update(..., mcp)]`, plugin `.mcp_tools()`, served via autumn-web `mount_mcp("/mcp")` — `docs/mcp-tools.md:1-50`; `autumn-harvest-plugin/src/mcp_tools.rs` (issue #597).
- Token streaming use case — `docs/streaming-progress.md:3-5`; `context.rs:13640-13644`.
- `examples/claude-agent-daemon`: agent sessions as durable workflows on the SQLite backend. The model call is an activity with retry. Tool approval is a durable signal with a deadline. The offline stub model is used when no API key is set — `examples/claude-agent-daemon/README.md:1-30`; `src/claude.rs`, `src/tools.rs`.
- Grep for `llm|anthropic|openai|tool_call` in `autumn-harvest/src` and `autumn-harvest-plugin/src` matches only `mcp_tools.rs`. There is no LLM integration in the engine.
- Claim-check offload names RAG ingestion as a target workload — `payload_store.rs:6-8`.

### Inferences
- Versus Inngest AgentKit, Restate/Temporal AI SDK integrations, and DBOS/Temporal OpenAI Agents SDK integrations, Harvest provides primitives and an example, not an agent framework.

### Gaps
- No roadmap issue for LLM or agent primitives was found in the files read.
