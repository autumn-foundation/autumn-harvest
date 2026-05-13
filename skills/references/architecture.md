# autumn-harvest Architecture Deep Dive

Detailed internals of the 0.3.0 event-sourced execution model, command/event
model, runtime components, data model, testing support, and operator surfaces.

## Event-Sourced Execution Model

The runtime follows this loop:

1. **Live workflow execution**: A `#[workflow]` function calls a `WorkflowContext`
   API such as `execute_activity_raw`, `timer`, `wait_for_signal`,
   `spawn_child_workflow_raw`, `execute_activity_external`, or `continue_as_new`.
2. **Command drain**: Live workflow calls emit `WorkflowCommand`s and suspend.
   The worker drains commands, appends durable events, and schedules work.
3. **Worker execution**: Workers claim tasks with Postgres `SELECT ... FOR UPDATE
   SKIP LOCKED`, enforce queue/concurrency/build/shard eligibility, run handlers,
   write terminal events, and notify listeners.
4. **Replay on resume**: The workflow is re-invoked from the top. The
   `HistoryMatcher` returns recorded results for every command already present in
   history. New commands are appended only after replay reaches the history tail.
5. **Determinism guarantee**: Activity results, timer fires, signals, child
   workflow results, side effects, version branches, local activity results,
   external completion results, and continue-as-new decisions are all recorded
   before later runs can observe them.

## Core Components

### `WorkflowContext` (`context.rs`)

Replay-aware API passed to workflow functions.

- Replay mode: matches commands against recorded events and returns stored data.
- Live mode: emits `WorkflowCommand` values and suspends via oneshot channels.
- Tracks deterministic start time, history policy, replay strictness, cancellation,
  query/update registries, search-attribute patches, and typed shared state.
- Important author APIs: `execute_activity_raw`, `execute_local_activity_raw`,
  `execute_activity_external`, `timer`, `wait_for_signal`,
  `spawn_child_workflow_raw`, `version`, `side_effect`, `random_uuid`,
  `continue_as_new`, `upsert_search_attrs`, `register_query`,
  `register_update_handler`, `check_cancellation`.

### `ActivityContext` (`context.rs`)

Context passed to activity handlers.

- Exposes typed shared state, trace context, attempt number, stable idempotency
  key, heartbeat details from prior attempts, heartbeat writing, and cooperative
  cancellation checks.
- Local activities share the same handler shape but reject heartbeats at runtime.

### `HistoryMatcher` (`replay.rs`)

Walks `WorkflowEvent`s during replay.

- Matches activities, local activities, external activities, timers, signals,
  child workflows, markers/version gates, side effects, updates, and
  continue-as-new.
- Strict mode, used by `WorkflowReplayer`, also checks input payload changes.
- Returns `Matched`, `Failed`, `TimedOut`, `NoMatch`, `Diverged`,
  `AwaitingExternalCompletion`, `ChildInProgress`, and
  `LocalActivityInProgress`.

### `executor.rs`

Builds a `WorkflowContext` from event history and invokes the registered
workflow handler. Results are:

- completed output,
- failed error,
- suspended commands,
- non-determinism,
- cancellation,
- continue-as-new command.

### Worker Runtime (`worker.rs`)

Tokio runtime for workflow and activity tasks.

- Polls assigned queues/shards.
- Claims work with SKIP LOCKED.
- Enforces worker-level semaphores and activity-level `max_concurrent` /
  `concurrency_key` budgets.
- Honors build-id routing and worker drain/cancellation grace periods.
- Emits heartbeats/fleet status and samples queue/DLQ metrics.

### `queue.rs`, `notify.rs`, `store.rs`

- `store.rs`: append/load workflow history and related event queries.
- `queue.rs`: task enqueue/claim/complete/fail/wake operations.
- `notify.rs`: Postgres LISTEN/NOTIFY wrapper for low-latency workflow result
  waiting and worker wakeups.

### `scheduler.rs` and DAG Runtime

- `#[dag]` builders compile into `DagInfo`.
- `DagBuilder` models activity dependency graphs and trigger rules.
- Schedules support cron/interval/manual execution, catchup, and max active runs.
- Workflow schedules are separate: use `WorkflowSchedule` for "run this workflow
  on a cadence"; use DAGs for graph fan-out with trigger rules.

### `plugin` and API Runtime

`autumn-harvest-plugin` owns the Autumn integration.

- `HarvestPlugin::new()` wraps a `HarvestBuilder`.
- `.api(path)` mounts the management API and Vantage UI.
- `.api_with_auth(path, middleware)` protects the management API.
- Plugin startup applies Harvest and outbox migrations, starts `HarvestRunner`,
  installs `WorkflowHandleClient`, and starts the workflow start outbox relay.
- Plugin shutdown drains/stops the runner and background outbox task.

## Event Types

Events are stored in `harvest_events.event_data` as adjacently-tagged JSON:
`{"type":"Variant","data":{...}}`. Never rename old variants or change their
wire meaning.

Major event groups:

- Workflow lifecycle: `WorkflowStarted`, `WorkflowCompleted`, `WorkflowFailed`,
  cancellation/termination/reset/continue-as-new markers.
- Activity lifecycle: `ActivityScheduled`, `ActivityStarted`,
  `ActivityCompleted`, `ActivityFailed`, `ActivityTimedOut`,
  `ActivityHeartbeat`.
- Local activities: `LocalActivityScheduled`, `LocalActivityCompleted`,
  `LocalActivityFailed`.
- Timers/signals: `TimerStarted`, `TimerFired`, `SignalReceived`.
- Child workflows: `ChildWorkflowStarted`, `ChildWorkflowCompleted`,
  `ChildWorkflowFailed`.
- Replay markers: `MarkerRecorded`, version markers, side-effect markers.
- External activities: scheduled/waiting/completed/failed/timed-out task-token
  events.
- Updates: `UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed`.

Check `autumn-harvest/src/event.rs` for the canonical list before changing
serialization or replay behavior.

## WorkflowCommand Types

Commands emitted during live execution include:

```text
ScheduleActivity { activity_id, name, input, queue, result_tx }
RunLocalActivity { activity_id, name, input, retry_policy, start_to_close_secs, ... }
ScheduleExternalActivity { activity_id, token, name, input, queue, schedule_to_close_secs, result_tx }
StartTimer { timer_id, duration_secs, result_tx }
StartChildWorkflow { child_id, workflow_name, input, result_tx }
WaitForSignal { signal_name, result_tx }
RecordMarker { name, details }
RecordUpdateResult { update_id, result }
UpsertSearchAttributes { patch }
ContinueAsNew { input }
Complete { output }
Fail { error }
```

Command additions require matching event persistence, replay matching, tests, and
operator visibility where applicable. Tiny omissions here become runtime seances.

## Sharding

Harvest supports multiple independent Postgres shards without cross-shard
transactions.

- `ExecutionId::new_for_shard(ShardId)` embeds shard id in the UUID's first two
  bytes.
- `ExecutionId::shard()` routes reads in O(1).
- `ShardRouter` uses stable hashing for new workflow placement.
- `ShardedDbPool` maps shard id to pool and has `single(pool)` for the default
  deployment shape.
- Shard health/preflight check schema readiness, read/write reachability, worker
  coverage, scheduler freshness, queue/DLQ pressure, and reason codes.

Existing single-shard deployments behave unchanged. Add-shard flow: provision
and migrate DB, add as readable, run candidate readiness, then mark writable for
new workflows.

## Worker Build Routing and Draining

Build routing prevents workers running incompatible code from claiming old or new
tasks.

- Worker config: `with_build_id`, `with_deployment_name`,
  `with_shard_assignments`.
- Policy tables: `harvest_build_policies`, `harvest_build_compat`.
- Reachability APIs report whether active workers can process each assigned
  build.
- Drain controls move workers through active/draining/stopped and optionally wait
  for the terminal state.

Use the safe deploy runbook before changing workflow code in ways that require
worker compatibility gating.

## Data Model

Core tables include:

| Table | Purpose |
|-------|---------|
| `harvest_workflow_executions` | One row per workflow execution/run |
| `harvest_events` | Append-only event history; `BIGSERIAL` event id |
| `harvest_task_queue` | Workflow/activity task queue |
| `harvest_dead_letters` | Exhausted task failures and hard-cap failures |
| `harvest_timers` | Durable timers |
| `harvest_signals` | Pending/delivered signals |
| `harvest_dag_runs` | DAG run state |
| `harvest_schedules` | DAG/workflow schedules |
| `harvest_workers` | Worker fleet registry and heartbeat state |
| `harvest_build_policies` | Per-queue active build policy |
| `harvest_build_compat` | Build compatibility declarations |
| `harvest_external_tasks` | External task-token lookup/completion state |
| `harvest_audit_log` | Management mutation audit records |
| `harvest_backfill_log` | Schedule/backfill tracking |

Schema is Diesel-managed behind the default `db` feature and embedded in
`MIGRATIONS`.

## Testing Support

With `features = ["testing"]`:

- `WorkflowReplayer` validates workflow code against JSON history fixtures.
- `ReplayReport`, `ReplayStatus`, and `NonDeterminismKind` explain replay
  failures.
- `WorkflowContext::new_test()` and `ActivityContext::new_test()` are available
  outside unit-test cfg blocks.
- `harvest-replay` is the CLI-oriented replay validator shell.

Important tests:

- `tests/replayer_tests.rs` and `tests/replayer_integration_tests.rs`
- `tests/workflow_handle_tests.rs`
- `tests/det_check_tests.rs` and guardrail catalog tests
- plugin API integration tests for preflight, shard health, history export,
  version usage, reset, DLQ bulk operations, security, and UI
- `autumn-harvest-redis/tests/integration_redis.rs`

## Operator Surfaces

0.3.0 operator surfaces include:

- Management API under the configured plugin path.
- Vantage dashboard rendered with maud/autumn-web.
- `harvest` CLI: workflows, results, signals, updates, reset, stack, history
  export/batch export, version usage, version-gate retirement, preflight, shard
  health, workers, retention, schedules, concurrency, DLQ.
- Deployment preflight for release gates.
- Replay fixture export for workflow compatibility gates.
- Audited bulk DLQ replay/discard.
- External activity handoff completion/failure endpoints.
- Starter alert pack and runbooks.

## Dependencies

- Core async/runtime: tokio, tokio-util, futures, tracing.
- Serialization/types: serde, serde_json, uuid, chrono, base64.
- Database: diesel, diesel-async, diesel_migrations, deadpool, tokio-postgres.
- Scheduling/hash/cache: croner, seahash, lru.
- Macros: syn, quote, proc-macro2.
- Optional metrics bridge: metrics.
- Redis adapter: redis, async-trait.
- Test/bench: testcontainers, proptest, loom, criterion.

## Status

0.3.0 is pre-1.0 and follows Cargo 0.x semantics: minor releases may break API.
The current production-grade surface covers durable workflows, activities,
timers, signals, child workflows, DAGs, workflow schedules, replay testing,
query/update hooks, cancellation, saga primitives, sharding primitives, worker
fleet operations, build routing, history export/reset, preflight, telemetry, and
management UI/API coverage.
