# autumn-harvest Architecture Deep Dive

Detailed internals of the event-sourced execution model, core components, data model,
and operational details.

## Event-Sourced Execution Model

The execution follows a 4-step cycle:

1. **First-time execution**: Workflow calls `ctx.execute_activity(...)`, activity is
   enqueued to Postgres task queue, workflow suspends.
2. **Activity execution**: Worker claims task (`SELECT ... FOR UPDATE SKIP LOCKED`),
   runs it, writes result as event in workflow history.
3. **Replay on resume**: Workflow resumes by replaying entire history from scratch
   (or from cache on same worker).
4. **Determinism guarantee**: Every non-deterministic decision is recorded as an event
   first time and read back from history on replay.

## Core Components

### 1. WorkflowContext (`context.rs`)

Passed to every workflow function. Behavior changes based on mode:

- **Replay mode**: Commands matched against recorded events, returns stored results
- **Live mode**: Commands emit `WorkflowCommand` and suspend the coroutine
- Uses `Mutex` for interior mutability (macro takes `&self`)

### 2. WorkflowExecutor (`executor.rs`)

Builds `WorkflowContext` from event history and polls the workflow handler until
it returns or blocks on Harvest futures (issue #1797). Returns one of:

- `Completed { output }` — handler returned Ok
- `Failed { error }` — handler returned Err
- `Suspended { commands }` — handler blocked on a parked Harvest future (a oneshot or a park token)
- `TaskFailed { error }` — handler waited on a foreign future past the deadlock timeout; the task is retried
- `ContinuedAsNew { input, new_workflow_type }` — handler called `continue_as_new`; the worker starts a fresh execution of the same workflow id with `input`; `new_workflow_type` names the successor type, or `None` keeps it

### 3. HistoryMatcher (`replay.rs`)

Walks through recorded `WorkflowEvent`s during replay:

- Matches each workflow command against history
- Returns a `HistoryMatch`, such as `Matched`, `Failed`, `TimedOut`, `NoMatch`, `Diverged` or `ActivityInProgress`
- Handles interleaved signals and child workflows

### 4. Worker Runtime (`worker.rs`)

`tokio::select!`-driven poll loop:

- Receives shutdown signal or polls task queue
- Claims tasks via `SELECT ... FOR UPDATE SKIP LOCKED`
- Runs up to `max_concurrent_claims` claims at once (default 2): a leader
  poll loop plus followers that claim only while claims return work
- Dispatches via bounded Tokio tasks:
  - `max_concurrent_workflows` for workflow tasks
  - `max_concurrent_activities` for activity tasks
- Separate connection pools with shared ceiling

### 5. Task Queue & Notifications (`queue.rs`, `notify.rs`)

- Postgres-backed queue with `SKIP LOCKED`
- `LISTEN/NOTIFY` for low-latency dispatch (no polling backoff needed)
- Dead letter queue for exhausted retries

### 6. DAG Scheduler (`scheduler.rs`)

- Declare DAGs of activities with trigger rules
- Cron/interval scheduling via `croner` crate
- Built-in scheduler dispatches runs

## Event Types (`event.rs`)

Events stored in the Postgres workflow history:

```
WorkflowStarted { input, timestamp }
ActivityScheduled { activity_id, name, input, queue }
ActivityStarted { activity_id, timestamp }
ActivityCompleted { activity_id, output }
ActivityFailed { activity_id, error, attempt }
ActivityTimedOut { activity_id, timeout_type }
ActivityHeartbeat { activity_id, timestamp }
TimerStarted { timer_id, duration_secs }
TimerFired { timer_id }
ChildWorkflowStarted { child_id, workflow_name, input }
ChildWorkflowCompleted { child_id, output }
ChildWorkflowFailed { child_id, error }
SignalReceived { signal_name, payload }
MarkerRecorded { name, details }
WorkflowCompleted { output }
WorkflowFailed { error }
```

## WorkflowCommand Types (`context.rs`)

Commands emitted during live (non-replay) execution:

```
ScheduleActivity { activity_id, name, input, queue, result_tx }
StartTimer { timer_id, duration_secs, result_tx }
StartChildWorkflow { child_id, workflow_name, input, result_tx }
RecordMarker { name, details }
WaitForSignal { signal_name, result_tx }
Complete { output }
Fail { error }
```

## Pool Management (`pool.rs`)

- Separate worker and web connection pools
- Shared ceiling prevents worker bursts from starving HTTP
- `compute_pool_sizes(cpu_count)` helper for auto-sizing

## Testing Support

With the `testing` feature:

- Integration tests use `testcontainers` for ephemeral Postgres
- `HistoryMatcher` enables deterministic replay verification
- Example: `autumn-harvest/tests/integration/integration_e2e.rs`

## Crate Metrics

- Diesel-managed schema changes embedded in `MIGRATIONS` const

## Dependencies

**Core**: tokio, serde_json, chrono, uuid, thiserror, tracing, croner

**Optional (`db` feature, on by default)**: deadpool, diesel, diesel-async,
diesel_migrations, percent-encoding, tokio-postgres

**Macros**: autumn-harvest-macros (proc macros for `#[workflow]`, `#[activity]`, `#[dag]`)

## Status

**Shipped**: DAG scheduling, signals, queries, updates and the management API.
Cancellation, cancellation scopes, sagas and sticky cross-worker routing also ship.
Metrics, OpenTelemetry spans and the Vantage dashboard UI ship too. Integration tests
cover each one. `docs/shipped-work.md` holds the shipped-work record.

API stability: pre-1.0. Breaking changes in minor versions per Cargo 0.x semver.
