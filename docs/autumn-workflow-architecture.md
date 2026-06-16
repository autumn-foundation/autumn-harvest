# Autumn Harvest — Architecture Design Document

**Workflow Orchestration Engine for the Autumn Ecosystem**

*Version 0.1 — Draft*
*March 2026*

**Release status note (0.4.0):** DAG scheduling, signals, queries, the
management API, dead-letter list/replay endpoints, durable workflow
cancellation, pause/resume controls, DLQ aggregation, DAG retry from failed
nodes, timezone-aware cron schedules, scaling signals, and metrics endpoints
are implemented. First-class Saga compensation is implemented through the
`Saga` builder.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Name and Identity](#2-name-and-identity)
3. [Crate Organization](#3-crate-organization)
4. [User-Facing API](#4-user-facing-api)
5. [Execution Model](#5-execution-model)
6. [DAG Scheduler](#6-dag-scheduler)
7. [Worker Model](#7-worker-model)
8. [Persistence Schema](#8-persistence-schema)
9. [Task Queues](#9-task-queues)
10. [Failure Handling](#10-failure-handling)
11. [Signals and Queries](#11-signals-and-queries)
12. [Integration with Autumn](#12-integration-with-autumn)
13. [Scalability](#13-scalability)
14. [Configuration Schema](#14-configuration-schema)
15. [Architecture Diagrams](#15-architecture-diagrams)
16. [Implementation Roadmap](#16-implementation-roadmap)

---

## 1. Executive Summary

Autumn Harvest is a workflow orchestration engine that combines Airflow's DAG scheduling model with Temporal's durable execution guarantees, implemented natively in Rust as a companion to the Autumn web framework. It provides code-as-workflow definitions through proc macros, Postgres-backed persistence using the same diesel-async + deadpool stack as Autumn, and production-grade features including sharding, retries, timeouts, heartbeats, signals, and queries.

The core thesis: workflows should be defined in Rust with the same ergonomics as Autumn routes and tasks, scheduled like Airflow DAGs, and executed with Temporal's durability guarantees — all without requiring external brokers like Redis, RabbitMQ, or a separate Temporal server. Postgres is the only infrastructure dependency.

---

## 2. Name and Identity

**autumn-harvest** is the right name. In the Autumn metaphor, a harvest is the culmination of orchestrated work — planting, tending, and collecting results across time. This maps directly to workflow orchestration: defining work, scheduling it, executing steps in order, and gathering results. The name also suggests batch processing and scheduled operations, which are core to the crate's purpose.

Alternative considered: `autumn-orchard` (too pastoral, doesn't suggest execution), `autumn-mill` (too industrial), `autumn-weave` (suggests threading, not orchestration). Harvest wins on clarity and metaphor alignment.

---

## 3. Crate Organization

### Workspace Structure

Autumn Harvest introduces three new crates to the Autumn workspace, mirroring the existing pattern:

```
autumn/
├── autumn-web/              # existing — core web framework
├── autumn-macros/           # existing — proc macros for web
├── autumn-cli/              # existing — CLI binary
├── autumn-harvest/          # NEW — workflow engine core library
├── autumn-harvest-macros/   # NEW — proc macros for workflows
└── autumn-harvest-ui/       # NEW (Phase 3) — optional dashboard binary
```

### Why Separate Crates (Not Extending Existing)

Workflow orchestration is a heavyweight dependency. Many Autumn users will never need it, and bundling it into `autumn-web` would bloat compile times and pull in event-sourcing machinery for simple CRUD apps. Separate crates let users opt in:

```toml
# Cargo.toml — only when you need workflows
[dependencies]
autumn-web = "0.5"
autumn-harvest = "0.4"
```

The `autumn-harvest-macros` crate is separate from `autumn-macros` because proc-macro crates cannot export non-macro items. Harvest macros generate different companion functions (`__autumn_workflow_info_*`, `__autumn_activity_info_*`) that need their own expansion logic.

### Crate Responsibilities

**autumn-harvest** (lib crate):

- Workflow and activity trait definitions
- Event sourcing engine (history, replay, determinism checker)
- DAG scheduler (topological sort, trigger rules, timetables)
- Worker runtime (poller, executor, heartbeat manager)
- Task queue implementation (Postgres-backed)
- Signal/query dispatch
- Retry policies and timeout enforcement
- Diesel migrations for harvest tables
- `HarvestBuilder` fluent API
- Reexports module for diesel-async, deadpool, etc.

**autumn-harvest-macros** (proc-macro crate):

- `#[workflow]` attribute macro
- `#[activity]` attribute macro
- `#[dag]` attribute macro
- `#[signal]` and `#[query]` attribute macros
- `workflows![]`, `activities![]`, `dags![]` bang macros

**autumn-harvest-ui** (binary crate, Phase 3):

- Axum-based dashboard for workflow monitoring
- Built on Autumn itself (dogfooding)
- Optional — not required for operation

### Dependency Graph

```
autumn-harvest-macros ──► autumn-harvest ──► autumn-web
        │                       │                │
        └── syn, quote,         └── diesel-async  └── axum 0.8
            proc-macro2             deadpool           tokio 1
                                    chrono
                                    serde
                                    uuid
```

`autumn-harvest` depends on `autumn-web` for `AppState`, `AutumnResult`, `AutumnError`, `Db`, config layer, and the `AppBuilder` extension point. This is a hard dependency — Harvest is not designed to work without Autumn.

---

## 4. User-Facing API

### 4.1 Defining Activities

Activities are the atomic units of work — functions that can fail, be retried, and interact with the outside world. They map to Temporal's activity concept and Airflow's task operators.

```rust
use autumn_harvest::prelude::*;

#[activity]
async fn send_email(ctx: &ActivityContext, to: String, subject: String, body: String) -> AutumnResult<EmailId> {
    // Activities can do I/O, call external services, etc.
    // They receive an ActivityContext for heartbeating and accessing shared state.
    ctx.heartbeat("sending email").await?;

    let client = ctx.state::<EmailClient>()?;
    let id = client.send(&to, &subject, &body).await?;

    Ok(id)
}

#[activity(
    retry = RetryPolicy::exponential(3, Duration::from_secs(1)),
    start_to_close = "30s",
    heartbeat_timeout = "10s",
    queue = "email-workers"
)]
async fn send_bulk_email(ctx: &ActivityContext, batch: Vec<EmailRequest>) -> AutumnResult<Vec<EmailId>> {
    let mut results = Vec::new();
    for (i, req) in batch.iter().enumerate() {
        ctx.heartbeat(format!("sending {}/{}", i + 1, batch.len())).await?;
        let id = ctx.state::<EmailClient>()?.send(&req.to, &req.subject, &req.body).await?;
        results.push(id);
    }
    Ok(results)
}
```

#### Per-activity concurrency caps

Workflows commonly integrate with rate-limited downstream services (Stripe,
OpenAI, SendGrid, etc.). Without a cap the cluster can fan out many parallel
calls, hitting 429s or connection pool limits.

Declare `max_concurrent = N` on the activity to enforce a cluster-wide cap:

```rust
// At most 5 Stripe calls in flight at any instant across the whole cluster.
#[activity(start_to_close = "30s", max_concurrent = 5)]
async fn charge_stripe(ctx: &ActivityContext, amount_cents: u64) -> HarvestResult<String> {
    // …
}
```

When multiple activities share a single rate-limited dependency, give them the
same `concurrency_key` so the budget is shared rather than doubled:

```rust
#[activity(start_to_close = "30s", max_concurrent = 5, concurrency_key = "stripe")]
async fn charge_stripe(ctx: &ActivityContext, amount_cents: u64) -> HarvestResult<String> { /* … */ }

#[activity(start_to_close = "10s", max_concurrent = 5, concurrency_key = "stripe")]
async fn refund_stripe(ctx: &ActivityContext, charge_id: String) -> HarvestResult<()> { /* … */ }
```

`HarvestBuilder::build()` fails fast with
`HarvestBuilderError::ConcurrencyKeyMismatch` if activities sharing a key
disagree on `max_concurrent`. The cap is enforced by the Postgres claim query
without any extra table, background task, or dedicated worker process; tasks
whose key is saturated are simply not claimed until a slot becomes free.

### 4.2 Defining Workflows (Durable Execution)

Workflows are deterministic orchestration functions. They call activities, set timers, wait for signals, and maintain durable state through event sourcing. This is the Temporal-style model.

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding_workflow(ctx: &WorkflowContext, user_id: UserId) -> AutumnResult<OnboardingResult> {
    // Step 1: Create account (activity call — recorded in event history)
    let account = ctx.execute_activity(create_account, user_id.clone()).await?;

    // Step 2: Send welcome email
    ctx.execute_activity(send_email, SendEmailRequest {
        to: account.email.clone(),
        subject: "Welcome!".into(),
        body: format!("Hello {}", account.name),
    }).await?;

    // Step 3: Wait up to 7 days for email verification (durable timer)
    let verified = ctx.select(
        ctx.wait_for_signal::<EmailVerified>("email_verified"),
        ctx.timer(Duration::from_days(7)),
    ).await;

    match verified {
        Either::Left(signal) => {
            ctx.execute_activity(activate_account, account.id).await?;
            Ok(OnboardingResult::Activated)
        }
        Either::Right(_timeout) => {
            ctx.execute_activity(send_reminder, account.email).await?;
            Ok(OnboardingResult::TimedOut)
        }
    }
}
```

### 4.3 Defining DAGs (Scheduled Pipelines)

DAGs are the Airflow-style model — directed acyclic graphs of activities with dependency edges, scheduled on timetables. Under the hood, a DAG compiles to a workflow that executes activities according to the dependency graph.

```rust
use autumn_harvest::prelude::*;

#[dag(
    schedule = "0 2 * * *",        // daily at 2 AM
    catchup = false,
    max_active_runs = 1,
    default_queue = "etl-workers",
)]
fn daily_etl(dag: &mut DagBuilder) {
    let extract_users = dag.activity(extract_users_from_api)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));

    let extract_orders = dag.activity(extract_orders_from_db)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));

    let transform = dag.activity(transform_data)
        .upstream(&extract_users)
        .upstream(&extract_orders);  // runs after BOTH extracts complete

    let load = dag.activity(load_to_warehouse)
        .upstream(&transform)
        .start_to_close("10m");

    let notify = dag.activity(send_slack_notification)
        .upstream(&load)
        .trigger_rule(TriggerRule::AllDone);  // runs even if load failed
}
```

### 4.4 Signals and Queries

```rust
#[workflow]
async fn order_workflow(ctx: &WorkflowContext, order_id: OrderId) -> AutumnResult<OrderResult> {
    // Register a query handler — external systems can read state
    let status = ctx.state_cell("pending");
    ctx.register_query("get_status", {
        let status = status.clone();
        move || status.get().clone()
    });

    // Process the order...
    status.set("processing");
    ctx.execute_activity(charge_payment, order_id.clone()).await?;

    status.set("shipping");
    ctx.execute_activity(create_shipment, order_id.clone()).await?;

    // Wait for cancellation signal or shipment confirmation
    let result = ctx.select(
        ctx.wait_for_signal::<CancellationRequest>("cancel"),
        ctx.wait_for_signal::<ShipmentConfirmed>("shipped"),
    ).await;

    match result {
        Either::Left(cancel) => {
            // Compensate: refund and cancel shipment
            ctx.execute_activity(refund_payment, order_id.clone()).await?;
            ctx.execute_activity(cancel_shipment, order_id.clone()).await?;
            Ok(OrderResult::Cancelled(cancel.reason))
        }
        Either::Right(_shipped) => {
            status.set("completed");
            Ok(OrderResult::Delivered)
        }
    }
}
```

### 4.5 Macro Expansion

The `#[workflow]` macro generates a companion function following Autumn's pattern:

```rust
// User writes:
#[workflow]
async fn onboarding_workflow(ctx: &WorkflowContext, user_id: UserId) -> AutumnResult<OnboardingResult> {
    // ...
}

// Macro generates:
async fn onboarding_workflow(ctx: &WorkflowContext, user_id: UserId) -> AutumnResult<OnboardingResult> {
    // ... (original function body, unchanged)
}

#[doc(hidden)]
pub fn __autumn_workflow_info_onboarding_workflow() -> ::autumn_harvest::WorkflowInfo {
    ::autumn_harvest::WorkflowInfo {
        name: "onboarding_workflow",
        module: module_path!(),
        input_schema: <UserId as ::autumn_harvest::WorkflowInput>::schema(),
        output_schema: <OnboardingResult as ::autumn_harvest::WorkflowOutput>::schema(),
        handler: |ctx, input| {
            Box::pin(async move {
                let user_id: UserId = ::autumn_harvest::deserialize_input(input)?;
                let result = onboarding_workflow(ctx, user_id).await?;
                ::autumn_harvest::serialize_output(result)
            })
        },
    }
}
```

The `#[activity]` macro generates similarly:

```rust
// User writes:
#[activity(retry = RetryPolicy::exponential(3, Duration::from_secs(1)), start_to_close = "30s")]
async fn send_email(ctx: &ActivityContext, to: String, subject: String, body: String) -> AutumnResult<EmailId> {
    // ...
}

// Macro generates:
async fn send_email(ctx: &ActivityContext, to: String, subject: String, body: String) -> AutumnResult<EmailId> {
    // ... (original body)
}

#[doc(hidden)]
pub fn __autumn_activity_info_send_email() -> ::autumn_harvest::ActivityInfo {
    ::autumn_harvest::ActivityInfo {
        name: "send_email",
        module: module_path!(),
        default_retry_policy: Some(RetryPolicy::exponential(3, Duration::from_secs(1))),
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: None,
        handler: |ctx, input| {
            Box::pin(async move {
                let (to, subject, body) = ::autumn_harvest::deserialize_input(input)?;
                let result = send_email(ctx, to, subject, body).await?;
                ::autumn_harvest::serialize_output(result)
            })
        },
    }
}
```

The `#[dag]` macro generates a `DagDefinition` that is a pre-compiled representation of the dependency graph:

```rust
#[doc(hidden)]
pub fn __autumn_dag_info_daily_etl() -> ::autumn_harvest::DagInfo {
    ::autumn_harvest::DagInfo {
        name: "daily_etl",
        module: module_path!(),
        schedule: Some(::autumn_harvest::Schedule::Cron("0 2 * * *".parse().unwrap())),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("etl-workers"),
        builder: |dag| { daily_etl(dag); },
    }
}
```

The bang macros collect these:

```rust
// In main.rs or lib.rs:
use autumn_harvest::prelude::*;

let workflows = workflows![onboarding_workflow, order_workflow];
let activities = activities![send_email, send_bulk_email, create_account, charge_payment];
let dags = dags![daily_etl, weekly_report];
```

### 4.6 AppBuilder Integration

```rust
use autumn_web::prelude::*;
use autumn_harvest::prelude::*;

#[tokio::main]
async fn main() -> AutumnResult<()> {
    AppBuilder::new()
        .routes(routes![...])
        .tasks(tasks![...])
        // Harvest extensions:
        .workflows(workflows![onboarding_workflow, order_workflow])
        .activities(activities![send_email, create_account, charge_payment])
        .dags(dags![daily_etl])
        .worker(WorkerConfig::default()
            .queues(["default", "email-workers", "etl-workers"])
            .max_concurrent_activities(50)
            .max_concurrent_workflows(20)
        )
        .harvest_api("/api/harvest")  // optional management HTTP endpoints
        .run()
        .await
}
```

---

## 5. Execution Model

### 5.1 Event Sourcing for Durable Execution

Every workflow execution is backed by an append-only event history stored in Postgres. When a workflow calls `ctx.execute_activity(...)`, the engine does not simply call the activity function. Instead, it:

1. Appends an `ActivityScheduled` event to the history.
2. Enqueues the activity task on the appropriate task queue.
3. Suspends the workflow coroutine.
4. When the activity completes, appends `ActivityCompleted` (or `ActivityFailed`) to the history.
5. Resumes the workflow with the result.

If the worker crashes and the workflow must resume on a different worker, the engine replays the event history: it re-executes the workflow function from the beginning, but instead of actually scheduling activities, it reads the recorded results from history. The workflow code runs deterministically to the same point, receives the same results, and continues from where it left off.

### 5.2 Event Types

```rust
pub enum WorkflowEvent {
    // Lifecycle
    WorkflowStarted { input: serde_json::Value, timestamp: DateTime<Utc> },
    WorkflowCompleted { output: serde_json::Value },
    WorkflowFailed { error: String },
    WorkflowCancelled { reason: String },

    // Activities
    ActivityScheduled { activity_id: ActivityExecId, name: String, input: serde_json::Value, queue: String },
    ActivityStarted { activity_id: ActivityExecId, worker_id: WorkerId },
    ActivityCompleted { activity_id: ActivityExecId, output: serde_json::Value },
    ActivityFailed { activity_id: ActivityExecId, error: String, attempt: u32 },
    ActivityTimedOut { activity_id: ActivityExecId, timeout_type: TimeoutType },
    ActivityHeartbeat { activity_id: ActivityExecId, details: serde_json::Value },

    // Timers
    TimerStarted { timer_id: TimerId, duration: Duration },
    TimerFired { timer_id: TimerId },

    // Signals
    SignalReceived { signal_name: String, payload: serde_json::Value },

    // Child workflows
    ChildWorkflowStarted { child_id: WorkflowExecId, workflow_name: String, input: serde_json::Value },
    ChildWorkflowCompleted { child_id: WorkflowExecId, output: serde_json::Value },
    ChildWorkflowFailed { child_id: WorkflowExecId, error: String },

    // Markers (user-defined checkpoints)
    MarkerRecorded { name: String, details: serde_json::Value },
}
```

### 5.3 Deterministic Replay

The `WorkflowContext` operates in two modes:

**Normal mode** (no history to replay): Commands from the workflow code (schedule activity, start timer, etc.) generate new events and enqueue work.

**Replay mode** (resuming from history): Commands from the workflow code are matched against the existing event history. If the workflow calls `ctx.execute_activity(send_email, ...)` and the history contains a matching `ActivityScheduled` followed by `ActivityCompleted`, the context returns the recorded result immediately without scheduling anything.

```rust
// Simplified replay logic inside WorkflowContext::execute_activity
async fn execute_activity<A: Activity>(&self, activity: A, input: A::Input) -> AutumnResult<A::Output> {
    let command = Command::ScheduleActivity {
        name: A::NAME,
        input: serialize(&input)?,
    };

    match self.history.match_command(&command) {
        HistoryMatch::Matched { events } => {
            // Replay: return recorded result without executing
            match events.last() {
                Some(WorkflowEvent::ActivityCompleted { output, .. }) => {
                    Ok(deserialize(output)?)
                }
                Some(WorkflowEvent::ActivityFailed { error, .. }) => {
                    Err(AutumnError::ActivityFailed(error.clone()))
                }
                _ => Err(AutumnError::NonDeterministic("unexpected event during replay"))
            }
        }
        HistoryMatch::NoMatch => {
            // Normal: schedule activity and suspend
            let activity_id = self.next_activity_id();
            self.record_event(WorkflowEvent::ActivityScheduled {
                activity_id,
                name: A::NAME.into(),
                input: serialize(&input)?,
                queue: A::QUEUE.unwrap_or("default").into(),
            }).await?;
            self.enqueue_activity_task(activity_id).await?;
            self.suspend_until(activity_id).await
        }
        HistoryMatch::Diverged => {
            // History doesn't match — non-determinism detected
            Err(AutumnError::NonDeterministic(
                "workflow code diverged from recorded history"
            ))
        }
    }
}
```

### 5.4 Determinism Constraints

Workflow functions must be deterministic. The `#[workflow]` macro will emit a compile-time warning (and runtime detection) for common non-deterministic patterns:

**Prohibited in workflows** (enforced at runtime via the context):

- Direct I/O (network, file, database) — use activities instead
- `std::time::Instant::now()` or `SystemTime::now()` — use `ctx.now()` which returns the replayed timestamp
- `rand::random()` — use `ctx.random()` which returns seeded values from history
- `tokio::spawn` — use `ctx.execute_activity` or `ctx.spawn_child_workflow`
- `std::thread::sleep` — use `ctx.timer(duration)`

**The `WorkflowContext` provides deterministic alternatives** for every non-deterministic operation. If a workflow attempts to use `tokio::time::sleep` directly, the engine panics with a clear error message explaining to use `ctx.timer()` instead.

### 5.5 DAG Execution as Workflows

DAGs defined with `#[dag]` compile to a generated workflow function. The `daily_etl` DAG from Section 4.3 compiles to roughly:

```rust
async fn __dag_workflow_daily_etl(ctx: &WorkflowContext, run: DagRunInput) -> AutumnResult<DagRunOutput> {
    let graph = build_daily_etl_graph();  // topologically sorted
    let mut results: HashMap<String, TaskResult> = HashMap::new();

    for level in graph.levels() {
        // Execute all tasks in this level concurrently
        let futures: Vec<_> = level.tasks()
            .filter(|task| task.trigger_rule.should_run(&results))
            .map(|task| {
                let input = task.prepare_input(&results, &run);
                ctx.execute_activity_dynamic(task.activity_name, input)
            })
            .collect();

        let level_results = futures::future::join_all(futures).await;
        for (task, result) in level.tasks().zip(level_results) {
            results.insert(task.name.clone(), result);
        }
    }

    Ok(DagRunOutput { task_results: results })
}
```

This means DAG runs get the same durability guarantees as workflows — if a worker crashes mid-DAG, the run resumes from the last completed task, not from scratch.

---

## 6. DAG Scheduler

### 6.1 Scheduling Loop

The DAG scheduler runs as a background Tokio task within the Autumn application. It operates on a configurable tick interval (default: 30 seconds) and performs three operations per tick:

**Step 1: Create new DAG runs.** For each registered DAG with a cron schedule, check if a new run should be created based on the timetable and the last run's logical date. If `catchup = true`, create runs for all missed intervals. Insert a new row in `harvest_dag_runs` with state `QUEUED`.

**Step 2: Activate queued DAG runs.** For each DAG, check if the number of `RUNNING` runs is below `max_active_runs`. If so, transition the oldest `QUEUED` run to `RUNNING` by starting its compiled workflow.

**Step 3: Detect stuck runs.** Find DAG runs that have been `RUNNING` longer than their configured timeout (default: 24 hours). Mark them as `FAILED` and cancel their underlying workflow.

### 6.2 Timetable Model

```rust
pub enum Schedule {
    /// Standard cron expression, evaluated in configured timezone.
    Cron(CronExpr),
    /// Fixed interval from the end of the previous run.
    Interval(Duration),
    /// Run only when triggered manually or via API.
    Manual,
    /// Custom timetable implementing the Timetable trait.
    Custom(Box<dyn Timetable>),
}

pub trait Timetable: Send + Sync {
    /// Given the last run's data interval, compute the next one.
    fn next_run(&self, last_interval: Option<DataInterval>, now: DateTime<Utc>) -> Option<DataInterval>;
}

pub struct DataInterval {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
```

### 6.3 Trigger Rules

For DAG tasks with multiple upstream dependencies, trigger rules determine when a task should execute:

```rust
pub enum TriggerRule {
    /// Run when all upstream tasks succeeded (default).
    AllSuccess,
    /// Run when all upstream tasks completed (any state).
    AllDone,
    /// Run when at least one upstream succeeded.
    OneSuccess,
    /// Run when at least one upstream failed.
    OneFailed,
    /// Run when all upstream tasks failed.
    AllFailed,
    /// Never auto-trigger; must be triggered manually.
    Manual,
}
```

### 6.4 Dependency Resolution

The DAG builder performs a topological sort at definition time (inside the `#[dag]` macro expansion) and stores the result as execution levels. Tasks within the same level have no dependencies on each other and execute concurrently. Tasks in level N+1 depend on at least one task in level N or earlier.

```
Level 0: [extract_users, extract_orders]   ← run concurrently
Level 1: [transform_data]                   ← waits for level 0
Level 2: [load_to_warehouse]                ← waits for level 1
Level 3: [send_slack_notification]           ← waits for level 2
```

Cycle detection happens at compile time (via the `#[dag]` macro). If the dependency graph contains a cycle, the macro emits a compile error.

### 6.5 Per-Workflow Cron Schedules

For the common case of "run this one workflow on a schedule," wrapping a single
workflow in a DAG is unnecessary overhead. The scheduler tick has a parallel
branch that processes `harvest_schedules` rows where `workflow_name IS NOT NULL`,
dispatching each due run directly via `start_or_load_workflow_execution`.

**Decision tree:**

| You want… | Use… |
|---|---|
| One workflow, one cron string, one JSON input | `WorkflowSchedule` |
| Fan-out across multiple activities with trigger rules and `AllSuccess` / `OneFailed` dependencies | `DagBuilder` |
| Multi-step pipeline where step N waits for step N−1 | `DagBuilder` |
| Dynamic input that changes per run | `DagBuilder` with a trigger via API |

**Registration:**

```rust
use autumn_harvest::policy::{Schedule, WorkflowSchedule};

let sched = WorkflowSchedule::new(
    "daily_billing_report",           // must match a workflows![] registration
    Schedule::Cron("0 3 * * *".to_string()),
)
.with_input(serde_json::json!({"region": "us-east"}))
.with_max_active_runs(1)    // skip the next firing if the previous run is still RUNNING
.with_catchup(false);       // drop missed firings instead of replaying them

let app = autumn_web::app()
    .workflows(workflows![daily_billing_report])
    .workflow_schedule(sched)
    .worker(WorkerConfig::default());
```

**Schema shape:** `harvest_schedules` uses a single-table design where exactly
one of `dag_name` or `workflow_name` is set per row, enforced by a CHECK
constraint. The scheduler's `create_due_runs` and `activate_queued_runs`
helpers filter to `dag_name IS NOT NULL`; the `tick_workflow_schedules` branch
filters to `workflow_name IS NOT NULL`. Existing DAG-only deployments see no
behavioral change.

**`workflow_id` derivation:** each scheduled dispatch uses
`sched:{workflow_name}:{unix_seconds_of_logical_date}` as the `workflow_id`.
This makes retries after a crashed tick idempotent under the current
`AllowDuplicate` reuse policy. When issue #87 lands and
`WorkflowIdReusePolicy::RejectDuplicate` is available, the scheduler will
switch to it so a double-tick cannot start two executions for the same logical
date.

**`max_active_runs`:** enforced via a `COUNT(*)` query on
`harvest_workflow_executions` filtered by `workflow_name = ? AND state = 'RUNNING'`.
If the running count equals or exceeds the cap, the tick updates `next_run_at`
and increments the `harvest_schedule_skipped_total{reason="max_active_runs_reached"}`
metric without starting a new execution.

---

## 7. Worker Model

### 7.1 Worker Architecture

A worker is a Tokio task that polls Postgres-backed task queues for work. Each Autumn application instance runs one worker (or zero, if configured as scheduler-only). The worker manages two internal executors:

**Workflow executor:** Polls the workflow task queue, loads event histories, and runs workflow functions through the replay engine. Limited to `max_concurrent_workflows` (default: 20) concurrent executions.

**Activity executor:** Polls activity task queues, runs activity functions directly. Limited to `max_concurrent_activities` (default: 50) concurrent executions.

### 7.2 Polling Strategy

Workers use Postgres `LISTEN/NOTIFY` combined with periodic polling as a fallback. This avoids the latency of pure polling while maintaining reliability:

```sql
-- Worker listens on channels for its registered queues
LISTEN harvest_queue_default;
LISTEN harvest_queue_email_workers;

-- When a task is enqueued, the scheduler sends:
NOTIFY harvest_queue_default, '{"task_id": "..."}';
```

The worker's poll loop:

1. `LISTEN` on all configured queue channels.
2. On notification (or every 5 seconds as fallback), attempt to claim a task.
3. Claiming uses `SELECT ... FOR UPDATE SKIP LOCKED` to avoid contention:

```sql
UPDATE harvest_task_queue
SET state = 'RUNNING',
    worker_id = $1,
    started_at = NOW(),
    attempt = attempt + 1
WHERE id = (
    SELECT id FROM harvest_task_queue
    WHERE queue_name = ANY($2)
      AND state = 'PENDING'
      AND scheduled_at <= NOW()
    ORDER BY priority DESC, scheduled_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

### 7.3 Sticky Execution

For workflows, sticky execution dramatically reduces replay cost. After a worker processes a workflow task, it caches the workflow's in-memory state (the suspended coroutine and its replay position). Subsequent tasks for the same workflow execution are routed to the same worker when possible.

Implementation: When a workflow task completes, the worker writes its `worker_id` to the workflow execution's `sticky_worker_id` column. The next task for that workflow is first offered to the sticky worker (via a worker-specific NOTIFY channel). If the sticky worker doesn't claim it within `sticky_timeout` (default: 5 seconds), the task falls back to the general queue.

```rust
pub struct WorkflowCache {
    /// LRU cache of in-progress workflow states.
    /// Key: workflow execution ID, Value: suspended coroutine + replay position.
    cache: LruCache<WorkflowExecId, CachedWorkflowState>,
    /// Max entries. When exceeded, least recently used workflows are evicted.
    max_size: usize,  // default: 1000
}
```

### 7.4 Heartbeating

Long-running activities send heartbeats to indicate liveness. The activity executor runs a background task per active activity that:

1. Receives heartbeat payloads from the activity via `ctx.heartbeat(details)`.
2. Batches heartbeats (at most one DB write per second per activity).
3. Updates the `last_heartbeat_at` column in `harvest_task_queue`.
4. Returns cancellation signals to the activity if the workflow was cancelled.

The scheduler checks for heartbeat timeouts by querying:

```sql
SELECT id FROM harvest_task_queue
WHERE state = 'RUNNING'
  AND heartbeat_timeout IS NOT NULL
  AND last_heartbeat_at < NOW() - heartbeat_timeout
```

Timed-out activities are marked as `FAILED` with a `HeartbeatTimeout` reason and retried according to their retry policy.

### 7.5 Graceful Shutdown

On SIGTERM/SIGINT:

1. Stop accepting new tasks (drain the poller).
2. Wait up to `shutdown_timeout` (default: 30 seconds) for in-flight activities to complete.
3. For activities that don't complete, record a `WorkerShutdown` failure and let the retry policy handle rescheduling.
4. For workflows, flush the current state to the event history so replay can resume on another worker.

---

## 8. Persistence Schema

All tables are prefixed with `harvest_` to avoid collisions with application tables. Migrations are bundled in `autumn-harvest` and run via the standard Autumn migration system.

### 8.1 Core Tables

```sql
-- Workflow executions (one row per workflow run)
CREATE TABLE harvest_workflow_executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name   TEXT NOT NULL,
    workflow_id     TEXT NOT NULL,          -- user-provided idempotency key
    run_id          UUID NOT NULL DEFAULT gen_random_uuid(),
    shard_id        INT NOT NULL,           -- hash(workflow_id) % num_shards
    state           TEXT NOT NULL DEFAULT 'RUNNING',  -- RUNNING, COMPLETED, FAILED, CANCELLED, TIMED_OUT
    input           JSONB NOT NULL,
    output          JSONB,
    error           TEXT,
    parent_id       UUID REFERENCES harvest_workflow_executions(id),  -- for child workflows
    sticky_worker_id TEXT,
    queue_name      TEXT NOT NULL DEFAULT 'default',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    execution_timeout INTERVAL,
    memo            JSONB,                  -- user-attached metadata
    search_attrs    JSONB,                  -- indexed custom attributes
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (workflow_id, run_id)
);

CREATE INDEX idx_harvest_we_shard ON harvest_workflow_executions (shard_id);
CREATE INDEX idx_harvest_we_state ON harvest_workflow_executions (state) WHERE state = 'RUNNING';
CREATE INDEX idx_harvest_we_search ON harvest_workflow_executions USING GIN (search_attrs);

-- Event history (append-only log per workflow execution)
CREATE TABLE harvest_events (
    id              BIGSERIAL PRIMARY KEY,
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    event_id        INT NOT NULL,            -- sequential within workflow (0, 1, 2, ...)
    event_type      TEXT NOT NULL,
    event_data      JSONB NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (workflow_exec_id, event_id)
);

CREATE INDEX idx_harvest_events_exec ON harvest_events (workflow_exec_id, event_id);

-- Task queue (Postgres-backed work queue)
CREATE TABLE harvest_task_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name      TEXT NOT NULL,
    task_type       TEXT NOT NULL,           -- 'workflow' or 'activity'
    workflow_exec_id UUID REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    activity_name   TEXT,
    input           JSONB NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, RUNNING, COMPLETED, FAILED
    priority        INT NOT NULL DEFAULT 0,
    worker_id       TEXT,
    attempt         INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 1,
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    heartbeat_timeout INTERVAL,
    start_to_close  INTERVAL,
    schedule_to_start INTERVAL,
    retry_policy    JSONB,
    output          JSONB,
    error           TEXT,

    CONSTRAINT valid_state CHECK (state IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED'))
);

CREATE INDEX idx_harvest_tq_poll ON harvest_task_queue (queue_name, state, priority DESC, scheduled_at)
    WHERE state = 'PENDING';
CREATE INDEX idx_harvest_tq_running ON harvest_task_queue (state, last_heartbeat_at)
    WHERE state = 'RUNNING';
CREATE INDEX idx_harvest_tq_workflow ON harvest_task_queue (workflow_exec_id);

-- DAG runs
CREATE TABLE harvest_dag_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_name        TEXT NOT NULL,
    workflow_exec_id UUID REFERENCES harvest_workflow_executions(id),  -- underlying workflow
    state           TEXT NOT NULL DEFAULT 'QUEUED',  -- QUEUED, RUNNING, SUCCESS, FAILED
    logical_date    TIMESTAMPTZ NOT NULL,
    data_interval_start TIMESTAMPTZ NOT NULL,
    data_interval_end   TIMESTAMPTZ NOT NULL,
    conf            JSONB,                  -- run-specific parameters
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (dag_name, logical_date)
);

CREATE INDEX idx_harvest_dr_schedule ON harvest_dag_runs (dag_name, state, logical_date);

-- Schedules (registered DAG timetables and per-workflow cron schedules)
--
-- Single-table design: exactly one of dag_name or workflow_name is set per row.
-- Enforced by a NOT VALID CHECK constraint applied online-safely after the
-- 20260430000000_harvest_workflow_schedules migration.
CREATE TABLE harvest_schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_name        TEXT UNIQUE,            -- NULL for workflow-kind rows
    workflow_name   TEXT,                   -- NULL for dag-kind rows
    workflow_input  JSONB,                  -- per-firing input for workflow-kind rows
    schedule_expr   TEXT,                   -- cron expression or 'manual'
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    catchup         BOOLEAN NOT NULL DEFAULT FALSE,
    max_active_runs INT NOT NULL DEFAULT 1,
    is_paused       BOOLEAN NOT NULL DEFAULT FALSE,
    last_run_at     TIMESTAMPTZ,
    next_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_schedules_kind_check CHECK (
        (dag_name IS NOT NULL AND workflow_name IS NULL) OR
        (dag_name IS NULL    AND workflow_name IS NOT NULL)
    )
);

-- Signals (pending signals for running workflows)
CREATE TABLE harvest_signals (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    signal_name     TEXT NOT NULL,
    payload         JSONB NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed        BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_harvest_signals_pending ON harvest_signals (workflow_exec_id, signal_name)
    WHERE NOT consumed;

-- Timers (durable timers for sleeping workflows)
CREATE TABLE harvest_timers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    timer_id        TEXT NOT NULL,
    fires_at        TIMESTAMPTZ NOT NULL,
    fired           BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_harvest_timers_pending ON harvest_timers (fires_at)
    WHERE NOT fired;

-- Dead letter queue (tasks that exhausted all retries)
CREATE TABLE harvest_dead_letters (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_task_id UUID NOT NULL,
    queue_name      TEXT NOT NULL,
    task_type       TEXT NOT NULL,
    workflow_exec_id UUID,
    activity_name   TEXT,
    input           JSONB NOT NULL,
    error           TEXT NOT NULL,
    attempts        INT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 8.2 Partitioning Strategy

The `harvest_events` table will grow large in production. Partition it by `workflow_exec_id` hash to co-locate all events for a workflow:

```sql
-- Range-partition events by shard (matches workflow shard assignment)
CREATE TABLE harvest_events (
    -- ... columns as above ...
) PARTITION BY HASH (workflow_exec_id);

-- Create partitions (one per shard, configurable)
CREATE TABLE harvest_events_p0 PARTITION OF harvest_events FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE harvest_events_p1 PARTITION OF harvest_events FOR VALUES WITH (MODULUS 16, REMAINDER 1);
-- ... up to p15
```

The `harvest_task_queue` table is also partitioned by `queue_name` using list partitioning, enabling queue-specific vacuum and index tuning.

### 8.3 History Retention

Harvest ships an opt-in retention janitor for completed workflow histories. Operators configure `RetentionConfig` on `HarvestBuilder`; default behavior is disabled (`max_age = None`), so upgrading does not delete any rows until explicitly enabled.

When enabled, each tick selects terminal workflow executions older than `max_age` and deletes them transactionally. Rows in `harvest_events`, `harvest_task_queue`, `harvest_timers`, and `harvest_signals` are removed by `ON DELETE CASCADE`; `harvest_dead_letters` rows are deleted explicitly in the same retention transaction because `workflow_exec_id` is not a foreign key.

In-flight workflows are never eligible because retention only targets terminal states with `completed_at` older than the configured window.

The management API exposes retention introspection and control via `GET /admin/retention` and `POST /admin/retention/run-now`.

---

## 9. Task Queues

### 9.1 Design: Postgres as the Queue

Autumn Harvest uses Postgres as the task queue. No external broker (Redis, RabbitMQ, Kafka) is required. This is a deliberate choice:

**Why Postgres, not an external broker:**

- **Operational simplicity.** Autumn already requires Postgres. Adding Redis or RabbitMQ doubles the infrastructure surface area for a capability (queueing) that Postgres handles well at the scale Harvest targets.
- **Transactional consistency.** Enqueuing a task and recording an event in the workflow history happens in a single Postgres transaction. With an external broker, you need distributed transactions or outbox patterns.
- **Sufficient throughput.** With `SKIP LOCKED`, Postgres can handle thousands of dequeues per second. Harvest targets workloads up to ~10,000 tasks/second, which is well within Postgres' capability.
- **Simplicity of deployment.** One binary, one database. This matters enormously for adoption.

**When Postgres is not enough:** If a deployment needs >10,000 tasks/second sustained, Harvest will support an optional `autumn-harvest-redis` adapter crate (Phase 4) that uses Redis Streams for the task queue while keeping Postgres for history and state. But this is an escape hatch, not the default path.

### 9.2 Queue Semantics

Each task queue is identified by a name string. Tasks are enqueued with a `queue_name` and dequeued by workers that register interest in one or more queues. The dequeue operation is atomic via `SELECT ... FOR UPDATE SKIP LOCKED`:

- **At-most-once delivery** for the initial attempt (no duplicate execution).
- **At-least-once delivery** across retries (a task may execute multiple times if it fails).
- **Ordering:** Within a queue, tasks are ordered by `(priority DESC, scheduled_at ASC)`. Priority 0 is default; higher numbers execute first.
- **Delayed scheduling:** Tasks can be enqueued with a future `scheduled_at` for retry backoff or scheduled execution.
- **Visibility timeout:** If a worker claims a task and doesn't complete it within `start_to_close`, the scheduler marks it as failed and requeues it.

### 9.3 Worker-Queue Binding

Workers declare which queues they serve at startup:

```rust
WorkerConfig::default()
    .queues(["default", "email-workers"])
```

A worker only polls queues it's registered for. This enables workload isolation — CPU-intensive activities run on beefier workers, I/O-bound activities run on smaller instances with more concurrency.

### 9.4 Queue Backpressure

Workers track their own capacity and only poll when they have slots available:

```rust
// Simplified worker poll loop
loop {
    let available_slots = max_concurrent - currently_running.load(Ordering::Relaxed);
    if available_slots == 0 {
        // At capacity — wait for a running task to complete
        tokio::select! {
            _ = completion_signal.recv() => continue,
            _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
        }
    }

    match claim_task(&pool, &queues).await {
        Some(task) => { spawn_task(task); }
        None => {
            // No work available — wait for notification or poll interval
            tokio::select! {
                _ = pg_notify.recv() => continue,
                _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
            }
        }
    }
}
```

---

## 10. Failure Handling

### 10.1 Retry Policies

Every activity has a retry policy (explicit or default). The policy specifies how failures are handled:

```rust
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). 1 = no retries.
    pub max_attempts: u32,
    /// Initial delay before first retry.
    pub initial_interval: Duration,
    /// Backoff multiplier applied to each subsequent retry.
    pub backoff_coefficient: f64,
    /// Maximum delay between retries.
    pub max_interval: Duration,
    /// Errors that should not be retried (non-retryable error types).
    pub non_retryable_errors: Vec<String>,
}

impl RetryPolicy {
    pub fn exponential(max_attempts: u32, initial: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval: initial,
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(300),
            non_retryable_errors: vec![],
        }
    }

    pub fn fixed(max_attempts: u32, interval: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval: interval,
            backoff_coefficient: 1.0,
            max_interval: interval,
            non_retryable_errors: vec![],
        }
    }
}
```

When an activity fails, the scheduler computes the next retry time: `initial_interval * backoff_coefficient^(attempt - 1)`, capped at `max_interval`. The task is re-enqueued with `scheduled_at` set to the computed time.

### 10.2 Timeout Types

Four distinct timeouts, matching Temporal's model:

| Timeout | What it measures | Default | Effect on failure |
|---------|-----------------|---------|-------------------|
| **Schedule-to-Start** | Time from task enqueued to worker claiming it | None (unlimited) | Task marked `TIMED_OUT`, NOT retried (requeuing to same queue would repeat the problem) |
| **Start-to-Close** | Time from worker claiming task to completion | 5 minutes | Task marked `TIMED_OUT`, retried per policy |
| **Heartbeat** | Time between consecutive heartbeats from the activity | None (disabled unless set) | Task marked `TIMED_OUT`, retried per policy |
| **Schedule-to-Close** | Total time from enqueue to final completion (across all retries) | None (unlimited) | Task and all retries cancelled |

The scheduler enforces timeouts by running a periodic check (every 10 seconds):

```sql
-- Check start-to-close timeouts
UPDATE harvest_task_queue
SET state = 'FAILED', error = 'StartToCloseTimeout'
WHERE state = 'RUNNING'
  AND start_to_close IS NOT NULL
  AND started_at + start_to_close < NOW();

-- Check heartbeat timeouts
UPDATE harvest_task_queue
SET state = 'FAILED', error = 'HeartbeatTimeout'
WHERE state = 'RUNNING'
  AND heartbeat_timeout IS NOT NULL
  AND last_heartbeat_at IS NOT NULL
  AND last_heartbeat_at + heartbeat_timeout < NOW();

-- Check schedule-to-start timeouts
UPDATE harvest_task_queue
SET state = 'FAILED', error = 'ScheduleToStartTimeout'
WHERE state = 'PENDING'
  AND schedule_to_start IS NOT NULL
  AND scheduled_at + schedule_to_start < NOW();
```

### 10.3 Saga / Compensation Pattern

Harvest provides first-class support for the saga pattern through the `Saga` builder:

```rust
#[workflow]
async fn book_trip(ctx: &WorkflowContext, trip: TripRequest) -> AutumnResult<TripConfirmation> {
    let mut saga = Saga::new(ctx);

    // Each step has a forward action and a compensating action
    let flight = saga.step(
        || ctx.execute_activity(book_flight, trip.flight.clone()),
        |flight_id| ctx.execute_activity(cancel_flight, flight_id),
    ).await?;

    let hotel = saga.step(
        || ctx.execute_activity(book_hotel, trip.hotel.clone()),
        |hotel_id| ctx.execute_activity(cancel_hotel, hotel_id),
    ).await?;

    let car = saga.step(
        || ctx.execute_activity(rent_car, trip.car.clone()),
        |car_id| ctx.execute_activity(cancel_car_rental, car_id),
    ).await?;

    // If any step fails, all previous steps are compensated in reverse order.
    // saga.step() returns Err after prior successful steps are compensated.

    Ok(TripConfirmation { flight, hotel, car })
}
```

If `book_hotel` fails, the saga automatically calls `cancel_flight` (the compensation for the first step) before propagating the error. Compensation actions run with their own retry policies and are recorded in the event history for durability when they execute workflow commands such as `ctx.execute_activity(...)`. If a compensation fails, Harvest keeps unwinding remaining compensations and returns `HarvestError::SagaCompensationFailed` with the original failure plus the compensation failures.

### 10.4 Dead Letter Queue

Tasks that exhaust all retry attempts are moved to `harvest_dead_letters` with their full context (input, error, attempt count). The management API exposes endpoints to inspect and replay dead-lettered tasks.

---

## 11. Signals and Queries

### 11.1 Signals

Signals are asynchronous messages sent to a running workflow. They are written to `harvest_signals` and trigger a workflow task to process them.

**Sending a signal (from application code or HTTP API):**

```rust
// From Rust code
harvest.signal_workflow("onboarding_workflow", "user-123", "email_verified", json!({ "verified_at": "..." })).await?;

// From HTTP API
// POST /api/harvest/workflows/user-123/signal/email_verified
// Body: { "verified_at": "..." }
```

**Processing:** When a signal is inserted into `harvest_signals`, a `NOTIFY` is sent on the workflow's channel. The worker picks up a workflow task, replays the workflow to its current suspension point, and delivers the signal to the waiting `ctx.wait_for_signal()` call.

### 11.2 Queries

Queries are synchronous reads of workflow state. They do not modify the workflow and do not appear in the event history.

```rust
// From Rust code
let status: String = harvest.query_workflow("order_workflow", "order-456", "get_status").await?;

// From HTTP API
// GET /api/harvest/workflows/order-456/query/get_status
// Response: { "result": "shipping" }
```

**Processing:** Queries execute against the workflow's cached state on the sticky worker (if available). If no cached state exists, the engine replays the workflow to its current point and executes the query handler. Queries never block — if the workflow is in a suspended state, the query reads whatever state is available.

### 11.3 Workflow Cancellation

Cancellation is a special signal. When a workflow is cancelled:

1. A `WorkflowCancelled` event is recorded.
2. Running activities receive a cancellation token via their next heartbeat check.
3. The workflow's `ctx.cancelled()` future resolves, allowing cleanup logic.
4. If activities don't respond within `cancellation_timeout` (default: 30 seconds), they are forcibly terminated.

---

## 12. Integration with Autumn

### 12.1 AppBuilder Extension

Harvest integrates via extension traits on `AppBuilder`:

```rust
pub trait HarvestExt {
    fn workflows(self, workflows: Vec<WorkflowInfo>) -> Self;
    fn activities(self, activities: Vec<ActivityInfo>) -> Self;
    fn dags(self, dags: Vec<DagInfo>) -> Self;
    fn worker(self, config: WorkerConfig) -> Self;
    fn harvest_api(self, path: &str) -> Self;
}
```

When `.run()` is called, the builder:

1. Runs Harvest migrations against the shared Postgres pool.
2. Registers all workflows, activities, and DAGs.
3. Upserts schedules into `harvest_schedules`.
4. Starts the scheduler loop as a background Tokio task.
5. Starts the worker poller as a background Tokio task.
6. If `harvest_api` is configured, mounts management routes on the Axum router.

### 12.2 Shared State

Harvest reuses Autumn's connection pool, `AppState`, and DI system:

- **Database pool:** `AppState.pool` is shared between the web server, the scheduler, and the worker. No separate pool configuration needed.
- **State injection:** Activities access shared application state via `ctx.state::<T>()`, which reads from the same `AppState` that routes and tasks use.
- **Db extractor:** Activities can use `ctx.db()` to get a connection from the shared pool, same as `Db` in route handlers.
- **Repositories:** `PgXxxRepository` types created with `#[repository]` work identically inside activities.

### 12.3 Management HTTP API

When `harvest_api("/api/harvest")` is configured, the following endpoints are mounted:

```
GET    /api/harvest/workflows                    # list workflow executions
GET    /api/harvest/workflows/:id                # get workflow details + event history
POST   /api/harvest/workflows/:type/start        # start a new workflow
POST   /api/harvest/workflows/:id/signal/:name   # send signal to workflow
GET    /api/harvest/workflows/:id/query/:name    # query workflow state
POST   /api/harvest/workflows/:id/cancel         # cancel workflow
POST   /api/harvest/workflows/:id/terminate      # force-terminate workflow

GET    /api/harvest/dags                          # list DAG schedules
GET    /api/harvest/dags/:name/runs               # list DAG runs
POST   /api/harvest/dags/:name/trigger            # manually trigger DAG run
PATCH  /api/harvest/dags/:name                    # pause/unpause DAG

GET    /api/harvest/queues                        # list queues with depth
GET    /api/harvest/dead-letters                  # list dead-lettered tasks
POST   /api/harvest/dead-letters/:id/replay       # replay a dead-lettered task

GET    /api/harvest/workers                       # list connected workers
GET    /api/harvest/health                        # scheduler + worker health
```

### 12.4 Error Integration

Harvest uses `AutumnResult<T>` and `AutumnError` everywhere. Activity failures are wrapped in `AutumnError::ActivityFailed`. Workflow failures are wrapped in `AutumnError::WorkflowFailed`. Both integrate with Autumn's thiserror-based error hierarchy and blanket `From` implementations.

```rust
#[derive(Debug, thiserror::Error)]
pub enum HarvestError {
    #[error("activity failed: {name} (attempt {attempt}): {source}")]
    ActivityFailed { name: String, attempt: u32, source: Box<dyn std::error::Error + Send + Sync> },

    #[error("workflow failed: {name}: {reason}")]
    WorkflowFailed { name: String, reason: String },

    #[error("non-deterministic replay: {0}")]
    NonDeterministic(String),

    #[error("workflow cancelled: {0}")]
    Cancelled(String),

    #[error("saga compensation failed after original error: {original}; compensation errors: {compensation_errors:?}")]
    SagaCompensationFailed { original: String, compensation_errors: Vec<String> },

    #[error("timeout: {timeout_type} for {task_name}")]
    Timeout { timeout_type: TimeoutType, task_name: String },
}

impl From<HarvestError> for AutumnError {
    fn from(e: HarvestError) -> Self {
        AutumnError::Internal(e.to_string())
    }
}
```

---

## 13. Scalability

### 13.1 Sharding Strategy

Workflow executions are sharded by hashing the `workflow_id` to a shard number. The shard count is configured at deployment time and is immutable after initial migration (matching Temporal's model):

```rust
// In autumn.toml
[harvest]
num_shards = 16  # must be power of 2, default 16
```

Sharding provides two benefits:

**Write isolation.** Each shard is an independent unit of concurrency. Operations on different shards never contend. Within a shard, operations on the same workflow are serialized via Postgres row-level locks, preventing concurrent modification of a workflow's state.

**Horizontal scaling.** In a multi-instance deployment, each Autumn instance can be assigned a subset of shards. The scheduler only processes DAG runs and timers for its assigned shards, and workers preferentially (but not exclusively) claim tasks from their assigned shards.

Shard assignment in multi-instance mode uses Postgres advisory locks:

```sql
-- Each instance tries to acquire advisory locks for shards
-- pg_try_advisory_lock returns true if acquired, false if another instance holds it
SELECT pg_try_advisory_lock(hashtext('harvest_shard'), shard_id)
FROM generate_series(0, 15) AS shard_id;
```

### 13.2 Horizontal Scaling Model

```
                    ┌─────────────────────────┐
                    │        Postgres          │
                    │  (shared by all nodes)   │
                    └────────┬────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
        ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐
        │  Node A    │ │  Node B    │ │  Node C    │
        │ Shards 0-5 │ │ Shards 6-10│ │Shards 11-15│
        │ Scheduler ✓│ │ Scheduler ✓│ │ Scheduler ✓│
        │ Worker ✓   │ │ Worker ✓   │ │ Worker ✓   │
        │ Web API ✓  │ │ Web API ✓  │ │ Web API ✓  │
        └───────────┘ └───────────┘ └───────────┘
```

Every node runs the full stack (scheduler + worker + web). The scheduler loop on each node only creates DAG runs and processes timers for its assigned shards. Workers can claim tasks from any shard (not just their own) — shard assignment is a hint for affinity, not a hard boundary.

### 13.3 Connection Pool Management

The shared deadpool is configured per-instance:

```toml
[database]
max_connections = 20  # shared across web + harvest

[harvest.pool]
# Harvest reserves a portion of the shared pool
max_scheduler_connections = 2    # for scheduler loop
max_worker_connections = 8       # for task execution
# Remaining connections available for web requests
```

Activities that need their own connections use `ctx.db()` which draws from the shared pool. This means a burst of concurrent activities can temporarily starve web requests. To mitigate this, the worker's `max_concurrent_activities` should be tuned relative to the pool size.

### 13.4 Archival and Retention

For long-running deployments, the event history table will be the largest table. Harvest provides three retention strategies:

**Time-based deletion** (implemented): Configure `RetentionConfig { max_age, tick_interval, batch_size, dry_run }` on `HarvestBuilder` to prune terminal workflow history older than a fixed age.

**Infinite retention** (default): Leave `max_age = None` to keep append-only history forever.

---

## 14. Configuration Schema

Harvest configuration lives under the `[harvest]` section of `autumn.toml` and follows Autumn's 5-layer config system:

```toml
[harvest]
# Sharding — immutable after first migration
num_shards = 16

# Scheduler
scheduler_tick_interval = "30s"
dag_default_timeout = "24h"

# Worker
worker_enabled = true
worker_queues = ["default"]
max_concurrent_workflows = 20
max_concurrent_activities = 50
shutdown_timeout = "30s"

# Sticky execution
sticky_timeout = "5s"
workflow_cache_size = 1000

# Task defaults
default_start_to_close = "5m"
default_retry_policy.max_attempts = 3
default_retry_policy.initial_interval = "1s"
default_retry_policy.backoff_coefficient = 2.0
default_retry_policy.max_interval = "5m"

# History management
# configured via `HarvestBuilder::retention(RetentionConfig { ... })`

# Management API
api_enabled = true
api_path = "/api/harvest"

# Polling (fallback when LISTEN/NOTIFY misses)
poll_interval = "5s"

# Connection pool allocation
pool.max_scheduler_connections = 2
pool.max_worker_connections = 8
```

Environment variable overrides follow Autumn's pattern:

```bash
AUTUMN_HARVEST__NUM_SHARDS=32
AUTUMN_HARVEST__WORKER_QUEUES=default,email-workers
AUTUMN_HARVEST__MAX_CONCURRENT_ACTIVITIES=100
```

Profile-specific overrides:

```toml
# autumn-production.toml
[harvest]
num_shards = 64
max_concurrent_activities = 200
history_retention = "90d"
archive_enabled = true
```

---

## 15. Architecture Diagrams

### 15.1 Component Interaction

```
┌─────────────────────────────────────────────────────────────────┐
│                        Autumn Application                       │
│                                                                 │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐    │
│  │ Axum     │  │  Scheduler   │  │       Worker           │    │
│  │ Router   │  │              │  │                        │    │
│  │          │  │ • DAG tick   │  │ • Workflow executor    │    │
│  │ • Web    │  │ • Timer fire │  │   (replay engine)     │    │
│  │   routes │  │ • Timeout    │  │ • Activity executor   │    │
│  │ • Harvest│  │   enforce    │  │ • Heartbeat manager   │    │
│  │   API    │  │ • Archival   │  │ • Sticky cache (LRU)  │    │
│  └────┬─────┘  └──────┬───────┘  └────────┬───────────────┘    │
│       │               │                    │                    │
│       └───────────────┼────────────────────┘                    │
│                       │                                         │
│              ┌────────┴────────┐                                │
│              │    AppState     │                                │
│              │ • Pool<AsyncPg> │                                │
│              │ • Profile       │                                │
│              │ • Custom state  │                                │
│              └────────┬────────┘                                │
└───────────────────────┼─────────────────────────────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │    PostgreSQL    │
              │                 │
              │ • App tables    │
              │ • harvest_*     │
              │   tables        │
              │ • LISTEN/NOTIFY │
              └─────────────────┘
```

### 15.2 Workflow Execution Data Flow

```
Start Workflow
      │
      ▼
┌─────────────────┐     ┌─────────────────────┐
│ Insert execution │────▶│ Enqueue workflow     │
│ row + Started    │     │ task on task_queue   │
│ event            │     └─────────┬───────────┘
└──────────────────┘               │
                                   ▼
                          ┌────────────────┐
                          │ Worker claims  │
                          │ workflow task  │
                          └───────┬────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │ Load event history          │
                    │ Replay workflow to current  │
                    │ suspension point            │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │ Execute next workflow step  │
                    │ (e.g., schedule activity)   │
                    └─────────────┬───────────────┘
                                  │
              ┌───────────────────┼────────────────────┐
              │                   │                    │
              ▼                   ▼                    ▼
    ┌─────────────┐    ┌──────────────┐     ┌──────────────┐
    │ Activity    │    │ Timer        │     │ Wait for     │
    │ Scheduled   │    │ Started      │     │ Signal       │
    │ event +     │    │ event        │     │              │
    │ task queued │    └──────┬───────┘     └──────┬───────┘
    └──────┬──────┘           │                    │
           │            fires_at reached     signal received
           ▼                  │                    │
    ┌──────────────┐          ▼                    ▼
    │ Activity     │   ┌──────────────┐    ┌──────────────┐
    │ worker       │   │ Timer Fired  │    │ Signal event │
    │ executes     │   │ event →      │    │ → resume     │
    │ activity fn  │   │ resume wf    │    │ workflow     │
    └──────┬───────┘   └──────────────┘    └──────────────┘
           │
           ▼
    ┌──────────────┐
    │ Activity     │
    │ Completed/   │
    │ Failed event │
    │ → resume wf  │
    └──────────────┘
```

### 15.3 DAG Scheduling Flow

```
Scheduler tick (every 30s)
      │
      ├──▶ For each DAG with schedule:
      │       │
      │       ├── Is next_run_at <= now?
      │       │       │
      │       │       YES ──▶ Insert dag_run (QUEUED)
      │       │               Update next_run_at
      │       │
      │       └── Are active runs < max_active_runs?
      │               │
      │               YES ──▶ Oldest QUEUED run → RUNNING
      │                       Start compiled workflow
      │
      ├──▶ For each running DAG workflow:
      │       │
      │       └── Workflow engine handles execution
      │           (topological levels, concurrency,
      │            trigger rules, event sourcing)
      │
      └──▶ Check for stuck runs (> timeout)
              │
              └── Mark FAILED, cancel workflow
```

---

## 16. Implementation Roadmap

### Phase 1: Core Engine (8-10 weeks)

The foundation. After this phase, you can define and execute workflows and activities with durable execution.

**Week 1-2: Crate setup and persistence**
- Create `autumn-harvest` and `autumn-harvest-macros` crates in the workspace
- Define Diesel migrations for all `harvest_*` tables
- Implement Diesel models and basic CRUD operations
- Write `HarvestBuilder` fluent API skeleton

**Week 3-4: Event sourcing engine**
- Implement `WorkflowEvent` enum and serialization
- Build event history writer (append-only, transactional)
- Build event history reader (load full history for a workflow)
- Implement deterministic replay engine (`HistoryMatch` logic)
- Write `WorkflowContext` with replay-aware `execute_activity`, `timer`, `now`

**Week 5-6: Task queue and worker**
- Implement Postgres-backed task queue (enqueue, claim with `SKIP LOCKED`, complete)
- Implement `LISTEN/NOTIFY` integration for low-latency task pickup
- Build worker poll loop (workflow executor + activity executor)
- Implement heartbeat sending and checking
- Implement timeout enforcement (start-to-close, heartbeat)

**Week 7-8: Proc macros**
- Implement `#[workflow]` macro (companion function generation)
- Implement `#[activity]` macro (with retry policy, timeout attributes)
- Implement `workflows![]` and `activities![]` bang macros
- Implement `AppBuilder` extension trait for registration

**Week 9-10: Integration and testing**
- Wire Harvest into Autumn's startup lifecycle
- Shared pool, AppState, Db extractor in activities
- End-to-end integration tests (start workflow, execute activities, complete)
- Failure and replay tests (kill worker mid-workflow, verify replay)
- Clippy pedantic + nursery compliance

**Deliverable:** A working workflow engine where users can define workflows and activities with proc macros, execute them with durable guarantees, and retry on failure.

### Phase 2: DAG Scheduler + Signals (6-8 weeks)

**Week 11-13: DAG framework**
- Implement `DagBuilder` and topological sort
- Implement `#[dag]` macro and DAG-to-workflow compilation
- Implement trigger rules (`AllSuccess`, `AllDone`, `OneSuccess`, etc.)
- Implement timetable model (cron, interval, manual)
- Build scheduler loop (create runs, activate runs, detect stuck runs)

**Week 14-15: Signals and queries**
- Implement signal storage and delivery
- Implement query dispatch (cached state on sticky worker, fallback to replay)
- Implement `ctx.wait_for_signal()` and `ctx.register_query()`
- Implement durable workflow cancellation

**Week 16-18: Saga and management API**
- Implement `Saga` builder with compensation logic
- Build management HTTP API (list, inspect, signal, cancel workflows)
- Implement dead letter queue and replay
- DAG pause/unpause, manual trigger

**Deliverable:** Full DAG scheduling, signal/query interaction, saga compensation, and HTTP management API.

### Phase 3: Production Hardening (6-8 weeks)

**Week 19-21: Scalability**
- Implement shard assignment via advisory locks
- Multi-instance scheduler coordination
- Sticky execution with LRU cache and fallback
- Connection pool partitioning (scheduler vs. worker vs. web)
- Load testing and benchmarking (target: 1,000 workflows/sec, 10,000 activities/sec)

**Week 22-24: Observability and operations**
- Structured logging with tracing spans per workflow/activity execution
- Metrics export (Prometheus): queue depth, execution latency, retry rate, worker utilization
- History archival (time-based deletion + optional cold storage)
- Schema for `search_attrs` and workflow search/listing

**Week 25-26: Dashboard (autumn-harvest-ui)**
- Axum-based web UI for workflow monitoring
- DAG run visualization (task graph with status colors)
- Workflow event history inspector
- Queue depth monitoring and worker status

**Deliverable:** Production-ready system with horizontal scaling, observability, and an operational dashboard.

### Phase 4: Advanced Features (ongoing)

- Child workflow support (`ctx.spawn_child_workflow`)
- Continue-as-new (for infinite-running workflows)
- Workflow versioning (handle code changes across running workflows)
- Optional Redis-backed task queue adapter (`autumn-harvest-redis`)
- Cron workflow schedules (recurring workflows without the DAG model)
- Batch operations (start/signal/cancel many workflows at once)
- Workflow search with custom attributes and SQL-like query syntax

---

## Appendix A: Comparison with Existing Systems

| Feature | Temporal | Airflow | Autumn Harvest |
|---------|----------|---------|---------------|
| Execution model | Durable (event-sourced) | Non-durable (re-execute on failure) | Durable (event-sourced) |
| Workflow definition | SDK code (Go, Java, Python, TS) | Python DAGs | Rust proc macros |
| Scheduling | Timer-based in workflow | Cron/timetable on DAGs | Both (timers in workflows + cron on DAGs) |
| Persistence | Postgres/MySQL/Cassandra | Postgres/MySQL | Postgres only |
| Task queue | gRPC-based, in-memory + DB | Celery/K8s/Local executor | Postgres-backed (SKIP LOCKED + LISTEN/NOTIFY) |
| External broker | None (built into server) | Redis/RabbitMQ (for Celery) | None (Postgres only) |
| Infrastructure | Separate Temporal server | Separate Airflow server | Embedded in application |
| Sharding | Hash-based, immutable count | None (DB-level only) | Hash-based, immutable count |
| Signals/Queries | Yes | No (XCom for task data) | Yes |
| Compensation/Saga | Via workflow code | No built-in | First-class Saga builder |

## Appendix B: Why Not Just Use Temporal?

Temporal is excellent. If your team already runs Temporal in production and needs Rust workers, use the Temporal Rust SDK. Autumn Harvest is for teams that want workflow orchestration without deploying and operating a separate Temporal cluster.

Harvest's value proposition is operational simplicity: one Rust binary, one Postgres database, zero additional infrastructure. The tradeoff is lower throughput ceiling (Postgres queues vs. Temporal's optimized matching service) and a smaller ecosystem (no cross-language workers). For teams building Autumn applications that need workflow orchestration at moderate scale (up to ~10K tasks/second), Harvest is the simpler path.

## Appendix C: Open Questions

1. **Workflow versioning.** When a workflow's code changes while executions are in-flight, replay will fail due to non-determinism. Temporal solves this with versioning APIs (`workflow.GetVersion()`). Harvest needs an equivalent — likely a `ctx.version("change-id", min_version, max_version)` call that records version markers in the event history.

2. **Multi-tenancy.** Should Harvest support namespace isolation (like Temporal namespaces) for multi-tenant deployments? Initial answer: no, keep it simple. Namespaces can be added later by prefixing all table queries with a `namespace` column.

3. **Exactly-once semantics.** Activity execution is at-least-once by design (retries after failure). For operations that must not be duplicated (e.g., charging a credit card), users must implement idempotency keys in their activity code. Should Harvest provide built-in idempotency key management? Initial answer: provide a `ctx.idempotency_key()` helper that generates a deterministic key from the workflow ID + activity ID + attempt number, but leave enforcement to the activity implementation.
