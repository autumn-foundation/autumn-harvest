---
name: autumn-harvest
description: >
  How to use autumn-harvest, a Postgres-backed durable workflow engine for Rust.
  Use this skill whenever the user mentions autumn-harvest, durable workflows in Rust,
  or wants Temporal-style workflow orchestration with Postgres. Also trigger when
  you see #[workflow], #[activity], HarvestPlugin, WorkflowContext, ActivityContext,
  execute_activity, harvest signals/timers/child workflows, or DAG scheduling. If the
  user wants long-running background orchestration, retry policies, or event-sourced
  workflow execution in a Rust app (especially with autumn-web), use this skill
  proactively. This skill pairs with the autumn-web skill — read both when building
  an autumn-web app that includes durable workflows.
---

# autumn-harvest — Durable Workflow Engine for Rust

**Repository**: https://github.com/autumn-foundation/autumn-harvest
**Version**: 0.8.0 | **Edition**: 2024 | **MSRV**: 1.88.0
**Author**: autumn-foundation

autumn-harvest is a Postgres-backed durable workflow engine — Temporal-style semantics
with a single-Postgres operational footprint. Designed as a companion to autumn-web,
but usable standalone.

## When to read reference files

This SKILL.md covers workflow/activity patterns, integration, and all common usage.
For deeper internals, read:

- **`references/architecture.md`** — Event-sourced execution model, core components
  (WorkflowContext, Executor, HistoryMatcher, Worker, Queue, Scheduler), main event
  and command types, pool management, testing support. Read this when
  debugging replay behavior, understanding the event model, or tuning worker pools.

---

## Architecture in One Paragraph

Workflows are deterministic Rust async functions. When they call
`ctx.execute_activity(...)` for the first time, the activity is enqueued to a Postgres
task queue and the workflow suspends. A worker claims the activity (`SELECT ... FOR
UPDATE SKIP LOCKED`), runs it, and writes the result as an event in the workflow's
history. The workflow then resumes — on the same worker if cached, or by replaying its
history from scratch on any other worker. Replay is deterministic because every
non-deterministic decision (activity result, timer fire, signal arrival) is recorded
as an event the first time and read back from history on subsequent invocations.

## Workspace Structure

| Crate | Purpose |
|-------|---------|
| `autumn-harvest` | Core engine — types, executor, replay, queue, worker runtime |
| `autumn-harvest-plugin` | `HarvestPlugin` — wires into autumn-web `AppBuilder`, management API |
| `autumn-harvest-macros` | `#[workflow]`, `#[activity]`, `#[dag]`, `workflows![]`, `activities![]`, `dags![]` |
| `autumn-harvest-sqlite` | Embedded single-writer SQLite backend — `SqliteRuntime`, no database server |
| `autumn-harvest-agent` | Durable agent loop (`agent_loop`) — model and tool calls as activities, durable `Agent`s, agent meshes |
| `autumn-harvest-cli` | `harvest` CLI — management API client, schema migrations, offline replay debugger |
| `autumn-harvest-redis` | Optional Redis Streams dispatch channel and standalone task queue |

Use `autumn-harvest-plugin` for autumn-web apps. Use bare `autumn-harvest` for
non-web or other-framework contexts.

## Cargo.toml

```toml
[dependencies]
autumn-harvest = { version = "0.8", features = ["db"] }
autumn-harvest-plugin = "0.8"
autumn-web = { version = "0.9", features = ["ws"] }
# ... plus standard deps (serde_json, chrono, uuid, tokio, tracing, etc.)
```

The `db` feature (default) pulls Diesel + diesel-async. Requirements: Postgres 12+. CI runs Harvest's own DB suites on Postgres 16 and does not test 12 to 15.

## Defining Workflows

A workflow is a deterministic async function decorated with `#[workflow]`. It receives
a `&WorkflowContext` and an input parameter, and orchestrates activities via the context.

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<()> {
    // Step 1: Send welcome email
    ctx.execute_activity_raw(
        "send_welcome_email",
        serde_json::json!({ "user_id": user_id }),
        "default",  // queue name
    ).await?;

    // Step 2: Wait 24 hours
    ctx.timer("wait-24h", 86_400).await?;

    // Step 3: Check if user has engaged
    let result: serde_json::Value = ctx.execute_activity_raw(
        "check_user_engagement",
        serde_json::json!({ "user_id": user_id }),
        "default",
    ).await?;

    let engaged = result["engaged"].as_bool().unwrap_or(false);
    if !engaged {
        // Step 4: Send reminder
        ctx.execute_activity_raw(
            "send_reminder_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        ).await?;
    }

    Ok(())
}
```

### Determinism Rules

Because workflows replay from history, they must be deterministic:

- **Do**: Use `ctx.execute_activity_raw()` for all side effects
- **Do**: Use `ctx.timer()` instead of `tokio::time::sleep`
- **Do**: Use `ctx.wait_for_signal()` for external events
- **Do**: Use `ctx.spawn_child_workflow_raw()` or the typed `ctx.spawn_child_workflow()` for sub-orchestrations
- **Don't**: Call `Utc::now()`, `rand::random()`, or do I/O directly in workflows
- **Don't**: Use `tokio::spawn` or other non-deterministic operations

## Defining Activities

Activities are where real I/O happens. They're retried per their policy on failure.

```rust
#[activity(
    start_to_close = "30s",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
async fn send_welcome_email(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let user_id = input["user_id"].as_i64().unwrap();
    // Real I/O: send email, call API, write to external DB, etc.
    tracing::info!(user_id, "Sending welcome email");
    Ok(serde_json::json!({ "sent": true }))
}
```

### Activity Attributes

| Attribute | Purpose |
|-----------|---------|
| `start_to_close = "30s"` | Maximum execution time before timeout. An activity with no `start_to_close`, `schedule_to_close` or `heartbeat_timeout` gets the worker's `default_activity_start_to_close`: 10 minutes since 0.7.0 |
| `heartbeat_timeout = "10s"` | Max time between heartbeats for long-running activities |
| `retry = RetryPolicy::exponential(max_attempts, initial_delay)` | Retry policy on failure |

Activities that exhaust their retry policy are moved to a dead letter queue.

Since 0.7.0, a `start_to_close` or `heartbeat_timeout` timeout retries per the
retry policy, like a handler failure. An open circuit breaker defers the task
by default instead of failing it.

## Retry Policies

```rust
// Exponential backoff: up to 3 attempts, starting at 1 second
RetryPolicy::exponential(3, Duration::from_secs(1))

// The retry policy is configured in the #[activity] macro attribute
#[activity(
    start_to_close = "60s",
    retry = RetryPolicy::exponential(5, Duration::from_secs(2))
)]
async fn process_payment(_ctx: &ActivityContext, input: serde_json::Value)
    -> HarvestResult<serde_json::Value> { ... }
```

## Timers

Durable timers survive process restarts:

```rust
#[workflow]
async fn reminder(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<()> {
    // Wait 7 days (durable — survives restarts)
    ctx.timer("wait-7d", 7 * 24 * 3600).await?;

    ctx.execute_activity_raw(
        "send_weekly_digest",
        serde_json::json!({ "user_id": user_id }),
        "default",
    ).await?;

    Ok(())
}
```

## Signals

Inject external events into a running workflow:

```rust
#[workflow]
async fn approval(ctx: &WorkflowContext, request_id: i64) -> HarvestResult<()> {
    // Block until "approved" signal arrives
    let signal: serde_json::Value = ctx.wait_for_signal("approved").await?;

    let approved = signal["approved"].as_bool().unwrap_or(false);
    if approved {
        ctx.execute_activity_raw("process_approval", serde_json::json!({}), "default").await?;
    }
    Ok(())
}

// From a route handler, send signal to a running workflow:
// send_signal(workflow_id, "approved", json!({ "approved": true }))
```

## Child Workflows

Compose orchestrations from smaller workflows:

```rust
#[workflow]
async fn parent(ctx: &WorkflowContext, order_id: i64) -> HarvestResult<()> {
    // Start a child workflow and await its completion
    ctx.spawn_child_workflow_raw(
        "process_payment",
        serde_json::json!({ "order_id": order_id }),
    ).await?;

    ctx.spawn_child_workflow_raw(
        "ship_order",
        serde_json::json!({ "order_id": order_id }),
    ).await?;

    Ok(())
}
```

## Infinite Loop Workflows

For recurring work, use a loop with a timer:

```rust
#[workflow]
async fn weekly_digest(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<()> {
    loop {
        ctx.timer("weekly", 7 * 24 * 3600).await?;

        ctx.execute_activity_raw(
            "build_and_send_digest",
            serde_json::json!({ "user_id": user_id }),
            "default",
        ).await?;

        // Each pass adds history. A run fails at a hard cap (issue #1804).
        // Start a new run with an empty history before that cap.
        if ctx.should_continue_as_new() {
            ctx.continue_as_new(serde_json::json!(user_id)).await?;
        }
    }
    // The function never returns, so no Ok(()) follows the loop.
    // Each run hands off to a new run with the same workflow id.
}
```

## Integrating with autumn-web

Harvest 0.8 needs `autumn-web` 0.9. A leftover `autumn-web = "0.8"` builds a
second copy whose `Plugin` trait `HarvestPlugin` does not implement.

Outside the `dev` profile, a management API with no declared auth layer
answers every mutating route with `401` (0.7.0). Declare an auth layer before
you deploy. See `docs/upgrading/0.7.0.md` for every 0.7.0 default change.

Under the `prod` profile, `autumn-web` 0.9 refuses unknown top-level config
tables. `HarvestPlugin` declares `[harvest]`, so that table is accepted. See
`docs/upgrading/0.8.0.md` for the 0.8.0 changes.

### Building the HarvestPlugin

Create a `harvest_runtime.rs` module that registers all workflows and activities.
Use the `workflows!` and `activities!` macros, as the crate README does:
```rust
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest::prelude::*;

pub fn harvest_plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![onboarding, subscription_lifecycle])
        .activities(activities![send_email, process_payment])
        .api("/api/harvest")
}
```

### Registering the Plugin

In `main.rs`:

```rust
#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .migrations(MIGRATIONS)
        .routes(routes![...])
        .plugin(harvest_runtime::harvest_plugin())
        .run()
        .await;
}
```

### Starting Workflows from Route Handlers

```rust
use autumn_harvest::prelude::*;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_web::reexports::axum::extract::State;

#[post("/subscribe")]
#[secured]
async fn subscribe(
    State(state): State<autumn_web::AppState>,
    session: Session,
) -> AutumnResult<Markup> {
    let user_id: i64 = session.get("user_id").await?.unwrap();

    // Take a connection from the application pool.
    let pool = state.pool().cloned().ok_or_else(|| {
        AutumnError::service_unavailable_msg("subscribe needs an application database")
    })?;
    let mut conn = pool
        .get()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    // Start a durable subscription workflow
    let workflow_id = format!("sub-{user_id}"); // unique workflow ID
    let params = StartWorkflowParams::new(
        "subscription_lifecycle",
        &workflow_id,
        ExecutionId::new(),
        serde_json::json!({ "user_id": user_id }),
        "default", // the queue that WorkerConfig::default() polls
    );
    start_or_load_workflow_execution(&mut conn, params, None).await?;

    // Return immediately — workflow runs in background
    Ok(html! { p { "Subscription started!" } })
}
```

### Convenience Start Functions

A common pattern is to wrap `start_or_load_workflow_execution` in domain-specific
helper functions in `harvest_runtime.rs`:

```rust
use autumn_harvest::diesel_async::AsyncPgConnection;
use autumn_harvest::prelude::*;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};

pub async fn start_onboarding(conn: &mut AsyncPgConnection, user_id: i64) -> HarvestResult<()> {
    let workflow_id = format!("onboard-fan-{user_id}");
    let params = StartWorkflowParams::new(
        "fan_onboarding",
        &workflow_id,
        ExecutionId::new(),
        serde_json::json!({ "user_id": user_id }),
        "default",
    );
    start_or_load_workflow_execution(conn, params, None).await?;
    Ok(())
}
```

## Standalone Harvest Worker

For production scaling, run the worker as a separate binary. `HarvestEmbedding`
(issue #1613) runs the `HarvestPlugin` startup sequence without the plugin.
Mount its router on a plain Axum server. `examples/standalone-runner` is a
runnable example, and `docs/embedding.md` is the reference.

The `charts/autumn-harvest-worker` Helm chart (issue #1989) deploys a standalone
worker with probes, drain and a migration job. Its default image runs demo
workflows. To run your own workflows, build a worker image.

For development, the plugin starts the worker alongside the web server — the
standalone binary is only needed when scaling workers independently.

## DAG Scheduling

Declare directed acyclic graphs of activities with trigger rules:

```rust
#[dag(schedule = "0 0 * * *")] // daily at midnight, UTC
fn my_pipeline(dag: &mut DagBuilder) {
    let e = dag.activity(extract);
    let v = dag.activity(validate);
    // transform waits for both extract AND validate
    let t = dag.activity(transform).upstream(&e).upstream(&v);
    let _l = dag.activity(load).upstream(&t);
}
```

### Trigger Rules

| Rule | Behavior |
|------|----------|
| `TriggerRule::AllSuccess` | All upstream tasks succeeded (default) |
| `TriggerRule::AllDone` | All upstream tasks reached a terminal state |
| `TriggerRule::OneSuccess` | At least one upstream task succeeded |
| `TriggerRule::OneFailed` | At least one upstream task failed |
| `TriggerRule::AllFailed` | All upstream tasks failed |
| `TriggerRule::Manual` | Never fires. Harvest always skips the task, and every task that needs it to succeed |

### Scheduling

A DAG takes its cron expression from the `schedule` attribute, as above. The
scheduler evaluates the expression in UTC. Register the DAG on the plugin with
`.dags(dags![my_pipeline])`. The scheduler then registers the schedule.

A plain workflow takes a `Schedule` through `WorkflowSchedule::new` and
`HarvestBuilder::workflow_schedule`. Each name must match a registered
workflow:

```rust
let builder = HarvestBuilder::new()
    .workflows(workflows![nightly_report, hourly_sync])
    // Cron scheduling, in UTC
    .workflow_schedule(WorkflowSchedule::new(
        "nightly_report",
        Schedule::Cron("0 0 * * *".to_string()), // daily at midnight
    ))
    // Interval scheduling: each slot counts from the previous fire, not from
    // the end of the previous run. `max_active_runs` (default 1) bounds overlap.
    .workflow_schedule(WorkflowSchedule::new(
        "hourly_sync",
        Schedule::Interval(Duration::from_secs(3600)), // one hour
    ));
```

## Management API

When `.api("/api/harvest")` is set on the plugin, these endpoints are mounted:

- Inspect running workflow executions
- Send signals to workflows
- Query workflow state
- Trigger DAG runs manually

## Real-World Example: Content Publishing Workflow

This shows how a typical multi-step workflow looks in practice:

```rust
#[workflow]
async fn content_publishing(ctx: &WorkflowContext, post_id: i64) -> HarvestResult<()> {
    // 1. Validate content
    let validation: serde_json::Value = ctx.execute_activity_raw(
        "validate_content",
        serde_json::json!({ "post_id": post_id }),
        "default",
    ).await?;

    if !validation["valid"].as_bool().unwrap_or(false) {
        ctx.execute_activity_raw("mark_content_failed", serde_json::json!({ "post_id": post_id }), "default").await?;
        return Ok(());
    }

    // 2. Generate thumbnail + transcode (parallel via separate activities)
    ctx.execute_activity_raw("generate_thumbnail", serde_json::json!({ "post_id": post_id }), "default").await?;
    ctx.execute_activity_raw("transcode_media", serde_json::json!({ "post_id": post_id }), "default").await?;

    // 3. Content moderation
    let moderation = ctx.execute_activity_raw("moderate_content", serde_json::json!({ "post_id": post_id }), "default").await?;
    if !moderation["approved"].as_bool().unwrap_or(false) {
        ctx.execute_activity_raw("flag_for_review", serde_json::json!({ "post_id": post_id }), "default").await?;
        return Ok(());
    }

    // 4. Publish
    ctx.execute_activity_raw("publish_content", serde_json::json!({ "post_id": post_id }), "default").await?;

    // 5. Notify fans via child workflow
    ctx.spawn_child_workflow_raw("fan_content_notification", serde_json::json!({ "post_id": post_id })).await?;

    Ok(())
}
```

## Key Points

- **Postgres is the only infrastructure** — no separate workflow service to operate. The embedded SQLite backend needs no database server.
- **Process crashes are fine** — workflows resume from the last recorded event
- **Activities are the side-effect boundary** — all I/O goes in activities
- **Worker pools have shared ceilings** — worker bursts won't starve HTTP connections
- **LISTEN/NOTIFY** provides low-latency dispatch without polling backoff
- **Dead letter queue** catches tasks that exhaust their retry policy
