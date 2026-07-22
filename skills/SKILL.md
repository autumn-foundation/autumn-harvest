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
**Version**: 0.5.0 | **Edition**: 2024 | **MSRV**: 1.88.0
**Author**: autumn-foundation

autumn-harvest is a Postgres-backed durable workflow engine — Temporal-style semantics
with a single-Postgres operational footprint. Designed as a companion to autumn-web,
but usable standalone.

## When to read reference files

This SKILL.md covers workflow/activity patterns, integration, and all common usage.
For deeper internals, read:

- **`references/architecture.md`** — Event-sourced execution model, core components
  (WorkflowContext, Executor, HistoryMatcher, Worker, Queue, Scheduler), full event
  and command type definitions, pool management, testing support. Read this when
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
| `autumn-harvest-macros` | `#[workflow]`, `#[activity]`, `#[dag]`, `workflows![]`, `activities![]` |

Use `autumn-harvest-plugin` for autumn-web apps. Use bare `autumn-harvest` for
non-web or other-framework contexts.

## Cargo.toml

```toml
[dependencies]
autumn-harvest = { version = "0.4", features = ["db"] }
autumn-harvest-plugin = "0.4"
autumn-web = { version = "0.5", features = ["ws"] }
# ... plus standard deps (serde_json, chrono, uuid, tokio, tracing, etc.)
```

The `db` feature (default) pulls Diesel + diesel-async. Requirements: Postgres 12+.

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
    ctx.timer(Duration::from_secs(86400)).await;

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
- **Do**: Use `ctx.start_child_workflow()` for sub-orchestrations
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
| `start_to_close = "30s"` | Maximum execution time before timeout |
| `heartbeat_timeout = "10s"` | Max time between heartbeats for long-running activities |
| `retry = RetryPolicy::exponential(max_attempts, initial_delay)` | Retry policy on failure |

Activities that exhaust their retry policy are moved to a dead letter queue.

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
    ctx.timer(Duration::from_secs(7 * 24 * 3600)).await;

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
    ctx.start_child_workflow(
        "process_payment",
        serde_json::json!({ "order_id": order_id }),
    ).await?;

    ctx.start_child_workflow(
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
        ctx.timer(Duration::from_secs(7 * 24 * 3600)).await;

        ctx.execute_activity_raw(
            "build_and_send_digest",
            serde_json::json!({ "user_id": user_id }),
            "default",
        ).await?;
    }
    // Never returns — runs indefinitely
}
```

## Integrating with autumn-web

### Building the HarvestPlugin

Create a `harvest_runtime.rs` module that registers all workflows and activities.
There are two equivalent API styles:

**Macro style** (from the crate README):
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

**Builder style** (used in the reddit-clone example — more explicit):
```rust
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest::prelude::*;
use crate::workflows::{onboarding, subscription};

pub fn harvest_plugin() -> HarvestPlugin {
    HarvestPlugin::builder()
        .workflow("fan_onboarding", onboarding::fan_onboarding)
        .workflow("subscription_lifecycle", subscription::subscription_lifecycle)
        .activity("send_email", onboarding::send_email)
        .activity("process_payment", subscription::process_payment)
        .build()
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
        .plugins(harvest_runtime::harvest_plugin())
        .run()
        .await;
}
```

### Starting Workflows from Route Handlers

```rust
use autumn_harvest::prelude::*;

#[post("/subscribe")]
#[secured]
async fn subscribe(db: Db, session: Session) -> AutumnResult<Markup> {
    let user_id: i64 = session.get("user_id").await?.unwrap();

    // Start a durable subscription workflow
    start_or_load_workflow_execution(
        "subscription_lifecycle",
        &format!("sub-{}", user_id),  // unique workflow ID
        serde_json::json!({ "user_id": user_id }),
    ).await?;

    // Return immediately — workflow runs in background
    Ok(html! { p { "Subscription started!" } })
}
```

### Convenience Start Functions

A common pattern is to wrap `start_or_load_workflow_execution` in domain-specific
helper functions in `harvest_runtime.rs`:

```rust
pub async fn start_onboarding(user_id: i64) -> HarvestResult<()> {
    start_or_load_workflow_execution(
        "fan_onboarding",
        &format!("onboard-fan-{}", user_id),
        serde_json::json!({ "user_id": user_id }),
    ).await?;
    Ok(())
}
```

## Standalone Harvest Worker

For production scaling, run the worker as a separate binary:

```rust
// src/bin/harvest-runner.rs
fn main() {
    eprintln!("Harvest worker — standalone mode");
    // In a real deployment, this would start the worker runtime
    // without the HTTP server.
}
```

In `Cargo.toml`:

```toml
[[bin]]
name = "myapp-harvest-runner"
path = "src/bin/harvest-runner.rs"
```

For development, the plugin starts the worker alongside the web server — the
standalone binary is only needed when scaling workers independently.

## DAG Scheduling

Declare directed acyclic graphs of activities with trigger rules:

```rust
#[dag]
fn my_pipeline() -> DagDefinition {
    dag! {
        extract -> transform -> load;
        validate -> transform;  // transform waits for both extract AND validate
    }
}
```

### Trigger Rules

| Rule | Behavior |
|------|----------|
| `TriggerRule::AllSuccess` | All predecessors must succeed (default) |
| `TriggerRule::AllComplete` | All predecessors must finish (success or failure) |
| `TriggerRule::OneSuccess` | Any predecessor succeeded |
| `TriggerRule::AnyComplete` | Any predecessor finished |

### Scheduling

```rust
// Cron scheduling
register_schedules(vec![
    Schedule::cron("my_pipeline", "0 0 * * *"),  // daily at midnight
]);

// Interval scheduling
register_schedules(vec![
    Schedule::interval("my_pipeline", Duration::from_secs(3600)),  // every hour
]);
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
    ctx.start_child_workflow("fan_content_notification", serde_json::json!({ "post_id": post_id })).await?;

    Ok(())
}
```

## Key Points

- **Postgres is the only infrastructure** — no separate workflow service to operate
- **Process crashes are fine** — workflows resume from the last recorded event
- **Activities are the side-effect boundary** — all I/O goes in activities
- **Worker pools have shared ceilings** — worker bursts won't starve HTTP connections
- **LISTEN/NOTIFY** provides low-latency dispatch without polling backoff
- **Dead letter queue** catches tasks that exhaust their retry policy
