# autumn-harvest

[![Crates.io](https://img.shields.io/crates/v/autumn-harvest.svg)](https://crates.io/crates/autumn-harvest)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)
[![CI](https://github.com/madmax983/autumn-harvest/actions/workflows/ci.yml/badge.svg)](https://github.com/madmax983/autumn-harvest/actions/workflows/ci.yml)

Postgres-backed durable workflow engine for Rust, designed as a companion to the
[Autumn](https://github.com/madmax983/autumn) web framework. Provides
event-sourced workflow execution with activities, signals, timers, child
workflows, and DAG scheduling — Temporal-style durability semantics with a
single-Postgres operational footprint.

## Why

Most Rust async work is fire-and-forget. autumn-harvest is for the work that
*can't* be: long-running orchestrations that survive process restarts, retries
with exactly-once semantics, multi-step business processes with rollback, and
scheduled DAGs. If you've reached for Temporal, Cadence, or Inngest from a Rust
service, this is the same shape with one fewer service to operate.

## Quick example

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<()> {
    ctx.execute_activity_raw(
        "send_welcome_email",
        serde_json::json!({ "user_id": user_id }),
        "default",
    )
    .await?;
    Ok(())
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, std::time::Duration::from_secs(1)))]
async fn send_welcome_email(_ctx: &ActivityContext, input: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    // … real I/O. Failure here is retried per the policy above.
    Ok(serde_json::json!({ "sent": true }))
}
```

Wired into an Autumn app via the plugin:

```rust
use autumn_web::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![onboarding])
                .activities(activities![send_welcome_email])
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

## What you get

- **Event-sourced execution.** Workflows are deterministic functions; their
  history is a Postgres event log. Restart the process, replay the history, end
  up at the same state.
- **Activities with retries.** Side effects live in `#[activity]` functions
  with configurable `start_to_close`, `heartbeat_timeout`, and `retry` policies.
- **Signals & queries.** Send a signal into a running workflow, query its
  state, or block on a timer.
- **Child workflows.** Compose orchestrations from smaller workflows; parent
  failures cascade or compensate per your design.
- **DAG scheduling.** Declare DAGs of activities with trigger rules and
  cron/interval schedules; built-in scheduler dispatches them.
- **Management API.** Optional HTTP surface for inspecting executions, sending
  signals, querying state, and triggering DAG runs.
- **SKIP LOCKED task queue + LISTEN/NOTIFY** for low-latency dispatch without
  polling backoff.
- **Dead letter queue** for tasks that exhaust their retry policy.
- **Separate worker/web connection pools** with a shared ceiling so worker
  bursts can't starve HTTP request handling.

## Workspace

| Crate | Purpose |
|-------|---------|
| [`autumn-harvest`](autumn-harvest/) | Core engine — types, executor, replay, queue, worker runtime |
| [`autumn-harvest-plugin`](autumn-harvest-plugin/) | `HarvestPlugin` — wires the engine into an Autumn `AppBuilder`, mounts the management API, owns the runtime lifecycle |
| [`autumn-harvest-macros`](autumn-harvest-macros/) | `#[workflow]`, `#[activity]`, `#[dag]`, `workflows![]`, `activities![]` proc macros |

Use `autumn-harvest-plugin` if you're building an Autumn app. Use the bare
`autumn-harvest` crate if you want to embed the engine in another framework or
a non-web context.

## Requirements

- Rust 1.86.0 or newer (MSRV)
- Postgres 12+
- The `db` feature is enabled by default and pulls Diesel + diesel-async; build
  with `--no-default-features` for pure compile-checks on systems without
  libpq.

## Status

Phase 3 (DAG scheduling, signals, queries, management API) is implemented and
exercised by integration tests. Phase 4 (cancellation/saga semantics, sticky
cross-worker routing, richer observability, dashboard UI) is the next focus.

API stability: pre-1.0. Breaking changes happen in minor versions per Cargo's
0.x semver convention. Each release notes the migration where applicable.

## Architecture in one paragraph

Workflows are deterministic Rust async functions. When they hit
`ctx.execute_activity(...)` for the first time, the activity is enqueued to a
Postgres task queue and the workflow suspends. A worker claims the activity
(`SELECT … FOR UPDATE SKIP LOCKED`), runs it, and writes the result as an event
in the workflow's history. The workflow then resumes — on the *same* worker if
cached, or by replaying its history from scratch on any other worker. Replay is
deterministic because every non-deterministic decision (activity result, timer
fire, signal arrival, version branch) is recorded as an event the first time
and read back from history on subsequent invocations. This is the same model as
Temporal and Cadence; the operational difference is that you only need
Postgres, not a separate service.

## License

Dual-licensed under MIT or Apache 2.0 at your option.
