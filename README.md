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
with durable history, signal-driven waits, queryable state, and scheduled DAGs.
If you've reached for Temporal, Cadence, or Inngest from a Rust service, this is
the same shape with one fewer service to operate.

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
- **Child workflows.** Compose orchestrations from smaller workflows and model
  recovery paths in normal workflow code.
- **DAG scheduling.** Declare DAGs of activities with trigger rules and
  cron/interval schedules; built-in scheduler dispatches them.
- **Management API.** Optional HTTP surface for inspecting executions, sending
  signals, querying state, triggering DAG runs, and managing dead letters.
- **SKIP LOCKED task queue + LISTEN/NOTIFY** for low-latency dispatch without
  polling backoff.
- **Dead letter queue** for tasks that exhaust their retry policy, with
  management endpoints to inspect and replay entries.
- **Separate worker/web connection pools** with a shared ceiling so worker
  bursts can't starve HTTP request handling.

## Workspace

| Crate | Purpose |
|-------|---------|
| [`autumn-harvest`](autumn-harvest/) | Core engine — types, executor, replay, queue, worker runtime |
| [`autumn-harvest-plugin`](autumn-harvest-plugin/) | `HarvestPlugin` — wires the engine into an Autumn `AppBuilder`, mounts the management API, owns the runtime lifecycle |
| [`autumn-harvest-macros`](autumn-harvest-macros/) | `#[workflow]`, `#[activity]`, `#[dag]`, `workflows![]`, `activities![]` proc macros |
| [`autumn-harvest-cli`](autumn-harvest-cli/) | `harvest` CLI: thin operator client for the management API |

Use `autumn-harvest-plugin` if you're building an Autumn app. Use the bare
`autumn-harvest` crate if you want to embed the engine in another framework or
a non-web context.

## CLI

The `harvest` binary is a thin HTTP client for the optional management API. It
does not talk to Postgres directly, so workflow queries, DAG triggers, auth, and
runtime-owned behavior stay behind the same API surface your service exposes.

```bash
cargo run -p autumn-harvest-cli -- health
cargo run -p autumn-harvest-cli -- workflow list --limit 25
cargo run -p autumn-harvest-cli -- workflow get <execution-id>
cargo run -p autumn-harvest-cli -- workflow start approval_workflow --input-json '{"request_id":"42"}'
cargo run -p autumn-harvest-cli -- workflow signal <execution-id> approved --payload-json '{"approved":true}'
cargo run -p autumn-harvest-cli -- workflow query <execution-id> status
cargo run -p autumn-harvest-cli -- workflow cancel <execution-id> --reason "operator request"
cargo run -p autumn-harvest-cli -- dag list
cargo run -p autumn-harvest-cli -- dag trigger daily_pipeline --conf-json '{"date":"2026-04-21"}'
cargo run -p autumn-harvest-cli -- dag pause daily_pipeline
cargo run -p autumn-harvest-cli -- dlq list --limit 25
cargo run -p autumn-harvest-cli -- dlq replay <dead-letter-id>
```

Configure the API mount with `--base-url` or `HARVEST_URL` (default:
`http://localhost:3000/api/harvest`). Pass `--token` or `HARVEST_TOKEN` to send
a bearer token. Successful responses are printed as pretty JSON by default; use
`--output json` for compact script-friendly output. JSON request payloads accept
inline `--*-json` values or `--*-file PATH`; use `-` as the file path to read
from stdin.

## Requirements

- Rust 1.88.0 or newer (MSRV)
- Postgres 12+
- The `db` feature is enabled by default and pulls Diesel + diesel-async; build
  with `--no-default-features` for pure compile-checks on systems without
  libpq.

## Status

Version 0.2.0 wraps the Phase 3 surface: DAG scheduling, `#[dag]`, trigger
rules, signal delivery, `ctx.wait_for_signal`, query registration/dispatch, the
management API, and dead-letter list/replay endpoints are implemented and
covered by integration tests. Durable workflow cancellation is implemented with
management API support and activity heartbeat cancellation checks. First-class
Saga compensation is implemented through the `Saga` builder.

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

## Sharding

Workflow state can be spread across multiple Postgres databases without
cross-shard transactions. Each `ExecutionId` carries its `ShardId` in the
first two bytes of the UUID, so any caller holding an id resolves to the
owning shard in O(1) — no directory table required. Single-shard deployments
keep working unchanged; the plugin wires a `ShardRouter::single()` and a
`ShardedDbPool::single(pool)` by default.

```rust
use autumn_harvest::{ExecutionId, ShardId, ShardRouter};

let router = ShardRouter::new(
    vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)], // readable
    vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)], // writable
    ShardId::new(0),                                         // default
);
let shard = router.pick_for_new_workflow("onboarding", "user-42");
let exec_id = ExecutionId::new_for_shard(shard);
assert_eq!(exec_id.shard(), shard);
```

Adding a shard (new workflows only): provision and migrate the new database,
add it to `readable_shards`, restart the plugin, then flip it into
`writable_shards`. In-flight workflows drain on their original shard.

## License

Dual-licensed under MIT or Apache 2.0 at your option.
