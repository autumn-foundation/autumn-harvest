---
name: autumn-harvest
description: >
  Use when the user mentions autumn-harvest, HarvestPlugin, WorkflowContext,
  ActivityContext, durable Rust workflows, Temporal-style orchestration on
  Postgres, #[workflow], #[activity], #[dag], workflow signals, durable timers,
  child workflows, workflow updates, replay fixtures, DLQ operations, shard
  readiness, or the harvest CLI. Use with the autumn-web skill when embedding
  workflows in an Autumn application.
---

# autumn-harvest - Durable Workflow Engine for Rust

**Repository**: https://github.com/madmax983/autumn-harvest
**Version**: 0.3.0 | **Edition**: 2024 | **MSRV**: 1.88.0
**Author**: madmax983

autumn-harvest is a Postgres-backed durable workflow engine for Rust: Temporal-style
execution semantics, event-sourced replay, durable timers/signals/child workflows,
operator APIs, and a single-Postgres operational footprint. It is designed to plug
into autumn-web, but the core engine can run standalone.

## When to Read Reference Files

This file covers common usage and current 0.3.0 traps. For deeper internals, read:

- `references/architecture.md` - event-sourced execution, command/event model,
  worker/runtime architecture, sharding, testing support, and operational surfaces.

## Current Workspace

| Crate | Purpose |
|-------|---------|
| `autumn-harvest` | Core engine: types, executor, replay, queue, worker runtime, testing helpers |
| `autumn-harvest-plugin` | `HarvestPlugin`: autumn-web integration, runtime lifecycle, management API, Vantage UI |
| `autumn-harvest-macros` | `#[workflow]`, `#[activity]`, `#[dag]`, `workflows![]`, `activities![]`, `dags![]` |
| `autumn-harvest-cli` | `harvest` operator client and `harvest-replay` validator |
| `autumn-harvest-redis` | Redis Streams task queue adapter |
| `examples/quickstart` | Small end-to-end path |
| `examples/billing-autumn-web` | Full Autumn billing app integration |
| `examples/standalone-runner` | Non-Autumn runner path with mounted management API |

## Cargo.toml

For an Autumn app:

```toml
[dependencies]
autumn-harvest = "0.3"
autumn-harvest-plugin = "0.3"
autumn-web = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["full"] }
```

Feature notes:

- `autumn-harvest` enables `db` by default, pulling Diesel, diesel-async, and
  embedded Postgres migrations.
- Use `autumn-harvest = { version = "0.3", default-features = false }` for pure
  compile/test paths that must avoid libpq.
- Use `features = ["testing"]` for `WorkflowReplayer` and test contexts.
- Use `features = ["metrics-rs"]` to bridge Harvest metrics into the `metrics`
  crate.

## Execution Model

Workflows are deterministic async Rust functions. During live execution,
`WorkflowContext` commands append events and suspend. Workers claim runnable tasks
with Postgres `SKIP LOCKED`, execute activities or workflow tasks, write the next
events, and wake waiters through LISTEN/NOTIFY. During replay, the same workflow
function runs from the top and reads recorded events instead of re-running side
effects. Every non-deterministic decision must cross a Harvest boundary.

## Workflow Authoring

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<serde_json::Value> {
    let sent = ctx
        .execute_activity_raw(
            "send_welcome_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        )
        .await?;

    ctx.timer("welcome-followup", 86_400).await?;

    Ok(serde_json::json!({ "email": sent, "user_id": user_id }))
}
```

Important workflow APIs:

| API | Use |
|-----|-----|
| `ctx.execute_activity_raw(name, input, queue).await?` | Durable remote side effect on a task queue |
| `ctx.execute_local_activity_raw(name, input, retry, timeout).await?` | Fast in-process deterministic activity |
| `ctx.execute_activity_external(name, input, queue, secs).await?` | Activity completed later via task token API |
| `ctx.timer(timer_id, duration_secs).await?` | Durable sleep; use a stable timer id |
| `ctx.wait_for_signal(signal).await?` | Block until an external signal arrives |
| `ctx.spawn_child_workflow_raw(name, input).await?` | Start and await a child workflow |
| `ctx.version(change_id, min, max)` | Fence non-deterministic workflow code changes |
| `ctx.side_effect(id, f)?` / `ctx.random_uuid(id)?` | Record replay-safe derived values |
| `ctx.upsert_search_attrs(patch)?` | Add primitive query/filter attributes |
| `ctx.continue_as_new(input).await?` | Rotate long-running workflow history |
| `ctx.register_query(...)` / `ctx.register_update_handler(...)` | Workflow query/update surface |
| `ctx.check_cancellation()?` | Cooperative cancellation checkpoint |

Determinism rules:

- Put all I/O in activities, external activities, signals, timers, or child workflows.
- Use `ctx.timer`, not `tokio::time::sleep`.
- Use `ctx.now()`, `ctx.side_effect`, or `ctx.random_uuid`, not `Utc::now()` or
  ad hoc random generation inside workflow code.
- Do not `tokio::spawn` from workflow code.
- Before editing a deployed workflow, export histories and replay them with
  `WorkflowReplayer` or `harvest-replay`. New activity/timer/signal/child order
  requires a `ctx.version(...)` fence.

## Activity Authoring

```rust
use std::time::Duration;
use autumn_harvest::prelude::*;

#[activity(
    start_to_close = "30s",
    heartbeat_timeout = "10s",
    queue = "email-workers",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1)),
    max_concurrent = 25,
    concurrency_key = "email"
)]
async fn send_welcome_email(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let idempotency_key = ctx.idempotency_key()?;
    tracing::info!(%idempotency_key, ?input, "sending welcome email");
    ctx.heartbeat(serde_json::json!({ "phase": "submitted" })).await?;
    Ok(serde_json::json!({ "sent": true }))
}
```

Supported `#[activity]` attributes:

| Attribute | Notes |
|-----------|-------|
| `start_to_close = "30s"` | Handler runtime limit |
| `heartbeat_timeout = "10s"` | Max gap between heartbeats for long work |
| `schedule_to_start = "5m"` | Max queue wait before timeout |
| `queue = "email-workers"` | Task queue name |
| `retry = RetryPolicy::exponential(...)` | Any expression returning `RetryPolicy` |
| `max_concurrent = N` | Cluster-wide activity cap |
| `concurrency_key = "stripe"` | Share a cap across multiple activities |
| `local = true` | Inline local activity; no queue, heartbeat, or schedule-to-start |

Activity behavior notes:

- Activities are at-least-once. Use `ctx.idempotency_key()` for downstream calls.
- `ActivityFailure` gives typed retry semantics: `error_type`, `message`,
  optional `details`, and `non_retryable`. Legacy `Err(String)` still works.
- `RetryPolicy::non_retryable_errors` matches `ActivityFailure.error_type` first,
  then falls back to legacy string matching.
- Local activities are durable and replayed through `LocalActivity*` events, but
  they do not support heartbeats or custom queues.

## Long-Running Workflows

0.3.0 added history guardrails. Replaying a workflow loads its event history, so
pollers and monitors must rotate before history grows without bound.

```rust
#[workflow]
async fn polling_loop(ctx: &WorkflowContext, mut state: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    loop {
        let cycle = state["cycle"].as_u64().unwrap_or(0);
        let result = ctx
            .execute_activity_raw("poll_remote_system", state.clone(), "pollers")
            .await?;

        if result["done"].as_bool().unwrap_or(false) {
            return Ok(result);
        }

        let next_state = serde_json::json!({
            "cycle": cycle + 1,
            "cursor": result.get("next_cursor").cloned(),
        });

        if ctx.should_continue_as_new() {
            ctx.continue_as_new(next_state.clone()).await?;
        }

        let timer_id = format!("poll-delay-{cycle}");
        ctx.timer(&timer_id, 60).await?;
        state = next_state;
    }
}
```

Builder knobs:

```rust
let harvest = HarvestBuilder::new()
    .workflows(workflows![polling_loop])
    .history_continue_as_new_threshold(5_000)
    .history_event_hard_cap(20_000)
    .try_build()?;
```

The default soft threshold is `10_000` events. A configured hard cap fails the
execution and moves it to the DLQ with `HistoryCapExceeded` if workflow code does
not rotate.

## Embedding in autumn-web

```rust
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![onboarding])
                .activities(activities![send_welcome_email])
                .dags(dags![nightly_billing])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

Use `.api_with_auth(path, middleware)` for non-dev admin/API mounts. 0.3.0 has
route classification and tests for the management auth posture; do not expose
mutation routes without an auth boundary in production.

For short request/response workflows, use the shard-aware `WorkflowHandleClient`
installed into `AppState` by the plugin:

```rust
let started = workflow_handle_client
    .start_or_load(&mut conn, start_params)
    .await?;

let output = started.handle.result_raw().await?;
```

`result_raw()` waits on Postgres LISTEN/NOTIFY. Do not poll
`GET /workflows/{id}` and scan full histories. HTTP clients that already know an
execution id should use `GET /api/harvest/workflows/{id}/result?wait=5s`.

## Management API and CLI

The `harvest` CLI is a thin HTTP client for the plugin API. It does not talk to
Postgres directly. Configure with `--base-url`/`HARVEST_URL` and
`--token`/`HARVEST_TOKEN`.

Common commands:

```bash
cargo run -p autumn-harvest-cli -- health
cargo run -p autumn-harvest-cli -- preflight
cargo run -p autumn-harvest-cli -- workflow list --state RUNNING --search-attr tenant=acme
cargo run -p autumn-harvest-cli -- workflow start onboarding --input-json '{"user_id":42}'
cargo run -p autumn-harvest-cli -- workflow signal <execution-id> approved --payload-json '{"approved":true}'
cargo run -p autumn-harvest-cli -- workflow stack <execution-id>
cargo run -p autumn-harvest-cli -- workflow reset <execution-id> --to-event 42 --reason "bad deploy" --operator-id mark --dry-run
cargo run -p autumn-harvest-cli -- history export <execution-id> --payload-policy full --output-file fixtures/history.json
cargo run -p autumn-harvest-cli -- version-usage --change-id billing_v2 --version 1 --guard
cargo run -p autumn-harvest-cli -- preflight
cargo run -p autumn-harvest-cli -- shard health --candidate-shard 1
cargo run -p autumn-harvest-cli -- worker list --queue email-workers --shard-id 0
cargo run -p autumn-harvest-cli -- worker drain <worker-id> --wait
cargo run -p autumn-harvest-cli -- dlq list --limit 25
cargo run -p autumn-harvest-cli -- dlq bulk-replay --activity-name send_email --dry-run
cargo run -p autumn-harvest-cli -- concurrency status
```

`harvest preflight` gates release promotion: it checks runtime/API readiness,
migrations on every configured shard, shard read/write availability, catalog and
schedule resolvability, worker coverage, DLQ access, retention visibility, and
admin auth posture. Exit code `0` is pass, `2` is warn, `1` is fail or
transport/API error.

## Replay and Release Gates

Use `WorkflowReplayer` for code changes to deployed workflows:

```rust
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

#[tokio::test]
async fn onboarding_history_still_replays() {
    let json = std::fs::read_to_string("fixtures/onboarding_history.json").unwrap();

    let report = WorkflowReplayer::new()
        .register_fn("onboarding", onboarding_handler)
        .replay_from_json(&json)
        .await
        .expect("fixture parses");

    assert!(matches!(report.status, ReplayStatus::ReplaySucceeded), "{report}");
}
```

Batch exports from staging/production are release evidence. Treat partial shard
coverage as a blocked gate unless the release owner explicitly waives it. Use
`ctx.version(change_id, old, new)` before shipping non-deterministic workflow
changes, and run the version-gate usage/retirement checks before deleting old
branches.

## Operational Features in 0.3.0

- Workflow handle result waiting without full-history polling.
- Deterministic workflow guardrail catalog and replay fixture exports.
- History-size guardrails for long-running workflows and continue-as-new.
- External activity handoffs with task-token completion/failure endpoints.
- Worker drain controls, worker heartbeat/fleet visibility, and build-id routing.
- Shard readiness health gate and deployment preflight.
- Version-gate usage and retirement checks.
- Search-attribute workflow filtering.
- Audited bulk DLQ replay/discard paths.
- Starter alert pack and runbooks.
- Vantage dashboard rendered through maud + autumn-web extractors.

## Development Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --lib
cargo test -p autumn-harvest --no-default-features
cargo test -p autumn-harvest --features testing --no-default-features
cargo test -p autumn-harvest --features db
cargo test -p autumn-harvest-plugin
cargo test -p autumn-harvest-cli
cargo test -p autumn-harvest-redis
cargo bench -p autumn-harvest --features testing --no-default-features --bench replay_bench
```

Testcontainers-backed DB/Redis tests require Docker. Windows no-libpq paths
should use `--no-default-features`.

## Gotchas

- `ctx.timer` takes a stable string id plus seconds: `ctx.timer("delay", 60)`.
  Old `ctx.timer(Duration::...)` examples are wrong for 0.3.0.
- `spawn_child_workflow_raw` takes workflow name and input; Harvest assigns the
  child execution id.
- Macro-generated code must route through `::autumn_harvest::...` paths. Do not
  emit direct `::serde_json::` or `::autumn_web::` paths from macros; downstream
  users may not depend on those crates directly.
- `WorkflowEvent` JSON is adjacently tagged as `{"type": "...", "data": ...}`.
  Add variants; never rename, remove, or reorder stored event meanings.
- Local activities cannot set `queue`, `heartbeat_timeout`, or
  `schedule_to_start`.
- Activities sharing a `concurrency_key` must declare the same
  `max_concurrent`; `HarvestBuilder::try_build()` rejects mismatches.
- The management CLI is API-only. Do not add direct Postgres behavior to it.
- Use `trunk-dev` as the PR base. `trunk` is the production release branch.

## Key Points

- Postgres is the default durable substrate; Redis is an optional task queue
  adapter, not a replacement for workflow history.
- Activities are the side-effect boundary and are at-least-once.
- Workflows are replayed Rust code; determinism is a production invariant, not
  a style preference.
- Shard ownership is encoded in `ExecutionId`, so reads route in O(1).
- Operator APIs are part of the product surface: preflight, replay export,
  version gates, worker drain, DLQ bulk operations, audit, and Vantage UI should
  stay covered by tests when changed.
