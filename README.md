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

Try it end-to-end: `cargo run -p quickstart` (see [`examples/quickstart/`](examples/quickstart/)).

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
- **Per-activity concurrency caps.** Declare `max_concurrent = N` on an
  activity to enforce a cluster-wide cap without spinning up dedicated worker
  processes. Activities sharing a rate-limited dependency can share a budget
  via `concurrency_key = "stripe"`.
- **Signals & queries.** Send a signal into a running workflow, query its
  state, or block on a timer.
- **Child workflows.** Compose orchestrations from smaller workflows and model
  recovery paths in normal workflow code.
- **Workflow cron schedules.** Register any workflow on a cron/interval
  expression with one builder call — no DAG wrapper required. Choose
  `WorkflowSchedule` when you want "run this workflow on a schedule";
  choose `DagBuilder` when you need dependency-graph fan-out and trigger
  rules between tasks.
- **DAG scheduling.** Declare DAGs of activities with trigger rules and
  cron/interval schedules; built-in scheduler dispatches them.
- **Management API.** Optional HTTP surface for inspecting executions, sending
  signals, querying state, triggering DAG runs, and managing dead letters.
- **SKIP LOCKED task queue + LISTEN/NOTIFY** for low-latency dispatch without
  polling backoff.
- **Dead letter queue** for tasks that exhaust their retry policy, with
  management endpoints to inspect and replay entries.
- **Retention janitor** (opt-in) to prune completed workflow histories older
  than a configured max age, with status and run-now controls on the admin API.
  Running workflows are unaffected because only terminal-state executions with
  `completed_at` older than the retention window are eligible.
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
cargo run -p autumn-harvest-cli -- workflow list --state RUNNING --search-attr tenant=acme
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
cargo run -p autumn-harvest-cli -- dlq bulk-replay --activity-name send_email --dry-run
cargo run -p autumn-harvest-cli -- dlq bulk-replay --activity-name send_email
cargo run -p autumn-harvest-cli -- dlq bulk-discard --activity-name send_email --failed-before 2026-04-27T00:00:00Z
cargo run -p autumn-harvest-cli -- retention status
cargo run -p autumn-harvest-cli -- retention run-now
cargo run -p autumn-harvest-cli -- concurrency status
```

Configure the API mount with `--base-url` or `HARVEST_URL` (default:
`http://localhost:3000/api/harvest`). Pass `--token` or `HARVEST_TOKEN` to send
a bearer token. Successful responses are printed as pretty JSON by default; use
`--output json` for compact script-friendly output. JSON request payloads accept
inline `--*-json` values or `--*-file PATH`; use `-` as the file path to read
from stdin.

### Controlling duplicate workflow starts

By default, starting a workflow with a `(workflow_name, workflow_id)` pair that already exists returns the existing execution. This "allow duplicate" behaviour is correct for upstream services that retry a start whose response was lost. It is **not** correct when you need at-most-one, retry-after-failure, or terminate-and-replace semantics.

Pass `reuse_policy` in the HTTP body (or `--reuse-policy` on the CLI) to override the default:

| Value | Behaviour |
|---|---|
| `allow_duplicate` (default) | Return the existing execution unconditionally. |
| `reject_duplicate` | Return **409 Conflict** with `existing_execution_id` and `existing_state` in the body. Use for at-most-one semantics. |
| `allow_duplicate_failed_only` | Start fresh if the prior execution is FAILED or CANCELLED; return the existing execution if RUNNING or COMPLETED. Use for retry-after-failure. |
| `terminate_if_running` | Cancel a RUNNING prior execution and start a fresh run; start fresh unconditionally for any terminal prior state. |

An unknown `reuse_policy` value returns `400 Bad Request` with the offending value echoed in the error body; it does not silently fall back to the default.

**Retry-after-failure**: pass `reuse_policy: "allow_duplicate_failed_only"` so an upstream retry against a failed run gets a fresh execution while a successful or in-progress run is not superseded. Pass `reuse_policy: "reject_duplicate"` for strict at-most-one semantics.

```bash
# Retry-after-failure: start fresh only if the prior run failed or was cancelled
cargo run -p autumn-harvest-cli -- workflow start billing_workflow \
    --workflow-id "order-42" \
    --reuse-policy allow_duplicate_failed_only \
    --input-json '{"order_id": 42}'

# At-most-one: reject a second start with 409 while the workflow is still running
cargo run -p autumn-harvest-cli -- workflow start billing_workflow \
    --workflow-id "order-42" \
    --reuse-policy reject_duplicate

# Terminate-and-replace: cancel a stuck run and immediately start fresh
cargo run -p autumn-harvest-cli -- workflow start billing_workflow \
    --workflow-id "order-42" \
    --reuse-policy terminate_if_running
```

Via the HTTP API directly:

```json
POST /api/harvest/workflows/billing_workflow/start
{
  "workflow_id": "order-42",
  "reuse_policy": "allow_duplicate_failed_only",
  "input": {"order_id": 42}
}
```

A 409 response body looks like:

```json
{
  "existing_execution_id": "01234567-...",
  "existing_state": "RUNNING"
}
```

### Capping concurrency for rate-limited downstream dependencies

Workflows commonly integrate with rate-limited APIs (Stripe, OpenAI, SendGrid,
Twilio, S3 multipart, internal microservices with a `max_connections` cap).
Without a concurrency cap the cluster may fan out dozens of simultaneous calls,
triggering 429s or downstream timeouts.

The old workaround — a dedicated task queue with a single worker process set to
`max_concurrent_activities = N` — is operationally expensive and fragile
(autoscalers or accidental second instances break the invariant).

Instead, declare `max_concurrent = N` directly on the activity:

```rust
// At most 5 concurrent Stripe calls across the whole cluster.
#[activity(start_to_close = "30s", max_concurrent = 5)]
async fn charge_stripe(ctx: &ActivityContext, amount_cents: u64) -> HarvestResult<String> {
    // … Stripe SDK call
}
```

When multiple activities all touch the same rate-limited API, share the budget
with `concurrency_key`:

```rust
// "stripe" budget: combined in-flight count of both activities never exceeds 5.
#[activity(start_to_close = "30s", max_concurrent = 5, concurrency_key = "stripe")]
async fn charge_stripe(ctx: &ActivityContext, amount_cents: u64) -> HarvestResult<String> {
    // …
}

#[activity(start_to_close = "10s", max_concurrent = 5, concurrency_key = "stripe")]
async fn refund_stripe(ctx: &ActivityContext, charge_id: String) -> HarvestResult<()> {
    // …
}
```

Activities sharing a `concurrency_key` must declare the **same** `max_concurrent`
value. Disagreeing values are caught at `HarvestBuilder::build()` time:

```
HarvestBuilderError::ConcurrencyKeyMismatch { key: "stripe", activities: [...] }
```

The cap is enforced cluster-wide via a `WHERE NOT EXISTS` predicate on the
`harvest_task_queue` claim query. A task whose key is saturated is skipped and
re-evaluated on the next poll — no dedicated worker process, no extra table, no
background coordinator. The partial index
`harvest_task_queue_concurrency_key_running` makes the saturation check O(log n)
on RUNNING rows with a non-NULL key; activities without a cap pay zero overhead.

Inspect live stats with the CLI:

```bash
harvest concurrency status
# [{ "key": "stripe", "max_concurrent": 5, "in_flight": 3, "pending": 12 }, …]
```

Or via the management API directly:

```bash
GET /api/harvest/admin/concurrency
```

### Filtering the workflow list

`workflow list` (and `GET /workflows`) accept three additional filter knobs on
top of `limit`:

| CLI flag | Query param | Behavior |
|---|---|---|
| `--state RUNNING` (repeatable, also accepts `RUNNING,FAILED`) | `?state=RUNNING,FAILED` (repeatable) | Exact match on the workflow execution state. Allowed values: `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `TIMED_OUT`. |
| `--workflow-name onboarding` | `?workflow_name=onboarding` | Exact match on the registered workflow name. |
| `--search-attr tenant=acme` (repeatable) | `?search_attr=tenant:acme` (repeatable) | JSONB containment predicate on `search_attrs`. Multiple flags AND together; repeating a key narrows. Hits the existing `idx_harvest_we_search` GIN index. |

Invalid values (unknown state, malformed `search_attr` missing the `:`
separator) return `400 Bad Request` instead of silently matching nothing.

Triage example — find the running onboarding workflows for a single tenant:

```bash
cargo run -p autumn-harvest-cli -- workflow list \
    --state RUNNING \
    --workflow-name onboarding \
    --search-attr tenant=acme
```

### Post-incident DLQ drain

After an infrastructure incident (database blip, downstream 503, misconfigured retry policy), many activities end up in the dead-letter queue. Rather than replaying them one-by-one with `dlq replay <id>`, use the bulk endpoints to drain the queue by activity or time window.

**Always dry-run first** to see what will be acted on before committing:

```bash
# Preview — no writes, returns matched count and IDs
curl -s -X POST https://api.example.com/api/harvest/dead-letters/replay \
  -H 'Content-Type: application/json' \
  -d '{"activity_name":"send_email","failed_after":"2026-04-27T12:00:00Z","dry_run":true}' | jq .
```

The response reports `matched` (total rows satisfying the filter, before any limit clip), `acted_on`, and any per-row `failures`. When `matched` exceeds `acted_on` after a non-dry run, the limit clipped the result — call again until `matched == 0`.

```bash
# Real replay: re-enqueue all matching dead-letter entries
curl -s -X POST https://api.example.com/api/harvest/dead-letters/replay \
  -H 'Content-Type: application/json' \
  -d '{"activity_name":"send_email","failed_after":"2026-04-27T12:00:00Z"}' | jq .

# Discard (delete without re-enqueueing) — use when the work is no longer needed
curl -s -X POST https://api.example.com/api/harvest/dead-letters/discard \
  -H 'Content-Type: application/json' \
  -d '{"activity_name":"charge_card","failed_before":"2026-04-27T00:00:00Z"}' | jq .
```

Or via the CLI:

```bash
# Dry run first
cargo run -p autumn-harvest-cli -- dlq bulk-replay \
    --activity-name send_email \
    --failed-after 2026-04-27T12:00:00Z \
    --dry-run

# Commit the replay
cargo run -p autumn-harvest-cli -- dlq bulk-replay \
    --activity-name send_email \
    --failed-after 2026-04-27T12:00:00Z

# Discard stale failures
cargo run -p autumn-harvest-cli -- dlq bulk-discard \
    --activity-name charge_card \
    --failed-before 2026-04-27T00:00:00Z
```

At least one of `--activity-name`, `--workflow-name`, `--failed-after`, or `--failed-before` is required. A filter with only `--limit` or `--dry-run` is rejected with `400 Bad Request`. The default batch size is 100 rows; pass `--limit N` (max 1000) to replay more per call.

### Scheduling workflows on a cron

Use `WorkflowSchedule` when you want to fire a single workflow on a cron or
interval expression. Use `DagBuilder` when you need dependency-graph fan-out,
trigger rules, or multi-step pipelines between tasks.

```rust
use autumn_harvest::policy::{Schedule, WorkflowSchedule};

// Register a daily billing run at 03:00 UTC with at-most-1 concurrent run.
let sched = WorkflowSchedule::new(
    "daily_billing_report",
    Schedule::Cron("0 3 * * *".to_string()),
)
.with_input(serde_json::json!({"region": "us-east"}))
.with_max_active_runs(1);

// Wire it into the builder alongside your workflow registration.
let app = autumn_web::app()
    .workflows(workflows![daily_billing_report])
    .workflow_schedule(sched)
    .worker(WorkerConfig::default());
```

The scheduler tick derives a deterministic `workflow_id` of
`sched:{name}:{unix_ts}` so retries after a crashed tick are idempotent.
`max_active_runs = 1` (the default) means: if the previous run is still
`RUNNING` when the next cron fires, skip that firing rather than stack runs.
Set `catchup = true` to replay missed firings after a scheduler downtime window.

Manage schedules via the CLI:

```bash
# Create a workflow schedule via API
cargo run -p autumn-harvest-cli -- schedule create-workflow \
    --name daily_billing_report \
    --cron "0 3 * * *" \
    --max-active-runs 1

# List all schedules (both DAG and workflow kinds)
cargo run -p autumn-harvest-cli -- schedule list

# Pause / resume / delete by ID
cargo run -p autumn-harvest-cli -- schedule pause  <id>
cargo run -p autumn-harvest-cli -- schedule resume <id>
cargo run -p autumn-harvest-cli -- schedule delete <id>
```

Or via the HTTP API:

```bash
POST /api/harvest/admin/schedules/workflow
GET  /api/harvest/admin/schedules
POST /api/harvest/admin/schedules/{id}/pause
POST /api/harvest/admin/schedules/{id}/resume
DELETE /api/harvest/admin/schedules/{id}
```

### Worker Fleet

The management API exposes three routes for observing the live worker fleet. Workers register on startup and heartbeat every 5 s (configurable via `WorkerConfig`). Workers that have not heartbeated within `2 × heartbeat_interval` (default 10 s) are classified as `stale` at query time but are not auto-deleted.

```bash
# List all workers (supports ?queue=, ?shard_id=, ?status=, ?health= filters)
GET /api/harvest/workers

# Filter to workers polling a specific queue on a specific shard
GET /api/harvest/workers?queue=email-workers&shard_id=0

# Single worker detail — includes currently claimed task-queue item IDs
GET /api/harvest/workers/{worker_id}

# Fleet health roll-up
GET /api/harvest/workers/health
# → { "healthy": 4, "stale": 1, "draining": 0, "by_queue": {"default": 5}, "by_shard": {"0": 5} }
```

Equivalent CLI (thin HTTP client, no direct Postgres access required):

```bash
cargo run -p autumn-harvest-cli -- worker list
cargo run -p autumn-harvest-cli -- worker list --queue email-workers --shard-id 0
cargo run -p autumn-harvest-cli -- worker get <worker-id>
cargo run -p autumn-harvest-cli -- worker health
```

Worker lifecycle status values: `Active` (normal operation), `Draining` (received SIGTERM, waiting for in-flight tasks to complete), `Stopped` (shutdown complete). The `health` field in responses is derived at query time: `healthy` or `stale`.

In sharded deployments the API merges results across all shards via `iter_shards()`. The `harvest_workers` table lives on every shard; each worker row is pinned to the shard the worker is polling.

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

## Testing workflow code changes with the replayer

Before deploying any edit to a `#[workflow]` function, verify it is
replay-safe against recorded production histories using `WorkflowReplayer`
(available when the `testing` feature is enabled).

### CI pattern

```toml
# Cargo.toml — in your app's dev-dependencies
autumn-harvest = { version = "0.2", features = ["testing"] }
```

```rust
// tests/replay_regression.rs
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};

#[tokio::test]
async fn onboarding_is_replay_safe() {
    // Load a fixture exported from production or a previous run.
    let json = std::fs::read_to_string("fixtures/onboarding_history.json").unwrap();

    let report = WorkflowReplayer::new()
        .register_fn("onboarding", onboarding_handler)  // your #[workflow] fn
        .replay_from_json(&json)
        .await
        .expect("fixture must parse");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay regression:\n{report}"
    );
}
```

### Exporting a history fixture

Serialise a `HistorySnapshot` to JSON and check it in as a test fixture:

```rust
use autumn_harvest::testing::HistorySnapshot;

let snapshot = HistorySnapshot {
    workflow_name: "onboarding".to_string(),
    execution_id: exec_id,
    events,  // Vec<WorkflowEvent> loaded from harvest_events
};
let json = serde_json::to_string_pretty(&snapshot).unwrap();
std::fs::write("fixtures/onboarding_history.json", json).unwrap();
```

### CLI validator

```sh
cargo run --bin harvest-replay -- \
  --workflow onboarding \
  --history-source json \
  --json-path ./fixtures/onboarding_history.json
```

Exit code 0 = `ReplaySucceeded`. Exit code 1 = non-determinism or workflow
failure. Extend `harvest-replay/src/bin/harvest_replay.rs` with your own
workflow handlers so the binary can replay against live code.

### What the replayer detects

| Kind | Trigger |
|---|---|
| `ActivityScheduleMismatch` | Activity name changed or order swapped |
| `LocalActivityScheduleMismatch` | Local activity name changed |
| `TimerMismatch` | Timer inserted / removed / reordered |
| `SignalMismatch` | Signal wait inserted where history has a non-signal event |
| `ChildWorkflowMismatch` | Child workflow name or input changed |
| `SideEffectMismatch` | `ctx.side_effect` ID changed |
| `ContinueAsNewMismatch` | `continue_as_new` input changed |

Changes that are safe without a `ctx.version()` fence: none of the above.
Use `ctx.version("change_id", 1, 2)` and guard the new code path behind the
returned version number; old histories replay with version 1 and skip the new
path.

## Telemetry

Harvest emits [OpenTelemetry](https://opentelemetry.io/)-compatible spans via the [`tracing`](https://docs.rs/tracing) crate. Eight named spans cover every durable boundary:

| Span | Kind | When |
|---|---|---|
| `harvest.workflow.execute` | INTERNAL | Every workflow executor cycle (live and replay) |
| `harvest.workflow.schedule` | PRODUCER | HTTP `POST /workflows/{name}/start` |
| `harvest.activity.execute` | INTERNAL | Activity handler dispatch |
| `harvest.activity.schedule` | PRODUCER | Activity enqueued to task queue |
| `harvest.signal.send` | PRODUCER | `send_signal` call |
| `harvest.signal.deliver` | CONSUMER | Signal ingested into workflow history |
| `harvest.timer.fire` | INTERNAL | Durable timer fires |
| `harvest.child_workflow.start` | PRODUCER | Child workflow enqueued |

Replay cycles emit `harvest.workflow.execute` as a **new root span** (`harvest.replay = true`) linked to the original via `link.traceparent` so APM backends can navigate between them.

### Wiring harvest spans into your OTel pipeline

Install [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) alongside your existing OTel SDK and implement `TraceContextPropagator`:

```rust
use autumn_harvest::{TelemetryConfig, TraceContextCarrier, TraceContextPropagator};
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator as OtelPropagator;
use std::any::Any;
use std::sync::Arc;

struct OtelBridge;

impl TraceContextPropagator for OtelBridge {
    fn capture(&self) -> Option<TraceContextCarrier> {
        // Extract the active span context into a W3C traceparent.
        let cx = Context::current();
        let span = cx.span();
        let ctx = span.span_context();
        if !ctx.is_valid() {
            return None;
        }
        let traceparent = format!(
            "00-{}-{}-{:02x}",
            ctx.trace_id(),
            ctx.span_id(),
            ctx.trace_flags().to_u8(),
        );
        Some(TraceContextCarrier::from_traceparent(traceparent))
    }

    fn install(&self, carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        // Restore the producer's span context as the OTel active context.
        if let Some(tp) = carrier.traceparent.as_deref() {
            let mut map = std::collections::HashMap::new();
            map.insert("traceparent".to_string(), tp.to_string());
            let cx = OtelPropagator::new()
                .extract(&opentelemetry::propagation::TextMapPropagator::extract_with_context(
                    &Context::new(), &map,
                ));
            let guard = cx.attach();
            Box::new(guard)
        } else {
            Box::new(())
        }
    }
}

// Wire into HarvestBuilder:
HarvestBuilder::new()
    .telemetry(
        TelemetryConfig::builder()
            .propagator(Arc::new(OtelBridge))
            .build(),
    )
    // ...
```

With no `telemetry(...)` call (the default), all `info_span!` sites compile to branch-on-atomic no-ops — zero allocation, no subscriber lock taken.

## License

Dual-licensed under MIT or Apache 2.0 at your option.
