# Getting Started with Autumn Harvest

This is the long-form companion to [`examples/quickstart`](../examples/quickstart). The
quickstart gets a single workflow running in five minutes; this guide walks
through the rest of the surface — activities, retries, durable timers, signals,
child workflows, idempotency, and the management API — by growing one example
into something that resembles a real service.

By the end you'll have:

- A running Autumn web app with the Harvest plugin mounted at `/api/harvest`.
- One workflow that orchestrates two activities, a 30-second timer, and a
  `payment_captured` signal handoff.
- A child workflow for invoice generation.
- Idempotent downstream calls via `ctx.idempotency_key()`.
- The dashboard, preflight, and `harvest` CLI wired up against your local
  service.

Stop at any chapter — each one ends in a runnable state.

> **Prerequisites**
> - Stable Rust toolchain (`rustup default stable`)
> - Docker (for Postgres via the example's `compose.yaml`)
> - `jq` (optional, used in the curl examples)

---

## Chapter 1 — Project skeleton

Create a new Cargo project that depends on the engine, the Autumn plugin, and
the web framework:

```toml
# Cargo.toml
[package]
name = "harvest-tutorial"
version = "0.1.0"
edition = "2021"

[dependencies]
autumn-harvest = "0.1"
autumn-harvest-plugin = "0.1"
autumn-web = "0.2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
```

Add the boilerplate `main.rs` — at this point we register zero workflows and
zero activities, just to confirm the plugin mounts cleanly:

```rust
// src/main.rs
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

Drop in a Postgres `compose.yaml` next to your `Cargo.toml` (the
[quickstart's compose file](../examples/quickstart/compose.yaml) is a good
starting point) and an `autumn.toml` that points the framework at it:

```toml
# autumn.toml
[database]
url = "postgres://postgres:postgres@localhost:5432/autumn_harvest"
```

Bring it up:

```bash
docker compose up -d
AUTUMN_PROFILE=dev cargo run
```

`AUTUMN_PROFILE=dev` runs `diesel migration run` automatically on startup so
you don't need `diesel-cli` for the dev loop. The app will start on
`http://localhost:8080`. Hit the health endpoint to confirm the plugin
mounted:

```bash
curl -s http://localhost:8080/api/harvest/health | jq .
```

---

## Chapter 2 — Your first workflow and activity

A **workflow** is a deterministic async function annotated with `#[workflow]`.
An **activity** is a side-effecting async function annotated with `#[activity]`
— it's the place where I/O is allowed to live.

The split exists because workflows are *replayed*. When the process restarts,
the engine reads the event history out of Postgres and re-invokes the workflow
function from the top, returning recorded results from each activity call
without re-running them. That's only safe if workflow code is deterministic
and all real work lives behind activities.

```rust
use std::time::Duration;
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<String> {
    let result = ctx
        .execute_activity_raw(
            "send_welcome_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        )
        .await?;

    Ok(result["status"].as_str().unwrap_or("sent").to_owned())
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
async fn send_welcome_email(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let user_id = input["user_id"].as_i64().unwrap_or_default();
    tracing::info!(user_id, "sending welcome email");
    Ok(serde_json::json!({ "status": "sent" }))
}
```

Register both with the plugin:

```rust
HarvestPlugin::new()
    .workflows(workflows![onboarding])
    .activities(activities![send_welcome_email])
    .worker(WorkerConfig::default())
    .api("/api/harvest")
```

Restart, then start a workflow over HTTP:

```bash
curl -s -X POST http://localhost:8080/api/harvest/workflows/onboarding/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"user-42","input":42}' | jq .
```

Open `http://localhost:8080/api/harvest/ui` to watch it transition through
`RUNNING → COMPLETED` with the activity call recorded in the event history.

### What `#[activity]` accepts

| Key | Example | Meaning |
|---|---|---|
| `start_to_close` | `"30s"`, `"5m"`, `"1h"` | Hard cap on a single execution attempt |
| `schedule_to_start` | `"1m"` | How long the task may sit in the queue before failing |
| `heartbeat_timeout` | `"10s"` | Liveness window for long-running activities |
| `retry` | `RetryPolicy::exponential(3, Duration::from_secs(1))` | Retry policy on failure |
| `queue` | `"email-workers"` | Dedicated task queue (default `"default"`) |
| `max_concurrent` | `5` | Cluster-wide concurrent attempts cap |
| `concurrency_key` | `"stripe"` | Share the cap across activities touching the same dependency |
| `local` | `true` | Run inline on the workflow worker (no queue round-trip) |

Activities are **at-least-once**. A worker crash, a `start_to_close` timeout,
or a duplicate dispatch will re-run the activity. We'll fix that with
idempotency keys in Chapter 6.

---

## Chapter 3 — Durable timers

Workflows can sleep. Not with `tokio::time::sleep` — that wouldn't survive a
restart — but with `ctx.timer()`, which records a `TimerStarted` event in
Postgres and suspends the workflow until the deadline elapses.

```rust
#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<String> {
    ctx.execute_activity_raw(
        "send_welcome_email",
        serde_json::json!({ "user_id": user_id }),
        "default",
    )
    .await?;

    // 30-second drip — durably suspended.
    ctx.timer("post-welcome-drip", 30).await?;

    let nudge = ctx
        .execute_activity_raw(
            "send_followup_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        )
        .await?;

    Ok(nudge["status"].as_str().unwrap_or("sent").to_owned())
}
```

This is the durability demonstration to actually try in person:

1. Start the workflow.
2. While it's parked on the timer, hit `Ctrl+C`.
3. Run `cargo run` again.
4. Watch the dashboard. The welcome activity is **not re-executed** — its
   result is replayed from the event log. The timer resumes wherever the
   30-second budget left off, then the follow-up activity runs.

That replay-and-resume pattern is the whole point of the engine.

---

## Chapter 4 — Signals: waiting on the outside world

Real workflows wait on humans, webhooks, or other systems. Signals are
named, payload-carrying messages delivered over the management API and
buffered durably until the workflow consumes them.

Add a payment-confirmation hand-off:

```rust
#[workflow]
async fn checkout(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    ctx.execute_activity_raw(
        "reserve_inventory",
        serde_json::json!({ "order_id": order_id }),
        "default",
    )
    .await?;

    // Block until the payment gateway calls back.
    let payload = ctx.wait_for_signal("payment_captured").await?;
    let capture_id = payload["capture_id"].as_str().unwrap_or("").to_owned();

    ctx.execute_activity_raw(
        "fulfill_order",
        serde_json::json!({ "order_id": order_id, "capture_id": capture_id }),
        "default",
    )
    .await?;

    Ok(capture_id)
}
```

Start the workflow:

```bash
curl -s -X POST http://localhost:8080/api/harvest/workflows/checkout/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"order-42","input":"order-42"}' | jq .
```

It will run `reserve_inventory`, then suspend on the signal. Find the
execution ID in the response (or `harvest workflow list`), then deliver the
signal:

```bash
curl -s -X POST \
  http://localhost:8080/api/harvest/workflows/<EXECUTION_ID>/signal/payment_captured \
  -H 'Content-Type: application/json' \
  -d '{"capture_id":"cap_demo_123"}' | jq .
```

The workflow wakes up, runs `fulfill_order`, and completes.

> Signals delivered while the workflow isn't currently waiting are buffered.
> A workflow that hasn't reached its `wait_for_signal` call yet will see the
> already-arrived payload as soon as it gets there.

---

## Chapter 5 — Child workflows

Once your orchestration grows past a few activities, model the sub-flows as
their own workflows. A child workflow has its own event log, its own retry
policy, and its own dashboard entry — but its lifecycle is tied to the
parent.

```rust
#[workflow]
async fn issue_invoice(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    let pdf = ctx
        .execute_activity_raw(
            "render_invoice_pdf",
            serde_json::json!({ "order_id": order_id }),
            "default",
        )
        .await?;

    ctx.execute_activity_raw(
        "email_invoice",
        serde_json::json!({ "order_id": order_id, "pdf_url": pdf["url"] }),
        "default",
    )
    .await?;

    Ok(pdf["url"].as_str().unwrap_or("").to_owned())
}

#[workflow]
async fn checkout(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    // ... reserve inventory, wait for signal, fulfill ...

    let invoice_url = ctx
        .spawn_child_workflow_raw(
            "issue_invoice",
            &format!("invoice-{order_id}"),
            serde_json::json!(order_id),
        )
        .await?;

    Ok(invoice_url.as_str().unwrap_or("").to_owned())
}
```

Don't forget to register the child:

```rust
.workflows(workflows![checkout, issue_invoice])
```

The dashboard will show `checkout` as the parent with a clickable link to the
child execution. `harvest workflow children <execution-id>` lists them on the
CLI.

---

## Chapter 6 — Idempotency for safe retries

Activities are at-least-once. If the worker crashes after Stripe accepts a
charge but before the engine writes `ActivityCompleted` to Postgres, the
retry will charge the customer again unless the downstream system
deduplicates. Every activity gets a stable, retry-safe key from
`ctx.idempotency_key()`:

```rust
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(2)))]
async fn charge_card(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let amount_cents = input["amount_cents"].as_u64().unwrap_or(0);
    let customer_id = input["customer_id"].as_str().unwrap_or("").to_owned();

    let idem_key = ctx.idempotency_key()?.as_str().to_owned();

    // Pass idem_key as Stripe's Idempotency-Key header. Subsequent retries
    // for this attempt carry the same key, so Stripe returns the original
    // charge instead of creating a new one.
    let charge_id = stripe_charge(amount_cents, &customer_id, &idem_key)
        .await
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "charge_id": charge_id }))
}
```

The key is stable across worker restarts, duplicate dispatch, and replay,
**but it's distinct for every logical invocation** — calling `charge_card`
twice for two different orders gets two different keys.

For activities that make several outbound calls, derive named subkeys:

```rust
let key = ctx.idempotency_key()?;
create_db_user(&user_id, key.subkey("db").as_str()).await?;
send_welcome_email(&user_id, key.subkey("email").as_str()).await?;
```

---

## Chapter 7 — Reliability knobs you'll reach for

These come up in roughly the order you'll need them.

**Retries that aren't exponential.** Use `RetryPolicy::fixed(attempts, delay)`
for a flat retry, or build a `RetryPolicy` directly when you need a custom
shape (max interval, backoff coefficient, non-retryable error filters).

**Per-activity concurrency caps.** Add `max_concurrent = N` to bound the
cluster-wide in-flight count without provisioning a dedicated worker. Share
the budget across activities by giving them the same `concurrency_key`.
Inspect live counts with `harvest concurrency status`.

**Local activities.** Mark trivial in-process work with
`#[activity(local = true)]` to skip the task-queue round-trip. Local
activities still record `LocalActivityScheduled` / `LocalActivityCompleted`
events, so replay works identically — they just run inline on the workflow
worker. Use them for fast deterministic glue (formatting, hashing, cache
lookups under a few hundred ms). Don't use them for I/O that might exceed
the 60 s default cap.

**Dedicated task queues.** Add `queue = "email-workers"` to an activity and
spin up a worker that subscribes to it:

```rust
WorkerConfig::default().queues(vec!["default", "email-workers"])
```

Useful when one activity class (e.g. PDF rendering) needs its own resource
budget or its own scaling group.

**Workflow versioning.** When you change an in-flight workflow's logic,
fence the divergence with `ctx.version()` so old executions replay their
recorded path while new executions take the new branch:

```rust
if ctx.version("v2-tax-flow", 1, 2) >= 2 {
    ctx.execute_activity_raw("compute_tax_v2", input, "default").await?;
} else {
    ctx.execute_activity_raw("compute_tax_v1", input, "default").await?;
}
```

**Cron / interval schedules.** Register any workflow on a schedule with
`HarvestPlugin::schedule(...)`. For dependency-graph fan-out across multiple
activities, use `DagBuilder` and the `#[dag]` macro instead.

**Search attributes.** Tag executions with structured fields
(`tenant_id`, `customer_id`) at start time so you can filter the dashboard
and the CLI by them: `harvest workflow list --search-attr tenant=acme`.

---

## Chapter 8 — Operating the service

### Preflight

Before promoting a Harvest service, run the deploy gate:

```bash
cargo run -p autumn-harvest-cli -- \
  --base-url http://localhost:8080/api/harvest preflight
```

Exit codes are CI-friendly: `0 = pass`, `2 = warn`, `1 = fail`. The same
endpoint is available as `GET /api/harvest/admin/preflight` for release
scripts.

### Dashboard

`http://localhost:8080/api/harvest/ui` shows live executions, event histories,
the DLQ, schedules, and the worker fleet. It's served by the plugin — no
separate process.

### CLI

The `harvest` binary is a thin client for the management API:

```bash
harvest workflow list --state RUNNING
harvest workflow get <execution-id>
harvest workflow signal <execution-id> approved --payload-json '{"approved":true}'
harvest workflow cancel <execution-id> --reason "operator request"

harvest dlq list --limit 25
harvest dlq replay <dead-letter-id>

harvest concurrency status
```

It never talks to Postgres directly — every call goes through the API your
service already exposes, so auth and policy stay in one place.

### Dead letters

When a task exhausts its retry policy, it lands in `harvest_dead_letters` and
shows up on the DLQ tab. Inspect the failure context, then either replay
(`harvest dlq replay`) once you've fixed the root cause, or discard
(`harvest dlq bulk-discard --activity-name ...`) when the work is no longer
relevant.

### Worker fleet and graceful drain

Every worker process registers itself in `harvest_workers` and heartbeats on
a schedule. Inspect the fleet from the CLI:

```bash
harvest worker list                       # all workers
harvest worker list --status active --health stale
harvest worker get <worker-id>
harvest worker health                     # rollup: active / draining / stale
```

When you need to roll a node — deploy, autoscale-down, drain a host before
maintenance — request a remote drain instead of sending `SIGTERM`. The
worker stops claiming new tasks within two heartbeat intervals and finishes
its in-flight work before exiting:

```bash
# Dry run first: who would be affected, what's in-flight, on which shards.
harvest worker drain-preview --queue email-workers

# Then drain a specific worker, optionally with a deadline.
harvest worker drain <worker-id>
harvest worker drain <worker-id> --deadline 2026-05-08T15:00:00Z
```

The response echoes `outcome` (`accepted`, `already_draining`,
`already_stopped`, `stale_worker`, `not_found`), the in-flight task count,
the drain deadline, and which shards the worker owns. The same surface is
available over HTTP for orchestration systems:

```bash
curl -s -X POST http://localhost:8080/api/harvest/workers/<worker-id>/drain \
  -H 'Content-Type: application/json' \
  -d '{"deadline_at":"2026-05-08T15:00:00Z"}' | jq .

curl -s 'http://localhost:8080/api/harvest/workers/drain-preview?queue=email-workers' | jq .
```

Drain requests are recorded in the audit log under the `worker.drain`
operation, so you have a "who quiesced this node, when" record without
correlating shell history across machines.

### Reuse policies

By default, starting a workflow with an existing `(name, workflow_id)` pair
returns the existing execution — correct for retries of a lost-response
start. When you need stricter semantics, pass `reuse_policy`:

| Value | Use when… |
|---|---|
| `allow_duplicate` *(default)* | Upstream may retry a start whose response was lost. |
| `reject_duplicate` | At-most-one is a hard requirement; second start returns 409. |
| `allow_duplicate_failed_only` | Retry only if the prior run is FAILED/CANCELLED. |
| `terminate_if_running` | Cancel the prior run and start fresh. |

---

## Chapter 9 — Testing your workflow code

Workflow code is deterministic, so it's testable without a database. Two
levels:

**1. Unit-test handlers in isolation.** Build a `WorkflowContext::new_test()`
or `ActivityContext::new_test()` (gated by the `testing` feature) and call
your function directly. Activities that read inputs and produce outputs are
trivial under this.

**2. Replay-test against recorded histories.** When you change a workflow
function, run it against histories captured from production with
`autumn_harvest::testing::WorkflowReplayer`:

```rust
use autumn_harvest::testing::{WorkflowReplayer, ReplayStatus};

#[tokio::test]
async fn checkout_replays() {
    let history = std::fs::read_to_string("fixtures/checkout_v3.json").unwrap();

    let report = WorkflowReplayer::new()
        .register_fn("checkout", checkout_handler)
        .replay_from_json(&history)
        .await
        .expect("fixture parses");

    assert!(matches!(report.status, ReplayStatus::ReplaySucceeded), "{report}");
}
```

The replayer never executes activities or touches Postgres — it runs the
workflow function in pure replay mode and compares the commands it emits
against the recorded history. A failure tells you exactly which event
diverged. Run this in CI on every workflow code change to catch
non-determinism *before* it produces DLQ entries.

See [`docs/runbooks/replay-fixture-export.md`](runbooks/replay-fixture-export.md)
for capturing fixtures from a running service.

---

## Where to go next

- **Reference example.** [`examples/billing-autumn-web/`](../examples/billing-autumn-web/)
  is a full subscription-checkout integration: outbox → workflow start, saga
  compensation, child workflow, version gate, signal handoff, and a scheduled
  reconciliation DAG.
- **Standalone runner.** [`examples/standalone-runner/`](../examples/standalone-runner/)
  shows the engine without `HarvestPlugin` — useful when embedding in a
  non-Autumn service.
- **Runbooks.**
  [`audit-trail.md`](runbooks/audit-trail.md),
  [`external-activity-handoffs.md`](runbooks/external-activity-handoffs.md),
  [`replay-fixture-export.md`](runbooks/replay-fixture-export.md),
  [`version-gate-retirement.md`](runbooks/version-gate-retirement.md).
- **Telemetry.** [`telemetry.md`](telemetry.md) covers the OpenTelemetry
  surface and the `metrics-rs` adapter recipe.
- **Search attributes.** [`search-attributes.md`](search-attributes.md)
  explains how to index workflows for filtered queries.
- **Architecture.**
  [`autumn-workflow-architecture.md`](autumn-workflow-architecture.md) and the
  [ADRs](adr/) document the design decisions behind the engine.
