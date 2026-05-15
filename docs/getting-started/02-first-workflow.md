# Chapter 2 — Your first workflow and activity

[← Project skeleton](01-project-skeleton.md) · [Index](README.md) · [Next: Durable timers →](03-durable-timers.md)

---

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
curl -s -X POST http://localhost:3000/api/harvest/workflows/onboarding/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"user-42","input":42}' | jq .
```

Open `http://localhost:3000/api/harvest/ui` to watch it transition through
`RUNNING → COMPLETED` with the activity call recorded in the event history.

## What `#[activity]` accepts

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
idempotency keys in [Chapter 6](06-idempotency.md).

---

[← Project skeleton](01-project-skeleton.md) · [Index](README.md) · [Next: Durable timers →](03-durable-timers.md)
