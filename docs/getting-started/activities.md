# Activities

Activities are the units of work in a harvest workflow.  They are ordinary
async Rust functions annotated with `#[activity]`; they run on one or more
worker processes and report results back to the workflow executor via the durable
event log.

## Defining an activity

```rust
use autumn_harvest::prelude::*;

#[activity(start_to_close = "30s", queue = "email-workers")]
async fn send_welcome_email(ctx: &ActivityContext, addr: String) -> Result<(), String> {
    // I/O, external API calls, database writes …
    Ok(())
}
```

Key attribute options:

| Attribute | Default | Notes |
|---|---|---|
| `start_to_close` | none (no limit) | Wall-clock cap for a single attempt. |
| `heartbeat_timeout` | none | Fail the attempt if no heartbeat arrives within this window. |
| `schedule_to_start` | none | Fail if no worker claims the task within this window. |
| `queue` | `"default"` | Route the task to a named worker pool. |
| `retry` | `RetryPolicy::default()` | Override the back-off shape. |
| `local = true` | false | Run inline on the workflow worker (no queue round-trip). |

## Heartbeating

Long-running activities should periodically call `ctx.heartbeat()` to:

1. **Report liveness** — the `heartbeat_timeout` scanner marks an activity
   failed if no heartbeat arrives within the configured window.
2. **Checkpoint progress** — the payload is persisted to the database; the
   next retry attempt can read it back via `ctx.heartbeat_details::<T>()`.
3. **Receive cancellation signals** — `heartbeat()` returns
   `Err(ActivityCancelled)` when the owning workflow has been cancelled (see
   below).

```rust
#[activity(start_to_close = "10m", heartbeat_timeout = "30s")]
async fn import_records(ctx: &ActivityContext, source_url: String) -> Result<u64, String> {
    let start_offset: u64 = ctx
        .heartbeat_details::<u64>()
        .map_err(|e| e.to_string())?
        .unwrap_or(0);

    let mut processed = start_offset;
    for record in fetch_records(&source_url, start_offset) {
        write_record(&record).map_err(|e| e.to_string())?;
        processed += 1;
        ctx.heartbeat(processed).await.map_err(|e| e.to_string())?;
    }
    Ok(processed)
}
```

## Cooperative cancellation

When an operator calls `cancel_workflow_execution`, harvest:

1. Marks the workflow execution `CANCELLED` and appends a
   `WorkflowCancellationRequested` event.
2. Transitions the in-flight task queue rows to `CANCELLED` so worker polling
   stops scheduling them.
3. Sets the worker's `CancellationToken` so the next `heartbeat()` or
   `check_cancellation()` call inside the running activity returns
   `Err(HarvestError::ActivityCancelled)`.

Activities that check the return value of `heartbeat()` — or call
`check_cancellation()` explicitly — will observe the signal within one
heartbeat interval and can exit cleanly.  Activities that never heartbeat are
eventually hard-aborted by the worker after the configured
`cancellation_grace_period`.

### Pattern 1: check via `heartbeat()`

Use this when you are already heartbeating for liveness or checkpointing.
The cancellation signal comes for free on the same call.

```rust
#[activity(start_to_close = "5m", heartbeat_timeout = "15s")]
async fn process_batch(ctx: &ActivityContext, job_id: String) -> Result<(), String> {
    for item in load_batch(&job_id) {
        process_item(&item).map_err(|e| e.to_string())?;
        // Returns Err(ActivityCancelled) if the workflow was cancelled.
        ctx.heartbeat(serde_json::json!({"last_item": item.id}))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}
```

### Pattern 2: check via `check_cancellation()`

Use this when there is no meaningful checkpoint payload to report but you
still want to respect cancellation in a tight loop.

```rust
#[activity(start_to_close = "2m")]
async fn poll_external_status(ctx: &ActivityContext, task_id: String) -> Result<String, String> {
    loop {
        let status = check_status(&task_id).await.map_err(|e| e.to_string())?;
        if status == "done" {
            return Ok(status);
        }
        // Yield and check for cancellation before sleeping.
        ctx.check_cancellation().await.map_err(|e| e.to_string())?;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
```

### What happens without a cancellation check?

An activity that never calls `heartbeat()` or `check_cancellation()` will not
receive the cooperative cancellation signal.  The worker will wait for the
configured `cancellation_grace_period` (default 30 s) after triggering the
cancellation token; if the activity has still not exited by then the worker
hard-aborts the future.  The activity task is recorded as `FAILED` in the task
queue.

This means cooperative cancellation is *opt-in*: existing activities continue
to work unchanged after upgrading.

### `heartbeat_details` across a cancel signal

The checkpoint payload flushed to the database before cancellation is stable
from the perspective of the in-flight activity context — it is loaded once at
dispatch time and held in memory for the duration of the attempt.  The cancel
path clears `heartbeat_details` on the task row so that a *fresh* retry (on a
new worker claim) starts clean.  No action is required from the activity author.
