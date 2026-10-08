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
| `start_to_close` | `WorkerConfig::default_activity_start_to_close` (10 min) | Wall-clock cap for a single attempt. The worker default applies only when the activity sets no `start_to_close`, `schedule_to_close` or `heartbeat_timeout` (issue #1808). |
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

### Automatic liveness heartbeats (issue #682)

If an activity only needs the *liveness* signal — not a progress checkpoint —
and its own work makes it awkward to call `ctx.heartbeat()` on a regular cadence
(a single long blocking call, a tight CPU loop), start a background auto-heartbeat
ticker instead of hand-rolling one. It requires the activity to declare a
`heartbeat_timeout`:

```rust
#[activity(start_to_close = "10m", heartbeat_timeout = "30s")]
async fn transcode(ctx: &ActivityContext, job: Job) -> Result<(), String> {
    // Pings every ~10 s until the guard drops at end of scope.
    let _hb = ctx.start_auto_heartbeat(std::time::Duration::from_secs(10))
        .map_err(|e| e.to_string())?;
    // ... or ctx.start_auto_heartbeat_default() to derive the interval from
    // heartbeat_timeout ...
    run_long_blocking_work(&job).await.map_err(|e| e.to_string())?;
    Ok(())
}
```

`start_auto_heartbeat` returns a `#[must_use]` RAII `AutoHeartbeatGuard` — bind
it to a named local (`let _hb = ...`); binding to `_` drops it immediately and
stops the ticker. Manual `ctx.heartbeat(payload)` calls still work alongside it
and take over the checkpoint payload.

An activity with a `heartbeat_timeout` gets no default `start_to_close`
(issue #1808). An auto-heartbeat keeps a stuck attempt alive, so the heartbeat
timeout cannot stop it. Give such an activity its own `start_to_close`.

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
configured `cancellation_grace_period` (default 5 s) after triggering the
cancellation token; if the activity has still not exited by then the worker
hard-aborts the future.  The activity task is recorded as `FAILED` in the task
queue.

This means cooperative cancellation is *opt-in*: existing activities continue
to work unchanged after upgrading.

### Cancellation by a worker drain

A worker drain also cancels running activities, one join window before its
deadline (issue #1813). The same checks see it: `is_cancelled()` turns true,
and `heartbeat()` returns `ActivityCancelled`. The drain never aborts the
handler.

- A handler that returns a retryable error goes back to `PENDING` at once,
  and a peer runs the next attempt. `previous_failure()` then starts with
  `worker shutdown:`. The heartbeat details stay, so the next attempt can
  resume from the last checkpoint.
- A handler that returns `Ok` completes as usual.
- A handler that ignores the cancel keeps its claim until it returns. The
  worker keeps its lease alive meanwhile. The cancel stops the handler's own
  heartbeats, so the worker re-sends the last checkpoint at
  `heartbeat_timeout / 3`. If the process exits, orphan reclaim recovers the
  task.

### `heartbeat_details` across a cancel signal

The checkpoint payload flushed to the database before cancellation is stable
from the perspective of the in-flight activity context — it is loaded once at
dispatch time and held in memory for the duration of the attempt.  The cancel
path clears `heartbeat_details` on the task row so that a *fresh* retry (on a
new worker claim) starts clean.  No action is required from the activity author.

## Cross-cutting behavior — activity interceptors (issue #680)

To add behavior around **every** activity — structured logging, custom metrics,
header propagation, input/output shaping, test fault-injection — without editing
each `#[activity]` function, register an ordered interceptor chain on the builder
instead:

```rust
HarvestBuilder::new()
    .activity_interceptor(LoggingMetricsInterceptor)   // first registered = outermost
    // ...
```

Each interceptor implements the `ActivityInterceptor` trait and calls
`next.run(input).await` to proceed to the next interceptor (or the handler); not
calling it short-circuits. Interceptors wrap both regular and local activities;
an interceptor `Err`/panic is contained exactly like a handler failure. See
`examples/activity_interceptor.rs`.

## Run one durable job

Harvest has no standalone-activity start path.
[ADR 0006](../adr/0006-standalone-activity.md) records why: a one-step
workflow costs little more than a standalone job would. Wrap the job in a
one-step workflow:

```rust
use std::time::Duration;

use autumn_harvest::prelude::*;

#[activity(start_to_close = "5m", retry = RetryPolicy::exponential(5, Duration::from_secs(1)))]
async fn render_invoice(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    Ok(serde_json::json!({ "invoice": input["order_id"] }))
}

#[workflow]
async fn render_invoice_job(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    ctx.execute_activity_raw("render_invoice", input, "default").await
}
```

Use the job id as the `workflow_id`. With the default reuse policy,
`AllowDuplicate`, a second start of the same workflow and id returns the
first run, even a failed one. Use `AllowDuplicateFailedOnly` to rerun a
failed job. This holds while retention keeps the run. The run result is
the job result.

For short in-process work, use a local activity. The job then has one task
row and one claim, not two rows and three claims:

```rust
#[activity(local = true, start_to_close = "5s")]
async fn checksum(_ctx: &ActivityContext, input: serde_json::Value) -> HarvestResult<String> {
    Ok(format!("{:x}", input.to_string().len()))
}

#[workflow]
async fn checksum_job(ctx: &WorkflowContext, input: serde_json::Value) -> HarvestResult<String> {
    ctx.execute_local_activity(&checksum_info(), input).await
}
```

A local activity cannot heartbeat or use another queue.
`WorkerConfig::max_local_activity_start_to_close` caps its
`start_to_close` (60 s by default). Use the regular form when the job needs
any of these.

Cost per job, from
[One-step workflow overhead against a bare activity](../performance-standalone-activity-overhead.md):

| Pattern | Events | Task rows | Claims | Rows written |
|---|--:|--:|--:|--:|
| One-step workflow, regular activity | 5 | 2 | 3 | 17 |
| One-step workflow, local activity | 4 | 1 | 1 | 10 |
| Modelled standalone job (no such API) | 0 | 1 | 1 | 6 |
