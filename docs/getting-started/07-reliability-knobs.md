# Chapter 7 — Reliability knobs you'll reach for

[← Idempotency](06-idempotency.md) · [Index](README.md) · [Next: DAGs and schedules →](08-dags-and-schedules.md)

---

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

**Cross-retry wall-clock deadline (`schedule_to_close`).** All three
per-attempt timeouts (`start_to_close`, `schedule_to_start`, `heartbeat_timeout`)
bound a single attempt. Use `schedule_to_close` when you need a hard ceiling
on the *total* time an activity may consume across every attempt and all
back-off sleeps combined:

```rust
#[activity(
    schedule_to_close = "5m",   // total budget: 5 minutes from first enqueue
    start_to_close   = "30s",   // each attempt: 30 s
    retry = RetryPolicy::exponential(10, Duration::from_secs(1)),
)]
async fn call_payment_api(ctx: &ActivityContext, req: PaymentRequest)
    -> Result<PaymentId, String> { … }
```

If the deadline elapses while the task is queued (PENDING) or running (RUNNING),
the timeout scanner appends `ActivityTimedOut { ScheduleToClose }` to history
and fails the task. If the deadline would be exceeded by the next retry's
back-off delay, the retry is skipped and the same event is appended instead of
requeuing — so the workflow sees a clean `HarvestError::Timeout { ScheduleToClose }`
rather than an exhausted-retry failure.

**Decision matrix — which timeout to use:**

| Scenario | Use |
|---|---|
| Bound a single attempt | `start_to_close` |
| Bound queue wait before first attempt | `schedule_to_start` |
| Detect a stuck activity (liveness) | `heartbeat_timeout` |
| Bound all attempts + back-off combined | `schedule_to_close` |
| Bound the whole workflow end-to-end | `#[workflow(execution_timeout = "…")]` |

`schedule_to_close` does **not** support local activities (rejected at compile
time with a clear error). Local activities are fast in-process work; use
`start_to_close` + a low retry count instead.

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

**Cron / interval schedules for workflows.** Register any workflow on a
schedule with `HarvestPlugin::schedule(...)`. When you need a graph of
activities instead of a single workflow on a schedule, jump to
[Chapter 8 — DAGs and schedules](08-dags-and-schedules.md).

**Search attributes.** Tag executions with structured fields
(`tenant_id`, `customer_id`) at start time so you can filter the dashboard
and the CLI by them: `harvest workflow list --search-attr tenant=acme`.

---

[← Idempotency](06-idempotency.md) · [Index](README.md) · [Next: DAGs and schedules →](08-dags-and-schedules.md)
