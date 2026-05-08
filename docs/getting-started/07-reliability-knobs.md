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
