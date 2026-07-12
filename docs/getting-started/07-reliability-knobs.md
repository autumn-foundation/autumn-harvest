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

**Soft SLA — page before the customer notices (`#[workflow(sla = "…")]`).**
Every knob above is a *hard* deadline: when it fires it terminates, fails, or
skips the work. But the most common production question is softer — *"this run
is healthy and still making progress, but it's far slower than it should be —
alert me **before** it's a problem."* That's the soft SLA:

```rust
#[workflow(sla = "2h", execution_timeout = "6h")]
async fn nightly_reconciliation(ctx: &WorkflowContext, input: Input)
    -> Result<(), String> { /* … */ }
```

When the run passes its `sla` deadline, Harvest emits the
`harvest.workflow.sla_breached{workflow, queue}` counter **exactly once** and
sets the server-side `sla_breached` / `sla_breached_at` fields — and **does
nothing else**. The run keeps executing; if it later succeeds it reaches
`COMPLETED` normally. The signal carries **zero `harvest_events` footprint** (no
new event variant, replay-neutral, like query handlers). Override per-run at
start with `sla_secs` in the HTTP start body; omit the attribute and a run has
no SLA. Find breached-but-still-running work with
`GET /workflows?sla_breached=true`.

**SLA vs `execution_timeout` — they answer different questions:**

| Goal | Use | On deadline |
|---|---|---|
| Alert on a slow-but-healthy run | `sla` | metric + flag, run continues |
| Kill a runaway / hung run | `execution_timeout` | run is **terminated** (`TIMED_OUT`) |

Pair them: `sla` < `execution_timeout` gives you a page first, then a hard cap.
If you set `sla` **larger** than `execution_timeout`, the hard timeout would
fire first and the soft signal could never fire — so Harvest **clamps `sla`
down to `execution_timeout`** at start time. Pause suspends the SLA clock
(resume pushes `sla_deadline_at` forward by the paused span), so a deliberately
parked run never false-breaches.

**Checkpoint before the deadline kills you (deadline-aware `continue_as_new`).**
A long-lived entity workflow (a subscription, a cart, a device) can run for
weeks while recording only a handful of events — it never approaches the history
event-count threshold that `ctx.should_continue_as_new()` watches. But if it
declares an `execution_timeout` for runaway protection, that hard timeout will
eventually terminate it mid-flight even though it is perfectly healthy. To avoid
that, `should_continue_as_new()` has a **second trigger** (issue #772): it also
returns `true` once the run has consumed a configurable fraction (default
`0.8`) of its `execution_timeout` budget. Checkpoint with `continue_as_new`
*before* the hard deadline truncates you, carrying state forward into a fresh
run with a fresh deadline:

```rust
#[workflow(execution_timeout = "24h")]
async fn subscription_entity(ctx: &WorkflowContext, state: SubState) -> Result<SubState, String> {
    // Fires on history size OR ~80% of the execution_timeout budget.
    if ctx.should_continue_as_new() {
        ctx.continue_as_new(serde_json::to_value(&state).unwrap()).await?;
    }
    // ... one cycle of durable work ...
    Ok(state)
}
```

Two replay-safe, event-free accessors back this: `ctx.deadline()` (the
`WorkflowStarted` timestamp + effective `execution_timeout`, or `None` when
there is no timeout) and `ctx.time_until_deadline()` (measured against the
replay-safe recorded clock `ctx.system_now()`, **never** `chrono::Utc::now()`).
A workflow with **no** `execution_timeout` behaves exactly as before and records
nothing extra. Tune the fraction with
`HarvestBuilder::history_continue_as_new_deadline_fraction(f64)` (clamped to
`[0.0, 1.0]`). **No new event variant, no migration** — the deadline is derived
from the recorded start time and the clock read reuses the existing
`SideEffectRecorded{Now}` event, so a history that crossed the deadline replays
to the same `continue_as_new` on every worker. See
`examples/long_lived_entity_deadline.rs`.

**Call `should_continue_as_new()` once per decision cycle.** When the workflow
declares an `execution_timeout`, each `should_continue_as_new()` call reads the
deadline clock and records **one** `SideEffectRecorded{Now}` event on the live
frontier (a no-timeout workflow records nothing). Call it once at the top of a
decision cycle — not in a tight inner loop that fires it dozens of times per
cycle — or the history grows a `Now` event per call.

**Rollout is graceful — no "upgrade the fleet first" dance.** An in-flight
execution recorded *before* this feature shipped carries no deadline clock read
at the `should_continue_as_new()` call site. When it resumes under a
deadline-aware binary the deadline branch degrades to history-count-only for the
run's already-recorded portion (it does **not** nd-block), and it picks the
deadline feature up automatically once it executes live past its pre-upgrade
frontier. You can deploy the new binary while such runs are in flight without
any special sequencing.

**Workflow versioning.** When you change an in-flight workflow's logic,
fence the divergence with `ctx.patched()` — the recommended default for the
overwhelmingly common two-state (before/after) change — so old executions
replay their recorded path while new executions take the new branch:

```rust
if ctx.patched("v2-tax-flow") {
    ctx.execute_activity_raw("compute_tax_v2", input, "default").await?;
} else {
    ctx.execute_activity_raw("compute_tax_v1", input, "default").await?;
}
```

Retire the gate with a three-deploy sequence:

1. **Introduce** — the `if ctx.patched(id) { new } else { old }` fence above.
   New executions record a `patch:{id}` marker and take the new branch;
   pre-patch executions keep replaying the old branch. One caveat: a fresh
   run whose first-task history ends in un-awaited signals at the gate point
   — canonically every **signal-with-start** run, whose signal is staged
   before first dispatch — takes the *old* branch and records no marker
   (deliberate parity with `ctx.version()`), so drain checks must count
   these marker-less runs too.
2. **Deprecate** — once every pre-patch run has drained (see the "Patched
   gates" section of `docs/runbooks/version-gate-retirement.md` — the
   runbook's `version-usage` / retirement-check CLI tooling only sees
   `version:` markers, **not** patch gates; use that section's raw SQL
   drain queries), replace the fence with `ctx.deprecate_patch(id);`
   followed by the unconditional new code. The recorded markers become
   transparent to replay, so marker-bearing runs still replay cleanly.
3. **Remove** — once every marker-bearing run has drained, delete the
   `deprecate_patch` call entirely.

`ctx.version()` remains the explicit escape hatch for gates that need **more
than two** concurrent versions:

```rust
if ctx.version("v2-tax-flow", 1, 3) >= 2 {
    // …
}
```

Histories recorded by a two-version `ctx.version(id, 1, 2)` gate interop with
`ctx.patched(id)` — the version marker is observed as patched, so you can
migrate a gate in place.

**Cron / interval schedules for workflows.** Register any workflow on a
schedule with `HarvestPlugin::schedule(...)`. When you need a graph of
activities instead of a single workflow on a schedule, jump to
[Chapter 8 — DAGs and schedules](08-dags-and-schedules.md).

**Search attributes.** Tag executions with structured fields
(`tenant_id`, `customer_id`) at start time so you can filter the dashboard
and the CLI by them: `harvest workflow list --search-attr tenant=acme`.

---

[← Idempotency](06-idempotency.md) · [Index](README.md) · [Next: DAGs and schedules →](08-dags-and-schedules.md)
