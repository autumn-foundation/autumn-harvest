# Runbook: Activity circuit breakers (issue #369)

A **circuit breaker** lets an activity short-circuit its own dispatch when the
downstream service it depends on (email provider, payment gateway, search
index, …) is hard-down. Instead of retrying every failing attempt across its
full `RetryPolicy` curve — flooding `harvest_task_queue` and piling up identical
`harvest_dead_letters` entries across thousands of in-flight workflows — the
breaker **trips open** and doomed work stops consuming worker capacity. The
policy's `open_mode` sets what a dispatch does while the breaker is open
(issue #1809):

- **`CircuitOpenMode::Defer`** (default). The task goes back to `PENDING` until
  the next probe. It uses no attempt and appends no event. When the breaker
  closes, the work runs.
- **`CircuitOpenMode::FailFast`**. The dispatch fails with a non-retryable
  `CircuitOpen` error within seconds. Workflows that handle failure (Saga
  compensation, branching) reach their recovery path quickly.

[ADR 0005](../adr/0005-activity-timeout-retry-and-open-circuit.md) records why
defer is the default: a fail-fast breaker turns overload into permanent
failure.

This is opt-in per activity. Activities without a declared policy keep today's
behaviour exactly (no breaker; the full retry policy applies).

## State model

```
           failures >= threshold within window
  Closed ───────────────────────────────────────► Open
    ▲                                                │
    │ probe succeeds                                 │ cooldown elapsed
    │                                                ▼
    └───────────────────  HalfOpen  ◄────────────────┘
                              │ probe fails
                              └────────────────► Open
```

- **Closed** (normal): dispatches proceed unchanged.
- **Open** (tripped): in defer mode, each new dispatch goes back to
  `PENDING`. It waits for the time until the next probe, plus up to 25%
  jitter. When no probe time is known (a forced-open breaker, or a probe in
  flight), it waits for the cooldown, but at least 5 s. A short cooldown
  then cannot make a deferred backlog poll the database many times a
  second. The wait is clamped to 100 ms – 30 s.
  In fail-fast mode, new dispatches fail with
  `ActivityFailure { error_type: "CircuitOpen", non_retryable: true, .. }`.
  This is a terminal failure for the in-flight attempt — the workflow author
  chooses whether to compensate, branch, or fail the workflow.
- **Half-open** (cooldown elapsed): a single probe dispatch is admitted; the
  breaker re-closes on success or re-opens on failure.

## Declaring a policy

Via the `#[activity]` attribute:

```rust
use autumn_harvest::prelude::*;
use std::time::Duration;

#[activity(
    start_to_close = "30s",
    retry = RetryPolicy::exponential(5, Duration::from_secs(1)),
    // Trip after 10 failures within 30s; re-probe after 60s.
    // While open, fail at once so the workflow can compensate.
    circuit_breaker = CircuitBreakerPolicy::new(10, Duration::from_secs(30), Duration::from_secs(60))
        .with_open_mode(CircuitOpenMode::FailFast)
)]
async fn charge_card(ctx: &ActivityContext, req: ChargeRequest) -> Result<Receipt, ActivityFailure> {
    // ... call the payment gateway ...
}
```

The four knobs are:

| Field | Meaning |
|-------|---------|
| `failure_threshold` | Failures within `window` that trip the breaker open (min 1). |
| `window` | Rolling window over which failures are counted. |
| `cooldown` | Time the breaker stays open before admitting one half-open probe. |
| `open_mode` | `Defer` (default) or `FailFast`. Set it with `with_open_mode`. |

> **Defer mode and deadlines.** A deferred task waits as long as the breaker
> stays open. Set `schedule_to_close` on the activity to bound the wait. With
> no deadline, a breaker that an operator forced open holds the work until
> `force-close`.

> **Only retryable failures count toward a trip.** A *non-retryable*
> `ActivityFailure` (a permanent per-request error such as bad input or a
> validation failure) proves the downstream is reachable enough to give a
> definitive answer, so it never contributes to opening the breaker — a burst of
> bad requests cannot trip the circuit and starve healthy callers. Only
> transient/downstream-style retryable failures move the breaker toward open.
>
> **Only timeouts of started attempts count (issue #1809).** A start-to-close,
> heartbeat or running schedule-to-close timeout counts only when the attempt's
> handler started. It then counts as a retryable failure. A task that waited after its claim,
> or never left the queue, made no downstream call, so a backlog cannot trip
> the breaker.
>
> **Local activities cannot declare a circuit breaker.** The breaker is enforced
> on the task-dispatch path, which local activities (`local = true`) bypass by
> running inline on the workflow worker; the `#[activity]` macro rejects
> `circuit_breaker` on a local activity at compile time.

## Handling `CircuitOpen` in workflow code

This section applies to `FailFast` mode. In defer mode, the workflow never
sees `CircuitOpen`.

The failure flows through the typed activity-failure surface (#227), so workflow
code branches on the typed error class — **not** by parsing the human message.
`HarvestError` exposes the recorded `error_type` / `details` (preserved through
replay) via accessors:

```rust
match ctx.execute_activity(&charge_card_info(), req).await {
    Ok(receipt) => { /* happy path */ }
    Err(e) if e.is_circuit_open() => {
        // Downstream is down. Compensate / branch / defer instead of retrying.
        // `details.retry_after_secs` is present for a cooldown-based open;
        // `details.forced == true` (and no retry_after_secs) for an
        // operator-forced pin.
        let retry_after = e
            .activity_details()
            .and_then(|d| d.get("retry_after_secs"))
            .and_then(serde_json::Value::as_f64);
        let _ = retry_after;
        saga.compensate_all().await?;
    }
    Err(e) => return Err(e.to_string()),
}
```

`e.activity_error_type()` returns the stable class string (e.g. `"CircuitOpen"`)
for any `ActivityFailed`; `e.activity_details()` returns the structured payload.
These are deterministic on replay — a workflow that saw `CircuitOpen` sees the
same typed failure (and `retry_after_secs`) every replay, regardless of the
breaker's live state at replay time.

## Decision matrix — circuit breaker vs. retry / jitter / rate limit

Pick the tool that matches the failure mode. They compose; a single activity can
use all four.

| Failure mode | Reach for | Why |
|---|---|---|
| **Downstream is hard-down** (100% failures, will stay down for minutes/hours) | **Circuit breaker** (#369) | Stop calling it. Retrying a dead target just floods the queue and DLQ. The breaker defers the work until the downstream recovers, or, in `FailFast` mode, fails it so workflows recover in seconds, not hours. |
| **Transient, self-healing blip** (a few % failures, recovers on its own in seconds) | **Retry policy** + **jitter** (#342) | The next attempt will likely succeed. Jitter spreads the retries so a fleet-wide blip doesn't thundering-herd the recovering downstream. |
| **You are the overload** (downstream is fine but rate-limits you, or you'd overwhelm it) | **Rate limit** (#332) | Throttle dispatch to stay within the downstream's budget. The downstream is healthy — you don't want to stop calling it, just pace yourself. |
| **Permanent, per-request error** (bad input, validation failure) | **Non-retryable `ActivityFailure`** (#227) | The request will never succeed; skip retries for that one attempt without affecting other calls or tripping a breaker. |

Rules of thumb:

- **Breaker vs. retry**: retries assume the *next* attempt might work; the
  breaker assumes it *won't* and protects the fleet from finding out the hard
  way. Configure the breaker threshold above your normal transient failure rate
  so ordinary blips ride the retry curve and only a real outage trips it.
- **Breaker vs. rate limit**: a rate limit *paces* a healthy downstream; a
  breaker *stops* calling an unhealthy one. If your dashboards show the
  downstream returning 429s, rate-limit. If they show timeouts / 5xx / connection
  refused, the breaker is the right tool.
- **Order of operations on one activity**: rate limit gates how fast you
  dispatch; retry + jitter handle individual transient failures; the breaker
  trips when those retries are consistently failing; non-retryable failures
  bypass all of the above for permanent per-request errors.
- **Breaker vs. `retry_after` (#744)**: an activity can hand the engine a
  downstream-supplied `Retry-After` delay hint via
  `ActivityFailure::with_retry_after(Duration)`, overriding the policy-computed
  backoff for one attempt. If the SAME activity also carries a breaker, be
  aware the breaker's rolling failure window only counts a failure toward
  tripping if it lands within `window` of the prior one — a `retry_after` hint
  that regularly spaces consecutive attempts *wider* than `window` can defeat
  trip detection entirely. Configure the breaker's `window` wider than the
  largest `retry_after` hint you expect that downstream to send (see
  [Chapter 7](../getting-started/07-reliability-knobs.md) for the full
  `retry_after` writeup).
- **Rate limiting moves to dispatch for breaker activities**: when an activity
  declares **both** `rate_limit_*` and `circuit_breaker`, its rate limiting is
  enforced at *dispatch* rather than at claim time. The claim query skips the
  rate-limit gate and token debit for any activity with a breaker, so a
  `CircuitOpen` short-circuit is always claimable and defers or fast-fails at
  full speed during an outage (never paced by, or burning tokens from, the downstream's
  bucket). A *genuine* call — admitted by the authoritative `on_dispatch` check —
  atomically reserves one token at dispatch; if the bucket is empty the task is
  rescheduled (one refill interval ahead) instead of running, so a real call can
  never exceed the rate limit. This dispatch-time enforcement is gated on the
  real breaker decision, which avoids the claim-vs-dispatch staleness window (the
  breaker state is in-process and can change between claim and dispatch). Plain
  rate-limited activities without a breaker are unaffected — they still gate and
  debit at claim time.

## Observability

Each shard / worker process tracks its own breaker state in-process (per the
per-shard ACID model; an outage that hits every shard trips each independently).

### Metrics (ADR-0001)

| Metric | Type | Labels | Emitted when |
|---|---|---|---|
| `harvest.activity.circuit.tripped` | counter | `activity.name` | Breaker trips closed→open, or re-opens after a failed half-open probe. |
| `harvest.activity.circuit.closed` | counter | `activity.name` | Breaker recovers to closed after a successful half-open probe. |
| `harvest.activity.circuit.deferred` | counter | `activity.name` | An open breaker in defer mode puts a claimed task back to `PENDING` (issue #1809). |

Existing alerting (#176 rules, #355 Prometheus) picks these up for free. A useful
alert: `increase(harvest_activity_circuit_tripped_total[5m]) > 0` — a trip means
a downstream is down. Work waits in `PENDING` (defer mode) or takes its failure
path (fail-fast mode).

### Management API

| Route | Purpose |
|---|---|
| `GET /api/harvest/admin/circuits` | List every breaker's state. |
| `GET /api/harvest/admin/circuits/{activity_name}` | One breaker's state. |
| `POST /api/harvest/admin/circuits/{activity_name}/force-open` | Pin open (operator). |
| `POST /api/harvest/admin/circuits/{activity_name}/force-close` | Reset to closed (operator). |

Each response carries `state` (`closed`/`open`/`half_open`), `forced_open`,
`last_trip`, `rolling_failure_count`, `time_until_probe_secs`, and the configured
`failure_threshold` / `window_secs` / `cooldown_secs` / `open_mode` (`defer` or
`fail_fast`).

> The breaker state is in-process. The management API reflects the breaker state
> of the worker process serving the request. In a split web/worker deployment,
> query the worker-owning process to observe live dispatch state. Forcing
> open/close affects the breaker shared by that process's worker.

## Incident playbook

**Symptom:** a downstream is down and `harvest_task_queue` / `harvest_dead_letters`
are filling with retries for one activity.

1. If the activity already has a breaker, confirm it tripped:
   `GET /admin/circuits/{activity_name}` → `state: "open"`. The retry storm
   should already be curbed.
2. If you need to stop dispatch **now** (e.g. the breaker hasn't tripped yet, or
   you're taking the downstream down for maintenance):
   `POST /admin/circuits/{activity_name}/force-open`. New attempts wait in
   `PENDING` (defer mode) or fail with `CircuitOpen` (fail-fast mode) until you
   force-close.
3. When the downstream is confirmed healthy again:
   `POST /admin/circuits/{activity_name}/force-close`. Normal tracking resumes —
   if the downstream is actually still bad, the breaker re-trips on its own.
4. In fail-fast mode, replay any workflows that failed via the DLQ / reset
   surfaces once the downstream is stable. In defer mode, the waiting work
   runs on its own.

## Replay safety & durability

A deferral appends no event, so replay never sees it. A fail-fast
short-circuited attempt records an ordinary `ActivityFailed` event carrying the
typed `CircuitOpen` payload — **no new `WorkflowEvent` variant** is introduced and
circuit state lives entirely outside the event log. Replay therefore reproduces
the recorded outcome regardless of the breaker's state at replay time: a workflow
that saw `CircuitOpen` will see it again on replay, exactly like any other
recorded activity failure.

## Out of scope (this slice)

- Global / cross-activity breakers ("trip everything if Postgres is down").
- Per-tenant / per-key circuit scope (composes with #247 as a follow-up).
- Cross-shard coordination of circuit state (each shard is independent).
- Adaptive / latency-based (Hystrix-style) tripping — threshold is a simple
  failure-count-in-window value.
