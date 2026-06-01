# Runbook: Activity circuit breakers (issue #369)

A **circuit breaker** lets an activity short-circuit its own dispatch when the
downstream service it depends on (email provider, payment gateway, search
index, …) is hard-down. Instead of retrying every failing attempt across its
full `RetryPolicy` curve — flooding `harvest_task_queue` and piling up identical
`harvest_dead_letters` entries across thousands of in-flight workflows — the
breaker **trips open** and new dispatches fast-fail with a non-retryable
`CircuitOpen` error within seconds. Workflows that handle failure (Saga
compensation, branching, non-retryable surfaces) reach their recovery path
quickly; doomed work stops consuming worker capacity.

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
- **Open** (tripped): new dispatches fast-fail with
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
    circuit_breaker = CircuitBreakerPolicy::new(10, Duration::from_secs(30), Duration::from_secs(60))
)]
async fn charge_card(ctx: &ActivityContext, req: ChargeRequest) -> Result<Receipt, ActivityFailure> {
    // ... call the payment gateway ...
}
```

The three knobs are:

| Field | Meaning |
|-------|---------|
| `failure_threshold` | Failures within `window` that trip the breaker open (min 1). |
| `window` | Rolling window over which failures are counted. |
| `cooldown` | Time the breaker stays open before admitting one half-open probe. |

## Handling `CircuitOpen` in workflow code

The failure flows through the typed activity-failure surface (#227), so workflow
code matches on it like any other error:

```rust
use autumn_harvest::error::HarvestError;

match ctx.execute_activity(&charge_card_info(), req).await {
    Ok(receipt) => { /* happy path */ }
    Err(HarvestError::ActivityFailed { error_type, .. }) if error_type == "CircuitOpen" => {
        // Downstream is down. Compensate / branch / defer instead of retrying.
        saga.compensate_all().await?;
    }
    Err(e) => return Err(e.to_string()),
}
```

## Decision matrix — circuit breaker vs. retry / jitter / rate limit

Pick the tool that matches the failure mode. They compose; a single activity can
use all four.

| Failure mode | Reach for | Why |
|---|---|---|
| **Downstream is hard-down** (100% failures, will stay down for minutes/hours) | **Circuit breaker** (#369) | Stop calling it. Retrying a dead target just floods the queue and DLQ. The breaker fast-fails so workflows recover in seconds, not hours. |
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

## Observability

Each shard / worker process tracks its own breaker state in-process (per the
per-shard ACID model; an outage that hits every shard trips each independently).

### Metrics (ADR-0001)

| Metric | Type | Labels | Emitted when |
|---|---|---|---|
| `harvest.activity.circuit.tripped` | counter | `activity.name` | Breaker trips closed→open, or re-opens after a failed half-open probe. |
| `harvest.activity.circuit.closed` | counter | `activity.name` | Breaker recovers to closed after a successful half-open probe. |

Existing alerting (#176 rules, #355 Prometheus) picks these up for free. A useful
alert: `increase(harvest_activity_circuit_tripped_total[5m]) > 0` — a trip means
a downstream is down and workflows are taking their failure path.

### Management API

| Route | Purpose |
|---|---|
| `GET /api/harvest/admin/circuits` | List every breaker's state. |
| `GET /api/harvest/admin/circuits/{activity_name}` | One breaker's state. |
| `POST /api/harvest/admin/circuits/{activity_name}/force-open` | Pin open (operator). |
| `POST /api/harvest/admin/circuits/{activity_name}/force-close` | Reset to closed (operator). |

Each response carries `state` (`closed`/`open`/`half_open`), `forced_open`,
`last_trip`, `rolling_failure_count`, `time_until_probe_secs`, and the configured
`failure_threshold` / `window_secs` / `cooldown_secs`.

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
   `POST /admin/circuits/{activity_name}/force-open`. New attempts fast-fail with
   `CircuitOpen` until you force-close.
3. When the downstream is confirmed healthy again:
   `POST /admin/circuits/{activity_name}/force-close`. Normal tracking resumes —
   if the downstream is actually still bad, the breaker re-trips on its own.
4. Replay any workflows that failed via the DLQ / reset surfaces once the
   downstream is stable.

## Replay safety & durability

A short-circuited attempt records an ordinary `ActivityFailed` event carrying the
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
