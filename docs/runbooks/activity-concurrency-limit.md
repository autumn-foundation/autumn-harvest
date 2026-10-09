# Runbook: Adaptive concurrency limit per activity type (issue #1836)

An **adaptive concurrency limit** caps how many attempts of one activity type
run at once on one worker. The cap moves with the dependency. It grows while
the handler latency stays near its no-load value. It shrinks when the latency
inflates or retryable failures rise.

Use it for an activity that calls a dependency with a capacity you do not
know, or a capacity that changes: a payment gateway, a search cluster, a
partner API. A fixed cap is either too low on a good day or too high on a
bad one. The adaptive cap finds the point where more calls stop adding
throughput and only add latency.

The limit is opt-in. Without configuration, no activity type is limited and
the worker behaves as before.

A runnable demo is in
[`autumn-harvest/examples/adaptive_concurrency_limit.rs`](../../autumn-harvest/examples/adaptive_concurrency_limit.rs).
It drives the real limiter against a simulated dependency and prints the cap:

```sh
cargo run --example adaptive_concurrency_limit
```

## Pick the right tool

The limit is one of four controls. They are independent and compose.

| Symptom | Tool |
|---|---|
| A dependency slows down when you call it harder | **Adaptive concurrency limit** (this page) |
| A dependency is hard down and every call fails | [Circuit breaker](activity-circuit-breaker.md) |
| Retries of a failing type flood the queue | Retry budget, see [`architecture.md`](../architecture.md) design decision 11 |
| The worker itself has idle slots or a pool that runs dry | [Adaptive slot tuner](../operations/adaptive-slot-tuner.md) |
| You know the exact safe concurrency, fleet-wide | `#[activity(max_concurrent = N)]` |

The slot tuner and the limit react to different signals. The slot tuner reads
the worker's own permit waits. The limit reads only the handler latency and
the retryable failures of one type. When a dependency is the bottleneck, the
slot tuner can add slots while the limit holds that type back.

## Enable it

Pass an `AdaptiveLimitConfig` to the worker:

```rust
use autumn_harvest::prelude::*;

let limits = AdaptiveLimitConfig::disabled()
    // A policy for every activity type without an override.
    .with_default(Some(AdaptiveLimitPolicy::default()))
    // A policy for one type. The key is the activity name.
    .with_activity("charge_card", Some(AdaptiveLimitPolicy::new(2, 100)))
    // No limit for one type.
    .with_activity("load_cart", None);

let worker_config = WorkerConfig::default().with_adaptive_limit(limits);
```

- `with_default` sets the policy for every type. Leave it out to limit only
  the types that you name.
- `with_activity(name, Some(policy))` overrides one type.
  `with_activity(name, None)` turns the limit off for one type.
- An override for a name that the worker does not register has no effect.
  The worker logs a warning at startup.
- If you build a `HandlerRegistry` yourself instead of a `WorkerConfig`, call
  `HandlerRegistry::with_adaptive_limit` with the same config.

`GET /admin/config` shows the active settings as `adaptive_limit_default` and
`adaptive_limit_overrides`.

## The policy

`AdaptiveLimitPolicy::new(min_limit, max_limit)` keeps the other defaults.
Set a field directly to change it.

| Field | Default | What it does | When to change it |
|---|---|---|---|
| `min_limit` | `1` | Lowest cap. | Raise it if a cap of 1 starves the type during a slowdown. |
| `max_limit` | `200` | Highest cap. | Set it to a hard limit that the dependency must never pass, such as a connection pool size. |
| `tolerance` | `1.25` | Latency rise over the baseline that the limit accepts before it slows growth. | Raise it to trade latency for throughput. Lower it to keep latency close to the no-load value. |
| `backoff_ratio` | `0.9` | Factor for the cap after a window with too many retryable failures. | Lower it, for example to `0.7`, to back off harder on errors. |
| `error_threshold` | `0.05` | Share of retryable failures in a window that counts as overload. | Raise it for a dependency with a steady background error rate. At `0`, one failure cuts the cap. |
| `probe_interval` | `1000` | Samples between two measurements of the no-load baseline. | Raise it at a high cap. A probe drops the cap to 4, within `[min_limit, max_limit]`, and never raises it. It lasts until a window gets a successful answer, so in an outage it can last many windows. |

The config clamps out-of-range values. For example, `min_limit` becomes at
least 1, `tolerance` at least 1 and `probe_interval` at least 10.

## How the cap moves

The limit collects samples in windows of about one cap, so the cap moves
about once per round trip.

1. **Baseline.** The limit keeps the lowest window mean latency. This is its
   estimate of the latency at no load.
2. **Target.** Each window sets a target of `cap × gradient + 4`. The
   gradient is `tolerance × baseline / window mean`, in the range from 0.5
   to 1. The cap moves 20 % of the way to the target.
3. **Growth and shrink.** While the window mean stays within `tolerance` of
   the baseline, the gradient is 1 and the cap grows by up to 0.8 per
   window. When the mean passes `tolerance × baseline`, the gradient falls
   below 1 and the cap shrinks.
4. **Errors.** A window with more retryable failures than `error_threshold`
   cuts the cap by `backoff_ratio`.
5. **Low demand.** A window that used less than half of the cap leaves it
   unchanged. Little traffic says nothing about the dependency.
6. **Probe.** After `probe_interval` samples, the next window that is not
   overloaded starts a probe. The cap drops to 4, so the limit can measure
   the baseline again. The probe cap stays in `[min_limit, max_limit]`, and
   a cap below 4 is not raised. The probe ends with the first window that
   has at least one successful answer. Then the cap returns to its value
   from before the probe. Each overloaded probe window first cuts that value
   by `backoff_ratio`.

   A probe window with only failures, as in a hard outage, does not end the
   probe. The cap stays at the probe cap, and the value that it returns to
   falls with each such window, down to `min_limit`. When the dependency
   answers again, the cap returns to that reduced value and grows from
   there.

A new type starts with a probe at 4, within `[min_limit, max_limit]`. Against a dependency that slows down in proportion
to the load above a knee, the cap settles near `tolerance × knee + 4`. The
example shows this: a knee of 20 gives a cap of 29, and a knee of 8 gives 14.

What counts as a sample:

| Attempt result | Sample |
|---|---|
| Success | Answer, with its latency |
| Retryable failure | Overload |
| Timeout (start-to-close, schedule-to-close or heartbeat) | Overload |
| Non-retryable failure | None. A bad request says nothing about load. |
| Panic, WASM fault | None. The fault is in the worker. |
| Workflow cancel | None |

The full rules are in [`architecture.md`](../architecture.md) design decision 12.

## What happens at the cap

- The worker does not claim more tasks of that type. The tasks stay
  `PENDING` for another worker or a later poll. Other types on the same
  worker are not affected.
- When a slot frees, the worker polls again at once.
- Two claim loops can claim at the same moment and pass the cap. They can
  be loops of one worker (`max_concurrent_claims`) or of two workers. The
  worker then puts the task back to `PENDING` with a short delay. This uses no attempt,
  writes no history event and spends no retry-budget token. The counter
  `harvest.activity.concurrency_deferred` counts these deferrals.
- A circuit-breaker short-circuit and a half-open probe take no slot.

## Metrics

Each metric has the label `activity`. The starter dashboard shows them in the
row "Circuit breakers, retry budget and adaptive limit".

| Metric (Prometheus name) | Type | Meaning |
|---|---|---|
| `harvest_activity_concurrency_limit` | Gauge | The current cap. |
| `harvest_activity_concurrency_in_flight` | Gauge | Attempts that hold a slot. |
| `harvest_activity_latency_baseline_seconds` | Gauge | The no-load latency estimate. |
| `harvest_activity_concurrency_deferred_total` | Counter | Claims that raced past the cap and were deferred. |

Each worker learns its own cap, so compare values per worker. The queries
below keep the `instance` label that Prometheus adds to each scraped worker.
Use `sum by (activity)` for a fleet total and `min by (activity)` to find the
most limited worker.

Useful queries:

```promql
# The cap and the use of each type, per worker.
harvest_activity_concurrency_limit
harvest_activity_concurrency_in_flight

# Types that are at their cap. A backlog then waits on the dependency.
harvest_activity_concurrency_in_flight >= harvest_activity_concurrency_limit

# Latency inflation over the baseline, per worker. Near `tolerance`, the
# cap has settled. Only successful attempts count, as in the limiter.
(
  sum by (activity, instance) (rate(harvest_activity_duration_sum{status="completed"}[5m]))
  / sum by (activity, instance) (rate(harvest_activity_duration_count{status="completed"}[5m]))
)
/ max by (activity, instance) (harvest_activity_latency_baseline_seconds)
```

## Troubleshooting

**The cap stays at 4.** There are two common causes:

- The type does not get enough traffic. The limit does not grow a cap that
  a window does not use, so a quiet type stays at its start value. This is
  expected. The cap grows when demand grows.
- The dependency returns only failures during a probe. The probe then waits
  for a successful answer, and the cap stays at the probe cap. Check
  `harvest.activity.failed` for the type, and see the probe rule above.

**The cap falls and the type builds a backlog.** The limit is working: the
dependency is slower, or it returns retryable errors. Check the latency
inflation query above and `harvest.activity.failed` for the type. If the
dependency is healthy but noisy, raise `error_threshold`. If its latency
varies a lot by request, raise `tolerance`.

**The cap swings up and down.** A long activity gives few samples, so each
window is small and noisy. The limit suits request-and-response calls. For a
long activity, prefer `max_concurrent` or raise `min_limit`.

**`concurrency_deferred` rises steadily.** Claims often race past the cap,
for example when one poll claims a batch of tasks of the type. The claim
loops of one worker can also claim the type at once. A worker with
`max_concurrent_claims = 1` races less. Each deferral costs a short delay,
from 50 ms to 5 s, not an attempt. A steady low rate is harmless. A high rate while the cap falls means that the
dependency is degrading faster than the claims see it.

**One worker is limited and the others are not.** Each worker learns its own
cap. A worker on a slower network path, or in another zone, can settle lower.
N workers allow up to N caps in total. For a hard fleet-wide ceiling, also
set `#[activity(max_concurrent = N)]`.

## Scope

- The state lives in the worker process and starts again on restart.
- The limit never writes to the event history, so replay is unaffected.
- Local activities and the SQLite backend do not use the limit.
