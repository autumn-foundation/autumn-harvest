# Retry jitter (issues #342, #1792)

Harvest jitters retry delays by default. Tasks that fail together do not
retry together.

## Why deterministic jitter

Retry delays are part of workflow replay behavior. Harvest computes each
jittered delay from a stable seed. The seed comes from workflow and task
identity. The same history and code give the same delay after a worker
restart.

## Strategy guidance

- `Full` (default): uniform random in `[0, base]`. Strongest spread, highest
  tail variability.
- `Equal`: uniform random in `[base/2, base]`. Good spread, and it keeps a
  minimum pace.
- `Decorrelated`: random in `[initial, min(prev*3, max)]`. Avoids lockstep on
  long retries and stays bounded.
- `None`: classic exact backoff. Use it when you need exact timing.

`RetryPolicy::exponential`, `RetryPolicy::fixed` and `RetryPolicy::default`
all use `Full`. A stored policy with no `jitter` key also reads as `Full`.

## Opt out

Set `JitterPolicy::None` on the policy:

```rust
use std::time::Duration;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};

let retry = RetryPolicy::exponential(6, Duration::from_secs(1))
    .with_jitter(JitterPolicy::None);
assert_eq!(retry.next_delay(3), Some(Duration::from_secs(4)));
```

## Example

```rust
use std::time::Duration;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};

let retry = RetryPolicy::exponential(6, Duration::from_secs(1))
    .with_jitter(JitterPolicy::Equal);
let delay = retry.next_delay_with_seed(3, 0xdecafbad).unwrap();
assert!(delay >= Duration::from_secs(2));
```

A runnable example is available:

```bash
cargo run -p quickstart --bin retry-jitter-example
```

## Engine-internal backoffs

The engine also jitters its own backoffs. None of these has an opt-out.

| Backoff | Jitter | Range |
|---|---|---|
| Activity retry with no retry policy | `Full` | `[0, 1s]` |
| Non-determinism block re-dispatch | `Equal` | `[base/2, base]`, base `5s * 2^n` up to 300 s |
| Contained handler-panic re-dispatch | `Equal` | `[base/2, base]`, base `1s * 2^(n-1)` up to 30 s |

The two re-dispatch loops use `Equal`, not `Full`. A floor of half the base
stops a blocked or panicking cohort from a hot loop.

## Cron schedule fire jitter

A cron schedule gets a default fire jitter of 10 seconds
(`DEFAULT_CRON_JITTER`). Each fire moves forward by a deterministic offset in
`[0, 10s)`. The offset comes from the schedule id and the slot time.

The default applies to `WorkflowSchedule::new` and to `#[dag(schedule = ...)]`.
It does not apply to these schedules:

- A cron expression with a seconds field. It can fire more often than the
  window.
- An interval schedule or a manual schedule.

To opt out, set the jitter to zero:

```rust
use std::time::Duration;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};

let sched = WorkflowSchedule::new("report", Schedule::Cron("0 9 * * *".into()))
    .with_jitter(Duration::ZERO);
assert_eq!(sched.jitter, Duration::ZERO);
```

On a DAG, use `#[dag(schedule = "0 9 * * *", jitter = "0s")]`.

## Determinism validation

Replay determinism for jitter-derived timer durations is covered by
`replay_jitter_timer_is_exact_and_deterministic` in
`autumn-harvest/tests/integration/replayer_tests.rs`.

## Benchmark success metric

Measure overhead of jitter calculation with:

```bash
cargo bench -p autumn-harvest --bench retry_jitter_bench --features testing --no-default-features
```

Success metric: p95 latency for `next_delay_with_seed` remains under **250ns**
for `None` and under **500ns** for jittered modes on a laptop-class CPU.
