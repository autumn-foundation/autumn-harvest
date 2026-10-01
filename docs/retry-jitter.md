# Retry jitter (issues #342, #1792)

Harvest jitters retry delays by default. Tasks that fail together do not
retry together.

## Why deterministic jitter

Harvest computes each jittered delay from a stable seed. The seed comes from
the execution id and the activity id. The same build always gives the same
delay for one task and attempt, also after a worker restart.

## Strategy guidance

- `Full` (default): uniform random in `[0, base]`. It gives the widest spread
  and the largest delay variance.
- `Equal`: uniform random in `[base/2, base]`. It spreads retries and keeps
  half the base delay as a minimum.
- `Decorrelated`: random in `[initial, min(prev*3, max)]`. It prevents
  lockstep on long retry chains and stays below `max`.
- `None`: exact backoff. Use it when you need exact timing.

`RetryPolicy::exponential`, `RetryPolicy::fixed` and `RetryPolicy::default`
all use `Full`. A policy with no `jitter` key in its JSON also reads as
`Full`.

## Opt out

Set `JitterPolicy::None` on the policy:

```rust
use std::time::Duration;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};

let retry = RetryPolicy::exponential(6, Duration::from_secs(1))
    .with_jitter(JitterPolicy::None);
assert_eq!(retry.next_delay(3), Some(Duration::from_secs(4)));
```

## Timers in workflow code

Replay compares each recorded timer duration with the duration the code
computes. If a workflow sets a timer from a retry policy, the policy is part
of replay. Before issue #1792, a constructor-built policy gave the exact
backoff. It now gives a jittered value. A run recorded on an older build then
fails replay with a timer mismatch. Pin `.with_jitter(JitterPolicy::None)` on
such a policy, or gate the change with `ctx.patched`.

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

The two re-dispatch loops use `Equal`, not `Full`. Each delay is at least
half the base. This floor prevents a hot loop when many executions block or
panic together.

The SQLite backend ignores `JitterPolicy` and retries at the base delay. It
has a single writer, so no fleet retries together.

## Cron schedule fire jitter

A cron schedule gets a default fire jitter of 10 s (`DEFAULT_CRON_JITTER`).
Each fire moves forward by a deterministic offset in `[0, 10s)`. The offset
comes from the schedule id and the slot time.

The default applies to these entry points:

- `WorkflowSchedule::new` and `#[dag(schedule = ...)]`.
- `POST /admin/schedules/workflow` and `POST /admin/schedules/preview` with no
  `jitter_secs`.
- `harvest schedule create-workflow` with no `--jitter-secs`.

It does not apply to these schedules:

- A cron expression with a seconds field. Such a schedule can fire more often
  than once per 10 s.
- An interval schedule or a manual schedule.

A PATCH that changes the cadence and sends no `jitter_secs` re-derives a
defaulted jitter. Thus a 10 s default does not follow a cron onto a faster
cadence.

To opt out, set the jitter to zero:

```rust
use std::time::Duration;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};

let sched = WorkflowSchedule::new("report", Schedule::Cron("0 9 * * *".into()))
    .with_jitter(Duration::ZERO);
assert_eq!(sched.jitter, Duration::ZERO);
```

On a DAG, use `#[dag(schedule = "0 9 * * *", jitter = "0s")]`. On the API,
send `"jitter_secs": 0`.

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
