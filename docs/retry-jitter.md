# Retry jitter (issue #342)

Harvest supports deterministic, opt-in retry jitter for activity retries via
`RetryPolicy::jitter`.

## Why deterministic jitter

Retry delays are part of workflow replay behavior. Harvest therefore computes
jittered delays from a stable seed derived from workflow/task identity, so the
same history and code produce the same timer behavior across worker restarts.

## Strategy guidance

- `None` (default): classic deterministic backoff; use when exact legacy timing
  compatibility is required.
- `Full`: uniform random in `[0, base]`; strongest spread, highest tail
  variability.
- `Equal`: uniform random in `[base/2, base]`; good spread while preserving
  minimum pacing.
- `Decorrelated`: random in `[initial, min(prev*3, max)]`; useful to avoid lock
  step on long retries while remaining bounded.

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
