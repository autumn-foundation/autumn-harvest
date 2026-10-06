## Phase — Adaptive concurrency limit per activity type (issue #1836)

The slot tuner grows the worker slots when local permit waits rise. When a
dependency is the bottleneck, that is the wrong direction. More calls only
add latency and errors.

**What shipped.** An opt-in limit caps the in-flight attempts of one activity
type on one worker. The cap follows the handler latency and the retryable
failures. It never reads a queue wait or a permit wait.

- New module `adaptive_limit.rs`: `AdaptiveLimitConfig`,
  `AdaptiveLimitRegistry`, `Acquire`, `LimitPermit`, `SampleOutcome` and
  `LimitSnapshot`. `AdaptiveLimitConfig` is in the prelude.
- New `policy::AdaptiveLimitPolicy`. Defaults: `min_limit = 1`,
  `max_limit = 200`, `tolerance = 1.25`, `backoff_ratio = 0.9`,
  `probe_interval = 1000`.
- The rule is the Netflix Gradient2 gradient, `tolerance × baseline /
  latency` in the range from 0.5 to 1, applied once per window of about one
  cap of samples. The baseline is a probed minimum, as in Netflix Gradient.
  A window with a retryable failure cuts the cap by `backoff_ratio`.
- `WorkerConfig::with_adaptive_limit` and
  `HandlerRegistry::with_adaptive_limit` take an `AdaptiveLimitConfig`. The
  limit is off by default. An override for an unregistered name logs a
  warning.
- The claim skips a type at its cap, through the ineligible-activity list.
  A claim that races past the cap is deferred in `process_activity_task`
  with `queue::defer_claimed_retry_for_budget`. The gate runs before the
  retry budget, so the deferral spends no attempt and no budget token. A
  circuit short-circuit and a half-open probe take no slot.
- New gauges, each labeled by `activity`:
  `harvest.activity.concurrency_limit`,
  `harvest.activity.concurrency_in_flight` and
  `harvest.activity.latency_baseline_seconds`. Three new dashboard panels.
- `GET /admin/config` reports `adaptive_limit_default` and
  `adaptive_limit_overrides`.

The semantics are in `docs/architecture.md`, design decision 12. The limit is
per worker process. Local activities and the SQLite backend are not gated.

No new `WorkflowEvent` variant, no migration, no schema change.

**Tests, red then green.**

- `adaptive_limit::simulation` (unit): a closed-loop simulation against a
  downstream whose latency grows above a knee of 40. The cap settles at 54,
  the analytic fixed point `tolerance × knee + 4`, and peaks at 57. Over
  100 000 samples it shows no drift. With a baseline that follows the
  window mean, the same test drifts to 138 and fails. A downstream that
  fails above its knee holds the cap near the knee through the backoff.
- `adaptive_limit::tests` (unit): growth, app limit, backoff, floor and
  ceiling, probe, deferral delay, and gauge order under concurrency.
- `adaptive_limit_tests` (end to end, Postgres):
  - A fixed cap of 3 holds on a worker with 16 slots. Before the gate, the
    type ran 16 attempts at once. Another type on the same worker is not
    capped.
  - The limit is off by default.
  - Against a handler whose latency grows above a knee of 4, the cap
    settles near 9, below the 16 worker slots.
  - A claim past the cap is deferred without using an attempt. With the
    dispatch gate disabled, the type ran 12 attempts at once.
- `metrics_rs_adapter`: the adapter bridges the three gauges with the
  `activity` label, and skips an unknown baseline.
