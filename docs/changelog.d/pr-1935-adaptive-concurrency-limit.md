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
  `error_threshold = 0.05`, `probe_interval = 1000`.
- The rule is the Netflix Gradient2 gradient, `tolerance × baseline /
  latency` in the range from 0.5 to 1. It is applied once per window of
  about one cap of samples. The baseline is a probed minimum, as in Netflix
  Gradient.
- A window whose retryable failures pass `error_threshold` of its
  completions cuts the cap by `backoff_ratio`. A start-to-close,
  schedule-to-close or heartbeat timeout counts as a retryable failure. A
  non-retryable failure, a panic and a WASM module or runtime fault give no
  sample, even past the deadline.
- `WorkerConfig::with_adaptive_limit` and
  `HandlerRegistry::with_adaptive_limit` take an `AdaptiveLimitConfig`. The
  limit is off by default. An override for an unregistered name logs a
  warning.
- The claim skips a type at its cap. The saturated names ride in the
  ineligible-activity array `$6` with `queue::SATURATED_ACTIVITY_MARKER`. A
  new claim gate reads them, and it also covers rows with
  `required_capabilities`. When no type is saturated, the check reads one
  atomic.
- A claim that still races past the cap is deferred in
  `process_activity_task` with `queue::defer_claimed_retry_for_budget`. The
  deferral spends no attempt. The gate runs before the retry budget, so the
  deferral spends no budget token. A circuit short-circuit and a half-open
  probe take no slot.
- New metrics, each labeled by `activity`: the gauges
  `harvest.activity.concurrency_limit`,
  `harvest.activity.concurrency_in_flight` and
  `harvest.activity.latency_baseline_seconds`, and the counter
  `harvest.activity.concurrency_deferred` (Prometheus
  `harvest_activity_concurrency_deferred_total`). Four new dashboard panels.
  New `MetricsRecorder` methods `record_activity_concurrency_limit` and
  `record_activity_concurrency_deferred` have no-op defaults.
- `GET /admin/config` reports `adaptive_limit_default` and
  `adaptive_limit_overrides`.

The semantics are in `docs/architecture.md`, design decision 12. The limit is
per worker process. Local activities and the SQLite backend are not gated.

No new `WorkflowEvent` variant, no migration, no schema change.

**Tests, red then green.**

- `adaptive_limit::simulation` (unit): a closed-loop simulation against a
  downstream whose latency grows above a knee of 40. The cap settles at 54,
  the analytic fixed point `tolerance × knee + 4`, and peaks at 55. Over
  100 000 samples it shows no drift. With a baseline that follows the
  window mean, the cap drifts far above the knee (a median of 138 in the
  noise test) and three simulation tests fail. A downstream that fails
  above its knee holds the cap near the knee through the backoff. A 1 %
  background error rate keeps the cap of an error-free run.
- `adaptive_limit::tests` (unit): growth, app limit, backoff and its
  threshold, floor and ceiling, probe rules, deferral delay, and gauge
  order under concurrency.
- `adaptive_limit_tests` (end to end, Postgres):
  - A fixed cap of 3 holds on a worker with 32 slots. Before the gate, the
    type ran 16 attempts at once on 16 slots. Another type on the same
    worker is not capped.
  - The limit is off by default.
  - Against a handler whose latency grows above a knee of 8, the cap grows
    from 4 and settles near 14. The baseline gauge reads the no-load
    latency.
  - A type with capability requirements at its cap is not claimed. Before
    the claim gate covered such rows, 35 claims churned through a deferral.
  - The timeouts of a hung dependency cut the cap, for an attempt deadline
    and for a heartbeat timeout. The heartbeat case failed before the worker
    read the timeout error from the task row.
  - Answers that arrive 20 ms after a 300 ms deadline cut the cap. Before
    the deadline check covered uncancelled attempts, they grew it.
  - A freed slot wakes the idle poll loop. With a 1 s poll interval, 20
    fast runs at a cap of 2 took 5.8 s before and about 1 to 2 s after, as
    without a cap.
- `metrics_rs_adapter`: the adapter bridges the three gauges and the
  counter with the `activity` label, and skips an unknown baseline.
