## Phase X.Y — Breaker forecast

`breaker_forecast` replays an outage through a real circuit breaker. It
answers how much load the breaker keeps off a dead dependency, and how long
good work waits after the dependency recovers.

- `simulate` takes a `RetryPolicy`, a `CircuitBreakerPolicy` and the
  `StormScenario` of `retry_storm`, so one scenario compares both forecasts.
- Each bucket splits dependency attempts from the short circuits that the
  breaker absorbs. The report lists each trip and each recovery.
- `detection_lag` measures the outage start to the first trip.
  `recovery_lag` measures the outage end to the breaker close.
- `FailFast` mode ends a short-circuited call. `Defer` mode delays it with
  the clamps of the worker, so the report shows where the backlog lands.
- The module is pure and sends no SQL.
