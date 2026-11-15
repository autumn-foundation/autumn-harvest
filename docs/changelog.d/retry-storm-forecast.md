## Phase X.Y — Retry storm forecast

`retry_storm` tells an operator how much load an outage adds to a
dependency, and when that load lands, before the outage happens.

- `simulate` replays a steady stream of calls through one outage window. It
  reports the first attempts, the retries and the failures in each time
  bucket.
- `peak_ratio` compares the worst bucket with the load without an outage.
  `amplification` gives the attempts for each call. `exhausted` counts the
  calls whose last attempt fails.
- Retry delays come from `RetryPolicy::next_delay_with_seed`, seeded by the
  call index. So the forecast shows how much jitter flattens a herd.
- A `RetryBudgetPolicy` runs through a real `RetryBudgetRegistry`. The report
  counts the deferrals, and the calls still pending at the horizon.
- Each run is deterministic. The overflow deferral of the budget draws from a
  seed, not from `rand`. The module is pure and sends no SQL.
