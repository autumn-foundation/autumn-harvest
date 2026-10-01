## Phase — Retry budget per activity type (issue #1793)

Without a budget, nothing limits retries in aggregate. Each task retries up to
its own policy. During a dependency brownout, the retries multiply the load on
the dependency when it can least absorb it.

**Behavior change.** The budget is on by default. A burst of more than about
10 retries of one activity type on one worker now waits for tokens, at about 1
retry each second plus 0.1 for each first attempt. Use
`RetryBudgetConfig::disabled()` to keep the old behavior.

**What shipped.** Each worker keeps one token bucket for each activity type.
A first attempt deposits `ratio` tokens. A retry spends one token. Time adds
`min_retries_per_sec` tokens each second. An empty bucket defers the retry
through the new fenced write `queue::defer_claimed_retry_for_budget`. The row
stays `PENDING` and keeps its `attempt`, `error` and `crash_strikes`. **A
deferred retry is never lost.**

- New module `retry_budget.rs`: `RetryBudgetConfig`, `RetryBudgetRegistry`,
  `Admission` and `BudgetTicket`. `RetryBudgetConfig` is in the prelude.
- New `policy::RetryBudgetPolicy`. Defaults: `ratio = 0.1`,
  `max_tokens = 10`, `min_retries_per_sec = 1`.
- `WorkerConfig::with_retry_budget` and `HandlerRegistry::with_retry_budget`
  take a `RetryBudgetConfig`. `with_activity(name, policy)` overrides one
  type. `None` turns the budget off for that type. An override for an
  unregistered name logs a warning.
- The gate in `process_activity_task` runs after the circuit breaker and
  before `ActivityStarted`. A `CircuitOpen` short-circuit spends nothing, and a
  half-open probe is never deferred. A drop guard gives the tokens back for an
  attempt that does not run. A budget deferral refunds the claim-time
  rate-limit token.
- Deferral delays follow the refill rate, from 50 ms. Past 60 s, a delay is
  random from 30 s to 60 s, so a backlog does not wake at one instant.
- New metrics: gauge `harvest.retry.budget.available{activity}` and counter
  `harvest.retry.budget.exhausted{activity}` (Prometheus
  `harvest_retry_budget_exhausted_total`). Two new dashboard panels.
- `GET /admin/config` reports `retry_budget_default` and
  `retry_budget_overrides`.

The semantics are in `docs/architecture.md`, design decision 10. The budget is
per worker process. Local activities and the SQLite backend are not gated.

No new `WorkflowEvent` variant, no migration, no schema change.

**Tests, red then green.**

- `retry_budget_tests` (end to end, Postgres):
  - One activity type fails 100 % of its attempts. Before the fix, 160 retries
    ran in 4.7 s where the budget allows 16.7. After the fix, the retry count
    stays within the budget. Every task row stays live. The attempt counters
    and the `ActivityStarted` events match the attempts that ran.
  - A second type keeps its own budget while the first type is exhausted.
  - A disabled budget never defers.
  - A deferral keeps `crash_strikes`, `attempt` and `error`.
  - A deferral refunds the claim-time rate-limit token.
- `retry_budget::tests` (unit) and `retry_budget_props` (proptest): retries
  never exceed `max_tokens + ratio × first_attempts + min_retries_per_sec × T`,
  including after releases.
- `metrics_rs_adapter`: the adapter bridges both metrics with the `activity`
  label.
