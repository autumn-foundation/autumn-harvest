## Phase — Retry budget per activity type (issue #1793)

Nothing limited retries in aggregate. Each task retried up to its own policy.
During a dependency brownout, the retries multiplied the load on the
dependency when it could least absorb it.

**What shipped.** Each worker keeps one token bucket for each activity type.
A first attempt deposits `ratio` tokens. A retry spends one token. Time adds
`min_retries_per_sec` tokens each second. An empty bucket defers the retry
through the fenced `defer_claimed_rate_limited_task` write. The row stays
`PENDING` and keeps its `attempt` and `error`. **A deferred retry is never
lost.**

- New module `retry_budget.rs`: `RetryBudgetConfig`, `RetryBudgetRegistry`,
  `Admission` and `BudgetTicket`.
- New `policy::RetryBudgetPolicy`. Defaults: `ratio = 0.1`,
  `max_tokens = 10`, `min_retries_per_sec = 1`.
- **On by default.** `WorkerConfig::with_retry_budget` and
  `HandlerRegistry::with_retry_budget` take a `RetryBudgetConfig`.
  `with_activity(name, policy)` overrides one type. `None` turns the budget off
  for that type. `RetryBudgetConfig::disabled()` turns it off everywhere.
- The gate in `process_activity_task` runs after the circuit breaker and
  before `ActivityStarted`. A `CircuitOpen` short-circuit spends nothing. A
  rate-limit deferral and a no-op start give their tokens back. A budget
  deferral refunds the claim-time rate-limit token.
- New metrics: gauge `harvest.retry.budget.available{activity}` and counter
  `harvest.retry.budget.exhausted{activity}` (Prometheus
  `harvest_retry_budget_exhausted_total`). Two new dashboard panels.
- `GET /admin/config` reports `retry_budget_default` and
  `retry_budget_overrides`.

The semantics are in `docs/architecture.md`, design decision 10. The budget is
per worker process. Local activities and the SQLite backend are not gated.

No new `WorkflowEvent` variant, no migration, no schema change.

**Tests, red then green.**

- `retry_budget_tests` (end to end, Postgres): one activity type fails 100 %
  of its attempts. Before the fix, 160 retries ran in 4.7 s where the budget
  allows 16.7. After the fix, the retry count stays within the budget, every
  task row stays live and the attempt counters match the attempts that ran.
  A second test shows that another type keeps its own budget.
- `retry_budget::tests` (unit) and `retry_budget_props` (proptest): retries
  never exceed `max_tokens + ratio × first_attempts + min_retries_per_sec × T`.
- `metrics_rs_adapter`: both metrics are bridged with the `activity` label.
