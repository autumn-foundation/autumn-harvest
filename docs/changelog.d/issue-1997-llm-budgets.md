## Phase 3.18 — LLM token and cost budgets per run and per tenant (issue #1997)

A declared budget now stops the LLM spend of an agent loop. `QuotaPolicy`
takes four new caps and one window: `max_run_llm_tokens`,
`max_run_llm_cost_usd_micros`, `max_tenant_llm_tokens`,
`max_tenant_llm_cost_usd_micros` and `tenant_llm_window_secs`. The window is
one day by default. The `#[workflow(quota(...))]` macro takes the same keys
and rejects a zero window. The LLM caps do not change admission.

An LLM step calls `ActivityContext::check_llm_budget` before its model call,
and records the call with `ActivityContext::record_llm_call` (issue #1996)
after it. The check sums the agent cost ledger, `harvest_llm_ledger`, for the
run and for its tenant. The cost caps are in millionths of a US dollar, as
the ledger is. An unpriced call adds no cost. A call in an attempt that
fails is not counted, because the ledger writes completed attempts only.

A spent cap refuses the step with the non-retryable failure type
`LlmBudgetExceeded`. The details name the resource, the cap and the spend.
`LlmBudgetExceeded::is_refusal` tests for it, and
`LlmBudgetExceeded::from_error` reads it. The quota metric
`harvest.quota.rejected` counts it under four new `QuotaResource` labels:
`run_llm_tokens`, `run_llm_cost_usd_micros`, `tenant_llm_tokens` and
`tenant_llm_cost_usd_micros`. The check reads the policy in its own process,
so register the workflow type on each worker that runs its LLM steps.

The agent adapter checks the budget before each model turn, then records the
turn in the cost ledger as before. The check has a 10-second budget,
`AgentHarness::budget_timeout`, so the model turn stays inside its
`start_to_close`. `AgentTask::tenant` keys the tenant budget. A refused turn
ends the run under the new stop `budget_exceeded`.

The limiter question of issue #1997 is settled. `adaptive_limit.rs` (#1935)
is the current design. No token-keyed limit ships. See `docs/design/DESIGN-1997.md`
§0.5.

**Breaking changes.**

- `QuotaPolicy` has five new public fields. A struct literal must add them,
  or use `..QuotaPolicy::new(key)`.
- `QuotaResource` has four new variants. It is `#[non_exhaustive]`, so a
  `match` already has a wildcard arm.
- `AgentStop` has the new variant `BudgetExceeded`, and it is now
  `#[non_exhaustive]`. A `match` on it needs a wildcard arm.

**Migration.** `20261013032315_harvest_llm_tenant_index` adds the partial
expression index `idx_harvest_we_llm_tenant` on `harvest_workflow_executions`.
A tenant check reads the runs that are active or ended in its window through
it. On a large table, build it with `CREATE INDEX CONCURRENTLY` first, as the
migration header shows. No `WorkflowEvent` variant, no change to
`harvest_events`, no replay impact.

**Tests.** `llm_budget::tests` and two `quota::tests` cover the rule. The
Postgres suite `llm_budget_tests` covers a run cap, a tenant cap across runs
and types, and a cost cap. It also covers the window, the ledger row, a
failed attempt, a reset fork, no cap, an unresolvable key and the metric.
`macros_workflow` and a compile-fail case cover the macro keys. The agent
tests cover the budget check, its time budget, the stop and the `tenant`
field.
