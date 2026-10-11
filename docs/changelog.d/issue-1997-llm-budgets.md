## Phase 3.18 — LLM token and cost budgets per run and per tenant (issue #1997)

A declared budget now stops the LLM spend of an agent loop. `QuotaPolicy`
takes four new caps and one window: `max_run_llm_tokens`,
`max_run_llm_cost_micros`, `max_tenant_llm_tokens`,
`max_tenant_llm_cost_micros` and `tenant_llm_window_secs`. The window is one
day by default. The `#[workflow(quota(...))]` macro takes the same keys and
rejects a zero window. The LLM caps do not change admission.

An LLM step calls `ActivityContext::check_llm_budget` before its model call
and `ActivityContext::record_llm_usage` after it. A spent cap refuses the
step with the non-retryable failure type `LlmBudgetExceeded`. The details
name the resource, the cap and the spend. `LlmBudgetExceeded::is_refusal`
tests for it, and `LlmBudgetExceeded::from_error` reads it. The quota metric
`harvest.quota.rejected` counts it under four new `QuotaResource` labels:
`run_llm_tokens`, `run_llm_cost_micros`, `tenant_llm_tokens` and
`tenant_llm_cost_micros`. The check reads the policy in its own process, so
register the workflow type on each worker that runs its LLM steps.

The agent adapter checks and records on each model turn. `AgentModel` gets
the provided methods `model_id` and `cost_micros`. `AgentTask` gets an
optional `tenant`. A refused turn ends the run under the new stop
`budget_exceeded`.

This ships the part of the cost ledger (issue #1996) that the budgets need.
That part is the `harvest_llm_ledger` side table, the write, and the
security-posture note on its clear columns. A shard rebalance copies the
ledger rows of a run. The usage-report roll-up stays with issue #1996.

The limiter question of issue #1997 is settled. `adaptive_limit.rs` (#1935)
is the current design. No token-keyed limit ships. See `DESIGN-1997.md`
§0.5.

**Breaking changes.**

- `QuotaPolicy` has five new public fields. A struct literal must add them,
  or use `..QuotaPolicy::new(key)`.
- `QuotaResource` has four new variants. It is `#[non_exhaustive]`, so a
  `match` already has a wildcard arm.
- `AgentStop` has the new variant `BudgetExceeded`, and it is now
  `#[non_exhaustive]`. A `match` on it needs a wildcard arm.
- `LlmUsage` is `#[non_exhaustive]`. Build it with `LlmUsage::new`.

**Migration.** `20261009050206_harvest_llm_ledger` adds the side table and
two indexes. No `WorkflowEvent` variant, no change to `harvest_events`, no
replay impact.

**Tests.** `llm_budget::tests` and two `quota::tests` cover the rule. The
Postgres suite `llm_budget_tests` covers a run cap, a tenant cap across runs
and types, and a cost cap. It also covers the window, the ledger row, a
retry that pays again, no cap, an unresolvable key and the metric.
`shard_rebalance_db_tests::the_llm_ledger_moves_with_the_run` covers the
rebalance. `macros_workflow` and a compile-fail case cover the macro keys.
The agent tests cover the check, call and record order, the stop, the ledger
usage and the `tenant` field.
