## Phase 3.18 — LLM token and cost budgets per run and per tenant (issue #1997)

An agent loop can no longer spend without limit. `QuotaPolicy` takes five
new settings: `max_run_llm_tokens`, `max_run_llm_cost_micros`,
`max_tenant_llm_tokens`, `max_tenant_llm_cost_micros` and
`tenant_llm_window_secs` (one day by default). The `#[workflow(quota(...))]`
macro takes the same keys. The LLM caps do not change admission.

An LLM step calls `ActivityContext::check_llm_budget` before its model call
and `ActivityContext::record_llm_usage` after it. A spent cap refuses the
step with the non-retryable failure type `LlmBudgetExceeded`. The details
name the resource, the cap and the spend. `LlmBudgetExceeded::from_error`
reads it back. The quota metric `harvest.quota.rejected` counts it under four
new `QuotaResource` labels: `run_llm_tokens`, `run_llm_cost_micros`,
`tenant_llm_tokens` and `tenant_llm_cost_micros`.

The agent adapter checks and records on each model turn. `AgentModel` gets
the provided methods `model_id` and `cost_micros`. `AgentTask` gets an
optional `tenant`. A refused turn ends the run under the new stop
`budget_exceeded`.

This ships the part of the cost ledger (issue #1996) that the budgets need:
the `harvest_llm_ledger` side table, the write, and the security-posture
note on its clear columns. The usage-report roll-up stays with issue #1996.

The limiter question of issue #1997 is settled. `adaptive_limit.rs` (#1935)
is the current design. No token-keyed limit ships. See `DESIGN-1997.md`
§0.5.

**Migration.** `20261009050206_harvest_llm_ledger` adds the side table and
two indexes. No `WorkflowEvent` variant, no change to `harvest_events`, no
replay impact.

**Tests.** `llm_budget::tests` and two `quota::tests` cover the rule. The
Postgres suite `llm_budget_tests` covers a run cap, a tenant cap across runs,
a cost cap, the window, the ledger row, no cap, and an unresolvable key.
`macros_workflow` covers the macro keys. The agent tests cover the stop, the
ledger usage and the `tenant` field.
