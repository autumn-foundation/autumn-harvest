# Design — Issue #1997: token and cost budgets per run and per tenant

An agent loop can spend without limit. Issue #1997 asks for per-run and
per-tenant budgets on LLM steps. A step that would pass its budget must fail
or park with a typed error. The budgets plug into the quota machinery of
issue #946.

**One migration: a new side table. No new `WorkflowEvent` variant. No
replay change. No change to the claim or dispatch path.**

---

## 0. Planning record

### 0.1 The dependency on the cost ledger (#1996)

The budgets need a durable record of what each LLM step spent. That record is
the cost ledger of issue #1996, which is still open. This change ships the
part of the ledger that the budgets need:

- the `harvest_llm_ledger` side table, with clear columns for the model,
  the tokens, the cost and the latency;
- `ActivityContext::record_llm_usage`, which writes one row;
- the statement in `docs/security-posture.md` of which ledger fields are in
  clear.

Issue #1996 keeps the roll-up in the usage reports and its replay test.

### 0.2 Brainstorm — where can a budget live?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A gate in `process_activity_task` for activities that a flag marks as LLM steps. | Rejected. The flag needs a new `ActivityInfo` field, and over 200 struct literals build that type. The gate also adds a metadata lookup to every dispatch. |
| B2 | A check inside the workflow, from the recorded usage. | Rejected for tenants. A workflow cannot read the spend of other runs and stay deterministic. The agent loop already has the per-run form (`max_total_tokens`). |
| B3 | An `ActivityInterceptor` that the user registers. | Rejected. A budget that works only after an extra registration is easy to lose. |
| B4 | Caps on `QuotaPolicy`, checked by `ActivityContext::check_llm_budget` at the start of an LLM step. | **Adopted.** It reuses the quota key resolver, the `quota_key` column, the violation rule and the quota metric. The step that records the usage is also the step that checks it. |
| B5 | A new `LlmBudgetPolicy` type on `WorkflowInfo`. | Rejected. It needs its own key and its own key column. `QuotaPolicy` already resolves and stores the tenant key. |
| B6 | Park the step until the budget refills. | Deferred. A park needs a wake when the window moves. That is a scheduler change. A typed failure is enough to stop the spend. |

### 0.3 Reverse brainstorm — how can a budget fail to stop the spend?

| # | How it fails | Mitigation |
|---|--------------|------------|
| R1 | Concurrent steps all read the spend before any of them records. | The budget is a soft cap. The last step that passes can pass the cap by its whole usage. Steps in flight can add to that. The docs say so. |
| R2 | A tenant budget with no window blocks the tenant for ever. | The tenant cap counts a rolling window. The default is one day. |
| R3 | A failed ledger write hides a spend. | `record_llm_usage` returns a `HarvestResult`, not an activity payload, so `?` cannot turn it into a retry. The `agent_model_turn` activity logs it and keeps the answer, because a retry would pay for the call again. |
| R4 | A retry or a resume re-runs a paid call and the ledger misses it. | Each recorded call writes its own row with its attempt number. A retry that pays again counts again. A call that fails or times out records nothing, because it returns no usage. The docs say so. |
| R5 | The step fails as retryable, so the retry policy runs it again. | The failure is non-retryable. The circuit breaker ignores non-retryable failures. |
| R6 | A tenant key from caller input aliases another tenant. | The key follows the quota rules: bounded length, scoped by workflow type, fail-open when it does not resolve. |
| R7 | The budget check costs every activity a query. | Only a step that calls `check_llm_budget` pays. With no LLM cap declared, the check reads no row. |
| R8 | Retention deletes a run, and the tenant spend drops. | The ledger cascades with its run. Keep retention longer than the window. The docs say so. |
| R9 | The activity runs in a process that does not register the workflow type, so it cannot read the caps. | The check passes and logs one warning for each type. The docs say to register the workflow on each worker that runs its LLM steps. |
| R10 | A shard rebalance moves a run without its ledger, so the run cap starts again. | The rebalance copies the ledger rows of the run, with their `recorded_at`. The cutover deletes the source copy, so the tenant spend does not count twice. A test proves both. |
| R11 | A huge recorded value overflows the sum, and every check fails as a read error. | The sums are `NUMERIC`, clamped to the `BIGINT` range. |
| R12 | A zero window counts nothing, so the tenant caps never refuse. | The macro rejects 0. The builder raises 0 to one second. |

### 0.4 Six hats

- **White (facts).** `QuotaPolicy` is `Copy` and built by `const` methods. `quota_key` is stored whenever a policy exists, with or without a cap. `ActivityContext` holds a pool on the worker path. The agent model returns `TokenUsage` but no model id and no cost.
- **Red (feel).** Users expect "per tenant" to mean every run of that tenant. A step that just fails, with no reason, feels broken.
- **Black (risks).** R1 to R8. Model ids and token counts are visible without the codec key.
- **Yellow (value).** The budget reuses one key, one error rule and one metric. Replay does not change, because no event changes.
- **Green (options).** Park mode (B6). Token-keyed limits (§0.5). A roll-up in `GET /admin/usage` (#1996).
- **Blue (process).** Red, green, refactor. Pure rules first, then Postgres end-to-end, then the agent adapter. Review by several agents before the PR.

### 0.5 The limiter question (#1836, #1922, #1935)

Assay #14 (#1922) recommends not to build #1836 as specified. Its Gradient2
arm failed because the baseline chased a slow dependency. #1935 then shipped
`adaptive_limit.rs` with a changed design: a probed-minimum baseline and an
error-share backoff. #1935 merged on 2026-10-06, after the assay.

**Decision.** `adaptive_limit.rs` is the current limiter design. This change
does no token-keyed limiter work. A token-keyed limit must build on
`adaptive_limit.rs`. It must first run the assay #14 apparatus against that
design, because no assay has graded it.

---

## 1. Design

### 1.1 Declaration

Four optional caps and one window on `QuotaPolicy`:

| Builder | Cap |
|---|---|
| `with_max_run_llm_tokens(n)` | Tokens of one run. |
| `with_max_run_llm_cost_micros(n)` | Cost of one run, in millionths of a currency unit. |
| `with_max_tenant_llm_tokens(n)` | Tokens of one key in the window. |
| `with_max_tenant_llm_cost_micros(n)` | Cost of one key in the window. |
| `with_tenant_llm_window_secs(s)` | The rolling window. Default 86,400 s. |

The `#[workflow(quota(...))]` macro takes the same names. `has_any_cap`
keeps its meaning: an admission cap. `has_llm_budget` reports an LLM cap.

### 1.2 The ledger

`harvest_llm_ledger` holds one row for each recorded call. A row holds:

- the run, the workflow type and the `quota_key`;
- the activity and the attempt;
- the model, the input and output tokens, the cost and the latency.

The row cascades with its run. A shard rebalance moves it.

### 1.3 The check

`ActivityContext::check_llm_budget` reads the policy of the run's workflow
type. With no LLM cap, it returns at once. Otherwise one query reads the run
spend and the tenant spend. A cap that the spend has reached fails the step
with the non-retryable error type `LlmBudgetExceeded`. The details name the
resource, the cap and the spend. The rule is the quota rule:
`current >= limit` refuses.

The check fails open when the key does not resolve, as admission does. A
read error fails the step as retryable.

`LlmBudgetExceeded::is_refusal` tests for the failure by its type.
`LlmBudgetExceeded::from_error` reads its details.

The check reads the policy in its own process. A worker that does not
register the workflow type passes the check and logs one warning.

### 1.4 The agent adapter

- `AgentModel` gets two provided methods: `model_id` and `cost_micros`.
- `agent_model_turn` checks the budget, calls the model, then records the
  usage with the latency.
- `AgentTask` gets an optional `tenant`, for `QuotaPolicy::new("tenant")`.
- The loop ends under `AgentStop::BudgetExceeded` when a step is refused.

---

## 2. Test plan (red, then green)

| AC | Test |
|---|---|
| Per-run budget stops further LLM steps | `llm_budget_tests::a_run_budget_stops_further_llm_steps_once_exceeded` |
| Per-tenant budget, across runs | `llm_budget_tests::a_tenant_budget_stops_llm_steps_across_runs` |
| Cost (dollar) budget | `llm_budget_tests::a_cost_budget_stops_llm_steps` |
| Window | `llm_budget_tests::spend_outside_the_window_does_not_count` |
| Ledger in clear | `llm_budget_tests::the_ledger_row_holds_the_usage_in_clear_columns` |
| A retry records again | `llm_budget_tests::a_retry_that_pays_again_records_again_and_counts` |
| No policy, no effect | `llm_budget_tests::a_run_with_no_llm_cap_is_never_refused` |
| Rebalance keeps the spend | `shard_rebalance_db_tests::the_llm_ledger_moves_with_the_run` |
| Pure rules | `quota::tests` and `llm_budget::tests` |
| Macro keys | `macros_workflow` and the `quota_llm_zero_window` compile-fail case |
| Agent order | `harness::tests` on `metered_turn` |
| Agent stop | `autumn-harvest-agent/tests/llm_budget.rs` |
