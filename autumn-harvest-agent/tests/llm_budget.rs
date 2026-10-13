//! The loop and the LLM budgets of issue #1997.
//!
//! The engine refuses a model turn with the non-retryable failure type
//! `LlmBudgetExceeded`. The loop must end under `AgentStop::BudgetExceeded`,
//! not fail the run. Every other failure still fails the run.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use autumn_harvest::failure::IntoActivityErrorString as _;
use autumn_harvest::llm_budget::LlmBudgetExceeded;
use autumn_harvest::quota::QuotaResource;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{AgentReport, AgentStop, AgentTask};

fn refusal() -> String {
    LlmBudgetExceeded {
        resource: QuotaResource::TenantLlmTokens,
        limit: 100,
        current: 120,
    }
    .into_failure()
    .into_error_payload()
}

#[tokio::test]
async fn a_refused_model_turn_ends_the_run_under_budget_exceeded() {
    let outcome = WorkflowTestEnv::new()
        // This mock parses a typed failure, as the worker does.
        .mock_activity_retries("agent_model_turn", vec![Err(refusal())])
        .run(
            agent_loop_info().handler,
            serde_json::to_value(AgentTask::new("go").tenant("acme")).unwrap(),
        )
        .await;
    let report: AgentReport = serde_json::from_value(outcome.result.unwrap()).unwrap();
    assert_eq!(report.stop, AgentStop::BudgetExceeded);
    assert_eq!(report.steps_used, 0);
}

#[tokio::test]
async fn another_model_turn_failure_still_fails_the_run() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "agent_model_turn",
            vec![Err(
                autumn_harvest::failure::ActivityFailure::non_retryable("Provider", "boom")
                    .into_error_payload(),
            )],
        )
        .run(
            agent_loop_info().handler,
            serde_json::to_value(AgentTask::new("go")).unwrap(),
        )
        .await;
    assert!(outcome.result.is_err(), "{:?}", outcome.result);
}
