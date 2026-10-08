//! The two activities on the engine path: the `#[activity]` handlers read the
//! harness from worker state, as `HarvestBuilder::state` installs it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::context::{ActivityContext, SharedStateMap};
use autumn_harvest::failure::parse_typed_payload;
use autumn_harvest_agent::workflow::{agent_model_turn_info, agent_tool_call_info};
use autumn_harvest_agent::{
    AgentHarness, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome, TurnStop,
};
use autumn_plugin_agent::policy::{Rule, ToolRules};
use autumn_plugin_agent::{ChatMessage, ChatRole, TokenUsage, ToolCall, ToolDecision, ToolEffect};
use common::{Recorder, ScriptedModel, calls, recorded_tool};
use serde_json::json;

fn with_harness(harness: AgentHarness) -> ActivityContext {
    let mut state: SharedStateMap = HashMap::new();
    state.insert(TypeId::of::<AgentHarness>(), Box::new(harness));
    ActivityContext::new_test_with_state(Arc::new(state))
}

fn turn_request() -> ModelTurnRequest {
    ModelTurnRequest {
        run_id: "run-1".into(),
        session_id: None,
        steps_used: 0,
        max_steps: 4,
        usage: TokenUsage::default(),
        messages: vec![ChatMessage::text(ChatRole::User, "go")],
        max_output_tokens: Some(64),
    }
}

#[tokio::test]
async fn the_model_turn_handler_records_the_reply_and_each_decision() {
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![calls(
        &[("r", "read", json!({})), ("w", "write", json!({}))],
        3,
    )]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("read", ToolEffect::ReadOnly, &recorder))
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .temperature(0.5);
    let ctx = with_harness(harness);

    let info = agent_model_turn_info();
    let raw = (info.handler)(&ctx, serde_json::to_value(turn_request()).unwrap())
        .await
        .unwrap();
    let turn: ModelTurn = serde_json::from_value(raw).unwrap();

    assert_eq!(turn.stop, TurnStop::ToolUse);
    assert_eq!(turn.usage.input_tokens, 3);
    assert_eq!(turn.calls().len(), 2);
    assert_eq!(turn.decisions[0], ToolDecision::Allow);
    assert!(matches!(
        turn.decisions[1],
        ToolDecision::RequireApproval { .. }
    ));
    let sent = &model.requests()[0];
    assert_eq!(sent.max_tokens, Some(64));
    assert_eq!(sent.temperature, Some(0.5));
    assert_eq!(sent.tools.len(), 2);
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
}

#[tokio::test]
async fn the_tool_handler_runs_the_named_tool_with_its_context() {
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new())).tool(recorded_tool(
        "read",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let ctx = with_harness(harness);
    let request = ToolCallRequest {
        run_id: "run-1".into(),
        session_id: None,
        step: 0,
        call: ToolCall {
            id: "c1".into(),
            name: "read".into(),
            arguments: json!({"k": 1}),
        },
    };

    let info = agent_tool_call_info();
    let raw = (info.handler)(&ctx, serde_json::to_value(request).unwrap())
        .await
        .unwrap();
    let outcome: ToolOutcome = serde_json::from_value(raw).unwrap();

    assert!(!outcome.is_error);
    assert_eq!(recorder.runs(), vec![json!({"k": 1})]);
}

#[tokio::test]
async fn a_worker_without_a_harness_fails_the_call_and_names_the_fix() {
    let ctx = ActivityContext::new_test();
    let info = agent_model_turn_info();
    let err = (info.handler)(&ctx, serde_json::to_value(turn_request()).unwrap())
        .await
        .unwrap_err();
    let failure = parse_typed_payload(&err).expect("a typed failure");
    assert!(failure.non_retryable, "a retry cannot install the harness");
    assert_eq!(failure.error_type, "AgentHarnessMissing");
    assert!(failure.message.contains("HarvestBuilder::state"));
}

#[test]
fn the_activities_declare_their_retry_and_timeout() {
    let model = agent_model_turn_info();
    assert_eq!(model.name, "agent_model_turn");
    assert!(model.default_start_to_close.is_some());
    assert_eq!(model.default_retry_policy.unwrap().max_attempts, 4);
    let tool = agent_tool_call_info();
    assert_eq!(tool.name, "agent_tool_call");
    assert_eq!(tool.default_retry_policy.unwrap().max_attempts, 1);
}

#[tokio::test]
async fn a_model_call_over_its_budget_is_a_retryable_failure() {
    #[derive(Debug)]
    struct Stalled;
    impl autumn_plugin_agent::LlmClient for Stalled {
        fn chat<'a>(
            &'a self,
            _request: &'a autumn_plugin_agent::ChatRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            autumn_plugin_agent::ChatResponse,
                            autumn_plugin_agent::AgentError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(std::future::pending())
        }
        fn list_models(
            &self,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Vec<String>, autumn_plugin_agent::AgentError>,
                    > + Send
                    + '_,
            >,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn provider_name(&self) -> &'static str {
            "stalled"
        }
    }

    let harness =
        AgentHarness::new(Arc::new(Stalled)).model_timeout(std::time::Duration::from_millis(20));
    let ctx = with_harness(harness);
    let err =
        (agent_model_turn_info().handler)(&ctx, serde_json::to_value(turn_request()).unwrap())
            .await
            .unwrap_err();
    let failure = parse_typed_payload(&err).expect("a typed failure");
    assert_eq!(failure.error_type, "ModelTimeout");
    assert!(
        !failure.non_retryable,
        "a slow provider may answer next time"
    );
}

#[test]
fn the_engine_registration_builds() {
    use autumn_harvest::builder::HarvestBuilder;
    use autumn_harvest_agent::{activities, workflows};

    let built = HarvestBuilder::new()
        .workflows(workflows())
        .activities(activities())
        .state(AgentHarness::new(ScriptedModel::new(Vec::new())))
        .build();
    assert_eq!(built.workflow_count(), 1);
    assert!(built.state::<AgentHarness>().is_some());
    let names: Vec<&str> = activities().iter().map(|a| a.name).collect();
    assert_eq!(names, ["agent_model_turn", "agent_tool_call"]);
    assert_eq!(workflows()[0].name, autumn_harvest_agent::WORKFLOW_NAME);
}

#[tokio::test]
async fn a_huge_tool_error_still_fits_the_result_cap() {
    use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
    use autumn_plugin_agent::{AgentError, ErrorKind, FnTool};

    let failing = FnTool::new(
        "fail",
        "Fails with a huge message.",
        json!({"type": "object"}),
        |_input| async {
            // Control characters and quotes escape to the most JSON bytes.
            let message = "\u{0}\"".repeat(2 * 1024 * 1024);
            Err::<serde_json::Value, _>(AgentError::new(ErrorKind::Tool, message))
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new()))
        .tool(failing)
        .tool_output_limit(usize::MAX);
    let outcome = harness
        .tool_call(ToolCallRequest {
            run_id: "run-1".into(),
            session_id: None,
            step: 0,
            call: ToolCall {
                id: "c".into(),
                name: "fail".into(),
                arguments: json!({}),
            },
        })
        .await
        .unwrap();

    assert!(outcome.is_error);
    let size = serde_json::to_vec(&outcome).unwrap().len() as u64;
    assert!(size <= DEFAULT_MAX_ACTIVITY_RESULT_BYTES, "{size}");
    let body: serde_json::Value = serde_json::from_str(&outcome.content).unwrap();
    assert!(body["error"].as_str().unwrap().ends_with("…[truncated]"));
}

#[tokio::test]
async fn a_stalled_policy_denies_the_call_instead_of_hanging_the_turn() {
    #[derive(Debug)]
    struct Stalled;
    impl autumn_plugin_agent::ToolPolicy for Stalled {
        fn decide<'a>(
            &'a self,
            _call: &'a ToolCall,
            _tool: Option<&'a dyn autumn_plugin_agent::Tool>,
            _info: &'a autumn_plugin_agent::hooks::RunInfo,
        ) -> futures::future::BoxFuture<'a, ToolDecision> {
            Box::pin(std::future::pending())
        }
    }

    let model = ScriptedModel::new(vec![calls(&[("c", "read", json!({}))], 1)]);
    let harness = AgentHarness::new(model)
        .policy(Arc::new(Stalled))
        .policy_timeout(std::time::Duration::from_millis(20));
    let turn = harness.model_turn(turn_request()).await.unwrap();

    assert!(
        matches!(&turn.decisions[0], ToolDecision::Deny { reason } if reason.contains("did not decide")),
        "{:?}",
        turn.decisions
    );
}
