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
    assert_eq!(turn.calls[0].decision, ToolDecision::Allow);
    assert!(matches!(
        turn.calls[1].decision,
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
