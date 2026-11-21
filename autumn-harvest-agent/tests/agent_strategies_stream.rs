//! A streaming durable agent publishes the selected answer of a strategy
//! op on its own durable stream.
//!
//! The draws are child runs, so their turns go to their own streams. The
//! op publishes the answer that the run selected, once, on the stream of
//! the agent. The simulation engine records the durable chunks, so the
//! test reads them directly.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;

use autumn_harvest::entity::{ENTITY_OP_SIGNAL, EntityMessage};
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest_agent::sampling::{
    OpSample, PolicyRead, SampleStrategy, SamplingPolicy, ScorerRef, Strategy,
};
use autumn_harvest_agent::{
    AgentOp, AgentProfile, AgentTask, TokenUsage, TurnText, Verdict, agent_info,
};
use serde_json::{Value, json};

/// The report of one draw: draw `n` answers `answers[n % len]`.
fn draw_report(input: &Value) -> Value {
    let task: AgentTask = serde_json::from_value(input.clone()).unwrap();
    let n = task.draw.map_or(0, |draw| draw.id.get());
    let text = ["a", "b", "c"][n as usize % 3];
    json!({
        "stop": "completed",
        "text": text,
        "steps_used": 0,
        "tool_calls": 0,
        "usage": TokenUsage::new(2, 3),
        "messages": [
            {"role": "user", "content": [{"text": task.input}]},
            {"role": "assistant", "content": [{"text": text}]},
        ],
        "run_id": format!("draw-{n}"),
    })
}

/// The score of one answer: `b` wins.
fn score(input: &Value) -> Value {
    let answer = input["input"]["answer"].as_str().unwrap_or_default();
    let value = match answer {
        "b" => 0.9,
        "c" => 0.5,
        _ => 0.2,
    };
    json!({"verdict": Verdict::score(value).unwrap()})
}

fn env(stream: bool) -> WorkflowTestEnv {
    let profile = if stream {
        AgentProfile::new("desk").stream()
    } else {
        AgentProfile::new("desk")
    };
    let policy = PolicyRead {
        policy: SamplingPolicy {
            strategies: BTreeSet::from([Strategy::BestOfN]),
            max_draws: 3,
            ..SamplingPolicy::default()
        },
        revision: 1,
    };
    let sample = OpSample::new(
        SampleStrategy::best_of_n(3),
        ScorerRef::deterministic("table").unwrap(),
    );
    let op = AgentOp::message("Pick a letter.").with_sample(sample);
    WorkflowTestEnv::new()
        .with_workflow_id("desk/one")
        .queue_signal(ENTITY_OP_SIGNAL, json!(EntityMessage::op(op)))
        .queue_signal(ENTITY_OP_SIGNAL, json!(EntityMessage::<AgentOp>::delete()))
        .mock_activity("agent_profile", move |_| {
            Ok(serde_json::to_value(&profile).unwrap())
        })
        .mock_activity("agent_sampling_policy", move |_| {
            Ok(serde_json::to_value(&policy).unwrap())
        })
        .mock_activity("agent_sample_score", |input| Ok(score(&input)))
        .mock_child_workflow("agent_loop", |input| Ok(draw_report(&input)))
}

fn turn_texts(chunks: &[autumn_harvest::testing::RecordedDurableChunk]) -> Vec<TurnText> {
    chunks
        .iter()
        .map(|chunk| serde_json::from_value(chunk.chunk.clone()).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streaming_agent_publishes_the_selected_answer_once() {
    let outcome = env(true).run(agent_info().handler, json!({})).await;
    assert!(outcome.result.is_ok(), "{:?}", outcome.result);

    let texts = turn_texts(outcome.recorded_durable_progress());
    let shown: Vec<&str> = texts.iter().map(|text| text.text.as_str()).collect();
    assert_eq!(shown, ["b"], "only the selected answer, once");
    assert_eq!(texts[0].kind, "turn");
    assert!(texts[0].run_id.ends_with(":s0"), "{}", texts[0].run_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_that_does_not_stream_publishes_nothing() {
    let outcome = env(false).run(agent_info().handler, json!({})).await;
    assert!(outcome.result.is_ok(), "{:?}", outcome.result);
    let chunks = outcome.recorded_durable_progress().len();
    assert_eq!(
        chunks, 0,
        "an agent that does not stream publishes no chunk"
    );
}
