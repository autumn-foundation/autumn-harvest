//! Delta encoding of agent context windows: the measurement (issue #2009).
//!
//! Each model turn records the whole transcript in its activity input. So
//! a run's history grows with the square of its turn count. This test
//! builds a 40-turn agent loop from the real `ModelTurnRequest` and
//! `ModelTurn` types and measures four stored forms:
//!
//! - plain JSON, as the identity codec stores it;
//! - plain JSON with each transcript stored as a delta on the turn before;
//! - plain JSON, gzip-compressed, as a compressing archive backend stores it;
//! - JSON under the AES-256-GCM codec.
//!
//! It also measures the longest shared prefix of two encoded transcripts.
//! A delta over stored bytes can save only that much.
//!
//! Run with `-- --nocapture` to print the table that
//! `docs/rnd/2026-10-11-agent-history-delta-encoding.md` records.

use std::io::Write as _;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::types::ActivityExecId;
use autumn_harvest_agent::{
    ChatMessage, ChatRole, ContentPart, ModelTurn, ModelTurnRequest, StopReason, TokenUsage,
    ToolCall,
};

const TURNS: usize = 40;
const TOOL_RESULT_WORDS: usize = 220;

/// Deterministic filler text, so every run measures the same bytes.
fn words(seed: u64, n: usize) -> String {
    const WORDS: [&str; 16] = [
        "harvest", "cohort", "replay", "ledger", "tenant", "signal", "worker", "fence", "payload",
        "codec", "shard", "commit", "event", "history", "queue", "timer",
    ];
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut out = String::new();
    for i in 0..n {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        if i > 0 {
            out.push(' ');
        }
        out.push_str(WORDS[(state >> 60) as usize]);
    }
    out
}

/// The events of one agent loop: model turn, tool call, model turn, ...
fn agent_loop_history() -> Vec<WorkflowEvent> {
    let mut messages = vec![
        ChatMessage::text(ChatRole::System, words(1, 120)),
        ChatMessage::text(ChatRole::User, words(2, 60)),
    ];
    let mut events = Vec::new();
    for turn in 0..TURNS {
        let request = ModelTurnRequest {
            run_id: "run-2009".into(),
            session_id: None,
            steps_used: u32::try_from(turn).unwrap(),
            max_steps: 64,
            usage: TokenUsage::default(),
            messages: messages.clone(),
            max_output_tokens: Some(4096),
            read_only: false,
            memory_scope: None,
            extra_tools: Vec::new(),
        };
        let call = ToolCall {
            id: format!("call-{turn}"),
            name: "search".into(),
            arguments: serde_json::json!({"query": words(100 + turn as u64, 8)}),
        };
        let model_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: model_id,
            name: "agent_model_turn".into(),
            input: serde_json::to_value(&request).unwrap(),
            queue: "agent".into(),
        });
        let reply = ModelTurn {
            content: vec![
                ContentPart::Text(words(200 + turn as u64, 30)),
                ContentPart::ToolCall {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                },
            ],
            stop: StopReason::ToolUse,
            usage: TokenUsage::default(),
            decisions: Vec::new(),
        };
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: model_id,
            output: serde_json::to_value(&reply).unwrap(),
        });
        let result = words(300 + turn as u64, TOOL_RESULT_WORDS);
        let tool_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: tool_id,
            name: "agent_tool_call".into(),
            input: serde_json::json!({"run_id": "run-2009", "step": turn, "call": call}),
            queue: "agent".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: tool_id,
            output: serde_json::json!({"content": result}),
        });
        messages.push(ChatMessage {
            role: ChatRole::Assistant,
            content: reply.content.clone(),
        });
        messages.push(ChatMessage {
            role: ChatRole::Tool,
            content: vec![ContentPart::ToolResult {
                tool_call_id: call.id.clone(),
                content: result,
            }],
        });
    }
    events
}

/// Replace each model-turn transcript with the messages it adds to the
/// transcript of the turn before.
fn delta_encode(events: &[WorkflowEvent]) -> Vec<serde_json::Value> {
    let mut prev: Vec<serde_json::Value> = Vec::new();
    events
        .iter()
        .map(|event| {
            let mut value = serde_json::to_value(event).unwrap();
            let Some(input) = value.pointer_mut("/data/input") else {
                return value;
            };
            let Some(serde_json::Value::Array(messages)) = input.get("messages").cloned() else {
                return value;
            };
            let shared = prev
                .iter()
                .zip(&messages)
                .take_while(|(a, b)| a == b)
                .count();
            input["messages"] =
                serde_json::json!({"base": shared, "append": messages[shared..].to_vec()});
            prev = messages;
            value
        })
        .collect()
}

fn jsonl(values: &[serde_json::Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        out.extend(serde_json::to_vec(value).unwrap());
        out.push(b'\n');
    }
    out
}

fn gzip(bytes: &[u8]) -> usize {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap().len()
}

fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn pct(part: usize, whole: usize) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let ratio = part as f64 / whole as f64;
    100.0 * ratio
}

#[test]
fn delta_encoding_shrinks_plaintext_but_not_ciphertext() {
    let events = agent_loop_history();
    let plain: Vec<serde_json::Value> = events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect();
    let raw = jsonl(&plain);
    let delta = jsonl(&delta_encode(&events));

    let codecs = PayloadCodecs::default();
    AeadCodec::new("delta-k1", &DataKey::from_bytes(&[0x42; 32]).unwrap())
        .unwrap()
        .register_with(&codecs)
        .unwrap();
    let sealed: Vec<serde_json::Value> = events
        .iter()
        .map(|e| codecs.encode_event(e).unwrap())
        .collect();
    let encrypted = jsonl(&sealed);

    // The two last model-turn inputs share all but one round of messages.
    let inputs: Vec<Vec<u8>> = sealed
        .iter()
        .filter(|v| v.pointer("/data/name") == Some(&serde_json::json!("agent_model_turn")))
        .map(|v| serde_json::to_vec(&v["data"]["input"]).unwrap())
        .collect();
    let n = inputs.len();
    let cipher_shared = shared_prefix(&inputs[n - 2], &inputs[n - 1]);

    let (raw_len, delta_len, enc_len) = (raw.len(), delta.len(), encrypted.len());
    let (raw_gz, enc_gz) = (gzip(&raw), gzip(&encrypted));
    println!("| Form | Bytes | Share of plain |");
    println!("|---|---:|---:|");
    println!("| Plain JSON | {raw_len} | 100.0% |");
    println!(
        "| Plain, delta | {delta_len} | {:.1}% |",
        pct(delta_len, raw_len)
    );
    println!("| Plain, gzip | {raw_gz} | {:.1}% |", pct(raw_gz, raw_len));
    println!(
        "| AES-GCM codec | {enc_len} | {:.1}% |",
        pct(enc_len, raw_len)
    );
    println!(
        "| AES-GCM, gzip | {enc_gz} | {:.1}% |",
        pct(enc_gz, raw_len)
    );
    println!("Shared prefix of the last two encoded transcripts: {cipher_shared} bytes");

    assert!(
        delta_len * 10 < raw_len,
        "on plaintext the delta removes over 90%: {delta_len} of {raw_len}"
    );
    assert!(
        enc_len > raw_len,
        "the codec adds bytes, it removes none: {enc_len} of {raw_len}"
    );
    assert!(
        cipher_shared < 64,
        "two encoded transcripts share only the envelope header: {cipher_shared} bytes"
    );
    assert!(
        enc_gz * 2 > enc_len,
        "ciphertext does not compress by half: {enc_gz} of {enc_len}"
    );
}
