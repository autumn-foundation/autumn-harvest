//! Delta encoding of agent context windows: the measurement (issue #2009).
//!
//! Each model turn records the whole transcript in its activity input. So
//! a run's history grows with the square of its turn count. This test
//! builds a 40-turn agent loop from the real `ModelTurnRequest` and
//! `ModelTurn` types. It measures five stored forms, each with two kinds of
//! filler text:
//!
//! - plain JSON, as the identity codec stores it;
//! - plain JSON with each transcript stored as a delta on the turn before;
//! - plain JSON, gzip-compressed, as a compressing archive backend stores it;
//! - JSON under the AES-256-GCM codec, plain and gzip-compressed.
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

/// How the filler text is made.
#[derive(Clone, Copy)]
enum Filler {
    /// Words from a 16-word list. It compresses well, like repetitive logs.
    Vocabulary,
    /// Random lowercase words. Near the entropy of base64 or ids, so a
    /// compressor finds only the repeats.
    Random,
}

/// Deterministic filler text, so every run measures the same bytes.
fn words(filler: Filler, seed: u64, n: usize) -> String {
    const WORDS: [&str; 16] = [
        "harvest", "cohort", "replay", "ledger", "tenant", "signal", "worker", "fence", "payload",
        "codec", "shard", "commit", "event", "history", "queue", "timer",
    ];
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    };
    let mut out = String::new();
    for i in 0..n {
        if i > 0 {
            out.push(' ');
        }
        match filler {
            Filler::Vocabulary => out.push_str(WORDS[(next() % 16) as usize]),
            Filler::Random => {
                for _ in 0..(3 + next() % 6) {
                    out.push(char::from(b'a' + u8::try_from(next() % 26).unwrap()));
                }
            }
        }
    }
    out
}

/// The events of one agent loop: model turn, tool call, model turn, ...
fn agent_loop_history(filler: Filler) -> Vec<WorkflowEvent> {
    let mut messages = vec![
        ChatMessage::text(ChatRole::System, words(filler, 1, 120)),
        ChatMessage::text(ChatRole::User, words(filler, 2, 60)),
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
            arguments: serde_json::json!({"query": words(filler, 100 + turn as u64, 8)}),
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
                ContentPart::Text(words(filler, 200 + turn as u64, 30)),
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
        let result = words(filler, 300 + turn as u64, TOOL_RESULT_WORDS);
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

/// The sizes of one history in each stored form.
struct Sizes {
    raw: usize,
    delta: usize,
    raw_gz: usize,
    enc: usize,
    enc_gz: usize,
    /// The shared prefix of the last two encoded transcripts.
    cipher_shared: usize,
    /// The length of the last encoded transcript.
    cipher_len: usize,
}

fn measure(filler: Filler) -> Sizes {
    let events = agent_loop_history(filler);
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
    Sizes {
        raw: raw.len(),
        delta: delta.len(),
        raw_gz: gzip(&raw),
        enc: encrypted.len(),
        enc_gz: gzip(&encrypted),
        cipher_shared: shared_prefix(&inputs[n - 2], &inputs[n - 1]),
        cipher_len: inputs[n - 1].len(),
    }
}

#[test]
fn delta_encoding_shrinks_plaintext_but_not_ciphertext() {
    let vocab = measure(Filler::Vocabulary);
    let random = measure(Filler::Random);
    println!("| Form | Vocabulary text | Share | Random text | Share |");
    println!("|---|---:|---:|---:|---:|");
    let rows = [
        ("Plain JSON", vocab.raw, random.raw),
        ("Plain, delta", vocab.delta, random.delta),
        ("Plain, gzip", vocab.raw_gz, random.raw_gz),
        ("AES-GCM codec", vocab.enc, random.enc),
        ("AES-GCM, gzip", vocab.enc_gz, random.enc_gz),
    ];
    for (form, v, r) in rows {
        println!(
            "| {form} | {v} | {:.1}% | {r} | {:.1}% |",
            pct(v, vocab.raw),
            pct(r, random.raw)
        );
    }
    for (name, s) in [("vocabulary", &vocab), ("random", &random)] {
        println!(
            "{name}: the last two encoded transcripts share {} of {} bytes",
            s.cipher_shared, s.cipher_len
        );
    }

    for s in [&vocab, &random] {
        assert!(
            s.delta * 100 < s.raw * 15,
            "on plaintext the delta removes over 85%: {} of {}",
            s.delta,
            s.raw
        );
        assert!(
            s.enc > s.raw,
            "the codec adds bytes: {} of {}",
            s.enc,
            s.raw
        );
        assert!(
            s.cipher_shared * 100 < s.cipher_len,
            "two encoded transcripts share under 1%: {} of {}",
            s.cipher_shared,
            s.cipher_len
        );
        assert!(
            s.enc_gz * 2 > s.enc,
            "ciphertext does not compress by half: {} of {}",
            s.enc_gz,
            s.enc
        );
    }
    assert!(
        random.raw_gz > random.delta * 2,
        "on high-entropy text gzip misses repeats past its 32 KiB window: \
         gzip {} against delta {}",
        random.raw_gz,
        random.delta
    );
}
