//! An agent run on SQLite, end to end, with no API key and no network.
//!
//! The run reads a note, asks to write one, and waits for approval. The
//! process then "restarts": it drops the runtime and opens the file again.
//! The approval goes to the new runtime, and the run finishes. The recorded
//! model turn is not sent again.
//!
//! ```sh
//! cargo run -p autumn-harvest-agent --features sqlite --example sqlite_agent
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::{
    AgentError, AgentModel, Approval, ChatRequest, ChatResponse, ContentPart, FnTool, StopReason,
    TokenUsage, ToolEffect,
};
use autumn_harvest_agent::{AgentHarness, AgentReport, AgentTask, approval, sqlite};
use autumn_harvest_agent::{Rule, ToolRules};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::{Value, json};

/// A stand-in model. Turn one asks for two tools. Turn two answers.
#[derive(Debug, Default)]
struct OfflineModel {
    calls: AtomicUsize,
}

impl AgentModel for OfflineModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let has_results = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::ToolResult { .. }))
        });
        let response = if has_results {
            ChatResponse {
                content: vec![ContentPart::Text("The note is saved.".into())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(20, 5),
            }
        } else {
            ChatResponse {
                content: vec![
                    ContentPart::ToolCall {
                        id: "read_1".into(),
                        name: "read_note".into(),
                        arguments: json!({}),
                    },
                    ContentPart::ToolCall {
                        id: "write_1".into(),
                        name: "write_note".into(),
                        arguments: json!({"text": "remember the milk"}),
                    },
                ],
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::new(10, 5),
            }
        };
        Box::pin(async move { Ok(response) })
    }
}

/// The model, two tools, and a policy that asks before any write.
fn harness(model: Arc<OfflineModel>) -> AgentHarness {
    let read = FnTool::new(
        "read_note",
        "Read the current note.",
        json!({"type": "object"}),
        |_input: Value| async { Ok(json!({"note": "empty"})) },
    )
    .effect(ToolEffect::ReadOnly);
    let write = FnTool::new(
        "write_note",
        "Replace the note.",
        json!({"type": "object", "properties": {"text": {"type": "string"}}}),
        |input: Value| async move {
            println!("  write_note ran with {input}");
            Ok(json!({"saved": true}))
        },
    )
    .effect(ToolEffect::Write);
    AgentHarness::new(model)
        .tool(read.shared())
        .tool(write.shared())
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::temp_dir().join(format!("sqlite-agent-{}.db", std::process::id()));

    // Process one: start the run and drive it to the approval wait.
    let first = Arc::new(OfflineModel::default());
    let (exec, signal) = {
        let mut rt = SqliteRuntime::open(&db)?;
        sqlite::register(&mut rt, Arc::new(harness(Arc::clone(&first))));
        let exec = sqlite::start(&mut rt, &AgentTask::new("Save a note."))?;
        let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await? else {
            return Err("expected an approval wait".into());
        };
        println!(
            "waiting on {signal} for call {:?}",
            approval::approval_call_id(&signal)
        );
        (exec, signal)
    };

    // Process two: open the same file, approve, and finish.
    let second = Arc::new(OfflineModel::default());
    let mut rt = SqliteRuntime::open(&db)?;
    sqlite::register(&mut rt, Arc::new(harness(Arc::clone(&second))));
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve)?;
    let RunState::Completed(value) = rt.run_until_blocked(exec).await? else {
        return Err("expected a completed run".into());
    };
    let report: AgentReport = serde_json::from_value(value)?;

    println!("stop:   {:?}", report.stop);
    println!("answer: {}", report.text);
    println!(
        "model calls: {} before the restart, {} after it",
        first.calls.load(Ordering::SeqCst),
        second.calls.load(Ordering::SeqCst)
    );
    drop(rt);
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    Ok(())
}
