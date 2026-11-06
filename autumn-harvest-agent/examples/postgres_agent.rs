//! An agent run and a durable agent on Postgres, with no API key.
//!
//! Part one is the agent loop. The run reads a note, asks to write one, and
//! waits for approval. The first worker then stops, as a crashed process
//! does. A second worker, with a fresh model, takes the approval and
//! finishes. The recorded model turn is not sent again.
//!
//! Part two is a durable agent, `support/acct-42`. It gets two messages. A
//! retry of the first message runs no second op. A later run of the example
//! finds the same agent, with its transcript, and adds two more ops.
//!
//! ```sh
//! export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest
//! cargo run -p autumn-harvest-agent --features postgres --example postgres_agent
//! ```
//!
//! The example applies the engine migrations to that database first. Point
//! it at a database that you can change.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{HarvestBuilder, WorkerConfig, migrate};
use autumn_harvest_agent::postgres::{self, PostgresAgents, RunState};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentReport, AgentTask,
    Approval, ChatRequest, ChatResponse, ContentPart, FnTool, Rule, StopReason, TokenUsage,
    ToolEffect, ToolRules, approval,
};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};

/// How long the example waits for a worker.
const LIMIT: Duration = Duration::from_secs(30);

/// A stand-in model. A run with no tool result asks for two tools. A run
/// with a result answers. A durable agent message gets a short answer.
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
        let asks_for_a_note = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::Text(text) if text == "Save a note."))
        });
        let response = if has_results {
            text("The note is saved.", 20)
        } else if asks_for_a_note {
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
        } else {
            text("Your order ships tomorrow.", 8)
        };
        Box::pin(async move { Ok(response) })
    }
}

/// A final answer.
fn text(answer: &str, tokens: u32) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(answer.into())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(tokens, 5),
    }
}

/// The model, two tools, a policy that asks before any write, and the
/// profile of the durable support agent.
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
        .agent(AgentProfile::new("support").system("You answer support mail."))
}

/// A pool of connections to `url`.
fn pool(url: &str) -> Result<DbPool, Box<dyn std::error::Error>> {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    Ok(deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()?)
}

/// A running worker. Dropping it stops the worker.
struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.worker.shutdown();
        self.handle.abort();
    }
}

/// Start a worker of the agent workflows with `harness`.
///
/// The agent activities name no queue, so they go to `default`.
fn run_worker(pool: DbPool, harness: AgentHarness) -> Result<Running, Box<dyn std::error::Error>> {
    let config = WorkerConfig::default().with_queues(["default"]);
    let (registry, _dags, _schedules, config) = postgres::register(HarvestBuilder::new(), harness)
        .worker(config)
        .build()
        .into_worker_parts();
    let mut runtime = WorkerRuntimeConfig::from(config);
    runtime.poll_interval = Duration::from_millis(50);
    let worker = Arc::new(Worker::new(runtime, Arc::new(registry))?);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        async move { worker.run(&pool).await }
    });
    Ok(Running { worker, handle })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "set DATABASE_URL to a Postgres database that the example may migrate")?;
    let mut conn = AsyncPgConnection::establish(&url).await?;
    migrate::apply_to_connection(&mut conn, &migrate::embedded()).await?;
    drop(conn);
    let pg = PostgresAgents::single(pool(&url)?);

    // Part one, worker one: start the run and let it reach the approval wait.
    let first = Arc::new(OfflineModel::default());
    let worker = run_worker(pool(&url)?, harness(Arc::clone(&first)))?;
    let exec = postgres::start(&pg, &AgentTask::new("Save a note.")).await?;
    let RunState::WaitingSignal(signal) = postgres::wait_run(&pg, exec, LIMIT).await? else {
        return Err("expected an approval wait".into());
    };
    println!(
        "waiting on {signal} for call {:?}",
        approval::approval_call_id(&signal)
    );
    drop(worker);

    // Part one, worker two: a fresh process takes the approval and finishes.
    let second = Arc::new(OfflineModel::default());
    let worker = run_worker(pool(&url)?, harness(Arc::clone(&second)))?;
    postgres::decide(&pg, exec, &signal, &Approval::Approve).await?;
    let RunState::Completed(value) = postgres::wait_run(&pg, exec, LIMIT).await? else {
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

    // Part two: a durable agent. A retry of the first message delivers it
    // once. The ids name this run, so a second run of the example sends new
    // messages to the same agent.
    let id = AgentId::new("support", "acct-42")?;
    let before = postgres::state(&pg, &id)
        .await?
        .map_or(0, |state| state.segments);
    let run = exec.to_string();
    let (m1, m2) = (format!("{run}:m-1"), format!("{run}:m-2"));
    let order = AgentOp::message_with_id("Where is my order?", m1.clone());
    postgres::send(&pg, &id, &order, Some(&m1)).await?;
    postgres::send(&pg, &id, &order, Some(&m1)).await?;
    let invoice = AgentOp::message_with_id("Can I get the invoice?", m2.clone());
    postgres::send(&pg, &id, &invoice, Some(&m2)).await?;
    let state = postgres::wait_for(&pg, &id, LIMIT, |state| state.segments >= before + 2)
        .await?
        .ok_or("the agent did not answer in time")?;
    println!(
        "agent {id}: {} ops, {} tokens, last answer {:?}",
        state.segments, state.tokens_used, state.last_answer
    );
    drop(worker);
    Ok(())
}
