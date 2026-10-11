//! Measures the resident-state hit rate of the agent loop (issue #2007).
//!
//! The binary runs `autumn_harvest_agent::agent_loop` on one Postgres
//! worker with sticky routing and resident workflows on. An offline model
//! asks for one tool call per turn, then answers. The binary needs no API
//! key and no network. At the end the binary prints the resident hits and the
//! resident misses by reason.
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/agent_hit_rate \
//!   cargo run -p agent-loop-hit-rate --release
//! ```
//!
//! `RUNS` sets the number of runs (default 20). `TURNS` sets the tool turns
//! of each run (default 4). The binary applies the Harvest migrations first.
//! Use an empty database: the worker also polls the `default` queue, where
//! the agent activities run.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::migrate;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpPropagator, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{HarvestBuilder, StartWorkflowParams, WorkerConfig};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentTask, ChatRequest, ChatResponse, ContentPart,
    FnTool, StopReason, TokenUsage, ToolEffect,
};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The time that one batch of runs may take.
const RUN_TIMEOUT: Duration = Duration::from_secs(300);

/// A model that asks for one tool call per turn, `turns` times, then
/// answers.
#[derive(Debug)]
struct OfflineModel {
    turns: usize,
}

impl AgentModel for OfflineModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        let response = reply(self.turns, request);
        Box::pin(async move { Ok(response) })
    }
}

/// The reply to `request`. The tool results so far set the turn.
fn reply(turns: usize, request: &ChatRequest) -> ChatResponse {
    let results = request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter(|part| matches!(part, ContentPart::ToolResult { .. }))
        .count();
    let content = if results < turns {
        vec![ContentPart::ToolCall {
            id: format!("call_{results}"),
            name: "lookup".into(),
            arguments: json!({ "turn": results }),
        }]
    } else {
        vec![ContentPart::Text("done".into())]
    };
    let stop_reason = if results < turns {
        StopReason::ToolUse
    } else {
        StopReason::EndTurn
    };
    ChatResponse {
        content,
        stop_reason,
        usage: TokenUsage::new(10, 5),
    }
}

/// Counts decisions by outcome. Every other recorder method is a no-op.
#[derive(Debug, Default)]
struct Counts {
    /// `hit`, or the miss reason, to its count.
    resident: Mutex<BTreeMap<String, u64>>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl Counts {
    fn add(&self, key: &str) {
        *self
            .resident
            .lock()
            .expect("counts lock")
            .entry(key.to_owned())
            .or_default() += 1;
    }
}

impl MetricsRecorder for Counts {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_workflow_resident_hit(&self, _workflow_name: &str, _queue: &str) {
        self.add("hit");
    }

    fn record_workflow_resident_miss(&self, _workflow_name: &str, _queue: &str, reason: &str) {
        self.add(reason);
    }
}

/// Renders the counts as a Markdown table.
fn render(resident: &BTreeMap<String, u64>, cache_hits: u64, cache_misses: u64) -> String {
    let total: u64 = resident.values().sum();
    let mut out = String::from("| Outcome | Decisions | Share |\n|---|---:|---:|\n");
    let mut rows: Vec<(&String, &u64)> = resident.iter().collect();
    // The hit row comes first. The miss rows follow by count.
    rows.sort_by_key(|(key, n)| (key.as_str() != "hit", std::cmp::Reverse(**n)));
    for (key, n) in rows {
        let label = if key == "hit" {
            "resident hit".to_owned()
        } else {
            format!("miss: `{key}`")
        };
        out.push_str(&format!("| {label} | {n} | {} |\n", share(*n, total)));
    }
    out.push_str(&format!("| **total** | {total} | |\n\n"));
    out.push_str(&format!(
        "Cache: {cache_hits} hits, {cache_misses} misses.\n"
    ));
    out
}

/// `n` as a percentage of `total`.
fn share(n: u64, total: u64) -> String {
    if total == 0 {
        return "n/a".to_owned();
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "decision counts here are far below 2^53"
    )]
    let pct = n as f64 * 100.0 / total as f64;
    format!("{pct:.1}%")
}

/// Reads a count from the environment.
fn env_count(name: &str, default: usize) -> Result<usize, BoxError> {
    match std::env::var(name) {
        Ok(text) => Ok(text.parse()?),
        Err(_) => Ok(default),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?;
    let runs = env_count("RUNS", 20)?;
    let turns = env_count("TURNS", 4)?;

    let mut conn = AsyncPgConnection::establish(&url).await?;
    migrate::apply_to_connection(&mut conn, &migrate::embedded()).await?;
    let pool: DbPool = Pool::builder(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
        url.as_str(),
    ))
    .max_size(16)
    .build()?;

    // A new queue for each process, so rows of an earlier process stay out.
    let token = ExecutionId::new().to_string().replace('-', "");
    let queue = format!("ahr-{}", &token[..12]);
    let counts = Arc::new(Counts::default());
    let lookup = FnTool::new(
        "lookup",
        "Look up a value.",
        json!({ "type": "object" }),
        |input: Value| async move { Ok(json!({ "echo": input })) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    let harness = AgentHarness::new(Arc::new(OfflineModel { turns })).tool(lookup);
    let built = HarvestBuilder::new()
        .workflows(autumn_harvest_agent::workflows())
        .activities(autumn_harvest_agent::activities())
        .state(harness)
        .telemetry(TelemetryConfig {
            service_name: Arc::from("agent-loop-hit-rate"),
            propagator: Arc::new(NoOpPropagator),
            metrics: Arc::clone(&counts) as Arc<dyn MetricsRecorder>,
        })
        // The agent activities run on the `default` queue.
        .worker(WorkerConfig::default().with_queues([queue.as_str(), "default"]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime: WorkerRuntimeConfig = worker_config.into();
    runtime.worker_id = format!("{queue}-worker");
    runtime.poll_interval = Duration::from_millis(50);
    let worker = Arc::new(Worker::new(runtime, Arc::new(registry))?);
    let handle = {
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        tokio::spawn(async move { worker.run(&pool).await })
    };

    let task = serde_json::to_value(AgentTask::new("Look up each value, then answer."))?;
    let mut started = Vec::with_capacity(runs);
    for run in 0..runs {
        let exec_id = ExecutionId::new();
        let workflow_id = format!("{queue}-run-{run}");
        let params = StartWorkflowParams::new(
            autumn_harvest_agent::WORKFLOW_NAME,
            &workflow_id,
            exec_id,
            task.clone(),
            &queue,
        );
        autumn_harvest::start_or_load_workflow_execution(&mut conn, params, None).await?;
        started.push(exec_id);
    }

    let deadline = Instant::now() + RUN_TIMEOUT;
    let mut failed = 0_usize;
    for exec_id in started {
        loop {
            let execution = autumn_harvest::execution::load_execution(&mut conn, exec_id).await?;
            match execution.state.as_str() {
                "COMPLETED" => break,
                "FAILED" | "CANCELLED" | "TERMINATED" | "TIMED_OUT" => {
                    failed += 1;
                    break;
                }
                _ if Instant::now() > deadline => {
                    return Err(format!("run {exec_id} did not end in {RUN_TIMEOUT:?}").into());
                }
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let resident = counts.resident.lock().expect("counts lock").clone();
    println!("## agent_loop resident hit rate (issue #2007)\n");
    println!("{runs} runs, {turns} tool turns each, one worker.\n");
    print!(
        "{}",
        render(
            &resident,
            counts.cache_hits.load(Ordering::Relaxed),
            counts.cache_misses.load(Ordering::Relaxed),
        )
    );
    if failed > 0 {
        return Err(format!("{failed} runs did not complete").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest_agent::{ChatMessage, ChatRole};

    fn request(results: usize) -> ChatRequest {
        let mut messages = vec![ChatMessage::text(ChatRole::User, "go")];
        for n in 0..results {
            messages.push(ChatMessage {
                role: ChatRole::Tool,
                content: vec![ContentPart::ToolResult {
                    tool_call_id: format!("call_{n}"),
                    content: "{}".into(),
                }],
            });
        }
        ChatRequest {
            messages,
            tools: Vec::new(),
            max_tokens: None,
            temperature: None,
        }
    }

    #[test]
    fn the_model_calls_one_tool_per_turn_then_answers() {
        for results in 0..3 {
            let response = reply(3, &request(results));
            assert_eq!(response.stop_reason, StopReason::ToolUse, "turn {results}");
            assert_eq!(response.content.len(), 1, "one call per turn");
        }
        let last = reply(3, &request(3));
        assert_eq!(last.stop_reason, StopReason::EndTurn);
    }

    #[test]
    fn the_table_puts_the_hit_row_first_and_sums_every_row() {
        let counts = BTreeMap::from([
            ("cold".to_owned(), 1),
            ("hit".to_owned(), 3),
            ("delta".to_owned(), 0),
        ]);
        let table = render(&counts, 3, 1);
        let rows: Vec<&str> = table.lines().collect();
        assert_eq!(rows[2], "| resident hit | 3 | 75.0% |");
        assert_eq!(rows[3], "| miss: `cold` | 1 | 25.0% |");
        assert!(table.contains("| **total** | 4 | |"));
        assert!(table.contains("Cache: 3 hits, 1 misses."));
    }
}
