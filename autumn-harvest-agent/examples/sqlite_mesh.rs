//! A small agent mesh on SQLite, with no API key and no network.
//!
//! A planner agent holds a mandate that names one peer, a researcher. The
//! researcher has one read-only tool. The planner has one write tool, and a
//! person must approve each write.
//!
//! 1. The planner delegates a question to the researcher. The researcher
//!    reads a fact and answers.
//! 2. The planner delegates to the publisher. The roster has a publisher,
//!    but the mandate does not name it, so the delegation is refused.
//! 3. The planner asks to write the plan, and the run waits for approval.
//! 4. The process "restarts". It drops the runtime and opens the file again.
//!    The approval goes to the new runtime, and the run finishes.
//!
//! The example prints the delegations of the report at the end. It exits
//! non-zero when a claim fails. The claims are that one delegation runs, one
//! is refused, and the restart sends one model call and no tool call again.
//!
//! ```sh
//! cargo run -p autumn-harvest-agent --features sqlite --example sqlite_mesh
//! ```
//!
//! The guide is section 2 of `docs/agent-mesh.md`.

use std::error::Error;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentReport, AgentStop, AgentTask, Approval, ChatRequest,
    ChatResponse, ChatRole, ContentPart, DelegationRecord, FnTool, Mandate, Peer, Rule, StopReason,
    TokenUsage, ToolEffect, ToolRules, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::{Value, json};

/// The system prompt of the planner.
const PLANNER: &str = "You plan trips.";

/// The system prompt of the researcher. The model reads it to know its role.
const RESEARCHER: &str = "You research. Use lookup_fact.";

/// The fact that the read tool returns.
const FACT: &str = "The ferry leaves at 9.";

/// A stand-in model for each agent of the mesh.
///
/// It reads the system prompt to know the agent, and it counts the tool
/// results to know the step. It keeps no other state, so a new process
/// gives the same answers.
#[derive(Debug, Default)]
struct OfflineModel {
    calls: AtomicUsize,
}

/// The system text of a request.
fn system_of(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::System)
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// The tool results of a request, in order.
fn results_of(request: &ChatRequest) -> Vec<&str> {
    request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.as_str()),
            _ => None,
        })
        .collect()
}

/// The string field `key` of the JSON text `result`, or empty text.
fn field(result: &str, key: &str) -> String {
    serde_json::from_str::<Value>(result)
        .ok()
        .and_then(|value| value.get(key)?.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// A turn that calls one tool.
fn call(id: &str, name: &str, arguments: Value) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::ToolCall {
            id: id.into(),
            name: name.into(),
            arguments,
        }],
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(10, 5),
    }
}

/// A turn that gives the final answer.
fn answer(text: String) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(text)],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(20, 5),
    }
}

/// The researcher reads the fact, then answers with it.
fn researcher_turn(results: &[&str]) -> ChatResponse {
    results.first().map_or_else(
        || call("read_1", "lookup_fact", json!({"topic": "ferry"})),
        |result| answer(field(result, "fact")),
    )
}

/// The planner delegates twice, writes the plan, then answers.
///
/// A delegation result holds only the final answer of the child.
fn planner_turn(results: &[&str]) -> ChatResponse {
    match results {
        [] => call(
            "ask_1",
            DELEGATE_TOOL,
            json!({"agent": "researcher", "task": "When does the ferry leave?"}),
        ),
        [_] => call(
            "ask_2",
            DELEGATE_TOOL,
            json!({"agent": "publisher", "task": "Publish the plan."}),
        ),
        [research, _] => call(
            "write_1",
            "write_plan",
            json!({"text": field(research, "answer")}),
        ),
        _ => answer("The plan is saved. Publishing is not allowed.".into()),
    }
}

impl AgentModel for OfflineModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let results = results_of(request);
        let response = if system_of(request).contains(RESEARCHER) {
            researcher_turn(&results)
        } else {
            planner_turn(&results)
        };
        Box::pin(std::future::ready(Ok(response)))
    }
}

/// The count of runs of each tool, over both processes.
#[derive(Debug, Default)]
struct ToolRuns {
    lookups: AtomicUsize,
    writes: AtomicUsize,
}

/// The roster, the tools and the policy. Each process builds the same one.
fn harness(model: Arc<OfflineModel>, runs: &Arc<ToolRuns>) -> AgentHarness {
    let reads = Arc::clone(runs);
    let lookup = FnTool::new(
        "lookup_fact",
        "Look up one fact about a topic.",
        json!({"type": "object", "properties": {"topic": {"type": "string"}}}),
        move |_input: Value| {
            reads.lookups.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({"fact": FACT})) }
        },
    )
    .effect(ToolEffect::ReadOnly);
    let writes = Arc::clone(runs);
    let write = FnTool::new(
        "write_plan",
        "Save the trip plan.",
        json!({"type": "object", "properties": {"text": {"type": "string"}}}),
        move |_input: Value| {
            writes.writes.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({"saved": true})) }
        },
    )
    .effect(ToolEffect::Write);
    AgentHarness::new(model)
        .tool(lookup.shared())
        .tool(write.shared())
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .peer(
            Peer::new("researcher", "Finds facts.")
                .system(RESEARCHER)
                .tools(["lookup_fact"])
                .read_only()
                .max_steps(2),
        )
        .peer(Peer::new("publisher", "Publishes plans."))
}

/// The planner task. Its mandate names the researcher only.
fn task() -> AgentTask {
    AgentTask::new("Plan a day trip to the island.")
        .system(PLANNER)
        .mandate(Mandate::new("planner").peers(["researcher"]))
}

/// What one run of the example gives.
#[derive(Debug)]
struct Outcome {
    report: AgentReport,
    /// Model calls after the restart.
    after: usize,
    /// Runs of the read tool.
    lookups: usize,
    /// Runs of the write tool.
    writes: usize,
    /// The text that `main` prints.
    lines: Vec<String>,
}

/// The wire name of a stop, as the parent model reads it.
fn wire(stop: Option<AgentStop>) -> String {
    serde_json::to_value(stop)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "not run".to_owned())
}

/// One line of the delegation roll-up.
fn roll_up(record: &DelegationRecord) -> String {
    let chain = record.chain.join(" -> ");
    record.refusal.map_or_else(
        || {
            format!(
                "  {chain}: {}, {} tokens, {} tool call(s)",
                wire(record.stop),
                record.usage.total(),
                record.tool_calls
            )
        },
        |refusal| format!("  {chain}: refused, {}", refusal.code()),
    )
}

/// Run the mesh in two processes on the database file `db`.
async fn run(db: &Path) -> Result<Outcome, Box<dyn Error>> {
    let runs = Arc::new(ToolRuns::default());
    let mut lines = vec!["process 1: start the planner".to_owned()];

    // Process one: delegate, get a refusal, and stop at the approval wait.
    let first = Arc::new(OfflineModel::default());
    let (exec, signal) = {
        let mut rt = SqliteRuntime::open(db)?;
        sqlite::register(&mut rt, Arc::new(harness(Arc::clone(&first), &runs)));
        let exec = sqlite::start(&mut rt, &task())?;
        let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await? else {
            return Err("expected an approval wait".into());
        };
        lines.push(format!("  waiting on {signal}"));
        (exec, signal)
    };

    // Process two: open the same file, approve, and finish.
    lines.push("process 2: open the file again and approve".to_owned());
    let second = Arc::new(OfflineModel::default());
    let mut rt = SqliteRuntime::open(db)?;
    sqlite::register(&mut rt, Arc::new(harness(Arc::clone(&second), &runs)));
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve)?;
    let RunState::Completed(value) = rt.run_until_blocked(exec).await? else {
        return Err("expected a completed run".into());
    };
    let report: AgentReport = serde_json::from_value(value)?;

    let (before, after) = (
        first.calls.load(Ordering::SeqCst),
        second.calls.load(Ordering::SeqCst),
    );
    let (lookups, writes) = (
        runs.lookups.load(Ordering::SeqCst),
        runs.writes.load(Ordering::SeqCst),
    );
    lines.extend([
        format!("  stop:   {}", wire(Some(report.stop))),
        format!("  answer: {}", report.text),
        format!("model calls: {before} before the restart, {after} after it"),
        format!("tool runs: lookup_fact {lookups}, write_plan {writes}"),
        "delegations:".to_owned(),
    ]);
    lines.extend(report.delegations.iter().map(roll_up));
    Ok(Outcome {
        report,
        after,
        lookups,
        writes,
        lines,
    })
}

impl Outcome {
    /// Check the claims of the example.
    ///
    /// # Errors
    ///
    /// Returns the first claim that does not hold.
    fn check(&self) -> Result<(), &'static str> {
        let delegations = &self.report.delegations;
        if !delegations
            .iter()
            .any(|record| record.stop == Some(AgentStop::Completed))
        {
            return Err("no delegation ran to completion");
        }
        if !delegations.iter().any(|record| record.refusal.is_some()) {
            return Err("no delegation was refused");
        }
        if self.after != 1 {
            return Err("the restart sent a recorded model turn again");
        }
        if self.lookups != 1 || self.writes != 1 {
            return Err("a tool ran more than once");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db = std::env::temp_dir().join(format!("sqlite-mesh-{}.db", std::process::id()));
    let outcome = run(&db).await;
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let outcome = outcome?;
    for line in &outcome.lines {
        println!("{line}");
    }
    Ok(outcome.check()?)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest_agent::Refusal;

    use super::*;

    async fn outcome() -> Outcome {
        let dir = tempfile::tempdir().unwrap();
        run(&dir.path().join("mesh.db")).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_delegation_runs_and_one_is_refused() {
        let outcome = outcome().await;
        let report = &outcome.report;
        assert_eq!(report.stop, AgentStop::Completed);
        assert_eq!(report.delegations.len(), 2);
        let ran = &report.delegations[0];
        assert_eq!(ran.chain, ["planner", "researcher"]);
        assert_eq!(ran.stop, Some(AgentStop::Completed));
        assert_eq!(ran.refusal, None);
        assert_eq!(ran.tool_calls, 1);
        let refused = &report.delegations[1];
        assert_eq!(refused.chain, ["planner", "publisher"]);
        assert_eq!(refused.refusal, Some(Refusal::NotPermitted));
        assert_eq!(refused.run_id, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_restart_sends_only_the_last_turn() {
        let outcome = outcome().await;
        assert_eq!(outcome.check(), Ok(()));
        assert_eq!(outcome.after, 1, "history answers each recorded turn");
        assert_eq!(outcome.lookups, 1, "the read tool runs once");
        assert_eq!(outcome.writes, 1, "the approved write runs once");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_guide_shows_the_real_output() {
        let outcome = outcome().await;
        let guide = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../docs/agent-mesh.md"
        ))
        .unwrap();
        let output = outcome.lines.join("\n");
        assert!(
            guide.contains(&format!("```text\n{output}\n```")),
            "docs/agent-mesh.md shows this output:\n{output}"
        );
    }
}
