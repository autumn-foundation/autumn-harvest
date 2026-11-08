//! A small agent mesh on SQLite, with no API key and no network.
//!
//! A planner agent holds a mandate that names one peer, a researcher. The
//! harness holds two tools, `lookup_fact` and `write_plan`. The roster entry
//! of the researcher limits it to `lookup_fact` and makes it read-only. Only
//! the planner calls `write_plan`, and a person must approve each write.
//!
//! 1. The planner delegates a question to the researcher. The researcher
//!    reads a fact and answers.
//! 2. The planner delegates to the publisher. The roster has a publisher,
//!    but the mandate does not name it. The harness refuses the delegation.
//! 3. The planner asks to write the plan, and the run waits for approval.
//! 4. The example stands in for a restart. It drops the runtime and opens
//!    the file again. The approval goes to the new runtime, and the run
//!    finishes.
//!
//! The example prints the delegations of the report at the end. It exits
//! non-zero when a claim of `Outcome::check` fails.
//!
//! ```sh
//! cargo run -p autumn-harvest-agent --features sqlite --example sqlite_mesh
//! ```
//!
//! The guide is "Run a small mesh", in section 2 of `docs/agent-mesh.md`.

use std::error::Error;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentReport, AgentStop, AgentTask, Approval, ChatRequest,
    ChatResponse, ChatRole, ContentPart, DelegationRecord, FnTool, Mandate, Peer, Refusal, Rule,
    StopReason, TokenUsage, ToolEffect, ToolRules, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::{Value, json};

/// The system prompt of the researcher. `OfflineModel` looks for it to find
/// the researcher.
const RESEARCHER: &str = "You research. Use lookup_fact.";

/// The fact that the read tool returns.
const FACT: &str = "The ferry leaves at 9.";

/// A stand-in model for each agent of the mesh.
///
/// It reads the system prompt to find the agent. It counts the tool results
/// to find the step. Its answer depends on the request only, so a new
/// process gets the same answers.
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
        // `tools` limits the researcher to the read tool. `read_only` refuses
        // each write, also when the list grows.
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
        .system("You plan trips.")
        .mandate(Mandate::new("planner").peers(["researcher"]))
}

/// What one run of the example gives.
#[derive(Debug)]
struct Outcome {
    report: AgentReport,
    /// Model calls before the restart.
    before: usize,
    /// Model calls after the restart.
    after: usize,
    /// Runs of the read tool.
    lookups: usize,
    /// Runs of the write tool.
    writes: usize,
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
        |refusal| {
            let peer = record.chain.last().map_or("", String::as_str);
            format!(
                "  {chain}: refused, {} ({})",
                refusal.code(),
                refusal.message(peer)
            )
        },
    )
}

/// Run the mesh on the database file `db`, with a restart between two
/// runtimes. Each step adds its text to `lines`, also when a later step
/// fails.
async fn run(db: &Path, lines: &mut Vec<String>) -> Result<Outcome, Box<dyn Error>> {
    let runs = Arc::new(ToolRuns::default());
    lines.push("process 1: start the planner".to_owned());

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

    let outcome = Outcome {
        before: first.calls.load(Ordering::SeqCst),
        after: second.calls.load(Ordering::SeqCst),
        lookups: runs.lookups.load(Ordering::SeqCst),
        writes: runs.writes.load(Ordering::SeqCst),
        report,
    };
    lines.extend([
        format!("  stop:   {}", wire(Some(outcome.report.stop))),
        format!("  answer: {}", outcome.report.text),
        format!(
            "model calls: {} before the restart, {} after it",
            outcome.before, outcome.after
        ),
        format!(
            "tool runs: lookup_fact {}, write_plan {}",
            outcome.lookups, outcome.writes
        ),
        "delegations:".to_owned(),
    ]);
    lines.extend(outcome.report.delegations.iter().map(roll_up));
    Ok(outcome)
}

/// The chain, stop and refusal of each delegation.
type Shape = Vec<(String, Option<AgentStop>, Option<Refusal>)>;

impl Outcome {
    /// Check the claims of the example. The delegation to the researcher
    /// completes, and the harness refuses the delegation to the publisher.
    /// History answers each turn before the restart, so each tool runs once.
    /// The error names the first claim that fails, with both values.
    fn check(&self) -> Result<(), String> {
        let shape: Shape = self
            .report
            .delegations
            .iter()
            .map(|record| (record.chain.join(" -> "), record.stop, record.refusal))
            .collect();
        let expected: Shape = vec![
            (
                "planner -> researcher".to_owned(),
                Some(AgentStop::Completed),
                None,
            ),
            (
                "planner -> publisher".to_owned(),
                None,
                Some(Refusal::NotPermitted),
            ),
        ];
        if shape != expected {
            return Err(format!("delegations: expected {expected:?}, got {shape:?}"));
        }
        for (what, got, want) in [
            ("model calls before the restart", self.before, 5),
            ("model calls after the restart", self.after, 1),
            ("runs of lookup_fact", self.lookups, 1),
            ("runs of write_plan", self.writes, 1),
        ] {
            if got != want {
                return Err(format!("{what}: expected {want}, got {got}"));
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // The directory and the database files go away when `dir` drops.
    let dir = tempfile::tempdir()?;
    let mut lines = Vec::new();
    let outcome = run(&dir.path().join("mesh.db"), &mut lines).await;
    for line in &lines {
        println!("{line}");
    }
    Ok(outcome?.check()?)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    async fn outcome() -> (Outcome, Vec<String>) {
        let dir = tempfile::tempdir().unwrap();
        let mut lines = Vec::new();
        let outcome = run(&dir.path().join("mesh.db"), &mut lines).await.unwrap();
        (outcome, lines)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_delegation_runs_and_one_is_refused() {
        let (outcome, _) = outcome().await;
        assert_eq!(outcome.report.stop, AgentStop::Completed);
        assert_eq!(outcome.check(), Ok(()));
        let [ran, refused] = outcome.report.delegations.as_slice() else {
            panic!("expected two delegations");
        };
        assert!(ran.run_id.is_some(), "the researcher runs as a child");
        assert_eq!(ran.tool_calls, 1, "the researcher reads once");
        assert_eq!(refused.run_id, None, "a refused delegation does not run");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_check_names_a_claim_that_fails() {
        let (mut outcome, _) = outcome().await;
        outcome.after = 2;
        assert_eq!(
            outcome.check(),
            Err("model calls after the restart: expected 1, got 2".to_owned())
        );
        outcome.report.delegations.pop();
        assert!(outcome.check().unwrap_err().starts_with("delegations:"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_guide_shows_the_real_output() {
        let (_, lines) = outcome().await;
        // A Windows checkout can hold CRLF line ends.
        let guide = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../docs/agent-mesh.md"
        ))
        .unwrap()
        .replace("\r\n", "\n");
        let output = lines.join("\n");
        assert!(
            guide.contains(&format!("```text\n{output}\n```")),
            "docs/agent-mesh.md shows this output:\n{output}"
        );
    }
}
