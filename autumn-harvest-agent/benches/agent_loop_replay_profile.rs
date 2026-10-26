//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for the replay of a recorded `agent_loop` run (issue #1973).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! One `agent_loop` run makes `AGENT_LOOP_REPLAY_PROFILE_N` tool rounds, then
//! answers. Each round is one model turn with one tool call. Each tool result
//! holds `AGENT_LOOP_REPLAY_PROFILE_RESULT_BYTES` bytes (default 1024).
//! `AGENT_LOOP_REPLAY_PROFILE_SHAPE` picks the request form:
//!
//! - `delta` (default): each model turn records only the new messages, as
//!   `AgentTask::new` sets it.
//! - `full`: each model turn records the whole transcript.
//!
//! `WorkflowTestEnv` records the run with a frozen clock and scripted mocks of
//! `agent_model_turn` and `agent_tool_call`. Each decision of the recording
//! replays the whole history so far, so the recording costs more than the
//! replay. Record once, outside valgrind, with
//! `AGENT_LOOP_REPLAY_PROFILE_RECORD=<path>`. That mode writes the history,
//! without its terminal event, to the file.
//!
//! The default mode reads the file that `AGENT_LOOP_REPLAY_PROFILE_HISTORY`
//! names. With no file, it records in the same process first. The bench
//! function `decode` decodes the file. The bench function `replay` then runs
//! the history once through `run_workflow_with_state_history_policy_and_caps`.
//! The SQLite runtime calls that entry point for each decision. The replay is
//! the last decision of the run, so it completes the run. Read the inclusive
//! cost of `decode` and `replay` apart. `AGENT_LOOP_REPLAY_PROFILE_REPS`
//! (default 1) repeats the replay.
//!
//! The replay must complete with the report of the recorded run. A recorded
//! command that the replay does not consume fails the outcome, so a passing
//! run matched every recorded event.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-agent --bench agent_loop_replay_profile \
//!   --bench durable_agent_replay_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="agent_loop_replay_profile") | .executable')
//! AGENT_LOOP_REPLAY_PROFILE_N=100 AGENT_LOOP_REPLAY_PROFILE_RECORD=delta-100.json "$BIN"
//! AGENT_LOOP_REPLAY_PROFILE_N=100 AGENT_LOOP_REPLAY_PROFILE_HISTORY=delta-100.json \
//!   valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use autumn_harvest::builder::{
    DEFAULT_MAX_ACTIVITY_INPUT_BYTES, DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
    DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
};
use autumn_harvest::context::{DEFAULT_CURRENT_DETAILS_CAP_BYTES, empty_shared_state};
use autumn_harvest::executor::{
    WorkflowExecuteSpanMeta, run_workflow_with_state_history_policy_and_caps,
};
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest::{
    ExecutionId, NoOpMetrics, WorkflowEvent, WorkflowHandlerFn, WorkflowHistoryPolicy,
    WorkflowOutcome,
};
use autumn_harvest_agent::workflow::{WORKFLOW_NAME, agent_loop_info};
use autumn_harvest_agent::{
    AgentReport, AgentStop, AgentTask, ContentPart, ModelTurn, ModelTurnRequest, StopReason,
    TokenUsage, ToolCallRequest, ToolDecision, ToolOutcome,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The model-turn activity name.
const MODEL_TURN: &str = "agent_model_turn";

/// The tool-call activity name.
const TOOL_CALL: &str = "agent_tool_call";

/// The tool that each round calls.
const TOOL: &str = "search_orders";

/// The workflow id of the run. The SQLite runtime passes one in the span
/// metadata.
const WORKFLOW_ID: &str = "agent-loop-profile";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Delta,
    Full,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("AGENT_LOOP_REPLAY_PROFILE_SHAPE") else {
            return Self::Delta;
        };
        match value.as_str() {
            "delta" => Self::Delta,
            "full" => Self::Full,
            other => panic!("AGENT_LOOP_REPLAY_PROFILE_SHAPE must be delta or full, got {other:?}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Full => "full",
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

/// The text of the tool result of round `step`, exactly `bytes` long.
///
/// Each round gets other text, so the loop guard sees no repeat.
fn tool_result(step: u32, bytes: usize) -> String {
    let mut text = String::with_capacity(bytes + 128);
    let mut line = 0_u32;
    while text.len() < bytes {
        write!(
            text,
            "order ord-{step:05}-{line:02}: shipped 2026-09-{day:02}, total {cents} cents, \
             refund requested, status pending review; ",
            day = 1 + (step + line) % 28,
            cents = 1_000 + (step * 37 + line * 11) % 90_000,
        )
        .expect("a write to a String never fails");
        line += 1;
    }
    text.truncate(bytes);
    text
}

/// The model turn of tool round `step`: a short narration and one call.
fn tool_turn(step: u32) -> ModelTurn {
    ModelTurn {
        content: vec![
            ContentPart::Text(format!(
                "Page {step} of the order history can hold the refund. I read it next."
            )),
            ContentPart::ToolCall {
                id: format!("call-{step:05}"),
                name: TOOL.to_owned(),
                arguments: json!({
                    "customer": "cust-0042",
                    "query": "refund status",
                    "page": step,
                }),
            },
        ],
        stop: StopReason::ToolUse,
        usage: TokenUsage::new(1_200, 40),
        decisions: vec![ToolDecision::Allow],
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: Vec::new(),
    }
}

/// The last model turn: the answer, with no call.
fn final_turn(rounds: u32) -> ModelTurn {
    ModelTurn {
        content: vec![ContentPart::Text(format!(
            "I read {rounds} pages. The refund of the last order waits for review."
        ))],
        stop: StopReason::EndTurn,
        usage: TokenUsage::new(1_200, 60),
        decisions: Vec::new(),
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: Vec::new(),
    }
}

/// The task of the run. Its step bound lets every round run.
fn task(shape: Shape, n: usize) -> AgentTask {
    let rounds = u32::try_from(n).expect("the round count fits in u32");
    AgentTask::new("Find the refund status of the last order of customer cust-0042.")
        .system("You are a support agent. Use the tools. Answer in one short paragraph.")
        .max_steps(rounds)
        .transcript_delta(shape == Shape::Delta)
}

/// A recorded history and what produced it.
#[derive(Serialize, Deserialize)]
struct Recording {
    shape: String,
    n: usize,
    result_bytes: usize,
    exec_id: ExecutionId,
    events: Vec<WorkflowEvent>,
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds")
}

/// Record one run of `n` tool rounds with `WorkflowTestEnv`.
fn record(rt: &tokio::runtime::Runtime, shape: Shape, n: usize, result_bytes: usize) -> Recording {
    let rounds = u32::try_from(n).expect("the round count fits in u32");
    let anchor = chrono::DateTime::from_timestamp(1_790_000_000, 0).expect("a fixed time");
    let env = WorkflowTestEnv::new()
        .with_frozen_anchor(anchor)
        .with_workflow_id(WORKFLOW_ID)
        .with_workflow_name(WORKFLOW_NAME)
        .with_max_iterations(4 * n + 1_000)
        .mock_activity(MODEL_TURN, move |input| {
            let request: ModelTurnRequest =
                serde_json::from_value(input).map_err(|e| e.to_string())?;
            let turn = if request.steps_used < rounds {
                tool_turn(request.steps_used)
            } else {
                final_turn(rounds)
            };
            serde_json::to_value(turn).map_err(|e| e.to_string())
        })
        .mock_activity(TOOL_CALL, move |input| {
            let request: ToolCallRequest =
                serde_json::from_value(input).map_err(|e| e.to_string())?;
            let outcome = ToolOutcome::ok(tool_result(request.step, result_bytes));
            serde_json::to_value(outcome).map_err(|e| e.to_string())
        });
    let input = serde_json::to_value(task(shape, n)).expect("the task serializes");
    let (exec_id, mut events) = rt.block_on(async {
        let run = env.start(agent_loop_info().handler, input);
        let exec_id = run.exec_id();
        let outcome = run.finish().await;
        let output = outcome.result.as_ref().expect("the recorded run completes");
        check_report(output, n);
        (exec_id, outcome.events().to_vec())
    });
    // The last decision of the run replays everything before the terminal.
    let terminal = events.pop();
    assert!(
        matches!(terminal, Some(WorkflowEvent::WorkflowCompleted { .. })),
        "the recorded run must end with its completion, got {terminal:?}"
    );
    Recording {
        shape: shape.name().to_owned(),
        n,
        result_bytes,
        exec_id,
        events,
    }
}

/// The number of scheduled activities named `name` in `events`.
fn scheduled(events: &[WorkflowEvent], name: &str) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::ActivityScheduled { name: scheduled, .. } if scheduled == name)
        })
        .count()
}

/// Check that `recording` is the run that the environment asks for.
fn check_recording(recording: &Recording, shape: Shape, n: usize, result_bytes: usize) {
    assert_eq!(
        recording.shape,
        shape.name(),
        "the history file has another shape"
    );
    assert_eq!(recording.n, n, "the history file has another round count");
    assert_eq!(
        recording.result_bytes, result_bytes,
        "the history file has another result size"
    );
    assert_eq!(
        scheduled(&recording.events, MODEL_TURN),
        n + 1,
        "the run must make one model turn per round, then answer"
    );
    assert_eq!(
        scheduled(&recording.events, TOOL_CALL),
        n,
        "the run must make one tool call per round"
    );
}

/// Check that `output` is the report of a run of `n` completed rounds. It
/// returns the serialized bytes of the transcript of the report.
fn check_report(output: &Value, n: usize) -> usize {
    let report: AgentReport =
        serde_json::from_value(output.clone()).expect("the output is a report");
    let rounds = u32::try_from(n).expect("the round count fits in u32");
    assert_eq!(report.stop, AgentStop::Completed, "the run must complete");
    assert_eq!(report.steps_used, rounds, "every round must run");
    assert_eq!(report.tool_calls, rounds, "every call must run");
    assert_eq!(
        report.messages.len(),
        2 * n + 2,
        "the transcript holds the input, two messages per round and the answer"
    );
    serde_json::to_vec(&report.messages)
        .expect("the transcript serializes")
        .len()
}

/// The span metadata that the SQLite runtime builds for a cycle.
fn span_meta() -> WorkflowExecuteSpanMeta {
    WorkflowExecuteSpanMeta {
        workflow_name: WORKFLOW_NAME.to_owned(),
        workflow_id: WORKFLOW_ID.to_owned(),
        shard_id: 0,
        queue_name: String::new(),
        drive_clock: chrono::DateTime::from_timestamp(1_790_000_000, 0),
        is_replay: false,
        link_traceparent: None,
        build_id: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        start_provenance: autumn_harvest::types::StartProvenance::default(),
        cancellation_reason: None,
    }
}

/// Decode the history file.
#[inline(never)]
fn decode(bytes: &[u8]) -> Recording {
    serde_json::from_slice(bytes).expect("the history file decodes")
}

/// Replay `events` once, as one decision of the SQLite runtime does.
#[inline(never)]
fn replay(
    rt: &tokio::runtime::Runtime,
    handler: WorkflowHandlerFn,
    exec_id: ExecutionId,
    events: Vec<WorkflowEvent>,
    input: Value,
) -> WorkflowOutcome {
    let meta = span_meta();
    let (outcome, _pending, _span, _router) =
        rt.block_on(run_workflow_with_state_history_policy_and_caps(
            exec_id,
            events,
            handler,
            input,
            empty_shared_state(),
            WorkflowHistoryPolicy::default(),
            Some(&meta),
            &[],
            &[],
            WORKFLOW_NAME,
            DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            None,
            HashMap::new(),
            None,
            Arc::new(NoOpMetrics),
            None,
            None,
        ));
    outcome
}

/// The input that the `WorkflowStarted` event of `events` holds.
fn start_input(events: &[WorkflowEvent]) -> Value {
    match events.first() {
        Some(WorkflowEvent::WorkflowStarted { input, .. }) => input.clone(),
        other => panic!("the history must start with WorkflowStarted, got {other:?}"),
    }
}

fn main() {
    let shape = Shape::from_env();
    let n = env_usize("AGENT_LOOP_REPLAY_PROFILE_N", 25);
    let reps = env_usize("AGENT_LOOP_REPLAY_PROFILE_REPS", 1);
    assert!(
        reps > 0,
        "AGENT_LOOP_REPLAY_PROFILE_REPS must be at least 1"
    );
    let result_bytes = env_usize("AGENT_LOOP_REPLAY_PROFILE_RESULT_BYTES", 1_024);
    let rt = runtime();
    let name = shape.name();

    if let Ok(path) = std::env::var("AGENT_LOOP_REPLAY_PROFILE_RECORD") {
        let recording = record(&rt, shape, n, result_bytes);
        check_recording(&recording, shape, n, result_bytes);
        let json = serde_json::to_vec(&recording).expect("the history serializes");
        std::fs::write(&path, &json).expect("the history file writes");
        println!(
            "agent_loop_replay_profile: mode=record shape={name} n={n} \
             result_bytes={result_bytes} events={} file_bytes={}",
            recording.events.len(),
            json.len()
        );
        return;
    }

    let (mut recording, file_bytes) = std::env::var("AGENT_LOOP_REPLAY_PROFILE_HISTORY")
        .map_or_else(
            |_| (record(&rt, shape, n, result_bytes), 0),
            |path| {
                let bytes = std::fs::read(&path).expect("the history file reads");
                (decode(&bytes), bytes.len())
            },
        );
    check_recording(&recording, shape, n, result_bytes);
    let events_len = recording.events.len();
    let input = start_input(&recording.events);
    let handler = agent_loop_info().handler;
    let mut transcript_bytes = 0;
    for rep in 0..reps {
        let events = if rep + 1 == reps {
            std::mem::take(&mut recording.events)
        } else {
            recording.events.clone()
        };
        let outcome = replay(&rt, handler, recording.exec_id, events, input.clone());
        match &outcome {
            WorkflowOutcome::Completed { output, .. } => {
                transcript_bytes = check_report(output, n);
            }
            other => panic!("the replay must complete the run, got {other:?}"),
        }
    }
    println!(
        "agent_loop_replay_profile: mode=replay shape={name} n={n} reps={reps} \
         result_bytes={result_bytes} events={events_len} file_bytes={file_bytes} \
         transcript_bytes={transcript_bytes}"
    );
}
