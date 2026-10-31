//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for the replay of a durable `agent` run (issue #2263).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A durable agent with the default profile window of 200 messages handles
//! `DURABLE_AGENT_REPLAY_PROFILE_N` user messages, one op each.
//! `DURABLE_AGENT_REPLAY_PROFILE_SHAPE` picks the work of one op:
//!
//! - `tool` (default): a model turn with one tool call, the call, and a model
//!   turn that answers. An op adds four messages, so op 50 fills the window.
//!   Each tool result holds `DURABLE_AGENT_REPLAY_PROFILE_RESULT_BYTES` bytes
//!   (default 512).
//! - `chat`: one model turn that answers. An op adds two messages, so op 100
//!   fills the window.
//!
//! `WorkflowTestEnv` records the run with a frozen clock and scripted mocks of
//! `agent_profile`, `agent_model_turn` and `agent_tool_call`. It sends one op,
//! then drives the run until the agent waits again. Each decision of the
//! recording replays the whole history so far, so the recording costs
//! O(N²). Record once, outside valgrind, with
//! `DURABLE_AGENT_REPLAY_PROFILE_RECORD=<path>`.
//!
//! The default mode reads the file that `DURABLE_AGENT_REPLAY_PROFILE_HISTORY`
//! names. With no file, it records in the same process first. The bench
//! function `decode` decodes the file. The bench function `replay` then runs
//! the history once through `run_workflow_with_state_history_policy_and_caps`.
//! The SQLite runtime calls that entry point for each decision. Read the
//! inclusive cost of `decode` and `replay` apart.
//! `DURABLE_AGENT_REPLAY_PROFILE_REPS` (default 1) repeats the replay.
//!
//! The replay must suspend at the next op wait. A recorded command that the
//! replay does not consume fails the outcome, so a passing run matched every
//! recorded event.
//!
//! A `tool` recording of 288 ops or more fails. Near op 288 the history
//! reaches half of its 50 MiB byte cap. The entity checkpoint then continues
//! the run as new, and this profile replays one run only.
//!
//! # Nondeterminism
//!
//! `upsert_search_attrs` collects each patch into a `HashMap` with a random
//! seed. The patch order then changes the heap layout, so the totals of two
//! runs differ by up to about 1.2%. Preload a `getrandom` that returns fixed
//! bytes to fix the seed. `docs/performance-durable-agent-replay.md` shows
//! how.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-agent --bench agent_loop_replay_profile \
//!   --bench durable_agent_replay_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="durable_agent_replay_profile") | .executable')
//! DURABLE_AGENT_REPLAY_PROFILE_N=200 DURABLE_AGENT_REPLAY_PROFILE_RECORD=tool-200.json "$BIN"
//! DURABLE_AGENT_REPLAY_PROFILE_N=200 DURABLE_AGENT_REPLAY_PROFILE_HISTORY=tool-200.json \
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
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_SIDE_EFFECT, EntityMessage};
use autumn_harvest::event::SideEffectKind;
use autumn_harvest::executor::{
    WorkflowExecuteSpanMeta, run_workflow_with_state_history_policy_and_caps,
};
use autumn_harvest::testing::{TestRunStatus, WorkflowTestEnv};
use autumn_harvest::{
    ExecutionId, NoOpMetrics, WorkflowCommand, WorkflowEvent, WorkflowHandlerFn,
    WorkflowHistoryPolicy, WorkflowOutcome,
};
use autumn_harvest_agent::agent::DEFAULT_WINDOW;
use autumn_harvest_agent::{
    AGENT_WORKFLOW_NAME, AgentOp, AgentProfile, AgentState, ChatRole, ContentPart, ModelTurn,
    ModelTurnRequest, StopReason, TokenUsage, ToolCallRequest, ToolDecision, ToolOutcome,
    agent_info,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The profile lookup activity name.
const PROFILE: &str = "agent_profile";

/// The model-turn activity name.
const MODEL_TURN: &str = "agent_model_turn";

/// The tool-call activity name.
const TOOL_CALL: &str = "agent_tool_call";

/// The tool that the `tool` shape calls.
const TOOL: &str = "lookup_ticket";

/// The agent id. It is the workflow id.
const AGENT_ID: &str = "support/acct-profile";

/// The kind of the agent, from its id.
const KIND: &str = "support";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Tool,
    Chat,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("DURABLE_AGENT_REPLAY_PROFILE_SHAPE") else {
            return Self::Tool;
        };
        match value.as_str() {
            "tool" => Self::Tool,
            "chat" => Self::Chat,
            other => {
                panic!("DURABLE_AGENT_REPLAY_PROFILE_SHAPE must be tool or chat, got {other:?}")
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Tool => "tool",
            Self::Chat => "chat",
        }
    }

    /// The messages that one op adds to the transcript.
    const fn messages_per_op(self) -> usize {
        match self {
            Self::Tool => 4,
            Self::Chat => 2,
        }
    }

    /// The model turns of one op.
    const fn turns_per_op(self) -> usize {
        match self {
            Self::Tool => 2,
            Self::Chat => 1,
        }
    }

    /// The tool calls of one op.
    const fn calls_per_op(self) -> usize {
        match self {
            Self::Tool => 1,
            Self::Chat => 0,
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

/// The profile of the kind. It keeps the default window.
fn profile() -> AgentProfile {
    AgentProfile::new(KIND)
        .system("You are a support agent. Use the tools. Answer in one short paragraph.")
}

/// The text of user message `op`.
fn user_text(op: u64) -> String {
    format!("Ticket {op}: the customer asks where the refund of order ord-{op:05} is.")
}

/// The answer of op `op`.
fn answer_text(op: u64) -> String {
    format!("Ticket {op}: the refund of order ord-{op:05} is approved. It reaches the card soon.")
}

/// The text of the tool result of op `op`, exactly `bytes` long.
fn tool_result(op: u64, bytes: usize) -> String {
    let mut text = String::with_capacity(bytes + 128);
    let mut line = 0_u64;
    while text.len() < bytes {
        write!(
            text,
            "ticket {op:05} event {line:02}: refund of ord-{op:05} moved to state {state}, \
             agent note added; ",
            state = (op + line) % 7,
        )
        .expect("a write to a String never fails");
        line += 1;
    }
    text.truncate(bytes);
    text
}

/// The op index of a segment run id `<incarnation>:s<segment>`. Each op
/// runs one segment, so the segment number is the op index.
fn op_of(run_id: &str) -> Result<u64, String> {
    run_id
        .rsplit_once(":s")
        .and_then(|(_, segment)| segment.parse().ok())
        .ok_or_else(|| format!("the run id {run_id:?} names no segment"))
}

fn turn(content: Vec<ContentPart>, stop: StopReason, calls: usize) -> ModelTurn {
    ModelTurn {
        content,
        stop,
        usage: TokenUsage::new(900, 40),
        decisions: vec![ToolDecision::Allow; calls],
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: vec![false; calls],
    }
}

/// The first model turn of op `op` in the `tool` shape.
fn tool_turn(op: u64) -> ModelTurn {
    let content = vec![
        ContentPart::Text(format!("I look up ticket {op} first.")),
        ContentPart::ToolCall {
            id: format!("call-{op:05}"),
            name: TOOL.to_owned(),
            arguments: json!({ "ticket": op }),
        },
    ];
    turn(content, StopReason::ToolUse, 1)
}

/// The model turn that answers op `op`.
fn final_turn(op: u64) -> ModelTurn {
    turn(
        vec![ContentPart::Text(answer_text(op))],
        StopReason::EndTurn,
        0,
    )
}

/// The model mock. A request whose last message is a tool result gets the
/// answer. Any other request gets the work of `shape`.
fn model_turn(shape: Shape, input: Value) -> Result<Value, String> {
    let request: ModelTurnRequest = serde_json::from_value(input).map_err(|e| e.to_string())?;
    let op = op_of(&request.run_id)?;
    let answered = request
        .messages
        .last()
        .is_some_and(|message| message.role == ChatRole::Tool);
    let turn = if answered || shape == Shape::Chat {
        final_turn(op)
    } else {
        tool_turn(op)
    };
    serde_json::to_value(turn).map_err(|e| e.to_string())
}

/// The tool mock. The result names the ticket of the call.
fn tool_call(result_bytes: usize, input: &Value) -> Result<Value, String> {
    let request: ToolCallRequest =
        serde_json::from_value(input.clone()).map_err(|e| e.to_string())?;
    let op = request.call.arguments["ticket"]
        .as_u64()
        .ok_or("the call names no ticket")?;
    serde_json::to_value(ToolOutcome::ok(tool_result(op, result_bytes))).map_err(|e| e.to_string())
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

/// Record `n` ops of `shape` with `WorkflowTestEnv`, one op per drive.
fn record(rt: &tokio::runtime::Runtime, shape: Shape, n: usize, result_bytes: usize) -> Recording {
    let anchor = chrono::DateTime::from_timestamp(1_790_000_000, 0).expect("a fixed time");
    let env = WorkflowTestEnv::new()
        .with_frozen_anchor(anchor)
        .with_workflow_id(AGENT_ID)
        .with_workflow_name(AGENT_WORKFLOW_NAME)
        .mock_activity(PROFILE, |_| {
            serde_json::to_value(Some(profile())).map_err(|e| e.to_string())
        })
        .mock_activity(MODEL_TURN, move |input| model_turn(shape, input))
        .mock_activity(TOOL_CALL, move |input| tool_call(result_bytes, &input));
    let (exec_id, events) = rt.block_on(async {
        let mut run = env.start(agent_info().handler, json!({}));
        let exec_id = run.exec_id();
        assert_eq!(
            run.run_until_blocked().await,
            TestRunStatus::Blocked,
            "the agent must wait for its first op"
        );
        for op in 0..n as u64 {
            let message = AgentOp::message(user_text(op));
            run.signal(ENTITY_OP_SIGNAL, json!(EntityMessage::op(message)))
                .expect("the agent run is live");
            // A long run reaches the entity checkpoint and continues as new.
            // This profile replays one run, so it stops with the event name.
            let status = run.run_until_blocked().await;
            assert!(
                status == TestRunStatus::Blocked,
                "the agent must wait for the next op after op {op}, but the run ended with {}. \
                 Record fewer ops: the entity checkpoint ends a run near half the history \
                 byte cap",
                run.events()
                    .last()
                    .map_or("no event", WorkflowEvent::type_name)
            );
        }
        (exec_id, run.events().to_vec())
    });
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

/// The values of the state snapshots of `events`.
fn snapshots(events: &[WorkflowEvent]) -> Vec<&Value> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::EntityState,
                name: Some(name),
                value,
            } if name == ENTITY_STATE_SIDE_EFFECT => Some(value),
            _ => None,
        })
        .collect()
}

/// Check that `recording` is the run that the environment asks for. It
/// returns the serialized bytes of all the snapshots and the last
/// transcript length.
fn check_recording(
    recording: &Recording,
    shape: Shape,
    n: usize,
    result_bytes: usize,
) -> (usize, usize) {
    assert_eq!(
        recording.shape,
        shape.name(),
        "the history file has another shape"
    );
    assert_eq!(recording.n, n, "the history file has another op count");
    assert_eq!(
        recording.result_bytes, result_bytes,
        "the history file has another result size"
    );
    let events = &recording.events;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            WorkflowEvent::WorkflowContinuedAsNew { .. }
                | WorkflowEvent::WorkflowCompleted { .. }
                | WorkflowEvent::WorkflowFailed { .. }
        )),
        "the recorded run must stay in one run"
    );
    assert_eq!(scheduled(events, MODEL_TURN), n * shape.turns_per_op());
    assert_eq!(scheduled(events, TOOL_CALL), n * shape.calls_per_op());
    let snapshots = snapshots(events);
    assert_eq!(snapshots.len(), n + 1, "each op must record one snapshot");
    let last = snapshots.last().expect("the start snapshot exists");
    let state: AgentState =
        serde_json::from_value(last["state"].clone()).expect("the snapshot holds a state");
    let window = usize::try_from(DEFAULT_WINDOW).expect("the window fits in usize");
    let expected = (n * shape.messages_per_op()).min(window);
    assert_eq!(state.transcript.len(), expected, "the transcript length");
    assert_eq!(state.segments, n as u64, "each op must run one segment");
    assert_eq!(state.failures, 0, "no op may fail");
    if let Some(op) = (n as u64).checked_sub(1) {
        assert_eq!(state.last_answer, Some(answer_text(op)), "the last answer");
    }
    let bytes = snapshots
        .iter()
        .map(|value| serde_json::to_vec(value).expect("a value serializes").len())
        .sum();
    (bytes, state.transcript.len())
}

/// The span metadata that the SQLite runtime builds for a cycle.
fn span_meta() -> WorkflowExecuteSpanMeta {
    WorkflowExecuteSpanMeta {
        workflow_name: AGENT_WORKFLOW_NAME.to_owned(),
        workflow_id: AGENT_ID.to_owned(),
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
            AGENT_WORKFLOW_NAME,
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

/// Check that the replay matched every recorded event and stopped at the
/// next op wait. It returns the number of commands.
fn check_outcome(outcome: &WorkflowOutcome) -> usize {
    let WorkflowOutcome::Suspended { commands } = outcome else {
        panic!("the replay must suspend at the next op wait, got {outcome:?}");
    };
    assert!(
        commands.iter().any(|command| matches!(
            command,
            WorkflowCommand::WaitForSignal { signal_name, .. } if signal_name == ENTITY_OP_SIGNAL
        )),
        "the replay must wait for the next op: {commands:?}"
    );
    assert!(
        !commands
            .iter()
            .any(|command| matches!(command, WorkflowCommand::ScheduleActivity { .. })),
        "the replay must schedule no new work: {commands:?}"
    );
    commands.len()
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
    let n = env_usize("DURABLE_AGENT_REPLAY_PROFILE_N", 50);
    let reps = env_usize("DURABLE_AGENT_REPLAY_PROFILE_REPS", 1);
    assert!(
        reps > 0,
        "DURABLE_AGENT_REPLAY_PROFILE_REPS must be at least 1"
    );
    let result_bytes = env_usize("DURABLE_AGENT_REPLAY_PROFILE_RESULT_BYTES", 512);
    let rt = runtime();
    let name = shape.name();

    if let Ok(path) = std::env::var("DURABLE_AGENT_REPLAY_PROFILE_RECORD") {
        let recording = record(&rt, shape, n, result_bytes);
        let (snapshot_bytes, transcript) = check_recording(&recording, shape, n, result_bytes);
        let json = serde_json::to_vec(&recording).expect("the history serializes");
        std::fs::write(&path, &json).expect("the history file writes");
        println!(
            "durable_agent_replay_profile: mode=record shape={name} n={n} \
             result_bytes={result_bytes} events={} file_bytes={} snapshot_bytes={snapshot_bytes} \
             transcript={transcript}",
            recording.events.len(),
            json.len()
        );
        return;
    }

    let (mut recording, file_bytes) = std::env::var("DURABLE_AGENT_REPLAY_PROFILE_HISTORY")
        .map_or_else(
            |_| (record(&rt, shape, n, result_bytes), 0),
            |path| {
                let bytes = std::fs::read(&path).expect("the history file reads");
                (decode(&bytes), bytes.len())
            },
        );
    let (snapshot_bytes, transcript) = check_recording(&recording, shape, n, result_bytes);
    let events_len = recording.events.len();
    let input = start_input(&recording.events);
    let handler = agent_info().handler;
    let mut commands = 0;
    for rep in 0..reps {
        let events = if rep + 1 == reps {
            std::mem::take(&mut recording.events)
        } else {
            recording.events.clone()
        };
        let outcome = replay(&rt, handler, recording.exec_id, events, input.clone());
        commands = check_outcome(&outcome);
    }
    println!(
        "durable_agent_replay_profile: mode=replay shape={name} n={n} reps={reps} \
         result_bytes={result_bytes} events={events_len} file_bytes={file_bytes} \
         snapshot_bytes={snapshot_bytes} transcript={transcript} commands={commands}"
    );
}
