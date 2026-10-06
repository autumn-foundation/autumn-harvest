//! Replay fuzz harness (issue #1835).
//!
//! The `fuzz_replay` target in `fuzz/` and the seed test in
//! `tests/integration/replay_fuzz_seeds.rs` share this module. It is not a
//! stable API.
//!
//! [`check_case`] runs one [`ReplayCase`] through the storage pipeline and the
//! replayer, and checks three oracles:
//!
//! 1. Nothing panics.
//! 2. A history that goes through the write path reads back, unchanged.
//!    Issues #1253 and #1758 broke this oracle: business data shaped like an
//!    envelope came back changed, or the read failed.
//! 3. Two runs of one case give the same report.
//!
//! The write path is codec encode, then offload. The read path is inflate,
//! then codec decode, as in `store::load_history_inflated`. The replayer then
//! runs the read history with no offloader, because the read path has
//! already inflated every payload. The replayer's own inflate path is not
//! fuzzed: `replay_from_db` decodes before it inflates, which is the reverse
//! order. The replayed workflow issues the commands
//! of an [`Op`] program. By default the program mirrors the history, so
//! replay goes past the first event.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arbitrary::{Arbitrary, Unstructured};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::{CHILD_TIMEOUT_TIMER_PREFIX, SESSION_ACQUIRE_ACTIVITY_NAME, WorkflowContext};
use crate::erase::ERASURE_TOMBSTONE_KEY;
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::failure::ERROR_TYPE_HANDLER_PANIC;
use crate::payload_codec::{
    CODEC_ENVELOPE_KEY, CodecError, PayloadCodec, PayloadCodecs, UNDECODABLE_MARKER_KEY,
};
use crate::payload_store::{
    OFFLOAD_ENVELOPE_KEY, PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use crate::replay::{DEADLINE_PROBE_SIDE_EFFECT_NAME, HistoryMatcher};
use crate::telemetry::NoOpMetrics;
use crate::testing::{ReplayReport, WorkflowReplayer};
use crate::types::{
    ActivityExecId, ExecutionId, ExternalAwaitId, ExternalCancelId, ExternalSignalId,
    ExternalTarget, ParentClosePolicy,
};

/// The deepest JSON nesting that the generator builds.
const MAX_DEPTH: u32 = 6;

/// The most items that the generator puts in one array or object.
const MAX_ITEMS: usize = 4;

/// The deepest nesting of [`Op::Concurrent`] and [`Op::Race`] that the
/// generator builds. The workflow reads its program back from JSON, and
/// `serde_json` stops at 128 levels, so a deeper program would not parse.
pub const MAX_OP_DEPTH: usize = 4;

std::thread_local! {
    static OP_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The error that the replayed program returned, if it returned one. The
    /// replayer polls the workflow on the thread of the current-thread
    /// runtime that [`check_case`] builds, so the harness can read it back.
    static RETURNED_FAILURE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// The nesting depth of [`Op::Concurrent`] and [`Op::Race`] in `ops`.
fn op_depth(ops: &[Op]) -> usize {
    ops.iter()
        .map(|op| match op {
            Op::Concurrent { ops } | Op::Sequence { ops } | Op::Race { branches: ops } => {
                1 + op_depth(ops)
            }
            _ => 0,
        })
        .max()
        .unwrap_or(0)
}

/// Generates the ops nested in a [`Op::Concurrent`] or [`Op::Race`]. Past
/// [`MAX_OP_DEPTH`] it returns none.
fn nested_ops(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<Op>> {
    // The op that holds these ops is one level already.
    let depth = OP_DEPTH.with(std::cell::Cell::get) + 1;
    if depth >= MAX_OP_DEPTH {
        return Ok(Vec::new());
    }
    OP_DEPTH.with(|d| d.set(depth));
    let ops = Vec::<Op>::arbitrary(u);
    OP_DEPTH.with(|d| d.set(depth - 1));
    ops
}

/// The context header that carries the program to the replayed workflow.
const PROGRAM_HEADER: &str = "harvest-fuzz-program";

/// The name of the replayed workflow.
const WORKFLOW: &str = "fuzz_replay";

/// The store id of the in-memory payload store.
pub const STORE_ID: &str = "fuzz-mem";

/// The key id of the keyed codec. Issue #1253 used this id.
pub const KEYED_CODEC_KEY_ID: &str = "2026-q3";

/// Strings that the reserved envelope shapes use.
const DICTIONARY: &[&str] = &[
    "identity",
    "fuzz-xor",
    KEYED_CODEC_KEY_ID,
    "legacy",
    STORE_ID,
    "s3-prod",
    "",
];

/// Key sets of the reserved shapes. The first key is the discriminator.
const SHAPES: &[&[&str]] = &[
    &[CODEC_ENVELOPE_KEY, "codec_id", "data"],
    &[CODEC_ENVELOPE_KEY, "codec_id", "data", "kid"],
    &[CODEC_ENVELOPE_KEY],
    &[OFFLOAD_ENVELOPE_KEY, "store_id", "key", "len", "checksum"],
    &[OFFLOAD_ENVELOPE_KEY],
    &[ERASURE_TOMBSTONE_KEY],
    &[UNDECODABLE_MARKER_KEY],
];

/// Generates a JSON value for a `serde_json::Value` field.
///
/// One draw in seven is a reserved envelope shape, such as a codec envelope
/// or an offload reference. Plain random JSON almost never has that shape.
///
/// # Errors
///
/// Returns the error of the underlying [`Unstructured`] draw.
pub fn value(u: &mut Unstructured<'_>) -> arbitrary::Result<Value> {
    value_at(u, 0)
}

/// Generates an optional JSON value. See [`value`].
///
/// # Errors
///
/// Returns the error of the underlying [`Unstructured`] draw.
pub fn opt_value(u: &mut Unstructured<'_>) -> arbitrary::Result<Option<Value>> {
    if u.arbitrary()? {
        Ok(Some(value(u)?))
    } else {
        Ok(None)
    }
}

fn value_at(u: &mut Unstructured<'_>, depth: u32) -> arbitrary::Result<Value> {
    let kinds = if depth >= MAX_DEPTH { 4 } else { 7 };
    Ok(match u.choose_index(kinds)? {
        0 => Value::Null,
        1 => Value::Bool(u.arbitrary()?),
        2 => number(u)?,
        3 => Value::String(text(u)?),
        4 => {
            let len = u.int_in_range(0..=MAX_ITEMS)?;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(value_at(u, depth + 1)?);
            }
            Value::Array(items)
        }
        5 => {
            let len = u.int_in_range(0..=MAX_ITEMS)?;
            let mut map = serde_json::Map::new();
            for _ in 0..len {
                let key = text(u)?;
                map.insert(key, value_at(u, depth + 1)?);
            }
            Value::Object(map)
        }
        _ => shape(u, depth)?,
    })
}

/// A reserved shape. A discriminator holds a small integer, `true` or a
/// nested object. Every other key holds a dictionary string or a leaf.
fn shape(u: &mut Unstructured<'_>, depth: u32) -> arbitrary::Result<Value> {
    let keys = *u.choose(SHAPES)?;
    let mut map = serde_json::Map::new();
    for (i, key) in keys.iter().enumerate() {
        let field = if i == 0 {
            match u.choose_index(3)? {
                0 => Value::from(u.int_in_range(0..=3_u8)?),
                1 => Value::Bool(true),
                _ => value_at(u, depth + 1)?,
            }
        } else if *key == "len" {
            number(u)?
        } else if u.arbitrary()? {
            Value::String(text(u)?)
        } else {
            value_at(u, MAX_DEPTH)?
        };
        map.insert((*key).to_string(), field);
    }
    Ok(Value::Object(map))
}

/// Generates a finite `f64`. JSON has no NaN or infinity, so `serde_json`
/// writes them as `null`, and the value then does not read back.
///
/// # Errors
///
/// Returns the error of the underlying [`Unstructured`] draw.
pub fn finite_f64(u: &mut Unstructured<'_>) -> arbitrary::Result<f64> {
    let x: f64 = u.arbitrary()?;
    Ok(if x.is_finite() { x } else { 0.0 })
}

fn number(u: &mut Unstructured<'_>) -> arbitrary::Result<Value> {
    Ok(match u.choose_index(3)? {
        0 => Value::from(u.arbitrary::<i64>()?),
        1 => Value::from(u.arbitrary::<u64>()?),
        _ => stable_float(u.arbitrary()?),
    })
}

/// A float that reads back unchanged from JSON text, else an integer.
/// `serde_json` parses some floats one digit off, and such a case would
/// have no stable JSON form.
fn stable_float(x: f64) -> Value {
    let Some(number) = serde_json::Number::from_f64(x) else {
        return Value::Null;
    };
    let text = number.to_string();
    if serde_json::from_str::<f64>(&text).ok() == Some(x) {
        Value::Number(number)
    } else {
        Value::from(x.to_bits())
    }
}

fn text(u: &mut Unstructured<'_>) -> arbitrary::Result<String> {
    if u.arbitrary()? {
        Ok((*u.choose(DICTIONARY)?).to_string())
    } else {
        Ok(u.arbitrary::<&str>()?.to_string())
    }
}

/// Reads an `f64`, or NaN for `null`, the JSON form of a non-finite value.
fn f64_or_nan<'de, D: serde::Deserializer<'de>>(de: D) -> Result<f64, D::Error> {
    Ok(Option::<f64>::deserialize(de)?.unwrap_or(f64::NAN))
}

fn fan_out_items(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<(String, Value, String)>> {
    let len = u.int_in_range(0..=MAX_ITEMS)?;
    (0..len)
        .map(|_| Ok((text(u)?, value(u)?, text(u)?)))
        .collect()
}

fn child_fan_out_items(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<(String, Value)>> {
    let len = u.int_in_range(0..=MAX_ITEMS)?;
    (0..len).map(|_| Ok((text(u)?, value(u)?))).collect()
}

/// One command of the replayed workflow.
#[derive(Debug, Clone, Serialize, Deserialize, Arbitrary)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// `ctx.execute_activity_raw`.
    Activity {
        /// Activity name.
        name: String,
        /// Activity input.
        #[arbitrary(with = value)]
        input: Value,
        /// Task queue.
        queue: String,
    },
    /// `ctx.execute_local_activity_raw`.
    LocalActivity {
        /// Activity name.
        name: String,
        /// Activity input.
        #[arbitrary(with = value)]
        input: Value,
    },
    /// `ctx.timer`.
    Timer {
        /// Timer id.
        id: String,
        /// Duration in seconds.
        secs: u64,
    },
    /// `ctx.wait_for_signal`.
    Signal {
        /// Signal name.
        name: String,
    },
    /// `ctx.system_now`.
    Now,
    /// `ctx.new_uuid`.
    NewUuid,
    /// `ctx.random_u64`.
    Random,
    /// `ctx.random_f64`.
    RandomF64,
    /// `ctx.random_range(value..=value)` over `i64`.
    RandomInt {
        /// The value, which is also both bounds.
        value: i64,
    },
    /// `ctx.random_range(value..=value)` over `f64`.
    RandomFloat {
        /// The value, which is also both bounds. JSON writes a non-finite
        /// value as `null`, so `null` reads back as NaN.
        #[arbitrary(with = finite_f64)]
        #[serde(deserialize_with = "f64_or_nan")]
        value: f64,
    },
    /// `ctx.execute_activity_fan_out_raw` over `(name, input, queue)` items,
    /// or the `_windowed` form when `window` is set.
    FanOut {
        /// The activities of the group.
        #[arbitrary(with = fan_out_items)]
        activities: Vec<(String, Value, String)>,
        /// The most activities in flight at once.
        window: Option<usize>,
        /// True for the collect-all form, which goes on past a failed item.
        collect: bool,
    },
    /// `ctx.spawn_child_workflow_fan_out_raw` over `(name, input)` items, or
    /// the `_collect_raw` form when `collect` is set.
    ChildFanOut {
        /// The children of the group.
        #[arbitrary(with = child_fan_out_items)]
        children: Vec<(String, Value)>,
        /// True for the collect-all form, which waits past a failed child.
        collect: bool,
    },
    /// `ctx.patched`.
    Patched {
        /// Patch id.
        id: String,
    },
    /// `ctx.side_effect`, with a `null` value.
    SideEffect {
        /// Side-effect id.
        name: String,
    },
    /// `ctx.spawn_child_workflow_raw`.
    Child {
        /// Child workflow name.
        name: String,
        /// Child input.
        #[arbitrary(with = value)]
        input: Value,
    },
    /// `ctx.spawn_child_workflow_detached_raw`.
    DetachedChild {
        /// Child workflow name.
        name: String,
        /// Child input.
        #[arbitrary(with = value)]
        input: Value,
        /// What happens to the child when this workflow closes.
        policy: ParentClosePolicy,
    },
    /// `ctx.execute_activity_external`.
    ExternalActivity {
        /// Activity name.
        name: String,
        /// Activity input.
        #[arbitrary(with = value)]
        input: Value,
        /// Task queue.
        queue: String,
        /// Schedule-to-close timeout in seconds.
        secs: u64,
    },
    /// `ctx.signal_external_workflow*_with_idempotency`.
    SignalExternal {
        /// The workflow to signal.
        target: ExternalTarget,
        /// Signal name.
        name: String,
        /// Signal payload.
        #[arbitrary(with = value)]
        payload: Value,
        /// Idempotency key.
        idempotency_key: Option<String>,
    },
    /// `ctx.request_cancel_external_workflow*`.
    CancelExternal {
        /// The workflow to cancel.
        target: ExternalTarget,
    },
    /// `ctx.await_external_workflow_value`.
    AwaitExternal {
        /// The execution to await.
        target: ExecutionId,
    },
    /// `ctx.mutex(key).acquire()`. The guard drops at once.
    Mutex {
        /// Lock key.
        key: String,
    },
    /// `ctx.spawn_child_workflow_timeout`, a child bounded by a timer.
    ChildTimeout {
        /// Workflow name of the child.
        name: String,
        /// Input of the child.
        #[arbitrary(with = value)]
        input: Value,
        /// Timeout in seconds.
        secs: u64,
    },
    /// A saga unwind that runs these compensation activities, in this order.
    SagaUnwind {
        /// `(name, input, queue)` of each compensation, in the order run.
        #[arbitrary(with = fan_out_items)]
        compensations: Vec<(String, Value, String)>,
    },
    /// `ctx.dag_skip_marker`.
    DagSkip {
        /// Index of the skipped task.
        task: usize,
        /// Activity name of the skipped task.
        activity: String,
        /// Indexes of its upstream tasks.
        upstreams: Vec<usize>,
    },
    /// `ctx.should_continue_as_new`, which reads the clock when the run has
    /// an execution deadline.
    DeadlineProbe,
    /// `ctx.create_session` on `queue`.
    Session {
        /// Queue of the session-acquire activity.
        queue: String,
    },
    /// `ctx.wait_for_signal_timeout`, a signal wait bounded by a timer.
    SignalTimeout {
        /// Signal name.
        name: String,
        /// Timeout in seconds.
        secs: u64,
    },
    /// `ctx.start_timer`, a cancellable timer. The handle drops at once.
    ArmTimer {
        /// Timer id.
        id: String,
        /// Duration in seconds.
        secs: u64,
    },
    /// `ctx.cancel_timer`.
    CancelTimer {
        /// Timer id.
        id: String,
    },
    /// `ctx.version(change_id, 0, version)`.
    Version {
        /// Change id.
        change_id: String,
        /// The recorded version, which is also the newest one.
        version: u32,
    },
    /// `ctx.continue_as_new`, or `ctx.continue_as_new_as_type` with a type.
    ContinueAsNew {
        /// Input of the next run.
        #[arbitrary(with = value)]
        input: Value,
        /// Workflow type of the next run.
        workflow_type: Option<String>,
    },
    /// `ctx.race()` over the branch ops, which waits for the first one.
    /// Only activity, child, timer and signal ops are branches.
    Race {
        /// The branches.
        #[arbitrary(with = nested_ops)]
        branches: Vec<Self>,
    },
    /// Runs the ops one after another, as one branch of a `join!` does.
    Sequence {
        /// The ops of the branch, in order.
        #[arbitrary(with = nested_ops)]
        ops: Vec<Self>,
    },
    /// Runs the ops concurrently, as `join!` does.
    Concurrent {
        /// The ops of one command batch.
        #[arbitrary(with = nested_ops)]
        ops: Vec<Self>,
    },
    /// Return `Ok(output)`.
    Complete {
        /// Workflow output.
        #[arbitrary(with = value)]
        output: Value,
    },
    /// Return `Err(error)`.
    Fail {
        /// Workflow error.
        error: String,
    },
}

/// The codec configuration of a case.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    /// The default identity codec.
    #[default]
    Identity,
    /// A non-identity codec, active under [`KEYED_CODEC_KEY_ID`].
    Keyed,
}

/// One fuzz input.
#[derive(Debug, Clone, Serialize, Deserialize, Arbitrary)]
pub struct ReplayCase {
    /// The program of the replayed workflow. `None` mirrors the history.
    #[serde(default)]
    pub program: Option<Vec<Op>>,
    /// `true`: the history is stored rows, so the write path is skipped and
    /// the round-trip oracle does not apply.
    #[serde(default)]
    pub stored: bool,
    /// The codec configuration.
    #[serde(default)]
    pub codec: Codec,
    /// Payloads over this many bytes are offloaded.
    #[serde(default = "default_threshold")]
    #[arbitrary(with = threshold)]
    pub offload_threshold: u32,
    /// The events to store and replay.
    #[arbitrary(with = history)]
    pub history: Vec<WorkflowEvent>,
}

/// The most events that the generator puts in one history.
const MAX_EVENTS: usize = 16;

/// Generates a history of 1 to [`MAX_EVENTS`] events.
///
/// Each event gets an equal share of the remaining input. A derived `Vec`
/// lets one `String` take the whole input, so most histories would hold
/// one event or none.
fn history(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<WorkflowEvent>> {
    let count = u.int_in_range(1..=MAX_EVENTS)?;
    let share = (u.len() / count).max(1);
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        if u.is_empty() {
            break;
        }
        let bytes = u.bytes(share.min(u.len()))?;
        // A share too short for an event is skipped, not fatal.
        if let Ok(event) = WorkflowEvent::arbitrary_take_rest(Unstructured::new(bytes)) {
            events.push(event);
        }
    }
    Ok(events)
}

/// Generates an offload threshold. Three draws in four are at most 256
/// bytes, so generated payloads cross it and the offload round trip runs.
fn threshold(u: &mut Unstructured<'_>) -> arbitrary::Result<u32> {
    if u.ratio(3, 4)? {
        u.int_in_range(0..=256)
    } else {
        Ok(default_threshold())
    }
}

const fn default_threshold() -> u32 {
    1 << 20
}

impl ReplayCase {
    /// Decodes fuzz input. Input that starts with `{` is a JSON case, as the
    /// seed files are. A UTF-8 byte order mark and leading white space are
    /// ignored for that check. Other input, and JSON that does not parse,
    /// feeds the `arbitrary` generator.
    #[must_use]
    pub fn from_fuzz_bytes(data: &[u8]) -> Option<Self> {
        Self::from_json(data).or_else(|| Self::arbitrary_take_rest(Unstructured::new(data)).ok())
    }

    /// Decodes a JSON case, or `None`.
    #[must_use]
    pub fn from_json(data: &[u8]) -> Option<Self> {
        let text = data.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(data);
        if !Self::is_json(text) {
            return None;
        }
        serde_json::from_slice(text).ok()
    }

    /// Reports whether `data` is a JSON case. See [`Self::from_fuzz_bytes`].
    #[must_use]
    pub fn is_json(data: &[u8]) -> bool {
        let text = data.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(data);
        text.trim_ascii_start().first() == Some(&b'{')
    }
}

/// The outcome of a case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The history or the program has no stable JSON form, so storage or
    /// the program header cannot hold it.
    Unstorable(String),
    /// The read path refused the stored rows.
    Unreadable(String),
    /// The replayer ran. The string is the `Debug` form of the status.
    Replayed(String),
}

/// Runs `case` twice and checks the oracles of this module.
///
/// # Panics
///
/// Panics when an oracle fails. That panic is the finding.
#[must_use]
pub fn check_case(case: &ReplayCase) -> Verdict {
    let Some(history) = normalize(&case.history) else {
        return Verdict::Unstorable("the history has no stable JSON form".to_string());
    };
    // The generator stays within the limit. A program built in code may not.
    if case.program.as_deref().map_or(0, op_depth) > MAX_OP_DEPTH {
        return Verdict::Unstorable("the program nests deeper than MAX_OP_DEPTH".to_string());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds");
    let (first, first_report) = runtime.block_on(run_once(case, &history));
    let (second, second_report) = runtime.block_on(run_once(case, &history));
    assert_eq!(
        first_report, second_report,
        "two runs of one case gave different reports"
    );
    assert_eq!(
        first, second,
        "two runs of one case gave different verdicts"
    );
    first
}

/// The history after a JSON text round trip reaches a fixed point, or `None`.
///
/// Storage keeps JSON text, and serde does not keep every value through it.
/// `Some(Value::Null)` reads back as `None`. A float can lose its last digit,
/// because `serde_json` parses floats without `float_roundtrip`. The oracle
/// compares against this form, so it reports only a change that the
/// pipeline makes.
///
/// A history with a NUL character is `None` too. Postgres `jsonb` refuses
/// `\u0000`, so production can never store it.
fn normalize(history: &[WorkflowEvent]) -> Option<Vec<WorkflowEvent>> {
    let mut current = text_round_trip(history)?;
    for _ in 0..4 {
        let next = text_round_trip(&current)?;
        if to_json(&next) == to_json(&current) {
            return Some(next);
        }
        current = next;
    }
    None
}

fn text_round_trip(history: &[WorkflowEvent]) -> Option<Vec<WorkflowEvent>> {
    history
        .iter()
        .map(|event| {
            let text = serde_json::to_string(event).ok()?;
            if text.contains("\\u0000") {
                return None;
            }
            serde_json::from_str(&text).ok()
        })
        .collect()
}

fn to_json(history: &[WorkflowEvent]) -> Value {
    serde_json::to_value(history).expect("a normalized history serializes")
}

async fn run_once(case: &ReplayCase, history: &[WorkflowEvent]) -> (Verdict, String) {
    RETURNED_FAILURE.with(|r| r.replace(None));
    let codecs = codecs(case.codec);
    let offloader = PayloadOffloader::new(
        Arc::new(MemStore::default()),
        u64::from(case.offload_threshold),
        Arc::new(NoOpMetrics),
    );

    let mut rows = Vec::with_capacity(history.len());
    for event in history {
        let row = if case.stored {
            serde_json::to_value(event).map_err(|e| e.to_string())
        } else {
            write_row(&codecs, &offloader, event).await
        };
        match row {
            Ok(row) => rows.push(row),
            // The codec and the store here never fail, so a write error is
            // an engine defect, such as refusing a look-alike value.
            Err(error) => panic!("the write path refused a stored history: {error}"),
        }
    }

    let mut read = Vec::with_capacity(rows.len());
    for row in rows {
        if !case.stored {
            assert_lossy_read(&codecs, &offloader, row.clone()).await;
        }
        match read_row(&codecs, &offloader, row).await {
            Ok(event) => read.push(event),
            // A row that this write path stored must read back.
            Err(error) if !case.stored => {
                panic!("the read path refused a row that the write path stored: {error}")
            }
            Err(error) => return verdict_only(Verdict::Unreadable(error)),
        }
    }
    if !case.stored {
        assert_eq!(
            to_json(&read),
            to_json(history),
            "the read path did not return what the write path stored"
        );
    }

    let program = case.program.clone().unwrap_or_else(|| mirror(&read));
    let program_json = serde_json::to_string(&program).expect("a program serializes");
    let mut replayer = WorkflowReplayer::new();
    // A deadline probe reads the clock only when the run has a deadline.
    if read.iter().any(is_deadline_probe) {
        replayer = replayer.with_execution_timeout(chrono::Duration::hours(1));
    }
    let report = replayer
        .register_fn(WORKFLOW, program_workflow)
        .with_execution_id(ExecutionId::from_uuid(uuid::Uuid::nil()))
        .with_context_headers(HashMap::from([(PROGRAM_HEADER.to_string(), program_json)]))
        .replay_from_events(read)
        .await;
    assert_no_contained_panic(&report);
    (
        Verdict::Replayed(format!("{:?}", report.status)),
        report.to_string(),
    )
}

fn verdict_only(verdict: Verdict) -> (Verdict, String) {
    let report = format!("{verdict:?}");
    (verdict, report)
}

/// The write path of `store::append_event`: codec encode, then offload.
async fn write_row(
    codecs: &PayloadCodecs,
    offloader: &PayloadOffloader,
    event: &WorkflowEvent,
) -> Result<Value, String> {
    let mut row = codecs.encode_event(event).map_err(|e| e.to_string())?;
    offloader
        .offload_event_value(&mut row)
        .await
        .map_err(|e| e.to_string())?;
    Ok(row)
}

/// The read path of `store::load_history_inflated`: inflate, then codec
/// decode.
async fn read_row(
    codecs: &PayloadCodecs,
    offloader: &PayloadOffloader,
    mut row: Value,
) -> Result<WorkflowEvent, String> {
    offloader
        .inflate_event_value(&mut row)
        .await
        .map_err(|e| e.to_string())?;
    codecs.decode_event(row).map_err(|e| e.to_string())
}

/// The operator read path, such as history export, decodes with
/// `decode_value_lossy`. That walk looks for envelopes at every depth, so it
/// must also return the stored value and mark nothing undecodable.
async fn assert_lossy_read(codecs: &PayloadCodecs, offloader: &PayloadOffloader, mut row: Value) {
    let Ok(()) = offloader.inflate_event_value(&mut row).await else {
        // The strict read below reports the error.
        return;
    };
    let mut lossy = row.clone();
    let outcome = codecs.decode_value_lossy(&mut lossy);
    let strict = codecs
        .decode_event(row)
        .ok()
        .map(|event| serde_json::to_value(event).expect("a decoded event serializes"));
    assert_eq!(
        outcome.failed, 0,
        "the lossy read marked a stored value undecodable"
    );
    if let Some(strict) = strict {
        assert_eq!(lossy, strict, "the lossy read and the strict read disagree");
    }
}

/// The executor contains a workflow panic as a `HandlerPanic` failure.
/// [`program_workflow`] never panics, so such a failure is an engine panic.
/// A `Fail` op that returns the same text is not one.
fn assert_no_contained_panic(report: &ReplayReport) {
    let returned = RETURNED_FAILURE.with(std::cell::RefCell::take);
    let Some(message) = report.failure_message() else {
        return;
    };
    // Only the failure that the program returned is a programmed one. Text
    // in an op that never ran must not hide a real panic.
    let from_program = returned.as_deref() == Some(message);
    let panicked = crate::failure::parse_workflow_typed_payload(message)
        .is_some_and(|failure| failure.error_type == ERROR_TYPE_HANDLER_PANIC);
    assert!(
        from_program || !panicked,
        "the engine panicked inside the replayed workflow: {message}"
    );
}

/// The program that issues the command behind each recorded event.
///
/// Only events that a command creates map to an op. The replayer matches
/// the other events itself, or reports them. The match lists every variant,
/// so a new variant needs a decision here: an op, or `None`.
#[must_use]
///
/// Consecutive commands that park, with no other event between them, came
/// from one batch, such as a `join!` of two activities. An immediate command
/// that follows a parking one in the same run joins that batch too. A batch
/// runs as one [`Op::Concurrent`], so the replayer matches all of it.
///
/// A `race:{seq}` marker opens a race, and `race_winner:{seq}` closes it.
/// The commands between them become one [`Op::Race`]. The race cancels its
/// losing timers itself, so their `TimerCancelled` events map to no op.
///
/// A `WorkflowFailed` that a later `WorkflowRedriven` supersedes maps to no
/// op. The replayer skips such a failure too.
pub fn mirror(history: &[WorkflowEvent]) -> Vec<Op> {
    let armed = armed_timer_starts(history);
    let last_redrive = history
        .iter()
        .rposition(|event| matches!(event, WorkflowEvent::WorkflowRedriven { .. }));
    // Records that a redrive superseded: the failure, abandoned dispatches
    // and the rest of that cycle's tail. The matcher passes over them, so
    // the mirror does too.
    let superseded: HashSet<usize> = last_redrive.map_or_else(HashSet::new, |redrive| {
        let matcher = HistoryMatcher::new(history.to_vec());
        (0..redrive)
            .filter(|i| matcher.is_transparent(*i))
            .collect()
    });
    let mut race_timers: HashSet<&str> = HashSet::new();
    // Commands that a fan-out or race claimed. They are passed over, so the
    // group can share a batch with a sibling command.
    let mut claimed: HashSet<usize> = HashSet::new();
    // Other events that a race claimed, such as the signal that won.
    let mut silenced: HashSet<usize> = HashSet::new();
    // Signal waits that a `__signal_timeout:` timer bounds, by signal name.
    // The op takes the signal that wins, or the timer fire.
    let mut timeouts: HashMap<&str, usize> = HashMap::new();
    let mut program = Vec::new();
    let mut batch = Batch::default();
    let mut index = 0;
    while index < history.len() {
        let event = &history[index];
        index += 1;
        let claimed_before = claimed.clone();
        let op = match event {
            _ if superseded.contains(&(index - 1)) => continue,
            WorkflowEvent::MarkerRecorded { name, details } if name.starts_with("fan_out:") => {
                // The count comes from the input. Padding allocates that many
                // items, so a huge count would exhaust memory in the harness.
                let count = details.as_u64().and_then(|n| usize::try_from(n).ok());
                let count = count.unwrap_or(0).min(MAX_FAN_OUT_ITEMS);
                Some(mirror_fan_out(history, index, count, &mut claimed))
            }
            _ if claimed.remove(&(index - 1)) => continue,
            _ if silenced.remove(&(index - 1)) => None,
            WorkflowEvent::MarkerRecorded { name, details } if race_seq(name).is_some() => {
                let count = details.as_u64().and_then(|n| usize::try_from(n).ok());
                let race = RaceOpen {
                    marker: name,
                    count: count.unwrap_or(0),
                };
                let mut taken = Taken {
                    commands: &mut claimed,
                    others: &mut silenced,
                    race_timers: &mut race_timers,
                };
                Some(mirror_race(history, index, &race, &mut taken))
            }
            WorkflowEvent::TimerCancelled { timer_id }
                if race_timers.contains(timer_id.as_str()) =>
            {
                None
            }
            _ => match paired_op(history, index, &mut claimed, &mut timeouts) {
                Paired::Mapped(op) => op,
                Paired::Other => mirror_event(event, armed.contains(&(index - 1))),
            },
        };
        match op {
            Some(op) if op.parks() || (!batch.is_empty() && op.is_immediate()) => {
                // A group op waits for the commands that it claimed.
                let mut keys: HashSet<Pending> = pending_key(event).into_iter().collect();
                for i in claimed.difference(&claimed_before) {
                    keys.extend(pending_key(&history[*i]));
                }
                // A signal timeout also settles when its signal wins.
                if let Op::SignalTimeout { name, .. } = &op {
                    keys.insert(Pending::SignalWait(name.clone()));
                }
                let any = matches!(
                    op,
                    Op::Race { .. } | Op::ChildTimeout { .. } | Op::SignalTimeout { .. }
                );
                let fail_fast = matches!(
                    op,
                    Op::FanOut { collect: false, .. } | Op::ChildFanOut { collect: false, .. }
                );
                let waits = (!keys.is_empty()).then_some(Waits {
                    keys,
                    any,
                    fail_fast,
                });
                // The event of a signal or a mutex grant is the outcome of
                // its wait, so its branch goes on with the next command.
                let arrived = matches!(op, Op::Signal { .. } | Op::Mutex { .. });
                batch.push(op, waits);
                if arrived {
                    batch.resume_last();
                }
            }
            None if batch.settle(event) => {}
            other => {
                batch.flush(&mut program);
                program.extend(other);
            }
        }
    }
    batch.flush(&mut program);
    program
}

/// What a batch member waits for: the outcome of an activity, a child, a
/// timer, an external operation, or the signal of a signal timeout.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Pending {
    Activity(ActivityExecId),
    Child(ExecutionId),
    Timer(String),
    Signal(ExternalSignalId),
    Cancel(ExternalCancelId),
    Await(ExternalAwaitId),
    SignalWait(String),
}

/// The outcome that the command `event` waits for, if any.
fn pending_key(event: &WorkflowEvent) -> Option<Pending> {
    match event {
        WorkflowEvent::ActivityScheduled { activity_id, .. }
        | WorkflowEvent::ActivityAwaitingExternal { activity_id, .. } => {
            Some(Pending::Activity(*activity_id))
        }
        WorkflowEvent::ChildWorkflowStarted { child_id, .. } => Some(Pending::Child(*child_id)),
        WorkflowEvent::TimerStarted { timer_id, .. } => {
            Some(Pending::Timer(timer_id.as_str().to_string()))
        }
        WorkflowEvent::ExternalSignalRequested { signal_id, .. } => {
            Some(Pending::Signal(*signal_id))
        }
        WorkflowEvent::ExternalCancelRequested { cancel_id, .. } => {
            Some(Pending::Cancel(*cancel_id))
        }
        WorkflowEvent::ExternalAwaitRequested { await_id, .. } => Some(Pending::Await(*await_id)),
        _ => None,
    }
}

/// The outcome that `event` reports, if it is one.
fn settled_key(event: &WorkflowEvent) -> Option<Pending> {
    match event {
        WorkflowEvent::ActivityCompleted { activity_id, .. }
        | WorkflowEvent::ActivityFailed { activity_id, .. }
        | WorkflowEvent::ActivityTimedOut { activity_id, .. }
        | WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
        | WorkflowEvent::ActivityFailedExternally { activity_id, .. } => {
            Some(Pending::Activity(*activity_id))
        }
        WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
        | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => Some(Pending::Child(*child_id)),
        WorkflowEvent::TimerFired { timer_id } => {
            Some(Pending::Timer(timer_id.as_str().to_string()))
        }
        WorkflowEvent::ExternalSignalDelivered { signal_id }
        | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
            Some(Pending::Signal(*signal_id))
        }
        WorkflowEvent::ExternalCancelDelivered { cancel_id }
        | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
            Some(Pending::Cancel(*cancel_id))
        }
        WorkflowEvent::ExternalAwaitResolved { await_id, .. }
        | WorkflowEvent::ExternalAwaitFailed { await_id, .. } => Some(Pending::Await(*await_id)),
        // A plain signal maps to an op. Only a signal that a signal timeout
        // or a race took reaches this match.
        WorkflowEvent::SignalReceived { signal_name, .. } => {
            Some(Pending::SignalWait(signal_name.clone()))
        }
        _ => None,
    }
}

/// The activity that `event` reports progress for, if it is a start, a
/// heartbeat or a deadline extension.
const fn progress_key(event: &WorkflowEvent) -> Option<Pending> {
    match event {
        WorkflowEvent::ActivityStarted { activity_id, .. }
        | WorkflowEvent::ActivityHeartbeat { activity_id, .. }
        | WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, .. } => {
            Some(Pending::Activity(*activity_id))
        }
        _ => None,
    }
}

/// The outcomes that one batch branch waits for. With `any`, the first
/// outcome settles the branch, as for a race. With `fail_fast`, the first
/// failure settles it too, as for a fail-fast fan-out. Otherwise it waits
/// for all.
struct Waits {
    keys: HashSet<Pending>,
    any: bool,
    fail_fast: bool,
}

/// The commands of one decision, as the branches of a `join!`.
///
/// A batch stays open past the outcome of one member while another member
/// still waits. A command right after that outcome came from the same
/// branch, as in `join!(slow, async { fast.await; next.await })`, so it
/// joins that branch as a sequence. A received signal or a mutex grant
/// settles its own branch the same way. An activity start or heartbeat
/// leaves the batch as it is. The batch closes at any other event, or once
/// every member has settled.
#[derive(Default)]
struct Batch {
    branches: Vec<Vec<Op>>,
    /// What each branch waits for. `None` once it settled.
    pending: Vec<Option<Waits>>,
    /// The branch that settled last, while another one still waits.
    resumed: Option<usize>,
}

impl Batch {
    const fn is_empty(&self) -> bool {
        self.branches.is_empty()
    }

    fn push(&mut self, op: Op, waits_for: Option<Waits>) {
        if let Some(branch) = self.resumed {
            self.branches[branch].push(op);
            // An immediate op does not park, so the branch goes on with the
            // next command. A parking one ends the resume.
            if waits_for.is_some() {
                self.pending[branch] = waits_for;
                self.resumed = None;
            }
        } else {
            self.branches.push(vec![op]);
            self.pending.push(waits_for);
        }
    }

    /// Lets the branch that the last push touched go on with the next
    /// command.
    const fn resume_last(&mut self) {
        if self.resumed.is_none() {
            self.resumed = self.branches.len().checked_sub(1);
        }
    }

    /// Settles the member that `event` reports. True while the batch stays
    /// open; false when `event` should close it.
    fn settle(&mut self, event: &WorkflowEvent) -> bool {
        if let Some(key) = progress_key(event) {
            // Progress of a member that still waits does not settle it.
            return self
                .pending
                .iter()
                .any(|p| p.as_ref().is_some_and(|w| w.keys.contains(&key)));
        }
        let Some(key) = settled_key(event) else {
            return false;
        };
        let Some(branch) = self
            .pending
            .iter()
            .position(|p| p.as_ref().is_some_and(|w| w.keys.contains(&key)))
        else {
            return false;
        };
        let failed = matches!(
            event,
            WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
        );
        let settled = self.pending[branch].as_mut().is_some_and(|w| {
            w.keys.remove(&key);
            w.any || w.keys.is_empty() || (w.fail_fast && failed)
        });
        if settled {
            self.pending[branch] = None;
            self.resumed = Some(branch);
        }
        self.pending.iter().any(Option::is_some)
    }

    fn flush(&mut self, program: &mut Vec<Op>) {
        let mut ops: Vec<Op> = std::mem::take(&mut self.branches)
            .into_iter()
            .map(|mut branch| {
                if branch.len() == 1 {
                    branch.remove(0)
                } else {
                    Op::Sequence { ops: branch }
                }
            })
            .collect();
        self.pending.clear();
        self.resumed = None;
        match ops.len() {
            0 => {}
            1 => program.append(&mut ops),
            _ => program.push(Op::Concurrent { ops }),
        }
    }
}

/// Builds the fan-out op that a `fan_out:{n}` marker opens. The next `count`
/// activity schedules, or child starts, are its items. Their indexes go to
/// `claimed`, so the main loop does not mirror them again.
fn mirror_fan_out(
    history: &[WorkflowEvent],
    start: usize,
    count: usize,
    claimed: &mut HashSet<usize>,
) -> Op {
    let mut group = FanOutGroup::default();
    // The first wave is the run of schedules right after the marker. A
    // windowed group starts its next wave only once every item settled, so
    // a schedule while an item still runs is not a refill.
    let mut first_wave = true;
    let mut outstanding = 0usize;
    // Schedules claimed in the current refill wave, which runs back to back.
    let mut wave = 0usize;
    let first_cap = first_wave_cap(history, start, count);
    let mut siblings = HashSet::new();
    for (index, event) in history.iter().enumerate().skip(start) {
        if group.activities.len() + group.children.len() >= count {
            break;
        }
        let open = if first_wave {
            group.first_wave_len < first_cap
        } else {
            (outstanding == 0 || wave > 0) && wave < group.first_wave_len
        };
        let claims_schedule = open
            && matches!(
                event,
                WorkflowEvent::ActivityScheduled { .. }
                    | WorkflowEvent::ChildWorkflowStarted { .. }
            );
        if !claims_schedule && !first_wave {
            wave = 0;
        }
        match event {
            // A sibling command in the same `join!`, past the first wave.
            WorkflowEvent::ActivityScheduled { activity_id, .. } if first_wave && !open => {
                siblings.insert(*activity_id);
                continue;
            }
            event if activity_outcome(event).is_some_and(|(id, _)| siblings.contains(&id)) => {
                continue;
            }
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name,
                input,
                queue,
            } if open && group.children.is_empty() => {
                group
                    .activities
                    .push((name.clone(), input.clone(), queue.clone()));
                group.activity_ids.insert(*activity_id);
            }
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                input,
            } if open && group.activities.is_empty() => {
                group.children.push((workflow_name.clone(), input.clone()));
                group.child_ids.insert(*child_id);
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
                if group.activity_ids.contains(activity_id) =>
            {
                first_wave = false;
                outstanding = outstanding.saturating_sub(1);
                continue;
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
                if group.child_ids.contains(child_id) =>
            {
                first_wave = false;
                outstanding = outstanding.saturating_sub(1);
                continue;
            }
            WorkflowEvent::ActivityStarted { activity_id, .. }
            | WorkflowEvent::ActivityHeartbeat { activity_id, .. }
                if group.activity_ids.contains(activity_id) =>
            {
                first_wave = false;
                continue;
            }
            // A failed item ends a fail-fast group. A collect-all group goes
            // on, and its next wave is a full run of schedules.
            WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
                if group.activity_ids.contains(activity_id)
                    && (group.collect
                        || next_wave_is_full(
                            &history[index + 1..],
                            &group.activity_ids,
                            group.first_wave_len.min(count - group.activities.len()),
                        )) =>
            {
                group.collect = true;
                first_wave = false;
                outstanding = outstanding.saturating_sub(1);
                continue;
            }
            // A signal is buffered, so it can arrive between the group's items.
            WorkflowEvent::SignalReceived { .. } => continue,
            // Any other event is not part of the group, so a later schedule
            // is the caller's own.
            _ => break,
        }
        if first_wave {
            group.first_wave_len += 1;
        } else {
            wave += 1;
        }
        outstanding += 1;
        claimed.insert(index);
    }
    group.into_op(&history[start..], count)
}

/// The items of a fan-out group, as [`mirror_fan_out`] collects them.
#[derive(Default)]
struct FanOutGroup {
    activities: Vec<(String, Value, String)>,
    children: Vec<(String, Value)>,
    activity_ids: HashSet<ActivityExecId>,
    child_ids: HashSet<ExecutionId>,
    /// The size of the first wave.
    first_wave_len: usize,
    /// True once a failed item was followed by a full wave.
    collect: bool,
}

impl FanOutGroup {
    /// The op that replays the group. `rest` starts after the marker.
    fn into_op(self, rest: &[WorkflowEvent], count: usize) -> Op {
        if !self.children.is_empty() {
            let collect = settles_after_failure(rest, &self.child_ids, child_outcome);
            return Op::ChildFanOut {
                children: pad_to(self.children, count),
                collect,
            };
        }
        let first = self.first_wave_len;
        let window = (first > 0 && first < count).then_some(first);
        let collect = if window.is_some() {
            self.collect
        } else {
            settles_after_failure(rest, &self.activity_ids, activity_outcome)
        };
        Op::FanOut {
            activities: pad_to(self.activities, count),
            window,
            collect,
        }
    }
}

/// How many schedules of the run right after a fan-out marker belong to the
/// group. A sibling command in the same `join!` can follow the first wave in
/// that run. The next wave shows the window. When it is the last wave, the
/// first one held `count - next`. When a later wave follows, the window is
/// `next`. With no next wave, the whole run counts.
fn first_wave_cap(history: &[WorkflowEvent], start: usize, count: usize) -> usize {
    let is_schedule = |e: &WorkflowEvent| {
        matches!(
            e,
            WorkflowEvent::ActivityScheduled { .. } | WorkflowEvent::ChildWorkflowStarted { .. }
        )
    };
    let is_signal = |e: &WorkflowEvent| matches!(e, WorkflowEvent::SignalReceived { .. });
    let rest = &history[start.min(history.len())..];
    let first = rest.iter().take_while(|e| is_schedule(e) || is_signal(e));
    let run = first.filter(|e| is_schedule(e)).count();
    // A wave starts only once every scheduled item settled. A schedule while
    // an item still runs is the caller's own command, such as a follow-up
    // after a fail-fast error.
    let mut open: HashSet<Pending> = rest
        .iter()
        .take_while(|e| is_schedule(e) || is_signal(e))
        .filter_map(pending_key)
        .collect();
    let mut waves = Vec::new();
    let mut current = 0;
    for event in rest.iter().skip_while(|e| is_schedule(e) || is_signal(e)) {
        match event {
            e if is_schedule(e) && current == 0 && !open.is_empty() => break,
            e if is_schedule(e) => {
                current += 1;
                open.extend(pending_key(e));
            }
            e if is_signal(e) => {}
            e if activity_outcome(e).is_some() || child_outcome(e).is_some() => {
                if let Some(key) = settled_key(e) {
                    open.remove(&key);
                }
                if current > 0 {
                    waves.push(current);
                    current = 0;
                }
            }
            _ => break,
        }
    }
    if current > 0 {
        waves.push(current);
    }
    match waves.as_slice() {
        // The first run holds every item, so no refill wave follows. A later
        // schedule is the caller's own.
        _ if run >= count => count,
        [next, _, ..] if *next < run => *next,
        [next] if count.saturating_sub(*next) < run && count.saturating_sub(*next) >= *next => {
            count - next
        }
        _ => run,
    }
}

/// True when `rest` holds a run of at least `need` schedules once the group's
/// own progress and terminal events are passed over. A collect-all group
/// schedules such a wave after a failed item. A fail-fast caller that catches
/// the error schedules its own work instead, which is seldom a full wave.
fn next_wave_is_full(rest: &[WorkflowEvent], group: &HashSet<ActivityExecId>, need: usize) -> bool {
    let of_group = |event: &WorkflowEvent| match event {
        WorkflowEvent::ActivityStarted { activity_id, .. }
        | WorkflowEvent::ActivityHeartbeat { activity_id, .. }
        | WorkflowEvent::ActivityCompleted { activity_id, .. }
        | WorkflowEvent::ActivityFailed { activity_id, .. }
        | WorkflowEvent::ActivityTimedOut { activity_id, .. } => group.contains(activity_id),
        _ => false,
    };
    let wave = rest
        .iter()
        .skip_while(|event| {
            of_group(event) || matches!(event, WorkflowEvent::SignalReceived { .. })
        })
        .take_while(|event| matches!(event, WorkflowEvent::ActivityScheduled { .. }))
        .count();
    need > 0 && wave >= need
}

/// What an event says about one item of a fan-out group.
enum Outcome {
    /// The item started or made progress.
    Progress,
    /// The item completed.
    Done,
    /// The item failed or timed out.
    Failed,
}

/// The activity and outcome that an event reports, if it is about one.
const fn activity_outcome(event: &WorkflowEvent) -> Option<(ActivityExecId, Outcome)> {
    match event {
        WorkflowEvent::ActivityScheduled { activity_id, .. }
        | WorkflowEvent::ActivityStarted { activity_id, .. }
        | WorkflowEvent::ActivityHeartbeat { activity_id, .. } => {
            Some((*activity_id, Outcome::Progress))
        }
        WorkflowEvent::ActivityCompleted { activity_id, .. } => Some((*activity_id, Outcome::Done)),
        WorkflowEvent::ActivityFailed { activity_id, .. }
        | WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
            Some((*activity_id, Outcome::Failed))
        }
        _ => None,
    }
}

/// The child and outcome that an event reports, if it is about one.
const fn child_outcome(event: &WorkflowEvent) -> Option<(ExecutionId, Outcome)> {
    match event {
        WorkflowEvent::ChildWorkflowStarted { child_id, .. } => {
            Some((*child_id, Outcome::Progress))
        }
        WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => Some((*child_id, Outcome::Done)),
        WorkflowEvent::ChildWorkflowFailed { child_id, .. } => Some((*child_id, Outcome::Failed)),
        _ => None,
    }
}

/// True when an item of an unbounded group failed and every item settled
/// before the next command. Only the collect-all form waits for every item.
/// A fail-fast caller goes on at the first failure, while items still run.
fn settles_after_failure<K: Copy + Eq + std::hash::Hash>(
    rest: &[WorkflowEvent],
    group: &HashSet<K>,
    outcome: fn(&WorkflowEvent) -> Option<(K, Outcome)>,
) -> bool {
    let mut settled = HashSet::new();
    let mut failed = false;
    for event in rest {
        match outcome(event) {
            Some((id, Outcome::Progress)) if group.contains(&id) => {}
            Some((id, Outcome::Done)) if group.contains(&id) => {
                settled.insert(id);
            }
            Some((id, Outcome::Failed)) if group.contains(&id) => {
                settled.insert(id);
                failed = true;
            }
            None if matches!(event, WorkflowEvent::SignalReceived { .. }) => {}
            _ => break,
        }
    }
    failed && settled.len() == group.len()
}

/// Repeats the last item until `items` holds `count` items. A windowed or
/// unfinished fan-out schedules fewer items than its marker counts. The
/// count must still match, so the last item repeats. Replay never reaches
/// the repeats, because history ends first.
fn pad_to<T: Clone>(mut items: Vec<T>, count: usize) -> Vec<T> {
    if let Some(last) = items.last().cloned() {
        items.resize(count.max(items.len()), last);
    }
    items
}

/// The `{seq}` of a `race:{seq}` marker. A `race_winner:` marker has none.
fn race_seq(name: &str) -> Option<&str> {
    name.strip_prefix("race:")
}

/// The `race:{seq}` marker that opens a race, and the branch count it holds.
struct RaceOpen<'h> {
    marker: &'h str,
    count: usize,
}

/// The error that a cancelled race loser fails with. `queue.rs` writes it.
const RACE_LOSER_ERROR: &str = "lost race to a sibling branch";

/// The signal name that no history holds. A race branch on it never wins.
const UNSEEN_SIGNAL: &str = "__fuzz_unseen_signal";

/// The most branches a mirrored race gets, whatever its marker claims.
const MAX_RACE_BRANCHES: usize = 64;

/// The most items a mirrored fan-out gets, whatever its marker claims. A
/// larger count replays as a count mismatch, which costs coverage only.
const MAX_FAN_OUT_ITEMS: usize = 1024;

/// The activity or child that a race branch started.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Branch {
    Activity(ActivityExecId),
    Child(ExecutionId),
}

/// The events that a race claims, and the race timers it starts.
struct Taken<'a, 'h> {
    /// Branch commands. The main loop passes over them.
    commands: &'a mut HashSet<usize>,
    /// Other events, such as the signal that won. They map to no op.
    others: &'a mut HashSet<usize>,
    /// Ids of the race timers. A race cancels its own losing timers.
    race_timers: &'a mut HashSet<&'h str>,
}

/// One branch command of a race, in the order the race issued it.
struct Claimed {
    /// The history index of the command.
    index: usize,
    /// The slot a race timer names in its id.
    fixed: Option<usize>,
    /// The activity or child it started, if any.
    branch: Option<Branch>,
    op: Op,
}

/// Builds the [`Op::Race`] that a `race:{seq}` marker at `start - 1` opens.
///
/// The branch commands are the run of commands right after the marker, at
/// most the branch count of the marker. A later command, such as a sibling
/// in the same `join!`, stays with the main loop. The race also claims the
/// signal that won and the `race_winner:{seq}` marker.
///
/// The race keeps the branch count of its marker. Branches keep the order in
/// which the race issued them. A race timer takes the slot in its id. The
/// race picks the lowest resolved branch, so the first finished command
/// takes the recorded winner slot, and a resolved loser takes a later slot.
/// A losing signal writes no event, so each slot left free becomes a signal
/// branch that never wins.
fn mirror_race<'h>(
    history: &'h [WorkflowEvent],
    start: usize,
    open: &RaceOpen<'_>,
    taken: &mut Taken<'_, 'h>,
) -> Op {
    let seq = race_seq(open.marker).unwrap_or_default();
    let winner_marker = format!("race_winner:{seq}");
    let timer_prefix = format!("__race:{seq}:");
    let count = open.count.min(MAX_RACE_BRANCHES);
    let mut commands = Vec::new();
    let mut finished = HashSet::new();
    let mut signal = None;
    let mut late_signal = None;
    let mut winner = None;
    let mut in_run = true;
    for (index, event) in history.iter().enumerate().skip(start) {
        let claim = in_run && commands.len() < count;
        match event {
            // Only a timer that this race started is a branch. Another timer
            // can be a sibling in the same `join!`.
            WorkflowEvent::TimerStarted { timer_id, .. }
                if claim && timer_id.as_str().starts_with(timer_prefix.as_str()) =>
            {
                taken.race_timers.insert(timer_id.as_str());
                let fixed = timer_id.as_str().strip_prefix(timer_prefix.as_str());
                let fixed = fixed.and_then(|i| i.parse::<usize>().ok());
                commands.extend(mirror_event(event, false).map(|op| Claimed {
                    index,
                    fixed,
                    branch: None,
                    op,
                }));
                taken.commands.insert(index);
            }
            WorkflowEvent::ActivityScheduled { activity_id, .. } if claim => {
                commands.extend(mirror_event(event, false).map(|op| Claimed {
                    index,
                    fixed: None,
                    branch: Some(Branch::Activity(*activity_id)),
                    op,
                }));
                taken.commands.insert(index);
            }
            WorkflowEvent::ChildWorkflowStarted { child_id, .. } if claim => {
                commands.extend(mirror_event(event, false).map(|op| Claimed {
                    index,
                    fixed: None,
                    branch: Some(Branch::Child(*child_id)),
                    op,
                }));
                taken.commands.insert(index);
            }
            WorkflowEvent::MarkerRecorded { name, details } if *name == winner_marker => {
                winner = details.as_u64().and_then(|w| usize::try_from(w).ok());
                taken.others.insert(index);
                release_late_siblings(&history[index + 1..], &finished, &mut commands, taken);
                // A released sibling frees a slot. Only then was the early
                // signal a branch of this race.
                if signal.is_none()
                    && commands.len() < count
                    && let Some((at, op)) = late_signal.take()
                {
                    signal = Some(op);
                    taken.others.insert(at);
                }
                break;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
                in_run = false;
                finished.insert(Branch::Activity(*activity_id));
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                in_run = false;
                finished.insert(Branch::Child(*child_id));
            }
            // A signal can win only when a slot has no command.
            WorkflowEvent::SignalReceived { .. } if signal.is_none() && commands.len() < count => {
                in_run = false;
                signal = mirror_event(event, false);
                taken.others.insert(index);
            }
            // The slots are full, but no claimed command finished yet. A
            // sibling may hold the signal's slot. The winner marker decides.
            WorkflowEvent::SignalReceived { .. }
                if signal.is_none() && late_signal.is_none() && finished.is_empty() =>
            {
                in_run = false;
                late_signal = mirror_event(event, false).map(|op| (index, op));
            }
            _ => in_run = false,
        }
    }
    let branches = place_race_branches(commands, &finished, signal, winner, count);
    Op::Race { branches }
}

/// Gives back to the main loop each claimed command that settles after the
/// race was decided. A losing signal writes no event, so the branch count
/// can claim a sibling command too. After the decision, a race loser only
/// fails with [`RACE_LOSER_ERROR`], so any other outcome marks a sibling.
fn release_late_siblings(
    after: &[WorkflowEvent],
    finished: &HashSet<Branch>,
    commands: &mut Vec<Claimed>,
    taken: &mut Taken<'_, '_>,
) {
    let completed_later: HashSet<Branch> = after
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityFailed { error, .. }
            | WorkflowEvent::ChildWorkflowFailed { error, .. }
                if error == RACE_LOSER_ERROR =>
            {
                None
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
                Some(Branch::Activity(*activity_id))
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => Some(Branch::Child(*child_id)),
            _ => None,
        })
        .filter(|branch| !finished.contains(branch))
        .collect();
    commands.retain(|command| {
        let sibling = command.branch.is_some_and(|b| completed_later.contains(&b));
        if sibling {
            taken.commands.remove(&command.index);
        }
        !sibling
    });
}

/// Puts race branches into slots, as [`mirror_race`] describes.
fn place_race_branches(
    commands: Vec<Claimed>,
    finished: &HashSet<Branch>,
    mut signal: Option<Op>,
    winner: Option<usize>,
    count: usize,
) -> Vec<Op> {
    let size = count.max(commands.len() + usize::from(signal.is_some()));
    let mut slots: Vec<Option<Op>> = (0..size).map(|_| None).collect();
    let winner_command = commands
        .iter()
        .position(|c| c.branch.is_some_and(|b| finished.contains(&b)));
    let winner = winner.filter(|w| *w < size);
    if winner_command.is_none()
        && let Some(w) = winner
    {
        slots[w] = signal.take();
    }
    // The winner slot waits for the winning command. Timers keep their slot.
    let reserved = winner_command.and(winner);
    let mut fixed = vec![false; commands.len()];
    for (i, command) in commands.iter().enumerate() {
        if let Some(s) = command
            .fixed
            .filter(|s| *s < size && slots[*s].is_none() && Some(*s) != reserved)
        {
            slots[s] = Some(command.op.clone());
            fixed[i] = true;
        }
    }
    let mut floor = 0;
    for (i, command) in commands.into_iter().enumerate() {
        if fixed[i] {
            floor = command.fixed.map_or(floor, |s| s + 1);
            continue;
        }
        let target = if Some(i) == winner_command {
            reserved.filter(|w| *w >= floor)
        } else {
            None
        };
        let target =
            target.or_else(|| (floor..size).find(|s| slots[*s].is_none() && Some(*s) != reserved));
        let target = target.or_else(|| (0..size).find(|s| slots[*s].is_none()));
        if let Some(s) = target {
            slots[s] = Some(command.op);
            floor = s + 1;
        }
    }
    if let Some(op) = signal {
        let after = winner.map_or(0, |w| w + 1);
        let free = (after..size).chain(0..after).find(|s| slots[*s].is_none());
        if let Some(s) = free {
            slots[s] = Some(op);
        }
    }
    slots
        .into_iter()
        .map(|slot| {
            slot.unwrap_or_else(|| Op::Signal {
                name: UNSEEN_SIGNAL.to_string(),
            })
        })
        .collect()
}

/// What [`paired_op`] made of an event.
enum Paired {
    /// The event belongs to a paired API. It maps to this op, or to none.
    Mapped(Option<Op>),
    /// Any other event.
    Other,
}

/// True for the clock capture that `should_continue_as_new` records.
fn is_deadline_probe(event: &WorkflowEvent) -> bool {
    matches!(
        event,
        WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: Some(name),
            ..
        } if name == DEADLINE_PROBE_SIDE_EFFECT_NAME
    )
}

/// The op of an event that a paired API wrote with its neighbour: a worker
/// session, a child or signal wait bounded by a timer. `next` is the index
/// after the event.
fn paired_op<'h>(
    history: &'h [WorkflowEvent],
    next: usize,
    claimed: &mut HashSet<usize>,
    timeouts: &mut HashMap<&'h str, usize>,
) -> Paired {
    match &history[next - 1] {
        WorkflowEvent::MarkerRecorded { name, details }
            if name.starts_with("saga_compensated:") =>
        {
            let count = details.as_u64().and_then(|n| usize::try_from(n).ok());
            let count = count.unwrap_or(0).min(MAX_FAN_OUT_ITEMS);
            Paired::Mapped(Some(mirror_saga(history, next, count, claimed)))
        }
        WorkflowEvent::MarkerRecorded { name, .. }
            if name.starts_with("saga_compensation_failed:") =>
        {
            Paired::Mapped(None)
        }
        WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("session:") => {
            let queue = match history.get(next) {
                Some(WorkflowEvent::ActivityScheduled { name, queue, .. })
                    if name == SESSION_ACQUIRE_ACTIVITY_NAME =>
                {
                    claimed.insert(next);
                    queue.clone()
                }
                _ => "default".to_string(),
            };
            Paired::Mapped(Some(Op::Session { queue }))
        }
        WorkflowEvent::ChildWorkflowStarted {
            workflow_name,
            input,
            ..
        } if child_timeout_secs(history.get(next), workflow_name).is_some() => {
            claimed.insert(next);
            Paired::Mapped(Some(Op::ChildTimeout {
                name: workflow_name.clone(),
                input: input.clone(),
                secs: child_timeout_secs(history.get(next), workflow_name).unwrap_or_default(),
            }))
        }
        WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id }
            if timer_id.as_str().starts_with(CHILD_TIMEOUT_TIMER_PREFIX) =>
        {
            Paired::Mapped(None)
        }
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs,
        } if signal_timeout_name(timer_id.as_str()).is_some() => {
            let name = signal_timeout_name(timer_id.as_str()).unwrap_or_default();
            *timeouts.entry(name).or_default() += 1;
            Paired::Mapped(Some(Op::SignalTimeout {
                name: name.to_string(),
                secs: *duration_secs,
            }))
        }
        WorkflowEvent::SignalReceived { signal_name, .. }
            if take_one(timeouts, signal_name.as_str()) =>
        {
            Paired::Mapped(None)
        }
        WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id }
            if signal_timeout_name(timer_id.as_str()).is_some() =>
        {
            take_one(
                timeouts,
                signal_timeout_name(timer_id.as_str()).unwrap_or_default(),
            );
            Paired::Mapped(None)
        }
        _ => Paired::Other,
    }
}

/// Builds the [`Op::SagaUnwind`] that a `saga_compensated:{seq}` marker
/// opens. The compensations are the activities the saga runs one by one
/// after the marker, at most the `count` that the marker records. Their
/// schedules go to `claimed`; a sibling schedule stays with the main loop.
fn mirror_saga(
    history: &[WorkflowEvent],
    start: usize,
    count: usize,
    claimed: &mut HashSet<usize>,
) -> Op {
    let mut compensations = Vec::new();
    let mut ids = HashSet::new();
    // A saga runs its compensations one by one. A schedule while one still
    // runs is a sibling command, such as another branch of a `join!`.
    let mut running = None;
    let mut siblings = HashSet::new();
    for (index, event) in history.iter().enumerate().skip(start) {
        match (event, activity_outcome(event)) {
            (
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name,
                    input,
                    queue,
                },
                _,
            ) => {
                if running.is_none() && compensations.len() < count {
                    compensations.push((name.clone(), input.clone(), queue.clone()));
                    ids.insert(*activity_id);
                    running = Some(*activity_id);
                    claimed.insert(index);
                } else {
                    siblings.insert(*activity_id);
                }
            }
            (_, Some((id, Outcome::Done | Outcome::Failed))) if running == Some(id) => {
                running = None;
            }
            (_, Some((id, _))) if ids.contains(&id) || siblings.contains(&id) => {}
            _ => break,
        }
    }
    Op::SagaUnwind { compensations }
}

/// The timeout of a `spawn_child_workflow_timeout` call, when `next` is the
/// `__child_timeout:{seq}:{name}` timer that it starts right after the child.
fn child_timeout_secs(next: Option<&WorkflowEvent>, workflow_name: &str) -> Option<u64> {
    let Some(WorkflowEvent::TimerStarted {
        timer_id,
        duration_secs,
    }) = next
    else {
        return None;
    };
    let rest = timer_id.as_str().strip_prefix(CHILD_TIMEOUT_TIMER_PREFIX)?;
    let (_, name) = rest.split_once(':')?;
    (name == workflow_name).then_some(*duration_secs)
}

/// The signal name in a `__signal_timeout:{seq}:{name}` timer id, which
/// `wait_for_signal_timeout` and a timer-and-signal race write.
fn signal_timeout_name(timer_id: &str) -> Option<&str> {
    let rest = timer_id.strip_prefix("__signal_timeout:")?;
    rest.split_once(':').map(|(_, name)| name)
}

/// Takes one from the count for `name`. False when the count is zero.
fn take_one(counts: &mut HashMap<&str, usize>, name: &str) -> bool {
    match counts.get_mut(name) {
        Some(n) if *n > 0 => {
            *n -= 1;
            true
        }
        _ => false,
    }
}

/// True for an event that wakes the workflow for a new decision, such as a
/// completed activity or a fired timer.
const fn is_decision_boundary(event: &WorkflowEvent) -> bool {
    matches!(
        event,
        WorkflowEvent::ActivityCompleted { .. }
            | WorkflowEvent::ActivityFailed { .. }
            | WorkflowEvent::ActivityTimedOut { .. }
            | WorkflowEvent::ChildWorkflowCompleted { .. }
            | WorkflowEvent::ChildWorkflowFailed { .. }
            | WorkflowEvent::TimerFired { .. }
            | WorkflowEvent::SignalReceived { .. }
    )
}

/// The indexes of the `TimerStarted` events that the cancellable timer API
/// wrote. Such a start is followed by a `TimerCancelled` for its id before
/// any `TimerFired`. A start with neither event is also cancellable when the
/// history ends the run, or when a later decision issues a command. A
/// classic timer parks the workflow until it fires, so it allows neither.
/// An id can be reused, so each start is judged alone.
fn armed_timer_starts(history: &[WorkflowEvent]) -> HashSet<usize> {
    let mut next_is_cancel: HashMap<&str, bool> = HashMap::new();
    let mut armed = HashSet::new();
    let mut run_ends = false;
    // A command seen later, and a decision boundary followed by a command.
    let mut command_later = false;
    let mut later_decision = false;
    for (index, event) in history.iter().enumerate().rev() {
        if is_decision_boundary(event) && command_later {
            later_decision = true;
        }
        if mirror_event(event, false).is_some() {
            command_later = true;
        }
        match event {
            WorkflowEvent::WorkflowCompleted { .. }
            | WorkflowEvent::WorkflowFailed { .. }
            | WorkflowEvent::WorkflowContinuedAsNew { .. } => run_ends = true,
            WorkflowEvent::TimerCancelled { timer_id } => {
                next_is_cancel.insert(timer_id.as_str(), true);
            }
            WorkflowEvent::TimerFired { timer_id } => {
                next_is_cancel.insert(timer_id.as_str(), false);
            }
            WorkflowEvent::TimerStarted { timer_id, .. } => {
                // The start consumes the next event, so an earlier start of a
                // reused id looks further back.
                let next = next_is_cancel.remove(timer_id.as_str());
                if next == Some(true) || (next.is_none() && (run_ends || later_decision)) {
                    armed.insert(index);
                }
            }
            _ => {}
        }
    }
    armed
}

impl Op {
    /// True for an op that waits on a later event, such as an activity
    /// result. Only such ops can share a batch. A local activity cannot
    /// join a batch, so it is not one.
    const fn parks(&self) -> bool {
        matches!(
            self,
            Self::Activity { .. }
                | Self::Timer { .. }
                | Self::Signal { .. }
                | Self::Child { .. }
                | Self::ExternalActivity { .. }
                | Self::AwaitExternal { .. }
                | Self::SignalExternal { .. }
                | Self::CancelExternal { .. }
                | Self::Mutex { .. }
                | Self::FanOut { .. }
                | Self::ChildFanOut { .. }
                | Self::SignalTimeout { .. }
                | Self::Race { .. }
                | Self::ChildTimeout { .. }
                | Self::Session { .. }
                | Self::SagaUnwind { .. }
                | Self::Sequence { .. }
        )
    }

    /// True for a command that resolves in the same decision, such as a
    /// side effect. It can share a batch that a parking command opened.
    const fn is_immediate(&self) -> bool {
        matches!(
            self,
            Self::Now
                | Self::NewUuid
                | Self::Random
                | Self::RandomF64
                | Self::RandomInt { .. }
                | Self::RandomFloat { .. }
                | Self::DagSkip { .. }
                | Self::DeadlineProbe
                | Self::Patched { .. }
                | Self::SideEffect { .. }
                | Self::DetachedChild { .. }
                | Self::ArmTimer { .. }
                | Self::CancelTimer { .. }
                | Self::Version { .. }
        )
    }
}

/// The op behind a lifecycle, activity, timer, signal or marker event.
fn mirror_event(event: &WorkflowEvent, armed_timer: bool) -> Option<Op> {
    match event {
        WorkflowEvent::ActivityScheduled {
            name, input, queue, ..
        } => Some(Op::Activity {
            name: name.clone(),
            input: input.clone(),
            queue: queue.clone(),
        }),
        WorkflowEvent::LocalActivityScheduled { name, input, .. } => Some(Op::LocalActivity {
            name: name.clone(),
            input: input.clone(),
        }),
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs,
        } if armed_timer => Some(Op::ArmTimer {
            id: timer_id.as_str().to_string(),
            secs: *duration_secs,
        }),
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs,
        } => Some(Op::Timer {
            id: timer_id.as_str().to_string(),
            secs: *duration_secs,
        }),
        WorkflowEvent::TimerCancelled { timer_id } => Some(Op::CancelTimer {
            id: timer_id.as_str().to_string(),
        }),
        WorkflowEvent::SignalReceived { signal_name, .. } => Some(Op::Signal {
            name: signal_name.clone(),
        }),
        WorkflowEvent::SideEffectRecorded { kind, name, value } => match kind {
            SideEffectKind::Now if name.as_deref() == Some(DEADLINE_PROBE_SIDE_EFFECT_NAME) => {
                Some(Op::DeadlineProbe)
            }
            SideEffectKind::Now => Some(Op::Now),
            SideEffectKind::Uuid => Some(Op::NewUuid),
            SideEffectKind::Random => Some(random_op(value)),
            SideEffectKind::Custom => name.clone().map(|name| Op::SideEffect { name }),
        },
        WorkflowEvent::MarkerRecorded { name, details } => marker_op(name, details),
        WorkflowEvent::WorkflowCompleted { output } => Some(Op::Complete {
            output: output.clone(),
        }),
        WorkflowEvent::WorkflowContinuedAsNew {
            input,
            new_workflow_type,
            ..
        } => Some(Op::ContinueAsNew {
            input: input.clone(),
            workflow_type: new_workflow_type.clone(),
        }),
        WorkflowEvent::WorkflowFailed { error, .. } => Some(Op::Fail {
            error: error.clone(),
        }),
        other => mirror_external(other),
    }
}

/// The op that captured a random value. `random_u64` writes any `u64`, and
/// `random_f64` writes a float in `[0, 1)`. Any other number came from
/// `random_range`, so a range of that one value replays it.
fn random_op(value: &Value) -> Op {
    if value.is_u64() {
        return Op::Random;
    }
    if let Some(value) = value.as_i64() {
        return Op::RandomInt { value };
    }
    match value.as_f64() {
        Some(value) if (0.0..1.0).contains(&value) => Op::RandomF64,
        Some(value) => Op::RandomFloat { value },
        None => Op::RandomF64,
    }
}

/// The op behind a `patch:`, `version:` or legacy `side_effect:` marker.
/// Other markers have none. A history from before #384 records a named
/// side effect as a `side_effect:{name}` marker, and the replayer still
/// matches it.
fn marker_op(name: &str, details: &Value) -> Option<Op> {
    if let Some(id) = name.strip_prefix("patch:") {
        return Some(Op::Patched { id: id.to_string() });
    }
    if let Some(task) = name.strip_prefix("dag_skip:") {
        return Some(Op::DagSkip {
            task: task.parse().unwrap_or_default(),
            activity: details["task"].as_str().unwrap_or_default().to_string(),
            upstreams: details["upstreams"]
                .as_array()
                .map(|u| {
                    u.iter()
                        .filter_map(|i| usize::try_from(i.as_u64()?).ok())
                        .collect()
                })
                .unwrap_or_default(),
        });
    }
    if let Some(name) = name.strip_prefix("side_effect:") {
        return Some(Op::SideEffect {
            name: name.to_string(),
        });
    }
    let change_id = name.strip_prefix("version:")?;
    let version = details.as_u64().and_then(|v| u32::try_from(v).ok());
    Some(Op::Version {
        change_id: change_id.to_string(),
        version: version.unwrap_or(0),
    })
}

/// The op behind a child, external or mutex event.
fn mirror_external(event: &WorkflowEvent) -> Option<Op> {
    match event {
        WorkflowEvent::ChildWorkflowStarted {
            workflow_name,
            input,
            ..
        } => Some(Op::Child {
            name: workflow_name.clone(),
            input: input.clone(),
        }),
        WorkflowEvent::ChildWorkflowSpawnedDetached {
            workflow_name,
            input,
            parent_close_policy,
            ..
        } => Some(Op::DetachedChild {
            name: workflow_name.clone(),
            input: input.clone(),
            policy: *parent_close_policy,
        }),
        WorkflowEvent::ActivityAwaitingExternal {
            name,
            input,
            queue,
            schedule_to_close_secs,
            ..
        } => Some(Op::ExternalActivity {
            name: name.clone(),
            input: input.clone(),
            queue: queue.clone(),
            secs: *schedule_to_close_secs,
        }),
        WorkflowEvent::ExternalSignalRequested {
            target,
            signal_name,
            payload,
            idempotency_key,
            ..
        } => Some(Op::SignalExternal {
            target: target.clone(),
            name: signal_name.clone(),
            payload: payload.clone(),
            idempotency_key: idempotency_key.clone(),
        }),
        WorkflowEvent::ExternalCancelRequested { target, .. } => Some(Op::CancelExternal {
            target: target.clone(),
        }),
        WorkflowEvent::ExternalAwaitRequested { target, .. } => {
            Some(Op::AwaitExternal { target: *target })
        }
        WorkflowEvent::MutexGranted { key, .. } => Some(Op::Mutex { key: key.clone() }),
        // Every other variant, listed so that a new one fails to compile here.
        // `mirror_event` handles the first ten.
        WorkflowEvent::ActivityScheduled { .. }
        | WorkflowEvent::LocalActivityScheduled { .. }
        | WorkflowEvent::TimerStarted { .. }
        | WorkflowEvent::SignalReceived { .. }
        | WorkflowEvent::SideEffectRecorded { .. }
        | WorkflowEvent::MarkerRecorded { .. }
        | WorkflowEvent::TimerCancelled { .. }
        | WorkflowEvent::WorkflowCompleted { .. }
        | WorkflowEvent::WorkflowFailed { .. }
        | WorkflowEvent::WorkflowContinuedAsNew { .. }
        | WorkflowEvent::WorkflowStarted { .. }
        | WorkflowEvent::WorkflowCancelled { .. }
        | WorkflowEvent::ActivityStarted { .. }
        | WorkflowEvent::ActivityCompleted { .. }
        | WorkflowEvent::ActivityFailed { .. }
        | WorkflowEvent::ActivityTimedOut { .. }
        | WorkflowEvent::ActivityHeartbeat { .. }
        | WorkflowEvent::TimerFired { .. }
        | WorkflowEvent::ChildWorkflowCompleted { .. }
        | WorkflowEvent::ChildWorkflowFailed { .. }
        | WorkflowEvent::LocalActivityCompleted { .. }
        | WorkflowEvent::LocalActivityFailed { .. }
        | WorkflowEvent::ActivityCompletedExternally { .. }
        | WorkflowEvent::ActivityFailedExternally { .. }
        | WorkflowEvent::ActivityExternalDeadlineExtended { .. }
        | WorkflowEvent::UpdateAdmitted { .. }
        | WorkflowEvent::UpdateCompleted { .. }
        | WorkflowEvent::UpdateFailed { .. }
        | WorkflowEvent::WorkflowResetFork { .. }
        | WorkflowEvent::WorkflowResetTerminated { .. }
        | WorkflowEvent::LocalActivityExhausted { .. }
        | WorkflowEvent::ExternalSignalDelivered { .. }
        | WorkflowEvent::ExternalSignalFailed { .. }
        | WorkflowEvent::ChildWorkflowCascadeApplied { .. }
        | WorkflowEvent::WorkflowExecutionTimedOut { .. }
        | WorkflowEvent::WorkflowExecutionPaused { .. }
        | WorkflowEvent::WorkflowExecutionResumed { .. }
        | WorkflowEvent::ExternalCancelDelivered { .. }
        | WorkflowEvent::ExternalCancelFailed { .. }
        | WorkflowEvent::WorkflowRedriven { .. }
        | WorkflowEvent::WorkflowRetryScheduled { .. }
        | WorkflowEvent::ExternalAwaitResolved { .. }
        | WorkflowEvent::ExternalAwaitFailed { .. } => None,
    }
}

/// A workflow that runs the program in the [`PROGRAM_HEADER`] header.
///
/// It ignores command errors and continues. The replayer still records a
/// mismatch, and the later commands reach more replay code.
fn program_workflow(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        let program: Vec<Op> = ctx
            .header(PROGRAM_HEADER)
            .map(|json| serde_json::from_str(json).expect("the program header parses"))
            .unwrap_or_default();
        for op in program {
            if let Some(result) = run_op(ctx, op).await {
                if let Err(error) = &result {
                    RETURNED_FAILURE.with(|r| r.replace(Some(error.clone())));
                }
                return result;
            }
        }
        Ok(Value::Null)
    })
}

/// Runs one op. Returns the workflow result for `Complete` and `Fail`.
async fn run_op(ctx: &WorkflowContext, op: Op) -> Option<Result<Value, String>> {
    match op {
        Op::Activity { name, input, queue } => {
            let _ = ctx.execute_activity_raw(&name, input, &queue).await;
        }
        Op::LocalActivity { name, input } => {
            let _ = ctx
                .execute_local_activity_raw(&name, input, None, None)
                .await;
        }
        Op::Timer { id, secs } => {
            let _ = ctx.timer(&id, secs).await;
        }
        Op::Signal { name } => {
            let _ = ctx.wait_for_signal(&name).await;
        }
        op @ (Op::Now
        | Op::DagSkip { .. }
        | Op::DeadlineProbe
        | Op::NewUuid
        | Op::Random
        | Op::RandomF64
        | Op::RandomInt { .. }
        | Op::RandomFloat { .. }
        | Op::Patched { .. }
        | Op::SideEffect { .. }) => run_capture_op(ctx, op),
        op @ (Op::FanOut { .. } | Op::ChildFanOut { .. } | Op::Race { .. }) => {
            run_group_op(ctx, op).await;
        }
        Op::SignalTimeout { name, secs } => {
            let timeout = std::time::Duration::from_secs(secs);
            let _ = ctx.wait_for_signal_timeout(&name, timeout).await;
        }
        Op::Complete { output } => return Some(Ok(output)),
        Op::Fail { error } => return Some(Err(error)),
        // An empty timer id is a documented caller panic.
        Op::ArmTimer { id, .. } | Op::CancelTimer { id } if id.is_empty() => {}
        Op::ArmTimer { id, secs } => {
            let _ = ctx.start_timer(&id, secs);
        }
        Op::CancelTimer { id } => {
            let _ = ctx.cancel_timer(&id);
        }
        Op::Version { change_id, version } => {
            let _ = ctx.version(&change_id, 0, version);
        }
        Op::ContinueAsNew {
            input,
            workflow_type,
        } => {
            let _ = match workflow_type {
                Some(workflow_type) => ctx.continue_as_new_as_type(&workflow_type, input).await,
                None => ctx.continue_as_new(input).await,
            };
        }
        Op::Sequence { ops } => {
            for op in ops {
                if let Some(result) = Box::pin(run_op(ctx, op)).await {
                    return Some(result);
                }
            }
        }
        Op::Concurrent { ops } => {
            let runs = ops.into_iter().map(|op| Box::pin(run_op(ctx, op)));
            // The first result in program order wins, as in a sequence.
            return futures::future::join_all(runs)
                .await
                .into_iter()
                .flatten()
                .next();
        }
        other => run_external_op(ctx, other).await,
    }
    None
}

/// Runs a fan-out or a race, an op that starts a group of commands.
async fn run_group_op(ctx: &WorkflowContext, op: Op) {
    match op {
        Op::FanOut {
            activities,
            window: None,
            collect: false,
        } => {
            let _ = ctx.execute_activity_fan_out_raw(activities).await;
        }
        Op::FanOut {
            activities,
            window: None,
            collect: true,
        } => {
            let _ = ctx.execute_activity_fan_out_collect_raw(activities).await;
        }
        Op::FanOut {
            activities,
            window: Some(window),
            collect: false,
        } => {
            let _ = ctx
                .execute_activity_fan_out_raw_windowed(activities, window)
                .await;
        }
        Op::FanOut {
            activities,
            window: Some(window),
            collect: true,
        } => {
            let _ = ctx
                .execute_activity_fan_out_collect_raw_windowed(activities, window)
                .await;
        }
        Op::ChildFanOut {
            children,
            collect: false,
        } => {
            let _ = ctx.spawn_child_workflow_fan_out_raw(children).await;
        }
        Op::ChildFanOut {
            children,
            collect: true,
        } => {
            let _ = ctx.spawn_child_workflow_fan_out_collect_raw(children).await;
        }
        Op::Race { branches } => {
            let mut race = ctx.race();
            for branch in branches {
                race = match branch {
                    Op::Activity { name, input, queue } => race.activity_raw(&name, input, &queue),
                    Op::Child { name, input } => race.child_workflow_raw(&name, input),
                    Op::Timer { secs, .. } => race.timer(std::time::Duration::from_secs(secs)),
                    Op::Signal { name } => race.signal(&name),
                    _ => race,
                };
            }
            let _ = race.run().await;
        }
        _ => {}
    }
}

/// Runs an op that captures a value in the same decision and never parks.
fn run_capture_op(ctx: &WorkflowContext, op: Op) {
    match op {
        Op::Now => {
            let _ = ctx.system_now();
        }
        Op::NewUuid => {
            let _ = ctx.new_uuid();
        }
        Op::Random => {
            let _ = ctx.random_u64();
        }
        Op::RandomF64 => {
            let _ = ctx.random_f64();
        }
        Op::RandomInt { value } => {
            let _ = ctx.random_range(value..=value);
        }
        // `gen_range` panics on a NaN bound, a documented caller error.
        Op::RandomFloat { value } if !value.is_finite() => {}
        Op::RandomFloat { value } => {
            let _ = ctx.random_range(value..=value);
        }
        // An empty patch id is a documented caller panic.
        Op::Patched { id } if id.is_empty() => {}
        Op::Patched { id } => {
            let _ = ctx.patched(&id);
        }
        Op::SideEffect { name } => {
            let _ = ctx.side_effect::<_, Value>(&name, || Value::Null);
        }
        Op::DagSkip {
            task,
            activity,
            upstreams,
        } => {
            let _ = ctx.dag_skip_marker(task, &activity, &upstreams);
        }
        Op::DeadlineProbe => {
            let _ = ctx.should_continue_as_new();
        }
        _ => {}
    }
}

/// Runs a child, external or mutex op. `run_op` and `run_capture_op` run
/// the others.
async fn run_external_op(ctx: &WorkflowContext, op: Op) {
    match op {
        Op::ChildTimeout { name, input, secs } => {
            let timeout = std::time::Duration::from_secs(secs);
            let _ = ctx
                .spawn_child_workflow_timeout(&name, input, timeout)
                .await;
        }
        Op::SagaUnwind { compensations } => {
            let mut saga = crate::saga::Saga::new(ctx);
            // A saga runs its compensations last first.
            for (name, input, queue) in compensations.into_iter().rev() {
                saga.push_compensation(move || async move {
                    ctx.execute_activity_raw(&name, input, &queue)
                        .await
                        .map(|_| ())
                });
            }
            let _ = saga.compensate_all().await;
        }
        Op::Session { queue } => {
            let _ = ctx
                .create_session(crate::context::SessionOptions::new(queue))
                .await;
        }
        Op::Child { name, input } => {
            let _ = ctx.spawn_child_workflow_raw(&name, input).await;
        }
        Op::DetachedChild {
            name,
            input,
            policy,
        } => {
            let _ = ctx.spawn_child_workflow_detached_raw(&name, input, policy);
        }
        Op::ExternalActivity {
            name,
            input,
            queue,
            secs,
        } => {
            let _ = ctx
                .execute_activity_external(&name, input, &queue, secs)
                .await;
        }
        Op::SignalExternal {
            target,
            name,
            payload,
            idempotency_key,
        } => {
            let _ = match target {
                ExternalTarget::ExecutionId(id) => {
                    ctx.signal_external_workflow_with_idempotency(
                        id,
                        &name,
                        payload,
                        idempotency_key,
                    )
                    .await
                }
                ExternalTarget::WorkflowId {
                    workflow_name,
                    workflow_id,
                } => {
                    ctx.signal_external_workflow_by_id_with_idempotency(
                        &workflow_name,
                        &workflow_id,
                        &name,
                        payload,
                        idempotency_key,
                    )
                    .await
                }
            };
        }
        Op::CancelExternal { target } => {
            let _ = match target {
                ExternalTarget::ExecutionId(id) => ctx.request_cancel_external_workflow(id).await,
                ExternalTarget::WorkflowId {
                    workflow_name,
                    workflow_id,
                } => {
                    ctx.request_cancel_external_workflow_by_id(&workflow_name, &workflow_id)
                        .await
                }
            };
        }
        Op::AwaitExternal { target } => {
            let _ = ctx.await_external_workflow_value(target).await;
        }
        Op::Mutex { key } => {
            let _ = ctx.mutex(key).acquire().await;
        }
        _ => {}
    }
}

fn codecs(codec: Codec) -> PayloadCodecs {
    let mut codecs = PayloadCodecs::default();
    if codec == Codec::Keyed {
        codecs.register(Arc::new(XorCodec));
        codecs.set_default(Arc::new(XorCodec));
        codecs
            .register_key(KEYED_CODEC_KEY_ID, Arc::new(XorCodec))
            .expect("the keyed codec registers");
    }
    codecs
}

/// A reversible non-identity codec. It flips bits, so the stored bytes
/// differ from the plaintext.
struct XorCodec;

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "fuzz-xor"
    }

    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|b| b ^ 0x5A).collect())
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.encode(encoded)
    }
}

/// An in-memory payload store with sequential keys.
#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &str {
        STORE_ID
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = {
            let mut blobs = self.blobs.lock().expect("the blob map is not poisoned");
            let key = format!("blob-{}", blobs.len());
            blobs.insert(key.clone(), bytes.to_vec());
            key
        };
        Box::pin(async move { Ok(key) })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let blob = self
            .blobs
            .lock()
            .expect("the blob map is not poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| PayloadStoreError(format!("no blob under {key:?}")));
        Box::pin(async move { blob })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.blobs
            .lock()
            .expect("the blob map is not poisoned")
            .remove(key);
        Box::pin(async { Ok(()) })
    }
}
