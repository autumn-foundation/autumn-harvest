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

use crate::context::WorkflowContext;
use crate::erase::ERASURE_TOMBSTONE_KEY;
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::failure::ERROR_TYPE_HANDLER_PANIC;
use crate::payload_codec::{
    CODEC_ENVELOPE_KEY, CodecError, PayloadCodec, PayloadCodecs, UNDECODABLE_MARKER_KEY,
};
use crate::payload_store::{
    OFFLOAD_ENVELOPE_KEY, PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use crate::telemetry::NoOpMetrics;
use crate::testing::{ReplayReport, WorkflowReplayer};
use crate::types::{ExecutionId, ExternalTarget, ParentClosePolicy};

/// The deepest JSON nesting that the generator builds.
const MAX_DEPTH: u32 = 6;

/// The most items that the generator puts in one array or object.
const MAX_ITEMS: usize = 4;

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
    /// `ctx.execute_activity_fan_out_raw` over `(name, input, queue)` items.
    FanOut {
        /// The activities of the group.
        #[arbitrary(with = fan_out_items)]
        activities: Vec<(String, Value, String)>,
    },
    /// `ctx.spawn_child_workflow_fan_out_raw` over `(name, input)` items.
    ChildFanOut {
        /// The children of the group.
        #[arbitrary(with = child_fan_out_items)]
        children: Vec<(String, Value)>,
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
        branches: Vec<Self>,
    },
    /// Runs the ops concurrently, as `join!` does.
    Concurrent {
        /// The ops of one command batch.
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
    /// The history has no stable JSON form, so storage cannot hold it.
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
    let report = WorkflowReplayer::new()
        .register_fn(WORKFLOW, program_workflow)
        .with_execution_id(ExecutionId::from_uuid(uuid::Uuid::nil()))
        .with_context_headers(HashMap::from([(PROGRAM_HEADER.to_string(), program_json)]))
        .replay_from_events(read)
        .await;
    assert_no_contained_panic(&report, &program);
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
fn assert_no_contained_panic(report: &ReplayReport, program: &[Op]) {
    let Some(message) = report.failure_message() else {
        return;
    };
    let from_program = program.iter().any(|op| op.fails_with(message));
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
    let mut race_timers: HashSet<&str> = HashSet::new();
    let mut consumed: HashSet<usize> = HashSet::new();
    let mut program = Vec::new();
    let mut batch = Vec::new();
    let mut index = 0;
    while index < history.len() {
        let event = &history[index];
        index += 1;
        let op = match event {
            WorkflowEvent::MarkerRecorded { name, details } if name.starts_with("fan_out:") => {
                let count = details.as_u64().and_then(|n| usize::try_from(n).ok());
                Some(mirror_fan_out(
                    history,
                    index,
                    count.unwrap_or(0),
                    &mut consumed,
                ))
            }
            _ if consumed.remove(&(index - 1)) => None,
            WorkflowEvent::MarkerRecorded { name, .. } if race_seq(name).is_some() => {
                let (race, next) = mirror_race(history, index, name, &mut race_timers);
                index = next;
                Some(race)
            }
            WorkflowEvent::TimerCancelled { timer_id }
                if race_timers.contains(timer_id.as_str()) =>
            {
                None
            }
            WorkflowEvent::WorkflowFailed { .. } if last_redrive.is_some_and(|r| r >= index) => {
                None
            }
            _ => mirror_event(event, armed.contains(&(index - 1))),
        };
        match op {
            Some(op) if op.parks() || (!batch.is_empty() && op.is_immediate()) => batch.push(op),
            other => {
                flush_batch(&mut program, &mut batch);
                program.extend(other);
            }
        }
    }
    flush_batch(&mut program, &mut batch);
    program
}

/// Builds the fan-out op that a `fan_out:{n}` marker opens. The next `count`
/// activity schedules, or child starts, are its items. Their indexes go to
/// `consumed`, so the main loop does not mirror them again.
fn mirror_fan_out(
    history: &[WorkflowEvent],
    start: usize,
    count: usize,
    consumed: &mut HashSet<usize>,
) -> Op {
    let mut activities = Vec::new();
    let mut children = Vec::new();
    for (index, event) in history.iter().enumerate().skip(start) {
        if activities.len() + children.len() >= count {
            break;
        }
        match event {
            WorkflowEvent::ActivityScheduled {
                name, input, queue, ..
            } if children.is_empty() => {
                activities.push((name.clone(), input.clone(), queue.clone()));
            }
            WorkflowEvent::ChildWorkflowStarted {
                workflow_name,
                input,
                ..
            } if activities.is_empty() => children.push((workflow_name.clone(), input.clone())),
            _ => continue,
        }
        consumed.insert(index);
    }
    if children.is_empty() {
        Op::FanOut { activities }
    } else {
        Op::ChildFanOut { children }
    }
}

/// The `{seq}` of a `race:{seq}` marker. A `race_winner:` marker has none.
fn race_seq(name: &str) -> Option<&str> {
    name.strip_prefix("race:")
}

/// Builds the [`Op::Race`] that a `race:{seq}` marker opens. Reads from
/// `start` up to and past the matching `race_winner:{seq}` marker. Returns
/// the op and the index after the race.
fn mirror_race<'h>(
    history: &'h [WorkflowEvent],
    start: usize,
    open: &str,
    race_timers: &mut HashSet<&'h str>,
) -> (Op, usize) {
    let winner = format!("race_winner:{}", race_seq(open).unwrap_or_default());
    let mut branches = Vec::new();
    let mut index = start;
    while index < history.len() {
        let event = &history[index];
        index += 1;
        if matches!(event, WorkflowEvent::MarkerRecorded { name, .. } if *name == winner) {
            break;
        }
        if let WorkflowEvent::TimerStarted { timer_id, .. } = event {
            race_timers.insert(timer_id.as_str());
        }
        let op = mirror_event(event, false);
        if let Some(
            op @ (Op::Activity { .. } | Op::Child { .. } | Op::Timer { .. } | Op::Signal { .. }),
        ) = op
        {
            branches.push(op);
        }
    }
    (Op::Race { branches }, index)
}

/// The indexes of the `TimerStarted` events that the cancellable timer API
/// wrote. Such a start is followed by a `TimerCancelled` for its id before
/// any `TimerFired`. An id can be reused, so each start is judged alone.
fn armed_timer_starts(history: &[WorkflowEvent]) -> HashSet<usize> {
    let mut next_is_cancel: HashMap<&str, bool> = HashMap::new();
    let mut armed = HashSet::new();
    for (index, event) in history.iter().enumerate().rev() {
        match event {
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
                if next == Some(true) {
                    armed.insert(index);
                }
            }
            _ => {}
        }
    }
    armed
}

fn flush_batch(program: &mut Vec<Op>, batch: &mut Vec<Op>) {
    match batch.len() {
        0 => {}
        1 => program.append(batch),
        _ => program.push(Op::Concurrent {
            ops: std::mem::take(batch),
        }),
    }
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
                | Self::Patched { .. }
                | Self::SideEffect { .. }
                | Self::DetachedChild { .. }
                | Self::ArmTimer { .. }
                | Self::CancelTimer { .. }
                | Self::Version { .. }
        )
    }

    /// True when this op, or an op nested in it, is `Fail` with `message`.
    fn fails_with(&self, message: &str) -> bool {
        match self {
            Self::Fail { error } => error == message,
            Self::Concurrent { ops } => ops.iter().any(|op| op.fails_with(message)),
            _ => false,
        }
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
            SideEffectKind::Now => Some(Op::Now),
            SideEffectKind::Uuid => Some(Op::NewUuid),
            SideEffectKind::Random if value.is_u64() => Some(Op::Random),
            SideEffectKind::Random => Some(Op::RandomF64),
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

/// The op behind a `patch:` or `version:` marker. Other markers have none.
fn marker_op(name: &str, details: &Value) -> Option<Op> {
    if let Some(id) = name.strip_prefix("patch:") {
        return Some(Op::Patched { id: id.to_string() });
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
        Op::FanOut { activities } => {
            let _ = ctx.execute_activity_fan_out_raw(activities).await;
        }
        Op::ChildFanOut { children } => {
            let _ = ctx.spawn_child_workflow_fan_out_raw(children).await;
        }
        // An empty patch id is a documented caller panic.
        Op::Patched { id } if id.is_empty() => {}
        Op::Patched { id } => {
            let _ = ctx.patched(&id);
        }
        Op::SideEffect { name } => {
            let _ = ctx.side_effect::<_, Value>(&name, || Value::Null);
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

/// Runs a child, external or mutex op. `run_op` runs the others.
async fn run_external_op(ctx: &WorkflowContext, op: Op) {
    match op {
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
