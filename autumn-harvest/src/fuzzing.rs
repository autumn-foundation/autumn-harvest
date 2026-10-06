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
//! runs the read history. It gets no offloader, because the read path has
//! already inflated every payload. The replayed workflow issues the commands
//! of an [`Op`] program. By default the program mirrors the history, so
//! replay goes past the first event.

use std::collections::HashMap;
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
use crate::types::ExecutionId;

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

fn number(u: &mut Unstructured<'_>) -> arbitrary::Result<Value> {
    Ok(match u.choose_index(3)? {
        0 => Value::from(u.arbitrary::<i64>()?),
        1 => Value::from(u.arbitrary::<u64>()?),
        _ => serde_json::Number::from_f64(u.arbitrary()?).map_or(Value::Null, Value::Number),
    })
}

fn text(u: &mut Unstructured<'_>) -> arbitrary::Result<String> {
    if u.arbitrary()? {
        Ok((*u.choose(DICTIONARY)?).to_string())
    } else {
        Ok(u.arbitrary::<&str>()?.to_string())
    }
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
    /// `ctx.patched`.
    Patched {
        /// Patch id.
        id: String,
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
    /// The events to store and replay.
    pub history: Vec<WorkflowEvent>,
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
    pub offload_threshold: u32,
}

const fn default_threshold() -> u32 {
    1 << 20
}

impl ReplayCase {
    /// Decodes fuzz input. Input that starts with `{` is a JSON case, as the
    /// seed files are. Other input feeds the `arbitrary` generator.
    #[must_use]
    pub fn from_fuzz_bytes(data: &[u8]) -> Option<Self> {
        if data.first() == Some(&b'{') {
            return serde_json::from_slice(data).ok();
        }
        Self::arbitrary_take_rest(Unstructured::new(data)).ok()
    }
}

/// The outcome of a case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The history has no stable JSON form, or the write path refused it.
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
fn normalize(history: &[WorkflowEvent]) -> Option<Vec<WorkflowEvent>> {
    let once = text_round_trip(history)?;
    let twice = text_round_trip(&once)?;
    (to_json(&once) == to_json(&twice)).then_some(twice)
}

fn text_round_trip(history: &[WorkflowEvent]) -> Option<Vec<WorkflowEvent>> {
    history
        .iter()
        .map(|event| {
            let text = serde_json::to_vec(event).ok()?;
            serde_json::from_slice(&text).ok()
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
            Err(error) => return verdict_only(Verdict::Unstorable(error)),
        }
    }

    let mut read = Vec::with_capacity(rows.len());
    for row in rows {
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

/// The executor contains a workflow panic as a `HandlerPanic` failure.
/// [`program_workflow`] never panics, so such a failure is an engine panic.
/// A `Fail` op that returns the same text is not one.
fn assert_no_contained_panic(report: &ReplayReport, program: &[Op]) {
    let Some(message) = report.failure_message() else {
        return;
    };
    let from_program = program
        .iter()
        .any(|op| matches!(op, Op::Fail { error } if error == message));
    assert!(
        from_program || !message.contains(ERROR_TYPE_HANDLER_PANIC),
        "the engine panicked inside the replayed workflow: {message}"
    );
}

/// The program that issues the command behind each recorded event.
///
/// Only events that a command creates map to an op. The replayer matches
/// the other events itself, or reports them.
#[must_use]
pub fn mirror(history: &[WorkflowEvent]) -> Vec<Op> {
    history
        .iter()
        .filter_map(|event| match event {
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
            } => Some(Op::Timer {
                id: timer_id.as_str().to_string(),
                secs: *duration_secs,
            }),
            WorkflowEvent::SignalReceived { signal_name, .. } => Some(Op::Signal {
                name: signal_name.clone(),
            }),
            WorkflowEvent::SideEffectRecorded { kind, .. } => match kind {
                SideEffectKind::Now => Some(Op::Now),
                SideEffectKind::Uuid => Some(Op::NewUuid),
                SideEffectKind::Random => Some(Op::Random),
                SideEffectKind::Custom => None,
            },
            WorkflowEvent::MarkerRecorded { name, .. } => name
                .strip_prefix("patch:")
                .map(|id| Op::Patched { id: id.to_string() }),
            WorkflowEvent::WorkflowCompleted { output } => Some(Op::Complete {
                output: output.clone(),
            }),
            WorkflowEvent::WorkflowFailed { error, .. } => Some(Op::Fail {
                error: error.clone(),
            }),
            _ => None,
        })
        .collect()
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
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();
        for op in program {
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
                // An empty patch id is a documented caller panic.
                Op::Patched { id } if id.is_empty() => {}
                Op::Patched { id } => {
                    let _ = ctx.patched(&id);
                }
                Op::Complete { output } => return Ok(output),
                Op::Fail { error } => return Err(error),
            }
        }
        Ok(Value::Null)
    })
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
