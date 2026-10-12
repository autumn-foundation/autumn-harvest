//! Typed state snapshots: the R&D spike prototype (issue #2013).
//!
//! The write-up is `docs/rnd/typed-state-snapshots.md`. This file holds the
//! prototype that backs its claims. It adds no engine code.
//!
//! A snapshot here is the input of a continue-as-new successor. It holds a
//! typed state, a schema version and an effect ledger. The workflow writes
//! the state and the version. The engine side stamps the ledger from the
//! history when it persists the checkpoint, so the author never writes it.
//!
//! The machine check has four parts:
//!
//! - The stamp refuses a checkpoint with an open effect or an unread signal.
//!   A checkpoint then has no effect in flight to lose.
//! - The stamp signs the snapshot with an engine key. The loader refuses a
//!   snapshot with a wrong signature, so a caller cannot forge one in a
//!   start input.
//! - The loader refuses a ledger that differs from its source history.
//! - The loader refuses a state that does not cover its ledger. The state
//!   type states this rule. The rule catches a stale state, for example one
//!   that a wrong upgrade step builds.
//!
//! The ledger proves that an effect completed. It cannot prove that the
//! state holds the right result of that effect.
//!
//! The tests below back each claim in section 4 of the report.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use autumn_harvest::WorkflowContext;
use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::replay::{HistoryMatch, HistoryMatcher, SignalOrTimerMatch};
use autumn_harvest::types::{ExecutionId, TimerId};
use hmac::{Hmac, KeyInit, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;

// ── The prototype ────────────────────────────────────────────────────────────

/// An effect that a history opened and did not close.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpenEffect {
    Activity(String),
    Timer(String),
    Child(String),
    Update(String),
    /// A signal in history that the body has not taken yet.
    Signal(String),
    /// A command that the checkpoint decision issued. It has no event yet.
    Command(String),
}

/// The effects that completed in a history prefix, by kind and key.
///
/// The engine builds the ledger from history. The author never writes it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct EffectLedger {
    /// The ledger schema that wrote this ledger. Schema 1 had no failure
    /// lists and no teardown list. Schemas 1 and 2 listed a cancelled timer
    /// with the fired ones. An envelope with no schema field is schema 1.
    #[serde(default = "schema_1")]
    schema: u32,
    /// Completed activities, by name, in order.
    activities: Vec<String>,
    /// Activities that failed or timed out, by name, in order. Workflow
    /// code can catch a failure and change the state, so a cover rule must
    /// see it.
    #[serde(default)]
    failed_activities: Vec<String>,
    /// Timers that fired, by id.
    timers: Vec<String>,
    /// Timers that workflow code cancelled, by id. Code can branch on fired
    /// against cancelled.
    #[serde(default)]
    cancelled_timers: Vec<String>,
    /// Children that completed, by id.
    children: Vec<String>,
    /// Children that failed, by id. The parent can catch the failure and
    /// change the state.
    #[serde(default)]
    failed_children: Vec<String>,
    /// Recorded side effects.
    side_effects: u64,
    /// Signals that the run received.
    signals: u64,
    /// Updates that completed, by name, in order. An update handler can
    /// change the state, so a cover rule must see each one.
    #[serde(default)]
    updates: Vec<String>,
    /// Updates that failed, by name, in order. A handler can change the
    /// state before it fails.
    #[serde(default)]
    failed_updates: Vec<String>,
    /// Timers that a won race tore down, by id. The signature covers this
    /// list, so the loader can use it as teardown evidence. A timer that
    /// fired is not evidence of a teardown.
    #[serde(default)]
    torn_down_timers: Vec<String>,
}

/// The ledger schema that this code writes.
const LEDGER_SCHEMA: u32 = 3;

/// The schema of an envelope with no schema field.
const fn schema_1() -> u32 {
    1
}

/// Removes the open effect `id` and returns it.
fn close(open: &mut Vec<(String, OpenEffect)>, id: &str) -> Option<OpenEffect> {
    let at = open.iter().position(|(key, _)| key == id)?;
    Some(open.remove(at).1)
}

impl EffectLedger {
    /// This ledger as an older `schema` would have written it. Schema 1
    /// tracked no failures and no teardowns.
    fn project(&self, schema: u32) -> Self {
        let mut old = self.clone();
        old.schema = schema;
        if schema < 2 {
            old.failed_activities.clear();
            old.failed_updates.clear();
            old.failed_children.clear();
            old.torn_down_timers.clear();
        }
        if schema < 3 {
            // Schemas 1 and 2 listed a cancelled timer with the fired ones.
            // The order between the two kinds is lost, so compare them sorted.
            old.timers.append(&mut old.cancelled_timers);
            old.timers.sort();
        }
        old
    }

    /// Builds the ledger of `history`.
    ///
    /// # Errors
    ///
    /// `torn_down_timers` names the timers that a won race removed. That
    /// teardown writes no event, so history alone leaves them open.
    ///
    /// # Errors
    ///
    /// Returns the first effect that is still open at the end of `history`.
    fn from_history(
        history: &[WorkflowEvent],
        torn_down_timers: &[String],
    ) -> Result<Self, OpenEffect> {
        let mut ledger = Self {
            schema: LEDGER_SCHEMA,
            ..Self::default()
        };
        // Each open effect, by its id, in the order it opened.
        let mut open: Vec<(String, OpenEffect)> = Vec::new();
        for event in history {
            match event {
                // An external activity may skip `ActivityScheduled`, so it
                // opens here unless it is open already.
                WorkflowEvent::ActivityAwaitingExternal {
                    activity_id, name, ..
                } if !open.iter().any(|(key, _)| *key == activity_id.to_string()) => {
                    open.push((activity_id.to_string(), OpenEffect::Activity(name.clone())));
                }
                WorkflowEvent::ActivityScheduled {
                    activity_id, name, ..
                } => open.push((activity_id.to_string(), OpenEffect::Activity(name.clone()))),
                WorkflowEvent::ActivityCompleted { activity_id, .. }
                | WorkflowEvent::ActivityCompletedExternally { activity_id, .. } => {
                    if let Some(OpenEffect::Activity(name)) =
                        close(&mut open, &activity_id.to_string())
                    {
                        ledger.activities.push(name);
                    }
                }
                WorkflowEvent::ActivityFailed { activity_id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id, .. }
                | WorkflowEvent::ActivityFailedExternally { activity_id, .. } => {
                    if let Some(OpenEffect::Activity(name)) =
                        close(&mut open, &activity_id.to_string())
                    {
                        ledger.failed_activities.push(name);
                    }
                }
                WorkflowEvent::TimerStarted { timer_id, .. } => open.push((
                    format!("timer:{}", timer_id.as_str()),
                    OpenEffect::Timer(timer_id.as_str().to_owned()),
                )),
                WorkflowEvent::TimerFired { timer_id } => {
                    if close(&mut open, &format!("timer:{}", timer_id.as_str())).is_some() {
                        ledger.timers.push(timer_id.as_str().to_owned());
                    }
                }
                WorkflowEvent::TimerCancelled { timer_id } => {
                    if close(&mut open, &format!("timer:{}", timer_id.as_str())).is_some() {
                        ledger.cancelled_timers.push(timer_id.as_str().to_owned());
                    }
                }
                WorkflowEvent::ChildWorkflowStarted { child_id, .. } => {
                    open.push((
                        child_id.to_string(),
                        OpenEffect::Child(child_id.to_string()),
                    ));
                }
                WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
                    if close(&mut open, &child_id.to_string()).is_some() {
                        ledger.children.push(child_id.to_string());
                    }
                }
                WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                    if close(&mut open, &child_id.to_string()).is_some() {
                        ledger.failed_children.push(child_id.to_string());
                    }
                }
                WorkflowEvent::UpdateAdmitted {
                    update_id, name, ..
                } => {
                    open.push((update_id.to_string(), OpenEffect::Update(name.clone())));
                }
                WorkflowEvent::UpdateCompleted { update_id, .. } => {
                    if let Some(OpenEffect::Update(name)) = close(&mut open, &update_id.to_string())
                    {
                        ledger.updates.push(name);
                    }
                }
                WorkflowEvent::UpdateFailed { update_id, .. } => {
                    if let Some(OpenEffect::Update(name)) = close(&mut open, &update_id.to_string())
                    {
                        ledger.failed_updates.push(name);
                    }
                }
                WorkflowEvent::SideEffectRecorded { .. } => ledger.side_effects += 1,
                WorkflowEvent::SignalReceived { .. } => ledger.signals += 1,
                _ => {}
            }
        }
        for timer in torn_down_timers {
            if close(&mut open, &format!("timer:{timer}")).is_some() {
                ledger.torn_down_timers.push(timer.clone());
            }
        }
        match open.into_iter().next() {
            Some((_, effect)) => Err(effect),
            None => Ok(ledger),
        }
    }
}

/// A state type that a snapshot can carry.
///
/// `save` is `serde` serialization. `load` is deserialization after
/// [`Self::upgrade`]. These are the Golem-style save and load functions.
trait VersionedState: Serialize + DeserializeOwned {
    /// The schema version that this code writes.
    const VERSION: u32;

    /// Turns the raw state of an older `version` into this version.
    ///
    /// # Errors
    ///
    /// Returns an error when no upgrade path exists.
    fn upgrade(version: u32, raw: Value) -> Result<Value, String>;

    /// Whether this state reflects each completed effect in `ledger`.
    ///
    /// The loader calls it after the upgrade. The author states the rule,
    /// and the engine checks it on each load. It has no default, so no state
    /// type can skip the check.
    fn covers(&self, ledger: &EffectLedger) -> bool;
}

/// Why a snapshot did not load.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SnapshotError {
    /// The checkpoint had an open effect.
    Open(OpenEffect),
    /// The snapshot is from a newer schema than this code.
    UnknownVersion(u32),
    /// The ledger is from an older schema, and no source history can fill
    /// the effect kinds that it did not track.
    LedgerTooOld(u32),
    /// The stored ledger differs from the ledger of the source history.
    LedgerMismatch,
    /// The loaded state does not reflect each completed effect.
    NotCovered,
    /// The engine signature does not match, so no checkpoint wrote this
    /// snapshot.
    Forged,
    /// The state did not decode.
    Decode(String),
}

/// What the worker knows about a checkpoint that history cannot show.
///
/// The worker reads these lists from the replay matcher and the context.
#[derive(Debug, Clone, Default)]
struct ContextView {
    /// Signals in history that the body has not taken yet.
    unread_signals: Vec<String>,
    /// Commands that the checkpoint decision issued next to its
    /// continue-as-new. The executor drops them, so none reaches history.
    pending_commands: Vec<String>,
    /// Timers that a won race tore down. The teardown writes no event.
    torn_down_timers: Vec<String>,
}

/// The key that signs each stamped snapshot. A worker would use a key that
/// callers cannot read, such as a codec key.
const ENGINE_KEY: &[u8] = b"issue-2013-spike-engine-key";

/// The wire form of a snapshot: the continue-as-new input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    state: Value,
    ledger: EffectLedger,
    /// HMAC-SHA256 of the other fields under [`ENGINE_KEY`], in hex.
    mac: String,
}

/// The HMAC of one snapshot body.
///
/// It takes the JSON values of the envelope as written, not a typed form.
/// A newer loader can add defaulted fields to its types. Re-serializing an
/// older envelope would then change the signed bytes.
fn engine_mac(version: &Value, state: &Value, ledger: &Value) -> Hmac<Sha256> {
    let body = serde_json::to_vec(&(version, state, ledger)).expect("a snapshot body encodes");
    let mut mac =
        <Hmac<Sha256> as KeyInit>::new_from_slice(ENGINE_KEY).expect("HMAC takes any key length");
    mac.update(&body);
    mac
}

/// The hex signature of one envelope body.
fn sign(version: &Value, state: &Value, ledger: &Value) -> String {
    engine_mac(version, state, ledger)
        .finalize()
        .into_bytes()
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// Decodes lowercase hex. Returns `None` for any other text.
fn from_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

impl Snapshot {
    /// The unstamped form that a workflow passes to continue-as-new.
    fn unstamped<S: VersionedState>(state: &S) -> Value {
        json!({ "version": S::VERSION, "state": state })
    }

    /// Stamps the ledger of `history` on an unstamped snapshot.
    ///
    /// The engine side calls this when it persists the checkpoint. `view`
    /// gives what history alone cannot show: unread signals and torn-down
    /// race timers.
    ///
    /// # Errors
    ///
    /// Refuses a history with an open effect or an unread signal, or a bad
    /// wire form.
    fn stamp(
        unstamped: &Value,
        history: &[WorkflowEvent],
        view: &ContextView,
    ) -> Result<Value, SnapshotError> {
        if let Some(name) = view.unread_signals.first() {
            return Err(SnapshotError::Open(OpenEffect::Signal(name.clone())));
        }
        if let Some(name) = view.pending_commands.first() {
            return Err(SnapshotError::Open(OpenEffect::Command(name.clone())));
        }
        let ledger = EffectLedger::from_history(history, &view.torn_down_timers)
            .map_err(SnapshotError::Open)?;
        let version = unstamped
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| SnapshotError::Decode("no version".into()))?;
        let state = unstamped
            .get("state")
            .cloned()
            .ok_or_else(|| SnapshotError::Decode("no state".into()))?;
        let ledger_value =
            serde_json::to_value(&ledger).map_err(|e| SnapshotError::Decode(e.to_string()))?;
        let mac = sign(&Value::from(version), &state, &ledger_value);
        serde_json::to_value(Self {
            version,
            state,
            ledger,
            mac,
        })
        .map_err(|e| SnapshotError::Decode(e.to_string()))
    }

    /// Saves `state` at the end of `history`.
    ///
    /// # Errors
    ///
    /// Refuses a history with an open effect.
    fn save<S: VersionedState>(
        state: &S,
        history: &[WorkflowEvent],
    ) -> Result<Value, SnapshotError> {
        Self::stamp(&Self::unstamped(state), history, &ContextView::default())
    }

    /// Loads a snapshot under this code.
    ///
    /// With `source`, the ledger must equal the ledger of that history. The
    /// loaded state must then cover the ledger.
    ///
    /// # Errors
    ///
    /// Refuses an unknown version, a ledger mismatch, a state that does not
    /// cover its ledger, or a bad state.
    fn load<S: VersionedState>(
        raw: &Value,
        source: Option<&[WorkflowEvent]>,
    ) -> Result<(S, EffectLedger), SnapshotError> {
        // Verify the envelope as written, before any typed decode.
        let signed = raw
            .get("mac")
            .and_then(Value::as_str)
            .and_then(from_hex)
            .is_some_and(|tag| {
                let field = |name: &str| raw.get(name).cloned().unwrap_or(Value::Null);
                engine_mac(&field("version"), &field("state"), &field("ledger"))
                    .verify_slice(&tag)
                    .is_ok()
            });
        if !signed {
            return Err(SnapshotError::Forged);
        }
        let snapshot: Self = serde_json::from_value(raw.clone())
            .map_err(|e| SnapshotError::Decode(e.to_string()))?;
        if snapshot.version > S::VERSION {
            return Err(SnapshotError::UnknownVersion(snapshot.version));
        }
        if snapshot.ledger.schema > LEDGER_SCHEMA {
            return Err(SnapshotError::UnknownVersion(snapshot.ledger.schema));
        }
        let ledger = match source {
            Some(source) => {
                // Only the signed teardown list closes a timer that history
                // leaves open.
                let rebuilt = EffectLedger::from_history(source, &snapshot.ledger.torn_down_timers)
                    .map_err(SnapshotError::Open)?;
                // Check the fields that the stored schema knew, then use
                // the full ledger that the source gives.
                let mut stored = snapshot.ledger.clone();
                if stored.schema < 3 {
                    stored.timers.sort();
                }
                if rebuilt.project(snapshot.ledger.schema) != stored {
                    return Err(SnapshotError::LedgerMismatch);
                }
                rebuilt
            }
            // An older ledger cannot show the effect kinds it did not track.
            None if snapshot.ledger.schema < LEDGER_SCHEMA => {
                return Err(SnapshotError::LedgerTooOld(snapshot.ledger.schema));
            }
            None => snapshot.ledger,
        };
        let state = if snapshot.version == S::VERSION {
            snapshot.state
        } else {
            S::upgrade(snapshot.version, snapshot.state).map_err(SnapshotError::Decode)?
        };
        let state: S =
            serde_json::from_value(state).map_err(|e| SnapshotError::Decode(e.to_string()))?;
        if !state.covers(&ledger) {
            return Err(SnapshotError::NotCovered);
        }
        Ok((state, ledger))
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

/// Version 1 of the order state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OrderV1 {
    charged: u32,
}

/// Version 2 adds the phase of the order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    charged: u32,
    phase: String,
}

impl VersionedState for OrderV1 {
    const VERSION: u32 = 1;

    fn upgrade(version: u32, _raw: Value) -> Result<Value, String> {
        Err(format!("no upgrade from version {version}"))
    }

    /// Each completed charge is in `charged`.
    fn covers(&self, ledger: &EffectLedger) -> bool {
        let charges = ledger.activities.iter().filter(|a| *a == "charge").count();
        usize::try_from(self.charged).is_ok_and(|c| c == charges)
    }
}

impl VersionedState for Order {
    const VERSION: u32 = 2;

    fn upgrade(version: u32, raw: Value) -> Result<Value, String> {
        match version {
            1 => {
                let old: OrderV1 = serde_json::from_value(raw).map_err(|e| e.to_string())?;
                let phase = if old.charged >= 2 { "ship" } else { "charge" };
                Ok(json!({ "charged": old.charged, "phase": phase }))
            }
            other => Err(format!("no upgrade from version {other}")),
        }
    }

    /// Each completed charge is in `charged`. The phase is exactly
    /// `charge` below two charges and exactly `ship` from two on.
    fn covers(&self, ledger: &EffectLedger) -> bool {
        let charges = ledger.activities.iter().filter(|a| *a == "charge").count();
        let phase = if self.charged >= 2 { "ship" } else { "charge" };
        usize::try_from(self.charged).is_ok_and(|c| c == charges) && self.phase == phase
    }
}

type HandlerFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

const fn started(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: chrono::DateTime::from_timestamp(1_791_676_800, 0).expect("valid timestamp"),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

/// Version 1: charges twice, then checkpoints into a successor.
fn order_v1(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut state = OrderV1 { charged: 0 };
        while state.charged < 2 {
            ctx.execute_activity_raw("charge", json!({ "n": state.charged }), "default")
                .await
                .map_err(|e| e.to_string())?;
            state.charged += 1;
        }
        ctx.continue_as_new(Snapshot::unstamped(&state))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Version 2: audits before each charge, which changes the code before the
/// checkpoint. A loaded snapshot skips the charge phase.
fn order_v2(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut state = if input.get("version").is_some() {
            Snapshot::load::<Order>(&input, None)
                .map_err(|e| format!("{e:?}"))?
                .0
        } else {
            Order {
                charged: 0,
                phase: "charge".into(),
            }
        };
        while state.phase == "charge" {
            ctx.execute_activity_raw("audit", json!({ "n": state.charged }), "default")
                .await
                .map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("charge", json!({ "n": state.charged }), "default")
                .await
                .map_err(|e| e.to_string())?;
            state.charged += 1;
            if state.charged >= 2 {
                state.phase = "ship".into();
            }
        }
        ctx.execute_activity_raw("ship", json!({ "charged": state.charged }), "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Drives `handler` cold to its end. Each activity completes at once.
///
/// Returns the history and the final outcome.
async fn drive(handler: WorkflowHandlerFn, input: Value) -> (Vec<WorkflowEvent>, WorkflowOutcome) {
    let exec_id = ExecutionId::new();
    let mut history = vec![started(input.clone())];
    for _ in 0..16 {
        let outcome = run_workflow(exec_id, history.clone(), handler, input.clone()).await;
        let WorkflowOutcome::Suspended { commands } = &outcome else {
            return (history, outcome);
        };
        for cmd in commands {
            if let WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input,
                queue,
                ..
            } = cmd
            {
                history.push(WorkflowEvent::ActivityScheduled {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                });
                history.push(WorkflowEvent::ActivityCompleted {
                    activity_id: *activity_id,
                    output: json!({ "ok": name }),
                });
            }
        }
    }
    panic!("the run did not end in 16 decisions");
}

/// The activity names that `outcome` schedules.
fn scheduled(outcome: &WorkflowOutcome) -> Vec<String> {
    let WorkflowOutcome::Suspended { commands } = outcome else {
        return Vec::new();
    };
    commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::ScheduleActivity { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn activity_events(name: &str, completed: bool) -> Vec<WorkflowEvent> {
    let id = autumn_harvest::types::ActivityExecId::new();
    let mut events = vec![WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.into(),
        input: Value::Null,
        queue: "default".into(),
    }];
    if completed {
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    events
}

// ── Claim 1: a checkpoint needs a quiescent history ─────────────────────────

#[test]
fn a_quiescent_history_gives_a_ledger_of_its_completed_effects() {
    let mut history = vec![started(Value::Null)];
    history.extend(activity_events("charge", true));
    history.push(WorkflowEvent::TimerStarted {
        timer_id: TimerId::new("pause"),
        duration_secs: 5,
    });
    history.push(WorkflowEvent::TimerFired {
        timer_id: TimerId::new("pause"),
    });
    history.push(WorkflowEvent::SignalReceived {
        signal_name: "go".into(),
        payload: Value::Null,
    });
    history.extend(activity_events("charge", true));
    history.extend(update_events("bump"));

    let ledger = EffectLedger::from_history(&history, &[]).expect("no effect is open");
    assert_eq!(ledger.activities, ["charge", "charge"]);
    assert_eq!(ledger.timers, ["pause"]);
    assert_eq!(ledger.signals, 1);
    assert_eq!(ledger.updates, ["bump"]);
}

/// An admitted update named `name` and its completion.
fn update_events(name: &str) -> [WorkflowEvent; 2] {
    let update_id = autumn_harvest::types::UpdateId::new();
    [
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: name.into(),
            input: Value::Null,
            timestamp: chrono::DateTime::from_timestamp(1_791_676_800, 0).expect("valid timestamp"),
        },
        WorkflowEvent::UpdateCompleted {
            update_id,
            output: Value::Null,
        },
    ]
}

/// A state that an update handler changes: each `bump` adds one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tally {
    bumps: u32,
}

impl VersionedState for Tally {
    const VERSION: u32 = 1;

    fn upgrade(version: u32, _raw: Value) -> Result<Value, String> {
        Err(format!("no upgrade from version {version}"))
    }

    /// Each completed `bump` update is in `bumps`.
    fn covers(&self, ledger: &EffectLedger) -> bool {
        let bumps = ledger.updates.iter().filter(|u| *u == "bump").count();
        usize::try_from(self.bumps).is_ok_and(|b| b == bumps)
    }
}

#[test]
fn a_state_in_an_unknown_phase_is_refused() {
    // No charge has run, but the phase claims the order is done. The
    // workflow would skip its charge loop.
    let history = vec![started(Value::Null)];
    let bad = Order {
        charged: 0,
        phase: "done".into(),
    };
    let raw = Snapshot::save(&bad, &history).expect("quiescent");
    assert_eq!(
        Snapshot::load::<Order>(&raw, Some(&history)),
        Err(SnapshotError::NotCovered)
    );
}

#[test]
fn a_state_that_misses_a_completed_update_is_refused() {
    let mut history = vec![started(Value::Null)];
    history.extend(update_events("bump"));
    history.extend(update_events("bump"));
    let stale = Snapshot::save(&Tally { bumps: 1 }, &history).expect("quiescent");
    assert_eq!(
        Snapshot::load::<Tally>(&stale, Some(&history)),
        Err(SnapshotError::NotCovered),
        "a state that dropped one update must not load"
    );
    let fresh = Snapshot::save(&Tally { bumps: 2 }, &history).expect("quiescent");
    assert!(Snapshot::load::<Tally>(&fresh, Some(&history)).is_ok());
}

#[test]
fn a_checkpoint_with_an_open_effect_is_refused() {
    let child = ExecutionId::new();
    let update = autumn_harvest::types::UpdateId::new();
    let external = autumn_harvest::types::ActivityExecId::new();
    let cases: [(&str, Vec<WorkflowEvent>, OpenEffect); 5] = [
        (
            "external activity",
            vec![WorkflowEvent::ActivityAwaitingExternal {
                activity_id: external,
                token: autumn_harvest::types::ExternalActivityToken::new(),
                name: "approve_invoice".into(),
                input: Value::Null,
                queue: "default".into(),
                schedule_to_close_secs: 60,
            }],
            OpenEffect::Activity("approve_invoice".into()),
        ),
        (
            "activity",
            activity_events("charge", false),
            OpenEffect::Activity("charge".into()),
        ),
        (
            "timer",
            vec![WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("pause"),
                duration_secs: 5,
            }],
            OpenEffect::Timer("pause".into()),
        ),
        (
            "child",
            vec![WorkflowEvent::ChildWorkflowStarted {
                child_id: child,
                workflow_name: "pack".into(),
                input: Value::Null,
            }],
            OpenEffect::Child(child.to_string()),
        ),
        (
            "update",
            vec![WorkflowEvent::UpdateAdmitted {
                update_id: update,
                name: "bump".into(),
                input: Value::Null,
                timestamp: chrono::DateTime::from_timestamp(1_791_676_800, 0)
                    .expect("valid timestamp"),
            }],
            OpenEffect::Update("bump".into()),
        ),
    ];
    for (name, events, expected) in cases {
        let mut history = vec![started(Value::Null)];
        history.extend(events);
        assert_eq!(
            EffectLedger::from_history(&history, &[]),
            Err(expected.clone()),
            "{name}: an open effect must refuse the ledger"
        );
        assert_eq!(
            Snapshot::save(&OrderV1 { charged: 0 }, &history),
            Err(SnapshotError::Open(expected)),
            "{name}: an open effect must refuse the snapshot"
        );
    }
}

#[test]
fn a_checkpoint_with_an_unread_signal_is_refused() {
    // The signal is in history, but the body has not taken it. The worker
    // reads this from the replay matcher. Continue-as-new would drop it.
    let history = vec![
        started(Value::Null),
        WorkflowEvent::SignalReceived {
            signal_name: "cancel".into(),
            payload: Value::Null,
        },
    ];
    let unstamped = Snapshot::unstamped(&OrderV1 { charged: 0 });
    assert_eq!(
        Snapshot::stamp(
            &unstamped,
            &history,
            &ContextView {
                unread_signals: vec!["cancel".to_owned()],
                ..ContextView::default()
            }
        ),
        Err(SnapshotError::Open(OpenEffect::Signal("cancel".into())))
    );
}

#[test]
fn the_engine_unread_count_misses_a_late_race_signal() {
    // The timer wins the race. The signal arrives after it, and a later wait
    // can still take it. The engine count excuses that signal, so a stamp
    // view built from the count alone would let continue-as-new drop it.
    let timer_id = TimerId::new("__signal_timeout:1:approval");
    let history = vec![
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        },
        WorkflowEvent::TimerFired {
            timer_id: timer_id.clone(),
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approval".into(),
            payload: json!({"approved": true}),
        },
    ];
    let mut matcher = HistoryMatcher::new(history);
    assert_eq!(
        matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300)),
        SignalOrTimerMatch::TimerWon
    );
    assert!(
        matcher.unconsumed_signals_by_name().is_empty(),
        "the engine count excuses the late signal"
    );
    assert_eq!(
        matcher.match_signal("approval"),
        HistoryMatch::Matched {
            output: json!({"approved": true})
        },
        "a later wait can still take it, so the stamp must count it as unread"
    );
}

#[test]
fn a_checkpoint_with_a_command_in_the_same_decision_is_refused() {
    // A join can poll an activity in the decision that continues as new.
    // The dispatch is a pending command, not an event, so history alone
    // shows nothing open. Continue-as-new would drop the dispatch.
    let history = vec![started(Value::Null)];
    let unstamped = Snapshot::unstamped(&OrderV1 { charged: 0 });
    assert_eq!(
        Snapshot::stamp(
            &unstamped,
            &history,
            &ContextView {
                pending_commands: vec!["charge".to_owned()],
                ..ContextView::default()
            }
        ),
        Err(SnapshotError::Open(OpenEffect::Command("charge".into())))
    );
}

#[test]
fn a_failed_activity_is_recorded_in_the_ledger() {
    // A workflow can catch the failure and change its state, so a cover rule
    // must see it.
    let mut history = vec![started(Value::Null)];
    let mut charge = activity_events("charge", false);
    let WorkflowEvent::ActivityScheduled { activity_id, .. } = charge[0].clone() else {
        unreachable!("activity_events starts with a schedule");
    };
    charge.push(WorkflowEvent::ActivityFailed {
        activity_id,
        error: "card declined".into(),
        attempt: 1,
        error_type: "Declined".into(),
        non_retryable: true,
        details: None,
    });
    history.extend(charge);
    let ledger = EffectLedger::from_history(&history, &[]).expect("no effect is open");
    assert_eq!(ledger.activities, Vec::<String>::new());
    assert_eq!(ledger.failed_activities, ["charge"]);
}

#[test]
fn a_wait_won_by_its_signal_can_checkpoint() {
    // The signal wins, so the context tears the deadline timer down. That
    // teardown writes no event. Only the context view shows it.
    let deadline = "__signal_timeout:1:approve";
    let history = vec![
        started(Value::Null),
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(deadline),
            duration_secs: 60,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approve".into(),
            payload: Value::Null,
        },
    ];
    let unstamped = Snapshot::unstamped(&OrderV1 { charged: 0 });
    assert_eq!(
        Snapshot::stamp(&unstamped, &history, &ContextView::default()),
        Err(SnapshotError::Open(OpenEffect::Timer(deadline.into()))),
        "history alone sees an open timer"
    );
    let view = ContextView {
        torn_down_timers: vec![deadline.to_owned()],
        ..ContextView::default()
    };
    let raw = Snapshot::stamp(&unstamped, &history, &view).expect("the view closes the timer");
    let (_, ledger) =
        Snapshot::load::<OrderV1>(&raw, Some(&history)).expect("the signed ledger closes it too");
    assert_eq!(ledger.torn_down_timers, [deadline]);
    assert_eq!(ledger.timers, Vec::<String>::new());
}

#[test]
fn a_failed_child_is_recorded_apart_from_a_completed_one() {
    // The parent can catch a child failure and change its state, so a
    // cover rule must tell a failed child from a completed one.
    let child = ExecutionId::new();
    let history = vec![
        started(Value::Null),
        WorkflowEvent::ChildWorkflowStarted {
            child_id: child,
            workflow_name: "ship".into(),
            input: Value::Null,
        },
        WorkflowEvent::ChildWorkflowFailed {
            child_id: child,
            error: "no stock".into(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
    ];
    let ledger = EffectLedger::from_history(&history, &[]).expect("no effect is open");
    assert_eq!(ledger.children, Vec::<String>::new());
    assert_eq!(ledger.failed_children, [child.to_string()]);
}

/// A history with one timer that fired and one that code cancelled.
fn fired_and_cancelled_timers() -> Vec<WorkflowEvent> {
    vec![
        started(Value::Null),
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("fired"),
            duration_secs: 5,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("fired"),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("stopped"),
            duration_secs: 5,
        },
        WorkflowEvent::TimerCancelled {
            timer_id: TimerId::new("stopped"),
        },
    ]
}

#[test]
fn a_cancelled_timer_is_recorded_apart_from_a_fired_one() {
    // Workflow code can branch on fired against cancelled, so a cover rule
    // must tell the two apart.
    let ledger =
        EffectLedger::from_history(&fired_and_cancelled_timers(), &[]).expect("no effect is open");
    assert_eq!(ledger.timers, ["fired"]);
    assert_eq!(ledger.cancelled_timers, ["stopped"]);
}

#[test]
fn a_failed_update_is_recorded_in_the_ledger() {
    // A handler can change the state and then fail, so a cover rule must
    // see the failed update too.
    let mut history = vec![started(Value::Null)];
    let [admitted, _] = update_events("bump");
    let WorkflowEvent::UpdateAdmitted { update_id, .. } = admitted.clone() else {
        unreachable!("update_events starts with an admission");
    };
    history.push(admitted);
    history.push(WorkflowEvent::UpdateFailed {
        update_id,
        error: "rejected".into(),
    });
    let ledger = EffectLedger::from_history(&history, &[]).expect("no effect is open");
    assert_eq!(ledger.updates, Vec::<String>::new());
    assert_eq!(ledger.failed_updates, ["bump"]);
}

// ── Claim 2: versioned save and load ─────────────────────────────────────────

#[test]
fn a_v1_snapshot_loads_under_v2_code() {
    let mut history = vec![started(Value::Null)];
    history.extend(activity_events("charge", true));
    history.extend(activity_events("charge", true));
    let raw = Snapshot::save(&OrderV1 { charged: 2 }, &history).expect("quiescent");

    let (order, ledger) = Snapshot::load::<Order>(&raw, Some(&history)).expect("v1 upgrades");
    assert_eq!(
        order,
        Order {
            charged: 2,
            phase: "ship".into()
        }
    );
    assert_eq!(ledger.activities, ["charge", "charge"]);
}

#[test]
fn a_snapshot_from_newer_code_is_refused() {
    let history = vec![started(Value::Null)];
    let raw = Snapshot::save(
        &Order {
            charged: 0,
            phase: "charge".into(),
        },
        &history,
    )
    .expect("quiescent");
    assert_eq!(
        Snapshot::load::<OrderV1>(&raw, None),
        Err(SnapshotError::UnknownVersion(2))
    );
}

#[test]
fn a_ledger_that_differs_from_its_source_history_is_refused() {
    let mut history = vec![started(Value::Null)];
    history.extend(activity_events("charge", true));
    let raw = Snapshot::save(&OrderV1 { charged: 1 }, &history).expect("quiescent");

    // The source run charged twice, but the snapshot claims once.
    history.extend(activity_events("charge", true));
    assert_eq!(
        Snapshot::load::<Order>(&raw, Some(&history)),
        Err(SnapshotError::LedgerMismatch)
    );
}

#[test]
fn a_state_that_does_not_cover_its_ledger_is_refused() {
    // A true ledger with two charges, and a state that claims one. A resume
    // from this state would charge again.
    let mut history = vec![started(Value::Null)];
    history.extend(activity_events("charge", true));
    history.extend(activity_events("charge", true));
    let raw = Snapshot::save(&OrderV1 { charged: 1 }, &history).expect("quiescent");
    assert_eq!(
        Snapshot::load::<Order>(&raw, Some(&history)),
        Err(SnapshotError::NotCovered)
    );
}

/// A start input that claims two charges that never ran.
fn forged_snapshot() -> Value {
    json!({
        "version": 2,
        "state": { "charged": 2, "phase": "ship" },
        "ledger": {
            "activities": ["charge", "charge"],
            "timers": [],
            "children": [],
            "side_effects": 0,
            "signals": 0
        },
        "mac": "00"
    })
}

#[test]
fn a_forged_snapshot_is_refused() {
    // State and ledger agree, so the cover rule passes. Only the engine
    // signature shows that no checkpoint wrote this snapshot.
    assert_eq!(
        Snapshot::load::<Order>(&forged_snapshot(), None),
        Err(SnapshotError::Forged)
    );
}

#[test]
fn a_fired_timer_is_not_teardown_evidence_for_another_history() {
    // The snapshot's timer fired. A different history starts the same
    // reserved id and leaves it open. The loader must not close it.
    let deadline = "__signal_timeout:1:approve";
    let started_timer = WorkflowEvent::TimerStarted {
        timer_id: TimerId::new(deadline),
        duration_secs: 60,
    };
    let fired = vec![
        started(Value::Null),
        started_timer.clone(),
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new(deadline),
        },
    ];
    let open = vec![started(Value::Null), started_timer];
    let raw = Snapshot::save(&OrderV1 { charged: 0 }, &fired).expect("quiescent");
    assert_eq!(
        Snapshot::load::<OrderV1>(&raw, Some(&open)),
        Err(SnapshotError::Open(OpenEffect::Timer(deadline.into()))),
        "a fired timer must not close an open one in another history"
    );
}

#[test]
fn a_snapshot_from_an_older_ledger_schema_still_verifies() {
    // An older writer had no `failed_updates` field. It signed the envelope
    // it wrote. A newer loader must verify those bytes, not its own
    // re-serialized form.
    let history = vec![started(Value::Null)];
    let mut raw = Snapshot::save(&OrderV1 { charged: 0 }, &history).expect("quiescent");
    let ledger = raw["ledger"].as_object_mut().expect("a ledger object");
    ledger.remove("failed_updates");
    let old_ledger = raw["ledger"].clone();
    raw["mac"] = Value::String(sign(&raw["version"], &raw["state"], &old_ledger));
    assert!(
        Snapshot::load::<OrderV1>(&raw, Some(&history)).is_ok(),
        "a valid older snapshot must load"
    );
}

/// A signed envelope as a schema-1 writer made it: no schema field and no
/// failure or teardown lists.
fn schema_1_envelope(history: &[WorkflowEvent]) -> Value {
    let mut raw = Snapshot::save(&Tally { bumps: 0 }, history).expect("quiescent");
    let ledger = raw["ledger"].as_object_mut().expect("a ledger object");
    for newer in [
        "schema",
        "failed_activities",
        "failed_updates",
        "failed_children",
        "torn_down_timers",
        "cancelled_timers",
    ] {
        ledger.remove(newer);
    }
    let old_ledger = raw["ledger"].clone();
    raw["mac"] = Value::String(sign(&raw["version"], &raw["state"], &old_ledger));
    raw
}

#[test]
fn an_older_ledger_needs_its_source_history() {
    // The schema-1 writer did not track failed updates. With no source,
    // nothing can show that a failed update ran, so the loader refuses.
    let mut history = vec![started(Value::Null)];
    let [admitted, _] = update_events("bump");
    let WorkflowEvent::UpdateAdmitted { update_id, .. } = admitted.clone() else {
        unreachable!("update_events starts with an admission");
    };
    history.push(admitted);
    history.push(WorkflowEvent::UpdateFailed {
        update_id,
        error: "rejected".into(),
    });
    let raw = schema_1_envelope(&history);
    assert_eq!(
        Snapshot::load::<Tally>(&raw, None),
        Err(SnapshotError::LedgerTooOld(1))
    );
    // With the source, the loader rebuilds the ledger under the current
    // schema, so the failed update is visible again.
    let (_, ledger) = Snapshot::load::<Tally>(&raw, Some(&history)).expect("the source fills it");
    assert_eq!(ledger.failed_updates, ["bump"]);
}

/// A signed envelope as a schema-2 writer made it: one timer list that
/// holds the fired and the cancelled timers.
fn schema_2_envelope(history: &[WorkflowEvent]) -> Value {
    let mut raw = Snapshot::save(&Tally { bumps: 0 }, history).expect("quiescent");
    let ledger = raw["ledger"].as_object_mut().expect("a ledger object");
    ledger.insert("schema".into(), Value::from(2));
    let cancelled = ledger.remove("cancelled_timers").expect("a cancelled list");
    let timers = ledger["timers"].as_array_mut().expect("a timer list");
    timers.extend(
        cancelled
            .as_array()
            .expect("a cancelled list")
            .iter()
            .cloned(),
    );
    let old_ledger = raw["ledger"].clone();
    raw["mac"] = Value::String(sign(&raw["version"], &raw["state"], &old_ledger));
    raw
}

#[test]
fn a_schema_2_ledger_does_not_read_a_cancelled_timer_as_fired() {
    // Schema 2 listed a cancelled timer with the fired ones. With no
    // source, nothing can split them, so the loader refuses.
    let history = fired_and_cancelled_timers();
    let raw = schema_2_envelope(&history);
    assert_eq!(
        Snapshot::load::<Tally>(&raw, None),
        Err(SnapshotError::LedgerTooOld(2))
    );
    // With the source, the loader matches the merged list and splits it.
    let (_, ledger) = Snapshot::load::<Tally>(&raw, Some(&history)).expect("the source splits it");
    assert_eq!(ledger.timers, ["fired"]);
    assert_eq!(ledger.cancelled_timers, ["stopped"]);
}

#[tokio::test]
async fn a_forged_start_input_cannot_skip_the_charges() {
    let input = forged_snapshot();
    let outcome = run_workflow(
        ExecutionId::new(),
        vec![started(input.clone())],
        order_v2,
        input,
    )
    .await;
    assert!(
        scheduled(&outcome).is_empty(),
        "a forged snapshot must not reach the ship step: {outcome:?}"
    );
    assert!(
        matches!(outcome, WorkflowOutcome::Failed { .. }),
        "the load must fail the run: {outcome:?}"
    );
}

// ── Claim 3: no determinism needed before the checkpoint ─────────────────────

#[tokio::test]
async fn changed_code_before_the_checkpoint_resumes_from_the_snapshot() {
    // The v1 run charges twice and checkpoints. The engine side stamps it.
    let (v1_history, v1_end) = drive(order_v1, Value::Null).await;
    let WorkflowOutcome::ContinuedAsNew {
        input: unstamped, ..
    } = v1_end
    else {
        panic!("v1 must checkpoint: {v1_end:?}");
    };
    let raw = Snapshot::stamp(&unstamped, &v1_history, &ContextView::default())
        .expect("the checkpoint is quiescent");

    // A full replay of the v1 history under v2 code diverges at the audit.
    let replayed = run_workflow(
        ExecutionId::new(),
        v1_history.clone(),
        order_v2,
        Value::Null,
    )
    .await;
    assert!(
        matches!(
            replayed,
            WorkflowOutcome::Failed {
                non_deterministic_details: Some(_),
                ..
            }
        ),
        "v2 must fail the replay of the v1 history as non-deterministic: {replayed:?}"
    );

    // The successor runs v2 from the snapshot. It runs no charge again.
    let resumed = run_workflow(
        ExecutionId::new(),
        vec![started(raw.clone())],
        order_v2,
        raw.clone(),
    )
    .await;
    assert_eq!(
        scheduled(&resumed),
        ["ship"],
        "a resume must not repeat a completed charge"
    );

    // The ledger proves which charges completed before the checkpoint.
    let (_, ledger) = Snapshot::load::<Order>(&raw, Some(&v1_history)).expect("ledger");
    assert_eq!(ledger.activities, ["charge", "charge"]);
}

// ── Claim 4: an in-place snapshot must carry the context counters ────────────

/// Waits for a signal with a deadline. The engine names the timer.
fn approval(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal_timeout("approve", std::time::Duration::from_secs(60))
            .await
            .map_err(|e| e.to_string())
            .map(|payload| json!(payload))
    })
}

#[tokio::test]
async fn reserved_names_restart_in_each_new_context() {
    let mut names = BTreeMap::new();
    for run in 0..2 {
        let outcome = run_workflow(
            ExecutionId::new(),
            vec![started(Value::Null)],
            approval,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the approval wait must suspend");
        };
        let timer = commands
            .iter()
            .find_map(|cmd| match cmd {
                WorkflowCommand::StartTimer { timer_id, .. } => Some(timer_id.as_str().to_owned()),
                _ => None,
            })
            .expect("a deadline timer");
        names.insert(run, timer);
    }
    assert_eq!(
        names[&0], names[&1],
        "a new context reuses the reserved name, so an in-place resume must restore the counter"
    );
    assert!(names[&0].starts_with("__signal_timeout:"), "{}", names[&0]);
}
