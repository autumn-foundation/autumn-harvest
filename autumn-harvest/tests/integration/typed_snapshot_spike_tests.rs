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
//! The machine check has three parts:
//!
//! - The stamp refuses a checkpoint with an open effect or an unread signal.
//!   A checkpoint then has no effect in flight to lose.
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

use autumn_harvest::WorkflowContext;
use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::types::{ExecutionId, TimerId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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
}

/// The effects that completed in a history prefix, by kind and key.
///
/// The engine builds the ledger from history. The author never writes it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct EffectLedger {
    /// Completed activities, by name, in order.
    activities: Vec<String>,
    /// Timers that fired or were cancelled, by id.
    timers: Vec<String>,
    /// Children that ended, by id.
    children: Vec<String>,
    /// Recorded side effects.
    side_effects: u64,
    /// Signals that the run received.
    signals: u64,
}

/// Removes the open effect `id` and returns it.
fn close(open: &mut Vec<(String, OpenEffect)>, id: &str) -> Option<OpenEffect> {
    let at = open.iter().position(|(key, _)| key == id)?;
    Some(open.remove(at).1)
}

impl EffectLedger {
    /// Builds the ledger of `history`.
    ///
    /// # Errors
    ///
    /// Returns the first effect that is still open at the end of `history`.
    fn from_history(history: &[WorkflowEvent]) -> Result<Self, OpenEffect> {
        let mut ledger = Self::default();
        // Each open effect, by its id, in the order it opened.
        let mut open: Vec<(String, OpenEffect)> = Vec::new();
        for event in history {
            match event {
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
                    close(&mut open, &activity_id.to_string());
                }
                WorkflowEvent::TimerStarted { timer_id, .. } => open.push((
                    format!("timer:{}", timer_id.as_str()),
                    OpenEffect::Timer(timer_id.as_str().to_owned()),
                )),
                WorkflowEvent::TimerFired { timer_id }
                | WorkflowEvent::TimerCancelled { timer_id } => {
                    if close(&mut open, &format!("timer:{}", timer_id.as_str())).is_some() {
                        ledger.timers.push(timer_id.as_str().to_owned());
                    }
                }
                WorkflowEvent::ChildWorkflowStarted { child_id, .. } => {
                    open.push((
                        child_id.to_string(),
                        OpenEffect::Child(child_id.to_string()),
                    ));
                }
                WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
                | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                    if close(&mut open, &child_id.to_string()).is_some() {
                        ledger.children.push(child_id.to_string());
                    }
                }
                WorkflowEvent::UpdateAdmitted { update_id, .. } => {
                    open.push((
                        update_id.to_string(),
                        OpenEffect::Update(update_id.to_string()),
                    ));
                }
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => {
                    close(&mut open, &update_id.to_string());
                }
                WorkflowEvent::SideEffectRecorded { .. } => ledger.side_effects += 1,
                WorkflowEvent::SignalReceived { .. } => ledger.signals += 1,
                _ => {}
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
    /// and the engine checks it on each load.
    fn covers(&self, ledger: &EffectLedger) -> bool {
        let _ = ledger;
        true
    }
}

/// Why a snapshot did not load.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SnapshotError {
    /// The checkpoint had an open effect.
    Open(OpenEffect),
    /// The snapshot is from a newer schema than this code.
    UnknownVersion(u32),
    /// The stored ledger differs from the ledger of the source history.
    LedgerMismatch,
    /// The loaded state does not reflect each completed effect.
    NotCovered,
    /// The state did not decode.
    Decode(String),
}

/// The wire form of a snapshot: the continue-as-new input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    state: Value,
    ledger: EffectLedger,
}

impl Snapshot {
    /// The unstamped form that a workflow passes to continue-as-new.
    fn unstamped<S: VersionedState>(state: &S) -> Value {
        json!({ "version": S::VERSION, "state": state })
    }

    /// Stamps the ledger of `history` on an unstamped snapshot.
    ///
    /// The engine side calls this when it persists the checkpoint.
    /// `unread_signals` names the signals in history that the body has not
    /// taken. The worker reads them from the replay matcher. History alone
    /// cannot show them.
    ///
    /// # Errors
    ///
    /// Refuses a history with an open effect or an unread signal, or a bad
    /// wire form.
    fn stamp(
        unstamped: &Value,
        history: &[WorkflowEvent],
        unread_signals: &[String],
    ) -> Result<Value, SnapshotError> {
        if let Some(name) = unread_signals.first() {
            return Err(SnapshotError::Open(OpenEffect::Signal(name.clone())));
        }
        let ledger = EffectLedger::from_history(history).map_err(SnapshotError::Open)?;
        let version = unstamped
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| SnapshotError::Decode("no version".into()))?;
        let state = unstamped
            .get("state")
            .cloned()
            .ok_or_else(|| SnapshotError::Decode("no state".into()))?;
        serde_json::to_value(Self {
            version,
            state,
            ledger,
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
        Self::stamp(&Self::unstamped(state), history, &[])
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
        let snapshot: Self = serde_json::from_value(raw.clone())
            .map_err(|e| SnapshotError::Decode(e.to_string()))?;
        if snapshot.version > S::VERSION {
            return Err(SnapshotError::UnknownVersion(snapshot.version));
        }
        if let Some(source) = source {
            let expected = EffectLedger::from_history(source).map_err(SnapshotError::Open)?;
            if expected != snapshot.ledger {
                return Err(SnapshotError::LedgerMismatch);
            }
        }
        let state = if snapshot.version == S::VERSION {
            snapshot.state
        } else {
            S::upgrade(snapshot.version, snapshot.state).map_err(SnapshotError::Decode)?
        };
        let state: S =
            serde_json::from_value(state).map_err(|e| SnapshotError::Decode(e.to_string()))?;
        if !state.covers(&snapshot.ledger) {
            return Err(SnapshotError::NotCovered);
        }
        Ok((state, snapshot.ledger))
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

    /// Each completed charge is in `charged`. The phase moves on after two.
    fn covers(&self, ledger: &EffectLedger) -> bool {
        let charges = ledger.activities.iter().filter(|a| *a == "charge").count();
        usize::try_from(self.charged).is_ok_and(|c| c == charges)
            && (self.charged >= 2) == (self.phase == "ship")
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

    let ledger = EffectLedger::from_history(&history).expect("no effect is open");
    assert_eq!(ledger.activities, ["charge", "charge"]);
    assert_eq!(ledger.timers, ["pause"]);
    assert_eq!(ledger.signals, 1);
}

#[test]
fn a_checkpoint_with_an_open_effect_is_refused() {
    let child = ExecutionId::new();
    let update = autumn_harvest::types::UpdateId::new();
    let cases: [(&str, Vec<WorkflowEvent>, OpenEffect); 4] = [
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
            OpenEffect::Update(update.to_string()),
        ),
    ];
    for (name, events, expected) in cases {
        let mut history = vec![started(Value::Null)];
        history.extend(events);
        assert_eq!(
            EffectLedger::from_history(&history),
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
        Snapshot::stamp(&unstamped, &history, &["cancel".to_owned()]),
        Err(SnapshotError::Open(OpenEffect::Signal("cancel".into())))
    );
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
    let raw = Snapshot::stamp(&unstamped, &v1_history, &[]).expect("the checkpoint is quiescent");

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
