//! Resident workflow state (issue #1798, step 2).
//!
//! A cold decision builds a new [`WorkflowContext`] from the full history and
//! runs the handler from the top. The replay work of a decision therefore
//! grows with history length, and a run of n decisions costs O(n²).
//!
//! A resident workflow keeps the suspended handler future and its context
//! alive between decisions. A warm decision sends the new result to the
//! parked future and polls it once more. It does not replay history, so its
//! cost does not grow with history length.
//!
//! # Which suspensions stay resident
//!
//! A warm decision must decide exactly as a cold replay would. The resident
//! path therefore accepts only a narrow set of suspensions:
//!
//! - The cycle awaits exactly one command: an activity, a timer or a signal.
//!   With one awaited command there is no race whose winner could differ.
//! - Each other command is a marker, a side effect, progress, current
//!   details, a log line or a search-attribute upsert.
//! - The context has no park token, push signal handler, held mutex, cancel
//!   request or non-determinism record, and no unread history.
//! - A signal wait is not for a name that a non-blocking claim probed with a
//!   scan that reached the end of history. A cold replay of the longer
//!   history could hand the new signal to that probe instead.
//!
//! # When a warm decision resumes
//!
//! The delta must start with the events of the last suspension, in order.
//! Then it must hold exactly one event that resolves the awaited command.
//! That event must be an activity success, a timer fire or a signal. The
//! start and heartbeat events of the awaited activity may come before it,
//! because replay skips those too. The live channels carry these payloads
//! exactly, as a replay reads them. Any other delta declines.
//!
//! A warm cycle appends the delta to the matcher as consumed events. The
//! replay position, the history length and the history scans then match a
//! cold replay.
//!
//! A decline is never an error. The worker drops the resident state and
//! runs a cold replay, which is always correct.
//!
//! # Foreign futures
//!
//! A resident future keeps the state of any foreign future it holds, such as
//! a raw `tokio::time::sleep` in a `select!`. That time passes between
//! decisions, so a warm decision can pick a different branch than a cold
//! replay. Such a workflow is not deterministic on a cold worker either. Use
//! a Harvest timer instead.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::oneshot;

use crate::context::{WorkflowCommand, WorkflowContext};
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::executor::{DriveResult, OwnedHandlerFuture, WorkflowExecuteSpanMeta, WorkflowOutcome};
use crate::info::WorkflowHandlerFn;
use crate::types::{ActivityExecId, ExecutionId, TimerId};

/// Why a resident workflow did not resume (issue #1798).
///
/// A decline is never an error. The worker drops the resident state and runs
/// a cold replay, which is always correct.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResumeDeclined {
    /// The context inputs changed since the suspension, for example the
    /// deadline, the shard or the build.
    KeyChanged,
    /// The delta does not start with the events of the last suspension.
    OwnEventsMismatch,
    /// The delta holds no event after the events of the last suspension.
    NoResolution,
    /// The delta holds more than one event after the events of the last
    /// suspension.
    ExtraEvents,
    /// The resolving event has a payload that the live channel cannot carry
    /// exactly, for example an activity failure. Holds the event type.
    InexactResolution(&'static str),
    /// The event after the own events does not resolve the awaited command.
    /// Holds the event type.
    UnexpectedEvent(&'static str),
    /// The parked future no longer waits for the result.
    ReceiverDropped,
}

/// Why a decision replays cold while resident workflows are on (issue
/// #2007).
///
/// The worker labels `harvest.workflow.resident_miss` with
/// [`Self::as_str`]. The closed set bounds the label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResidentMiss {
    /// This worker holds no resident state for the run, for example on the
    /// first decision or after an eviction.
    Cold,
    /// The last suspension awaited two or more commands.
    MultiAwait,
    /// The last suspension was part of a race, or dropped a wait.
    Race,
    /// The last suspension held or waited for a durable mutex.
    Mutex,
    /// The workflow runs in a hot-swapped module.
    HotSwap,
    /// The last suspension sent a command that the resident path does not
    /// take, for example a child workflow.
    Command,
    /// The last suspension waited for a condition.
    Condition,
    /// A push signal handler is registered.
    SignalHandler,
    /// Another context state blocked the capture, for example a cancel
    /// request.
    Context,
    /// A context input changed since the suspension.
    KeyChanged,
    /// The new events did not resolve the parked wait exactly.
    Delta,
    /// The awaited activity failed or timed out.
    Failure,
}

impl ResidentMiss {
    /// Every reason, in label order.
    pub const ALL: [Self; 12] = [
        Self::Cold,
        Self::MultiAwait,
        Self::Race,
        Self::Mutex,
        Self::HotSwap,
        Self::Command,
        Self::Condition,
        Self::SignalHandler,
        Self::Context,
        Self::KeyChanged,
        Self::Delta,
        Self::Failure,
    ];

    /// The `reason` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::MultiAwait => "multi_await",
            Self::Race => "race",
            Self::Mutex => "mutex",
            Self::HotSwap => "hot_swap",
            Self::Command => "command",
            Self::Condition => "condition",
            Self::SignalHandler => "signal_handler",
            Self::Context => "context",
            Self::KeyChanged => "key_changed",
            Self::Delta => "delta",
            Self::Failure => "failure",
        }
    }
}

impl From<&ResumeDeclined> for ResidentMiss {
    fn from(declined: &ResumeDeclined) -> Self {
        match declined {
            ResumeDeclined::KeyChanged => Self::KeyChanged,
            ResumeDeclined::InexactResolution(_) => Self::Failure,
            ResumeDeclined::OwnEventsMismatch
            | ResumeDeclined::NoResolution
            | ResumeDeclined::ExtraEvents
            | ResumeDeclined::UnexpectedEvent(_)
            | ResumeDeclined::ReceiverDropped => Self::Delta,
        }
    }
}

/// The context inputs that a resident workflow depends on (issue #1798).
///
/// The worker builds this key at each decision, mostly from the execution
/// row. A warm decision declines when any input changed, for example when a
/// pause moved the deadline or a rebalance moved the shard.
#[derive(Clone)]
pub(crate) struct ResidentKey {
    handler: WorkflowHandlerFn,
    workflow_name: String,
    workflow_id: String,
    queue_name: String,
    shard_id: Option<i64>,
    build_id: Option<String>,
    execution_timeout: Option<chrono::Duration>,
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    parent_execution_id: Option<ExecutionId>,
    context_headers: HashMap<String, String>,
}

impl ResidentKey {
    /// Builds the key of one decision.
    pub(crate) fn new(
        handler: WorkflowHandlerFn,
        span_meta: Option<&WorkflowExecuteSpanMeta>,
        context_headers: &HashMap<String, String>,
    ) -> Self {
        Self {
            handler,
            workflow_name: span_meta
                .map(|m| m.workflow_name.clone())
                .unwrap_or_default(),
            workflow_id: span_meta.map(|m| m.workflow_id.clone()).unwrap_or_default(),
            queue_name: span_meta.map(|m| m.queue_name.clone()).unwrap_or_default(),
            shard_id: span_meta.map(|m| m.shard_id),
            build_id: span_meta.and_then(|m| m.build_id.clone()),
            execution_timeout: span_meta.and_then(|m| m.execution_timeout),
            deadline_at: span_meta.and_then(|m| m.deadline_at),
            parent_execution_id: span_meta.and_then(|m| m.parent_execution_id),
            context_headers: context_headers.clone(),
        }
    }
}

impl PartialEq for ResidentKey {
    fn eq(&self, other: &Self) -> bool {
        // Two pointers to one function can differ across codegen units. A
        // false mismatch only declines, so it is safe.
        std::ptr::fn_addr_eq(self.handler, other.handler)
            && self.workflow_name == other.workflow_name
            && self.workflow_id == other.workflow_id
            && self.queue_name == other.queue_name
            && self.shard_id == other.shard_id
            && self.build_id == other.build_id
            && self.execution_timeout == other.execution_timeout
            && self.deadline_at == other.deadline_at
            && self.parent_execution_id == other.parent_execution_id
            && self.context_headers == other.context_headers
    }
}

impl std::fmt::Debug for ResidentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResidentKey")
            .field("workflow_name", &self.workflow_name)
            .field("workflow_id", &self.workflow_id)
            .field("queue_name", &self.queue_name)
            .field("shard_id", &self.shard_id)
            .field("build_id", &self.build_id)
            .finish_non_exhaustive()
    }
}

/// The one command that a resident workflow awaits, with its live channel.
enum Awaiting {
    Activity {
        activity_id: ActivityExecId,
        result_tx: oneshot::Sender<Result<Value, String>>,
    },
    Timer {
        timer_id: TimerId,
        result_tx: oneshot::Sender<()>,
    },
    Signal {
        signal_name: String,
        result_tx: oneshot::Sender<Value>,
    },
}

impl Awaiting {
    /// Takes the live channel out of `cmd` and leaves a closed one.
    ///
    /// The worker drops the drained commands after it persists them. The
    /// parked future keeps waiting, because this value now holds its sender.
    fn take_from(cmd: &mut WorkflowCommand) -> Option<Self> {
        match cmd {
            WorkflowCommand::ScheduleActivity {
                activity_id,
                result_tx,
                ..
            }
            | WorkflowCommand::WaitForActivity {
                activity_id,
                result_tx,
            } => Some(Self::Activity {
                activity_id: *activity_id,
                result_tx: std::mem::replace(result_tx, closed_sender()),
            }),
            WorkflowCommand::StartTimer {
                timer_id,
                result_tx,
                ..
            } => Some(Self::Timer {
                timer_id: timer_id.clone(),
                result_tx: std::mem::replace(result_tx, closed_sender()),
            }),
            WorkflowCommand::WaitForSignal {
                signal_name,
                result_tx,
            } => Some(Self::Signal {
                signal_name: signal_name.clone(),
                result_tx: std::mem::replace(result_tx, closed_sender()),
            }),
            _ => None,
        }
    }

    /// Whether `event` reports progress of the awaited activity.
    ///
    /// Replay skips the `ActivityStarted` and `ActivityHeartbeat` events of
    /// the activity it matches, so a warm decision skips them too.
    fn is_progress(&self, event: &WorkflowEvent) -> bool {
        let Self::Activity { activity_id, .. } = self else {
            return false;
        };
        matches!(
            event,
            WorkflowEvent::ActivityStarted { activity_id: id, .. }
                | WorkflowEvent::ActivityHeartbeat { activity_id: id, .. }
                if id == activity_id
        )
    }

    /// Sends the result in `event` to the parked future.
    ///
    /// The payload is the value that a replay of `event` returns.
    fn deliver(self, event: &WorkflowEvent) -> Result<(), ResumeDeclined> {
        let sent = match (self, event) {
            (
                Self::Activity {
                    activity_id,
                    result_tx,
                },
                WorkflowEvent::ActivityCompleted {
                    activity_id: id,
                    output,
                },
            ) if activity_id == *id => result_tx.send(Ok(output.clone())).is_ok(),
            (
                Self::Activity { activity_id, .. },
                WorkflowEvent::ActivityFailed {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityTimedOut {
                    activity_id: id, ..
                },
            ) if activity_id == *id => {
                return Err(ResumeDeclined::InexactResolution(event.type_name()));
            }
            (
                Self::Timer {
                    timer_id,
                    result_tx,
                },
                WorkflowEvent::TimerFired { timer_id: id },
            ) if timer_id == *id => result_tx.send(()).is_ok(),
            (
                Self::Signal {
                    signal_name,
                    result_tx,
                },
                WorkflowEvent::SignalReceived {
                    signal_name: name,
                    payload,
                },
            ) if signal_name == *name => result_tx.send(payload.clone()).is_ok(),
            (_, event) => return Err(ResumeDeclined::UnexpectedEvent(event.type_name())),
        };
        if sent {
            Ok(())
        } else {
            Err(ResumeDeclined::ReceiverDropped)
        }
    }
}

/// A sender whose receiver is gone.
fn closed_sender<T>() -> oneshot::Sender<T> {
    let (tx, _rx) = oneshot::channel();
    tx
}

/// An event that the worker writes for a suspension, by kind and key.
#[derive(Debug, Clone, PartialEq)]
enum OwnEvent {
    ActivityScheduled(ActivityExecId),
    TimerStarted(TimerId),
    MarkerRecorded(String),
    SideEffectRecorded(SideEffectKind, Option<String>),
}

impl OwnEvent {
    fn matches(&self, event: &WorkflowEvent) -> bool {
        match (self, event) {
            (Self::ActivityScheduled(id), WorkflowEvent::ActivityScheduled { activity_id, .. }) => {
                id == activity_id
            }
            (Self::TimerStarted(id), WorkflowEvent::TimerStarted { timer_id, .. }) => {
                id == timer_id
            }
            (Self::MarkerRecorded(name), WorkflowEvent::MarkerRecorded { name: recorded, .. }) => {
                name == recorded
            }
            (
                Self::SideEffectRecorded(kind, name),
                WorkflowEvent::SideEffectRecorded {
                    kind: recorded_kind,
                    name: recorded_name,
                    ..
                },
            ) => kind == recorded_kind && name == recorded_name,
            _ => false,
        }
    }
}

/// Reads one suspension's commands. Returns the index of the one awaited
/// command and the events the worker writes, or why the suspension cannot
/// stay resident.
fn plan_suspension(commands: &[WorkflowCommand]) -> Result<(usize, Vec<OwnEvent>), ResidentMiss> {
    let mut awaited = None;
    let mut own_events = Vec::new();
    for (index, cmd) in commands.iter().enumerate() {
        let awaits = match cmd {
            WorkflowCommand::ScheduleActivity { activity_id, .. } => {
                own_events.push(OwnEvent::ActivityScheduled(*activity_id));
                true
            }
            WorkflowCommand::StartTimer { timer_id, .. } => {
                own_events.push(OwnEvent::TimerStarted(timer_id.clone()));
                true
            }
            WorkflowCommand::WaitForActivity { .. } | WorkflowCommand::WaitForSignal { .. } => true,
            WorkflowCommand::RecordMarker { name, .. } => {
                own_events.push(OwnEvent::MarkerRecorded(name.clone()));
                false
            }
            WorkflowCommand::RecordSideEffect { kind, name, .. } => {
                own_events.push(OwnEvent::SideEffectRecorded(*kind, name.clone()));
                false
            }
            WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. }
            | WorkflowCommand::RecordLog { .. } => false,
            WorkflowCommand::AcquireMutex { .. } | WorkflowCommand::ReleaseMutex { .. } => {
                return Err(ResidentMiss::Mutex);
            }
            _ => return Err(ResidentMiss::Command),
        };
        if awaits {
            // A wait whose receiver is gone is what a `select!` leaves.
            if !cmd.awaits_result() {
                return Err(ResidentMiss::Race);
            }
            // A second awaited command means a join or a race.
            if awaited.is_some() {
                return Err(ResidentMiss::MultiAwait);
            }
            awaited = Some(index);
        }
    }
    awaited
        .map(|index| (index, own_events))
        .ok_or(ResidentMiss::Command)
}

/// Whether a suspension that cannot stay resident shows a race (issue
/// #2007).
///
/// A `ctx.race()` cycle records its `race:{seq}` open marker on its first
/// cycle. A race timer has a reserved id. A settled race cancels its losers.
/// A raw `select!` leaves none of these, so it reads as a join.
fn is_race(commands: &[WorkflowCommand]) -> bool {
    commands.iter().any(|cmd| match cmd {
        WorkflowCommand::CancelRaceLosers { .. } => true,
        WorkflowCommand::RecordMarker { name, .. } => name
            .strip_prefix("race:")
            .is_some_and(|seq| seq.parse::<u32>().is_ok()),
        WorkflowCommand::StartTimer { timer_id, .. } => {
            let id = timer_id.as_str();
            id.starts_with(crate::context::RACE_TIMER_PREFIX)
                || crate::awaitables::reserved_signal_race_name(id).is_some()
                || crate::awaitables::reserved_child_race_name(id).is_some()
        }
        _ => false,
    })
}

/// A suspended workflow that stays in memory between decisions (issue #1798).
///
/// It holds the parked handler future, its context, and the live channel of
/// the one command the future awaits. Dropping it drops the future.
pub struct ResidentWorkflow {
    future: OwnedHandlerFuture,
    awaiting: Awaiting,
    own_events: Vec<OwnEvent>,
    key: ResidentKey,
    ctx: Arc<WorkflowContext>,
}

impl std::fmt::Debug for ResidentWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResidentWorkflow")
            .field("execution_id", &self.ctx.execution_id())
            .field("own_events", &self.own_events)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl ResidentWorkflow {
    /// Keeps a suspended cycle resident when a warm decision can resume it.
    ///
    /// On success the awaited command in `outcome` gets a closed channel,
    /// and the returned value holds the live one. On an error the caller
    /// drops `future` as on a cold cycle. The error is the miss reason of the
    /// next decision (issue #2007).
    pub(crate) fn capture(
        ctx: &Arc<WorkflowContext>,
        future: OwnedHandlerFuture,
        outcome: &mut WorkflowOutcome,
        key: ResidentKey,
    ) -> Result<Self, ResidentMiss> {
        let WorkflowOutcome::Suspended { commands } = outcome else {
            return Err(ResidentMiss::Command);
        };
        if let Some(blocker) = ctx.resident_blocker() {
            return Err(blocker);
        }
        // A race rejects for its shape. The reason names the race instead.
        let (index, own_events) = plan_suspension(commands).map_err(|miss| {
            if ctx.race_waiting() || is_race(commands) {
                ResidentMiss::Race
            } else {
                miss
            }
        })?;
        // A cold replay could hand the new signal to an earlier probe.
        if let WorkflowCommand::WaitForSignal { signal_name, .. } = &commands[index]
            && ctx.signal_probed_at_frontier(signal_name)
        {
            return Err(ResidentMiss::Context);
        }
        let awaiting = Awaiting::take_from(&mut commands[index]).ok_or(ResidentMiss::Command)?;
        Ok(Self {
            future,
            awaiting,
            own_events,
            key,
            ctx: Arc::clone(ctx),
        })
    }

    /// Returns the event in `delta` that resolves the awaited command.
    ///
    /// Decision boundaries (issue #1833) are skipped. Replay never reads them.
    fn resolving_event<'a>(
        &self,
        delta: &'a [WorkflowEvent],
    ) -> Result<&'a WorkflowEvent, ResumeDeclined> {
        let mut events = delta.iter().filter(|event| !event.is_decision_boundary());
        // A delta shorter than the own events is checked as far as it goes.
        let own_match = self
            .own_events
            .iter()
            .all(|expected| events.next().is_none_or(|event| expected.matches(event)));
        if !own_match {
            return Err(ResumeDeclined::OwnEventsMismatch);
        }
        // Replay skips progress events only up to the resolving event.
        let mut rest = events.skip_while(|event| self.awaiting.is_progress(event));
        match (rest.next(), rest.next()) {
            (Some(event), None) => Ok(event),
            (Some(_), Some(_)) => Err(ResumeDeclined::ExtraEvents),
            (None, _) => Err(ResumeDeclined::NoResolution),
        }
    }

    /// Resumes this workflow with the events written since it suspended.
    ///
    /// `key` is the key of this decision. `None` skips the key check.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) async fn resume_with(
        self,
        delta: &[WorkflowEvent],
        key: Option<&ResidentKey>,
        span_meta: Option<&WorkflowExecuteSpanMeta>,
    ) -> Result<DriveResult, ResumeDeclined> {
        Ok(self.wake(delta, key)?.drive(span_meta).await)
    }

    /// Sends the new result to the parked future (issue #2007).
    ///
    /// A success is the commit point of a resident hit. The worker records
    /// the hit here, before the drive, so a drive that times out still
    /// counts. `key` is the key of this decision. `None` skips the key check.
    #[cfg_attr(not(any(feature = "db", feature = "testing")), allow(dead_code))] // Resident paths need the worker or the test harness.
    pub(crate) fn wake(
        self,
        delta: &[WorkflowEvent],
        key: Option<&ResidentKey>,
    ) -> Result<WokenWorkflow, ResumeDeclined> {
        if key.is_some_and(|key| *key != self.key) {
            return Err(ResumeDeclined::KeyChanged);
        }
        let event = self.resolving_event(delta)?;
        let Self {
            future,
            awaiting,
            key,
            ctx,
            ..
        } = self;
        awaiting.deliver(event)?;
        ctx.begin_resident_cycle(delta);
        Ok(WokenWorkflow { future, key, ctx })
    }
}

/// A resident workflow that holds its new result and waits for its drive
/// (issue #2007).
#[cfg_attr(not(any(feature = "db", feature = "testing")), allow(dead_code))] // Resident paths need the worker or the test harness.
pub(crate) struct WokenWorkflow {
    future: OwnedHandlerFuture,
    key: ResidentKey,
    ctx: Arc<WorkflowContext>,
}

impl WokenWorkflow {
    /// Polls the parked future for one cycle.
    #[cfg_attr(not(any(feature = "db", feature = "testing")), allow(dead_code))] // Resident paths need the worker or the test harness.
    pub(crate) async fn drive(self, span_meta: Option<&WorkflowExecuteSpanMeta>) -> DriveResult {
        crate::executor::drive_resumed(self.ctx, self.future, span_meta, self.key).await
    }
}

/// Runs one cold decision and keeps the workflow resident when it can.
///
/// The context matches the one that [`crate::executor::run_workflow`]
/// builds. Test and bench entry point; the worker uses the internal path.
#[cfg(any(test, feature = "testing"))]
pub async fn start(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> (WorkflowOutcome, Option<ResidentWorkflow>) {
    let ctx = crate::executor::default_task_context(exec_id, history);
    let key = ResidentKey::new(handler, None, &HashMap::new());
    let drive = crate::executor::drive_workflow_keep(ctx, handler, input, None, Some(key)).await;
    (drive.outcome, drive.resident.ok())
}

#[cfg(any(test, feature = "testing"))]
impl ResidentWorkflow {
    /// Resumes this workflow with the events appended since it suspended.
    ///
    /// # Errors
    ///
    /// Returns [`ResumeDeclined`] when the events do not resolve the parked
    /// future exactly as a replay would. The caller then runs a cold replay.
    pub async fn resume(
        self,
        delta: &[WorkflowEvent],
    ) -> Result<(WorkflowOutcome, Option<Self>), ResumeDeclined> {
        let drive = self.resume_with(delta, None, None).await?;
        Ok((drive.outcome, drive.resident.ok()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{WorkflowCommand, WorkflowContext};
    use crate::types::{ActivityExecId, TimerId};
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type HandlerFuture<'a> =
        Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

    fn decision_boundary() -> WorkflowEvent {
        WorkflowEvent::DecisionCommitted {
            build_id: crate::types::BuildId::new("b"),
            worker_id: crate::types::WorkerId::new("w"),
        }
    }

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc
                .with_ymd_and_hms(2026, 10, 3, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    /// The events a worker appends for a suspension, in command order.
    fn own_events(commands: &[WorkflowCommand]) -> Vec<WorkflowEvent> {
        commands
            .iter()
            .filter_map(|cmd| match cmd {
                WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name,
                    input,
                    queue,
                    ..
                } => Some(WorkflowEvent::ActivityScheduled {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                }),
                WorkflowCommand::StartTimer {
                    timer_id,
                    duration_secs,
                    ..
                } => Some(WorkflowEvent::TimerStarted {
                    timer_id: timer_id.clone(),
                    duration_secs: *duration_secs,
                }),
                WorkflowCommand::RecordMarker { name, details } => {
                    Some(WorkflowEvent::MarkerRecorded {
                        name: name.clone(),
                        details: details.clone(),
                    })
                }
                WorkflowCommand::RecordSideEffect { kind, name, value } => {
                    Some(WorkflowEvent::SideEffectRecorded {
                        kind: *kind,
                        name: name.clone(),
                        value: value.clone(),
                    })
                }
                _ => None,
            })
            .collect()
    }

    /// The event that resolves the one awaiting command of a suspension.
    fn resolution(commands: &[WorkflowCommand]) -> WorkflowEvent {
        commands
            .iter()
            .find_map(|cmd| match cmd {
                WorkflowCommand::ScheduleActivity {
                    activity_id, input, ..
                } => Some(WorkflowEvent::ActivityCompleted {
                    activity_id: *activity_id,
                    output: json!({ "echo": input }),
                }),
                WorkflowCommand::WaitForActivity { activity_id, .. } => {
                    Some(WorkflowEvent::ActivityCompleted {
                        activity_id: *activity_id,
                        output: json!({ "echo": null }),
                    })
                }
                WorkflowCommand::StartTimer { timer_id, .. } => Some(WorkflowEvent::TimerFired {
                    timer_id: timer_id.clone(),
                }),
                WorkflowCommand::WaitForSignal { signal_name, .. } => {
                    Some(WorkflowEvent::SignalReceived {
                        signal_name: signal_name.clone(),
                        payload: json!({ "signal": signal_name }),
                    })
                }
                _ => None,
            })
            .expect("a suspension awaits one command")
    }

    /// A stable text form of a command. It leaves out random ids.
    fn shape(cmd: &WorkflowCommand) -> String {
        match cmd {
            WorkflowCommand::ScheduleActivity { name, input, .. } => {
                format!("ScheduleActivity({name}, {input})")
            }
            WorkflowCommand::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => format!("StartTimer({}, {duration_secs})", timer_id.as_str()),
            WorkflowCommand::WaitForSignal { signal_name, .. } => {
                format!("WaitForSignal({signal_name})")
            }
            WorkflowCommand::PublishProgress { seq, chunk } => {
                format!("PublishProgress({seq}, {chunk})")
            }
            WorkflowCommand::RecordMarker { name, .. } => format!("RecordMarker({name})"),
            WorkflowCommand::RecordSideEffect { kind, name, .. } => {
                format!("RecordSideEffect({kind:?}, {name:?})")
            }
            other => format!("{other:?}"),
        }
    }

    fn outcome_shape(outcome: &WorkflowOutcome) -> String {
        match outcome {
            WorkflowOutcome::Suspended { commands } => {
                let shapes: Vec<String> = commands.iter().map(shape).collect();
                format!("Suspended[{}]", shapes.join(", "))
            }
            WorkflowOutcome::Completed { output, .. } => format!("Completed({output})"),
            WorkflowOutcome::Failed {
                error,
                handler_panic,
                non_deterministic_details,
                ..
            } => format!(
                "Failed({error}, panic={handler_panic}, nd={})",
                non_deterministic_details.is_some()
            ),
            WorkflowOutcome::TaskFailed { error } => format!("TaskFailed({error})"),
            WorkflowOutcome::ContinuedAsNew { input, .. } => format!("ContinuedAsNew({input})"),
        }
    }

    /// The decisions of one run, and the history that the run wrote.
    struct Trace {
        shapes: Vec<String>,
        history: Vec<WorkflowEvent>,
        resumes: usize,
    }

    /// Drives a run with a cold replay for every decision.
    async fn drive_cold(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let exec_id = ExecutionId::new();
        let mut history = vec![started(input.clone())];
        let mut shapes = Vec::new();
        for _ in 0..32 {
            let outcome =
                crate::executor::run_workflow(exec_id, history.clone(), handler, input.clone())
                    .await;
            shapes.push(outcome_shape(&outcome));
            let WorkflowOutcome::Suspended { commands } = outcome else {
                return Trace {
                    shapes,
                    history,
                    resumes: 0,
                };
            };
            history.extend(own_events(&commands));
            // A live worker closes each decision with a boundary (issue #1833).
            history.push(decision_boundary());
            history.push(resolution(&commands));
        }
        panic!("the run did not finish in 32 decisions");
    }

    /// Drives a run that resumes the resident workflow when it can.
    async fn drive_warm(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let exec_id = ExecutionId::new();
        let mut history = vec![started(input.clone())];
        let mut shapes = Vec::new();
        let mut resumes = 0;
        let (mut outcome, mut resident) =
            start(exec_id, history.clone(), handler, input.clone()).await;
        for _ in 0..32 {
            shapes.push(outcome_shape(&outcome));
            let WorkflowOutcome::Suspended { commands } = &outcome else {
                return Trace {
                    shapes,
                    history,
                    resumes,
                };
            };
            let start_len = history.len();
            history.extend(own_events(commands));
            history.push(decision_boundary());
            history.push(resolution(commands));
            let delta = &history[start_len..];
            let warm_step = match resident.take() {
                Some(live) => live.resume(delta).await.ok(),
                None => None,
            };
            (outcome, resident) = match warm_step {
                Some(next) => {
                    resumes += 1;
                    next
                }
                None => start(exec_id, history.clone(), handler, input.clone()).await,
            };
        }
        panic!("the run did not finish in 32 decisions");
    }

    /// Asserts that the warm run decided like the cold run, and that its
    /// history replays cold to the same end.
    async fn assert_warm_matches_cold(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let cold = drive_cold(handler, input.clone()).await;
        let warm = drive_warm(handler, input.clone()).await;
        assert_eq!(warm.shapes, cold.shapes, "warm decisions must equal cold");
        let replayed =
            crate::executor::run_workflow(ExecutionId::new(), warm.history.clone(), handler, input)
                .await;
        assert_eq!(
            Some(outcome_shape(&replayed)),
            warm.shapes.last().cloned(),
            "the warm history must replay cold to the same end"
        );
        warm
    }

    /// Three activities with side effects and progress, then a timer and a
    /// signal. Returns values that depend on history length.
    fn chain_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let steps = input["steps"].as_u64().ok_or("missing steps")?;
            let mut echoes = Vec::new();
            for i in 0..steps {
                let _ = ctx.new_uuid();
                let out = ctx
                    .execute_activity_raw("step", json!({ "i": i }), "default")
                    .await
                    .map_err(|e| e.to_string())?;
                ctx.publish_progress(json!({ "done": i }))
                    .map_err(|e| e.to_string())?;
                echoes.push(out);
            }
            ctx.timer("pause", 5).await.map_err(|e| e.to_string())?;
            let signal = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
            Ok(json!({
                "echoes": echoes,
                "signal": signal,
                "events": ctx.history_event_count(),
            }))
        })
    }

    #[tokio::test]
    async fn warm_activity_timer_signal_chain_matches_cold_replay() {
        let warm = assert_warm_matches_cold(chain_workflow, json!({ "steps": 3 })).await;
        // Three activities, a timer and a signal: five suspensions.
        assert_eq!(
            warm.resumes, 5,
            "every decision after the first must resume the resident future"
        );
    }

    /// A long-lived signal loop, the O(n²) case of issue #1798.
    fn signal_loop_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let rounds = input["rounds"].as_u64().ok_or("missing rounds")?;
            let mut seen = 0_u64;
            for _ in 0..rounds {
                let _ = ctx
                    .wait_for_signal("tick")
                    .await
                    .map_err(|e| e.to_string())?;
                seen += 1;
            }
            Ok(json!(seen))
        })
    }

    /// Body starts of `counted_signal_loop_workflow`. Only one test runs it.
    static LOOP_BODY_STARTS: AtomicUsize = AtomicUsize::new(0);

    /// `signal_loop_workflow` with a body-start counter.
    fn counted_signal_loop_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
        LOOP_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
        signal_loop_workflow(ctx, input)
    }

    #[tokio::test]
    async fn warm_signal_loop_runs_the_body_once() {
        let warm = drive_warm(counted_signal_loop_workflow, json!({ "rounds": 6 })).await;
        assert_eq!(warm.shapes.last().map(String::as_str), Some("Completed(6)"));
        assert_eq!(warm.resumes, 6);
        assert_eq!(
            LOOP_BODY_STARTS.load(Ordering::SeqCst),
            1,
            "a warm decision must not run the body from the top"
        );
    }

    #[tokio::test]
    async fn warm_signal_loop_matches_cold_replay() {
        let warm = assert_warm_matches_cold(signal_loop_workflow, json!({ "rounds": 4 })).await;
        assert_eq!(
            warm.resumes, 4,
            "every signal must resume the resident future"
        );
    }

    /// Fails after the first activity, so the warm run must fail like the cold run.
    fn fail_after_activity_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.execute_activity_raw("step", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            Err("business rule".to_string())
        })
    }

    #[tokio::test]
    async fn warm_failure_after_resume_matches_cold_replay() {
        let warm = assert_warm_matches_cold(fail_after_activity_workflow, json!({})).await;
        assert_eq!(warm.resumes, 1);
    }

    /// Panics after the first activity.
    fn panic_after_activity_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.execute_activity_raw("step", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            panic!("boom after resume");
        })
    }

    #[tokio::test]
    async fn warm_panic_after_resume_matches_cold_replay() {
        let warm = assert_warm_matches_cold(panic_after_activity_workflow, json!({})).await;
        assert_eq!(warm.resumes, 1);
        assert!(
            warm.shapes.last().is_some_and(|s| s.contains("panic=true")),
            "the resumed panic must be contained: {:?}",
            warm.shapes
        );
    }

    /// Waits on a foreign future after the first activity.
    fn foreign_wait_after_activity_workflow(
        ctx: &WorkflowContext,
        _input: Value,
    ) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.execute_activity_raw("step", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            std::future::pending::<()>().await;
            Ok(Value::Null)
        })
    }

    #[tokio::test(start_paused = true)]
    async fn warm_foreign_wait_after_resume_fails_the_task() {
        let exec_id = ExecutionId::new();
        let history = vec![started(Value::Null)];
        let (outcome, resident) = start(
            exec_id,
            history,
            foreign_wait_after_activity_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let mut delta = own_events(&commands);
        delta.push(resolution(&commands));
        let resident = resident.expect("an activity suspension is resident");
        let (outcome, next) = resident.resume(&delta).await.expect("the delta resolves");
        assert!(
            matches!(outcome, WorkflowOutcome::TaskFailed { .. }),
            "a foreign wait must fail the task: {outcome:?}"
        );
        assert!(next.is_none(), "a failed task must drop the resident state");
    }

    /// Passes its replay position and history length to a second activity.
    fn position_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            let seen = json!({
                "position": ctx.replay_position(),
                "info": ctx.info().history_event_count,
                "count": ctx.history_event_count(),
            });
            ctx.execute_activity_raw("b", seen, "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    #[tokio::test]
    async fn warm_replay_position_and_history_length_match_cold_replay() {
        let warm = assert_warm_matches_cold(position_workflow, Value::Null).await;
        assert_eq!(warm.resumes, 2);
    }

    /// Probes for `approve`, then waits for it in the same cycle.
    fn probe_then_wait_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let early = ctx
                .try_wait_for_signal("approve")
                .map_err(|e| e.to_string())?;
            let (branch, value) = match early {
                Some(value) => ("fast", value),
                None => (
                    "slow",
                    ctx.wait_for_signal("approve")
                        .await
                        .map_err(|e| e.to_string())?,
                ),
            };
            ctx.execute_activity_raw(branch, value, "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Probes for `late`, waits for `first`, then uses the probe result.
    ///
    /// A cold replay of the full history lets the probe see `late`, because
    /// no command event bounds its scan. The warm run must not resume the
    /// wait for `late`.
    fn probe_across_cycles_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let early = ctx.try_wait_for_signal("late").map_err(|e| e.to_string())?;
            let first = ctx
                .wait_for_signal("first")
                .await
                .map_err(|e| e.to_string())?;
            let was_early = early.is_some();
            let late = match early {
                Some(value) => value,
                None => ctx
                    .wait_for_signal("late")
                    .await
                    .map_err(|e| e.to_string())?,
            };
            Ok(json!({ "early": was_early, "first": first, "late": late }))
        })
    }

    #[tokio::test]
    async fn a_signal_probed_at_the_frontier_is_never_resumed_warm() {
        for handler in [
            probe_then_wait_workflow as WorkflowHandlerFn,
            probe_across_cycles_workflow,
        ] {
            assert_warm_matches_cold(handler, Value::Null).await;
        }
    }

    // ── Shapes that must never stay resident ─────────────────────────

    /// Awaits two activities at once.
    fn join_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (a, b) = futures::join!(
                ctx.execute_activity_raw("a", json!({}), "default"),
                ctx.execute_activity_raw("b", json!({}), "default"),
            );
            Ok(json!([
                a.map_err(|e| e.to_string())?,
                b.map_err(|e| e.to_string())?
            ]))
        })
    }

    /// Registers a push signal handler, then awaits an activity.
    fn handler_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.register_signal_handler_raw("note", |_payload: Value| {});
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Parks on a condition, which holds a park token.
    fn condition_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.await_condition(|| false)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        })
    }

    #[tokio::test]
    async fn ineligible_suspensions_are_not_resident() {
        for (name, handler) in [
            ("join", join_workflow as WorkflowHandlerFn),
            ("signal handler", handler_workflow),
            ("condition", condition_workflow),
        ] {
            let (outcome, resident) = start(
                ExecutionId::new(),
                vec![started(Value::Null)],
                handler,
                Value::Null,
            )
            .await;
            assert!(
                matches!(outcome, WorkflowOutcome::Suspended { .. }),
                "{name}: the fixture must suspend: {outcome:?}"
            );
            assert!(resident.is_none(), "{name}: must not stay resident");
        }
    }

    // ── Miss reasons (issue #2007) ───────────────────────────────────

    /// Runs one cold decision and returns why it cannot stay resident.
    async fn capture_of(handler: WorkflowHandlerFn) -> Result<(), ResidentMiss> {
        capture_after(vec![started(Value::Null)], handler).await
    }

    /// Runs one cold decision over `history` and returns why it cannot stay
    /// resident.
    async fn capture_after(
        history: Vec<WorkflowEvent>,
        handler: WorkflowHandlerFn,
    ) -> Result<(), ResidentMiss> {
        let ctx = crate::executor::default_task_context(ExecutionId::new(), history);
        let key = ResidentKey::new(handler, None, &HashMap::new());
        crate::executor::drive_workflow_keep(ctx, handler, Value::Null, None, Some(key))
            .await
            .resident
            .map(drop)
    }

    /// Races two activities with `ctx.race()`.
    fn activity_race_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let winner = ctx
                .race()
                .activity_raw("a", json!({}), "default")
                .activity_raw("b", json!({}), "default")
                .run()
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(winner.index))
        })
    }

    /// Races an activity against a timer with `ctx.race()`.
    fn timer_race_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let winner = ctx
                .race()
                .activity_raw("a", json!({}), "default")
                .timer(std::time::Duration::from_secs(60))
                .run()
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(winner.index))
        })
    }

    /// Waits for a signal with a deadline.
    fn signal_timeout_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.wait_for_signal_timeout("go", std::time::Duration::from_secs(60))
                .await
                .map_err(|e| e.to_string())
                .map(|value| value.unwrap_or(Value::Null))
        })
    }

    /// Waits for a durable mutex.
    fn mutex_wait_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let _guard = ctx
                .mutex("ledger")
                .acquire()
                .await
                .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        })
    }

    /// Holds a durable mutex while it waits for an activity.
    fn mutex_held_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let _guard = ctx
                .mutex("ledger")
                .acquire()
                .await
                .map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Starts a child workflow and waits for it.
    fn child_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.spawn_child_workflow_raw("child", json!({}))
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Waits for a child workflow with a deadline.
    fn child_timeout_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.spawn_child_workflow_timeout("child", json!({}), std::time::Duration::from_secs(60))
                .await
                .map_err(|e| e.to_string())
                .map(|value| value.unwrap_or(Value::Null))
        })
    }

    /// Waits for a signal with a deadline, then runs one activity.
    fn signal_deadline_then_activity_workflow(
        ctx: &WorkflowContext,
        _input: Value,
    ) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.wait_for_signal_timeout("go", std::time::Duration::from_secs(60))
                .await
                .map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// The history after the first cold decision of `handler`.
    async fn after_first_decision(handler: WorkflowHandlerFn) -> Vec<WorkflowEvent> {
        let (outcome, _resident) = start(
            ExecutionId::new(),
            vec![started(Value::Null)],
            handler,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let mut history = vec![started(Value::Null)];
        history.extend(own_events(&commands));
        history.push(decision_boundary());
        history
    }

    #[tokio::test]
    async fn a_race_reports_race_after_its_first_cycle() {
        // An unrelated wake replays the race before any branch ends.
        let history = after_first_decision(activity_race_workflow).await;
        assert_eq!(
            capture_after(history, activity_race_workflow).await,
            Err(ResidentMiss::Race),
            "a pending race on a later cycle"
        );

        // The signal wins. The next cycle cancels the timer and runs work.
        let mut history = after_first_decision(signal_deadline_then_activity_workflow).await;
        history.push(WorkflowEvent::SignalReceived {
            signal_name: "go".into(),
            payload: json!(1),
        });
        assert_eq!(
            capture_after(history, signal_deadline_then_activity_workflow).await,
            Err(ResidentMiss::Race),
            "work after a settled race"
        );
    }

    #[tokio::test]
    async fn a_suspension_that_cannot_stay_resident_reports_why() {
        let cases: [(&str, WorkflowHandlerFn, ResidentMiss); 9] = [
            ("join", join_workflow, ResidentMiss::MultiAwait),
            ("activity race", activity_race_workflow, ResidentMiss::Race),
            ("timer race", timer_race_workflow, ResidentMiss::Race),
            (
                "signal deadline",
                signal_timeout_workflow,
                ResidentMiss::Race,
            ),
            ("mutex wait", mutex_wait_workflow, ResidentMiss::Mutex),
            ("child workflow", child_workflow, ResidentMiss::Command),
            ("child deadline", child_timeout_workflow, ResidentMiss::Race),
            (
                "signal handler",
                handler_workflow,
                ResidentMiss::SignalHandler,
            ),
            ("condition", condition_workflow, ResidentMiss::Condition),
        ];
        for (name, handler, expected) in cases {
            assert_eq!(capture_of(handler).await, Err(expected), "{name}");
        }
        let granted = WorkflowEvent::MutexGranted {
            key: "ledger".into(),
            lock_seq: 1,
            acquired_at: Utc
                .with_ymd_and_hms(2026, 10, 3, 0, 0, 1)
                .single()
                .expect("valid timestamp"),
        };
        assert_eq!(
            capture_after(vec![started(Value::Null), granted], mutex_held_workflow).await,
            Err(ResidentMiss::Mutex),
            "a held mutex"
        );
        assert_eq!(
            capture_of(one_activity_workflow).await,
            Ok(()),
            "one awaited activity stays resident"
        );
    }

    #[test]
    fn each_decline_maps_to_one_miss_reason() {
        let cases = [
            (ResumeDeclined::KeyChanged, ResidentMiss::KeyChanged),
            (ResumeDeclined::OwnEventsMismatch, ResidentMiss::Delta),
            (ResumeDeclined::NoResolution, ResidentMiss::Delta),
            (ResumeDeclined::ExtraEvents, ResidentMiss::Delta),
            (
                ResumeDeclined::UnexpectedEvent("TimerFired"),
                ResidentMiss::Delta,
            ),
            (ResumeDeclined::ReceiverDropped, ResidentMiss::Delta),
            (
                ResumeDeclined::InexactResolution("ActivityFailed"),
                ResidentMiss::Failure,
            ),
        ];
        for (declined, expected) in cases {
            assert_eq!(ResidentMiss::from(&declined), expected, "{declined:?}");
        }
    }

    #[test]
    fn miss_reasons_are_a_closed_label_set() {
        let labels: Vec<&str> = ResidentMiss::ALL.iter().map(|m| m.as_str()).collect();
        assert_eq!(
            labels,
            [
                "cold",
                "multi_await",
                "race",
                "mutex",
                "hot_swap",
                "command",
                "condition",
                "signal_handler",
                "context",
                "key_changed",
                "delta",
                "failure",
            ]
        );
    }

    // ── Deltas that must decline ─────────────────────────────────────

    /// One activity, then completes with its output.
    fn one_activity_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Suspends `one_activity_workflow` and returns its own events and id.
    async fn suspended_activity() -> (ResidentWorkflow, Vec<WorkflowEvent>, ActivityExecId) {
        let (outcome, resident) = start(
            ExecutionId::new(),
            vec![started(Value::Null)],
            one_activity_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let id = commands
            .iter()
            .find_map(|c| match c {
                WorkflowCommand::ScheduleActivity { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .expect("the decision schedules an activity");
        let own = own_events(&commands);
        (
            resident.expect("an activity suspension is resident"),
            own,
            id,
        )
    }

    fn completed(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: json!(1),
        }
    }

    /// Builds a delta from the own events and the id of the parked activity.
    type DeltaBuilder = Box<dyn Fn(&[WorkflowEvent], ActivityExecId) -> Vec<WorkflowEvent>>;

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // One table of decline cases.
    async fn deltas_that_replay_could_read_differently_decline() {
        let other = ActivityExecId::new();
        let cases: Vec<(&str, ResumeDeclined, DeltaBuilder)> = vec![
            (
                "empty delta",
                ResumeDeclined::NoResolution,
                Box::new(|_own, _id| Vec::new()),
            ),
            (
                "no own events",
                ResumeDeclined::OwnEventsMismatch,
                Box::new(|_own, id| vec![completed(id)]),
            ),
            (
                "own events only",
                ResumeDeclined::NoResolution,
                Box::new(|own, _id| own.to_vec()),
            ),
            (
                "wrong activity id",
                ResumeDeclined::UnexpectedEvent("ActivityCompleted"),
                Box::new(move |own, _id| [own.to_vec(), vec![completed(other)]].concat()),
            ),
            (
                "activity failure",
                ResumeDeclined::InexactResolution("ActivityFailed"),
                Box::new(|own, id| {
                    [
                        own.to_vec(),
                        vec![WorkflowEvent::ActivityFailed {
                            activity_id: id,
                            error: "boom".into(),
                            attempt: 1,
                            error_type: "Error".into(),
                            details: None,
                            non_retryable: false,
                        }],
                    ]
                    .concat()
                }),
            ),
            (
                "two resolutions",
                ResumeDeclined::ExtraEvents,
                Box::new(|own, id| [own.to_vec(), vec![completed(id), completed(id)]].concat()),
            ),
            (
                "an extra signal",
                ResumeDeclined::ExtraEvents,
                Box::new(|own, id| {
                    [
                        own.to_vec(),
                        vec![
                            completed(id),
                            WorkflowEvent::SignalReceived {
                                signal_name: "x".into(),
                                payload: Value::Null,
                            },
                        ],
                    ]
                    .concat()
                }),
            ),
            (
                "a cancel request",
                ResumeDeclined::ExtraEvents,
                Box::new(|own, id| {
                    [
                        own.to_vec(),
                        vec![
                            WorkflowEvent::WorkflowCancelled {
                                reason: "op".into(),
                            },
                            completed(id),
                        ],
                    ]
                    .concat()
                }),
            ),
            (
                "a timer fire for another timer",
                ResumeDeclined::UnexpectedEvent("TimerFired"),
                Box::new(|own, _id| {
                    [
                        own.to_vec(),
                        vec![WorkflowEvent::TimerFired {
                            timer_id: TimerId::new("other"),
                        }],
                    ]
                    .concat()
                }),
            ),
        ];
        for (name, expected, build) in cases {
            let (resident, own, id) = suspended_activity().await;
            let delta = build(&own, id);
            assert_eq!(
                resident.resume(&delta).await.err(),
                Some(expected),
                "{name}: the resume must decline for this reason"
            );
        }
    }

    #[tokio::test]
    async fn a_cold_replay_of_a_running_activity_stays_resident() {
        let id = ActivityExecId::new();
        let history = vec![
            started(Value::Null),
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: "a".into(),
                input: json!({}),
                queue: "default".into(),
            },
        ];
        let (outcome, resident) = start(
            ExecutionId::new(),
            history,
            one_activity_workflow,
            Value::Null,
        )
        .await;
        assert!(
            matches!(&outcome, WorkflowOutcome::Suspended { commands }
                if matches!(commands.as_slice(), [WorkflowCommand::WaitForActivity { .. }])),
            "the replay must wait for the running activity: {outcome:?}"
        );
        let resident = resident.expect("a wait for a running activity stays resident");
        let delta = [
            WorkflowEvent::ActivityStarted {
                activity_id: id,
                worker_id: crate::types::WorkerId::new("w"),
            },
            completed(id),
        ];
        let (outcome, _) = resident.resume(&delta).await.expect("the delta resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Completed { output, .. } if *output == json!(1)),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn unread_history_blocks_resident_state() {
        let history = vec![
            started(Value::Null),
            WorkflowEvent::SignalReceived {
                signal_name: "early".into(),
                payload: Value::Null,
            },
        ];
        let (outcome, resident) = start(
            ExecutionId::new(),
            history,
            one_activity_workflow,
            Value::Null,
        )
        .await;
        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert!(
            resident.is_none(),
            "a signal that the code has not read yet must block resident state"
        );
    }

    /// Reuses one timer id in a loop.
    fn timer_loop_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            for _ in 0..3 {
                ctx.timer("tick", 1).await.map_err(|e| e.to_string())?;
            }
            Ok(json!("done"))
        })
    }

    #[tokio::test]
    async fn warm_timer_loop_with_one_timer_id_matches_cold_replay() {
        let warm = assert_warm_matches_cold(timer_loop_workflow, Value::Null).await;
        assert_eq!(warm.resumes, 3);
    }

    #[tokio::test]
    async fn progress_events_of_the_awaited_activity_are_skipped() {
        let (resident, own, id) = suspended_activity().await;
        let progress = [
            WorkflowEvent::ActivityStarted {
                activity_id: id,
                worker_id: crate::types::WorkerId::new("w"),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: Value::Null,
            },
        ];
        let delta = [own, progress.to_vec(), vec![completed(id)]].concat();
        let (outcome, _) = resident.resume(&delta).await.expect("the delta resolves");
        assert!(
            matches!(outcome, WorkflowOutcome::Completed { .. }),
            "{outcome:?}"
        );

        let (resident, own, id) = suspended_activity().await;
        let other = WorkflowEvent::ActivityStarted {
            activity_id: ActivityExecId::new(),
            worker_id: crate::types::WorkerId::new("w"),
        };
        let delta = [own, vec![other, completed(id)]].concat();
        assert_eq!(
            resident.resume(&delta).await.err(),
            Some(ResumeDeclined::ExtraEvents),
            "progress of another activity must decline"
        );
    }

    #[tokio::test]
    async fn changed_context_inputs_decline() {
        let (resident, own, id) = suspended_activity().await;
        let delta = [own, vec![completed(id)]].concat();
        let meta = crate::executor::WorkflowExecuteSpanMeta {
            workflow_name: "moved".into(),
            workflow_id: String::new(),
            shard_id: 7,
            queue_name: String::new(),
            is_replay: true,
            link_traceparent: None,
            build_id: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
        };
        let key = ResidentKey::new(one_activity_workflow, Some(&meta), &HashMap::new());
        let declined = resident.resume_with(&delta, Some(&key), Some(&meta)).await;
        assert_eq!(
            declined.err(),
            Some(ResumeDeclined::KeyChanged),
            "a new shard or name must decline"
        );
    }

    #[tokio::test]
    async fn exact_delta_resumes_and_completes() {
        let (resident, own, id) = suspended_activity().await;
        let delta = [own, vec![completed(id)]].concat();
        let (outcome, next) = resident.resume(&delta).await.expect("the delta resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Completed { output, .. } if *output == json!(1)),
            "{outcome:?}"
        );
        assert!(next.is_none(), "a completed run is not resident");
    }

    #[tokio::test]
    async fn a_decision_boundary_in_the_delta_is_skipped() {
        let (resident, own, id) = suspended_activity().await;
        let delta = [own, vec![decision_boundary(), completed(id)]].concat();
        let (outcome, _) = resident
            .resume(&delta)
            .await
            .expect("a boundary must not decline a warm resume");
        assert!(
            matches!(outcome, WorkflowOutcome::Completed { .. }),
            "{outcome:?}"
        );
    }
}
