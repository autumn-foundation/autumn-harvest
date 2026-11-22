//! Resident workflow state (issue #1798, step 2).
//!
//! A cold decision builds a new [`WorkflowContext`] from the full history and
//! runs the handler from the top. The replay work of a decision therefore
//! grows with history length, and a run of n decisions costs O(n²).
//!
//! A resident workflow keeps the suspended handler future and its context
//! alive between decisions. A warm decision sends the new results to the
//! parked future and polls it once more. It does not replay history, so its
//! cost does not grow with history length.
//!
//! # Which suspensions stay resident
//!
//! A warm decision must decide exactly as a cold replay would. The resident
//! path therefore accepts only a narrow set of suspensions:
//!
//! - The cycle awaits one command: an activity, a timer or a signal. Or it
//!   awaits two or more activities, such as a join of parallel tool calls
//!   (issue #2008). A join resolves branch by branch, so no winner exists.
//! - Each other command is a marker, a side effect, progress, current
//!   details, a log line or a search-attribute upsert.
//! - The context has no open race, park token, push signal handler, held
//!   mutex, cancel request or non-determinism record, and no unread history.
//! - A signal wait is not for a name that a non-blocking claim probed with a
//!   scan that reached the end of history. A cold replay of the longer
//!   history could hand the new signal to that probe instead.
//!
//! # When a warm decision resumes
//!
//! The delta must start with all the events of the last suspension, in
//! order. Then it holds the events that resolve parked awaits. Each parked
//! await resolves at most once. A resolving event must be an activity
//! success, a timer fire or a signal. The start and heartbeat events of a
//! parked activity may come before its result, because replay skips those
//! too. The live channels carry these payloads exactly, as a replay reads
//! them. Any other delta declines.
//!
//! A delta with no resolving event is a redundant wake. Each parked
//! activity then waits again, as in a partial join. A lone timer or signal
//! cannot wait again, so its redundant wake declines.
//!
//! # Partial joins
//!
//! A delta can resolve only some activities of a join (issue #2008). Each
//! other activity gets a `WaitForActivity` command again, with its live
//! sender. A cold replay emits the same command for an activity with no
//! result, and that command writes no event. Then the executor sees a
//! parked future and suspends.
//!
//! Such a cycle must only wait. A branch that runs a command while a
//! sibling stays parked can order its commands differently from a cold
//! replay. A branch that fails can drop a sibling. After the poll, the
//! resume therefore checks that the cycle suspends with only its re-parked
//! waits. Otherwise it declines with [`ResumeDeclined::SiblingStillParked`].
//!
//! A cold replay can stop its cursor at a sibling command, so a read of
//! history state can differ too. Such a cycle is a speculation: it fires no
//! side effect that replay suppresses, and any history read declines it.
//!
//! A delta with several results runs as one cycle per result, in history
//! order. A cold replay matches the results in that order. A branch that
//! runs on after an early result thus faces the check of its own cycle.
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

use crate::context::{ResidentDelta, WorkflowCommand, WorkflowContext};
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::executor::{DriveResult, OwnedHandlerFuture, WorkflowExecuteSpanMeta, WorkflowOutcome};
use crate::info::WorkflowHandlerFn;
use crate::telemetry::ResidentOutcome;
use crate::types::{ActivityExecId, ExecutionId, TimerId};

/// Why a resident workflow did not resume (issue #1798).
///
/// A decline is not an error, with one exception. The worker drops the
/// resident state and runs a cold replay, which is always correct.
/// [`Self::HistoryNotReturned`] is the exception: the worker then has no
/// whole history to replay, so it fails the task.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResumeDeclined {
    /// The context inputs changed since the suspension, for example the
    /// deadline, the shard or the build.
    KeyChanged,
    /// The delta does not start with the events of the last suspension.
    OwnEventsMismatch,
    /// The delta resolves no parked await, and the resume cannot wait
    /// again. The delta is shorter than the events of the last suspension,
    /// or the lone parked await is a timer or a signal.
    NoResolution,
    /// The delta holds an event after every parked await resolved, or a
    /// second result for one await.
    ExtraEvents,
    /// The resolving event has a payload that the live channel cannot carry
    /// exactly, for example an activity failure. Holds the event type.
    InexactResolution(&'static str),
    /// An event after the own events neither resolves a parked await nor
    /// reports its progress. Holds the event type.
    UnexpectedEvent(&'static str),
    /// The parked future no longer waits for the result.
    ReceiverDropped,
    /// A cycle that re-parked a sibling activity did more than wait for it
    /// (issue #2008). A branch ran a command, failed or dropped a sibling.
    /// A cold replay can read that history another way.
    SiblingStillParked,
    /// The workflow shares the history of its driver, and the driver lent
    /// none, or one that the workflow did not give back.
    HistoryMismatch,
    /// A drive did not give the lent history back. The driver cannot go on
    /// with the history that it holds.
    HistoryNotReturned,
}

/// The resident state of one suspension (issue #1798, issue #2007).
///
/// The cache keeps it with the snapshot. The next decision reads it to
/// resume the workflow, or to report why the decision replays.
#[derive(Debug, Default)]
// A cache entry holds one value per run. A box would add an allocation to
// each suspension.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Residency {
    /// No capture ran, for example on a terminal cycle or with resident
    /// state off.
    #[default]
    Unknown,
    /// The suspension stayed resident.
    Parked(ResidentWorkflow),
    /// The suspension did not stay resident, for this reason. The reason is
    /// never `Hit` or `Cold`.
    Missed(
        // Only the db-gated worker reads the reason.
        #[cfg_attr(not(feature = "db"), allow(dead_code))] ResidentOutcome,
    ),
}

impl Residency {
    /// The parked workflow, if any.
    pub(crate) fn into_workflow(self) -> Option<ResidentWorkflow> {
        match self {
            Self::Parked(workflow) => Some(workflow),
            Self::Unknown | Self::Missed(_) => None,
        }
    }

    /// Whether the suspension stayed resident.
    pub(crate) const fn is_parked(&self) -> bool {
        matches!(self, Self::Parked(_))
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
    execution_timeout: Option<jiff::SignedDuration>,
    deadline_at: Option<jiff::Timestamp>,
    parent_execution_id: Option<ExecutionId>,
    start_provenance: crate::types::StartProvenance,
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
            start_provenance: span_meta.map(|m| m.start_provenance).unwrap_or_default(),
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
            && self.start_provenance == other.start_provenance
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

/// One command that a resident workflow awaits, with its live channel.
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

    /// Whether `event` is a terminal event of this await, of any kind.
    fn is_resolved_by(&self, event: &WorkflowEvent) -> bool {
        match (self, event) {
            (
                Self::Activity { activity_id, .. },
                WorkflowEvent::ActivityCompleted {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityFailed {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityTimedOut {
                    activity_id: id, ..
                },
            ) => id == activity_id,
            (Self::Timer { timer_id, .. }, WorkflowEvent::TimerFired { timer_id: id }) => {
                id == timer_id
            }
            (
                Self::Signal { signal_name, .. },
                WorkflowEvent::SignalReceived {
                    signal_name: name, ..
                },
            ) => name == signal_name,
            _ => false,
        }
    }

    /// The command that parks this activity again, with its live sender
    /// (issue #2008). A cold replay emits the same command for an activity
    /// with no result. A timer or a signal cannot wait again this way.
    fn into_wait(self) -> Option<WorkflowCommand> {
        match self {
            Self::Activity {
                activity_id,
                result_tx,
            } => Some(WorkflowCommand::WaitForActivity {
                activity_id,
                result_tx,
            }),
            Self::Timer { .. } | Self::Signal { .. } => None,
        }
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

/// The awaited commands and own events of one suspension.
struct SuspensionPlan {
    /// The index of each awaited command, in command order.
    awaited: Vec<usize>,
    /// The events that the worker writes for the suspension.
    own_events: Vec<OwnEvent>,
}

/// Whether `cmd` awaits an activity result.
const fn awaits_activity(cmd: &WorkflowCommand) -> bool {
    matches!(
        cmd,
        WorkflowCommand::ScheduleActivity { .. } | WorkflowCommand::WaitForActivity { .. }
    )
}

/// Reads one suspension's commands. Returns its plan, or the reason why the
/// suspension cannot stay resident.
fn plan_suspension(commands: &[WorkflowCommand]) -> Result<SuspensionPlan, ResidentOutcome> {
    let mut awaited = Vec::new();
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
            | WorkflowCommand::PublishDurableProgress { .. }
            | WorkflowCommand::RecordLog { .. } => false,
            WorkflowCommand::AcquireMutex { .. } | WorkflowCommand::ReleaseMutex { .. } => {
                return Err(ResidentOutcome::Mutex);
            }
            _ => return Err(ResidentOutcome::Unsupported),
        };
        if awaits {
            if !cmd.awaits_result() {
                return Err(ResidentOutcome::Unsupported);
            }
            awaited.push(index);
        }
    }
    match awaited.as_slice() {
        [] => Err(ResidentOutcome::Unsupported),
        // A join of activities resolves branch by branch (issue #2008).
        [_] => Ok(SuspensionPlan {
            awaited,
            own_events,
        }),
        _ if awaited
            .iter()
            .all(|&index| awaits_activity(&commands[index])) =>
        {
            Ok(SuspensionPlan {
                awaited,
                own_events,
            })
        }
        // A timer or a signal next to another await follows other replay
        // rules. The path does not cover the mix.
        _ => Err(ResidentOutcome::MultiAwait),
    }
}

/// Whether a cycle that re-parked the activities `reparked` only waits for
/// them again (issue #2008).
///
/// The cycle must suspend with exactly these waits, and the capture must
/// keep each of them. A dropped sibling has a closed receiver, so the
/// capture refuses it.
fn only_waits_again(drive: &DriveResult, reparked: &[ActivityExecId]) -> bool {
    let WorkflowOutcome::Suspended { commands } = &drive.outcome else {
        return false;
    };
    commands.len() == reparked.len()
        && commands.iter().zip(reparked).all(|(cmd, id)| {
            matches!(cmd, WorkflowCommand::WaitForActivity { activity_id, .. } if activity_id == id)
        })
        && drive.resident.is_parked()
}

/// A suspended workflow that stays in memory between decisions (issue #1798).
///
/// It holds the parked handler future, its context, and the live channel of
/// each command the future awaits. Dropping it drops the future.
pub struct ResidentWorkflow {
    future: OwnedHandlerFuture,
    /// The parked awaits, in command order. Two or more are all activities.
    parked: Vec<Awaiting>,
    own_events: Vec<OwnEvent>,
    key: ResidentKey,
    ctx: Arc<WorkflowContext>,
}

impl std::fmt::Debug for ResidentWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResidentWorkflow")
            .field("execution_id", &self.ctx.execution_id())
            .field("parked", &self.parked.len())
            .field("own_events", &self.own_events)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl ResidentWorkflow {
    /// Keeps a suspended cycle resident when a warm decision can resume it.
    ///
    /// On success each awaited command in `outcome` gets a closed channel,
    /// and the returned value holds the live ones. On an error the caller
    /// drops `future` as on a cold cycle. The error names the miss reason
    /// (issue #2007).
    pub(crate) fn capture(
        ctx: &Arc<WorkflowContext>,
        future: OwnedHandlerFuture,
        outcome: &mut WorkflowOutcome,
        key: ResidentKey,
    ) -> Result<Self, ResidentOutcome> {
        let WorkflowOutcome::Suspended { commands } = outcome else {
            return Err(ResidentOutcome::Unsupported);
        };
        if let Some(blocker) = ctx.resident_blocker() {
            return Err(blocker);
        }
        let SuspensionPlan {
            awaited,
            own_events,
        } = plan_suspension(commands)?;
        // A cold replay could hand the new signal to an earlier probe.
        for &index in &awaited {
            if let WorkflowCommand::WaitForSignal { signal_name, .. } = &commands[index]
                && ctx.signal_probed_at_frontier(signal_name)
            {
                return Err(ResidentOutcome::Blocked);
            }
        }
        let parked = awaited
            .iter()
            .map(|&index| Awaiting::take_from(&mut commands[index]))
            .collect::<Option<Vec<_>>>()
            .ok_or(ResidentOutcome::Unsupported)?;
        Ok(Self {
            future,
            parked,
            own_events,
            key,
            ctx: Arc::clone(ctx),
        })
    }

    /// Matches `delta` to the parked awaits. Returns the index in `delta` of
    /// the resolving event of each parked await, or `None` for one that
    /// stays parked.
    ///
    /// This checks the order of events only. [`Awaiting::deliver`] checks
    /// that the live channel can carry each payload exactly.
    ///
    /// Decision boundaries (issue #1833) are skipped. Replay never reads them.
    ///
    /// A delta that holds all the own events and no result resolves no
    /// await. That is a redundant wake, so every await stays parked.
    fn resolutions(&self, delta: &[WorkflowEvent]) -> Result<Vec<Option<usize>>, ResumeDeclined> {
        let mut events = delta
            .iter()
            .enumerate()
            .filter(|(_, event)| !event.is_decision_boundary());
        // A delta shorter than the own events is checked as far as it goes.
        let mut own_seen = 0;
        let own_match = self.own_events.iter().all(|expected| {
            events.next().is_none_or(|(_, event)| {
                own_seen += 1;
                expected.matches(event)
            })
        });
        if !own_match {
            return Err(ResumeDeclined::OwnEventsMismatch);
        }
        let mut resolved: Vec<Option<usize>> = vec![None; self.parked.len()];
        for (at, event) in events {
            if resolved.iter().all(Option::is_some) {
                return Err(ResumeDeclined::ExtraEvents);
            }
            // Replay skips progress events of an activity only up to its
            // result.
            let progress = self
                .parked
                .iter()
                .zip(&resolved)
                .any(|(parked, result)| result.is_none() && parked.is_progress(event));
            if progress {
                continue;
            }
            let Some(index) = self
                .parked
                .iter()
                .position(|parked| parked.is_resolved_by(event))
            else {
                return Err(ResumeDeclined::UnexpectedEvent(event.type_name()));
            };
            if resolved[index].is_some() {
                return Err(ResumeDeclined::ExtraEvents);
            }
            resolved[index] = Some(at);
        }
        // A short delta can still lack an event that the last decision
        // wrote, so only a complete one is a redundant wake.
        if resolved.iter().all(Option::is_none) && own_seen < self.own_events.len() {
            return Err(ResumeDeclined::NoResolution);
        }
        Ok(resolved)
    }

    /// Resumes this workflow with the events written since it suspended.
    ///
    /// `key` is the key of this decision. `None` skips the key check.
    ///
    /// A delta with several results runs as one step per result, in history
    /// order (issue #2008). A cold replay matches results in that order. A
    /// branch that runs on after an early result thus faces the check of its
    /// own step. Each step but the last must only wait.
    ///
    /// Test and bench entry point, for a workflow that owns its history. The
    /// worker uses [`Self::resume_lending_with`].
    #[cfg(any(test, feature = "testing"))]
    pub(crate) async fn resume_with(
        self,
        delta: &[WorkflowEvent],
        key: Option<&ResidentKey>,
        span_meta: Option<&WorkflowExecuteSpanMeta>,
    ) -> Result<DriveResult, ResumeDeclined> {
        if key.is_some_and(|key| *key != self.key) {
            return Err(ResumeDeclined::KeyChanged);
        }
        // This workflow holds no history of its own. Only
        // [`Self::resume_lending_with`] can resume it.
        if self.ctx.history_is_released() {
            return Err(ResumeDeclined::HistoryMismatch);
        }
        let ends = self.step_ends(delta)?;
        let mut current = self;
        let mut start = 0;
        for (step, end) in ends.iter().enumerate() {
            let delta = ResidentDelta::Copied(&delta[start..*end]);
            let (_, drive) = current.step(delta, span_meta).await;
            let drive = drive?;
            if step + 1 == ends.len() {
                return Ok(drive);
            }
            current = drive
                .resident
                .into_workflow()
                .ok_or(ResumeDeclined::SiblingStillParked)?;
            start = *end;
        }
        Err(ResumeDeclined::NoResolution)
    }

    /// Where each step of a resume ends in `delta`: the index after its last
    /// event. A delta with several results runs one step per result.
    ///
    /// A redundant wake resolves nothing and runs one step over the whole
    /// delta. That step only waits again. A lone timer or signal cannot wait
    /// again, so the step declines it.
    fn step_ends(&self, delta: &[WorkflowEvent]) -> Result<Vec<usize>, ResumeDeclined> {
        let mut ends: Vec<usize> = self
            .resolutions(delta)?
            .into_iter()
            .flatten()
            .map(|at| at + 1)
            .collect();
        ends.sort_unstable();
        // The last step also takes the progress events after the last result.
        match ends.last_mut() {
            Some(end) => *end = delta.len(),
            None => ends.push(delta.len()),
        }
        Ok(ends)
    }

    /// Resumes this workflow with the events written since it suspended. The
    /// workflow shares the history of its driver.
    ///
    /// `key` is the key of this decision. `None` skips the key check.
    ///
    /// A delta with several results runs as one step per result, in history
    /// order (issue #2008). A cold replay matches results in that order. A
    /// branch that runs on after an early result thus faces the check of its
    /// own step. Each step but the last must only wait.
    ///
    /// A driver that lends its history to each cold drive gets it back, and
    /// the parked workflow then holds no events. This call lends the history
    /// again. The events before `delta_start` are the ones that the workflow
    /// gave back. The events from `delta_start` are the delta. Each step
    /// takes its events with no copy. On return, `history` holds every event
    /// again, in order, with a resumed workflow or with a decline.
    ///
    /// # Errors
    ///
    /// Returns the reason of the decline. After
    /// [`ResumeDeclined::HistoryNotReturned`] alone, `history` is not whole,
    /// and the driver must load it again.
    pub(crate) async fn resume_lending_with(
        self,
        history: &mut Vec<WorkflowEvent>,
        delta_start: usize,
        key: Option<&ResidentKey>,
        span_meta: Option<&WorkflowExecuteSpanMeta>,
    ) -> Result<DriveResult, ResumeDeclined> {
        if key.is_some_and(|key| *key != self.key) {
            return Err(ResumeDeclined::KeyChanged);
        }
        let Some(delta) = history.get(delta_start..) else {
            return Err(ResumeDeclined::HistoryMismatch);
        };
        let ends = self.step_ends(delta)?;
        // Cut the delta into the events of each step, last step first, so
        // that each event moves once.
        let mut steps: Vec<Vec<WorkflowEvent>> = Vec::with_capacity(ends.len());
        let starts: Vec<usize> = std::iter::once(0)
            .chain(ends.iter().copied())
            .take(ends.len())
            .collect();
        for start in starts.into_iter().rev() {
            steps.push(history.split_off(delta_start + start));
        }
        steps.reverse();
        // `history` now holds the events that the workflow gave back.
        let ctx = Arc::clone(&self.ctx);
        let mut held = Some(std::mem::take(history));
        let mut current = Some(self);
        let mut result = Err(ResumeDeclined::NoResolution);
        let last_step = steps.len().saturating_sub(1);
        for (index, step) in steps.iter_mut().enumerate() {
            let Some(workflow) = current.take() else {
                break;
            };
            let Some(events) = held.take() else {
                break;
            };
            if let Err(events) = ctx.restore_history(events) {
                held = Some(events);
                result = Err(ResumeDeclined::HistoryMismatch);
                break;
            }
            let (returned, drive) = workflow
                .step(ResidentDelta::Lent(std::mem::take(step)), span_meta)
                .await;
            // A step that declined before its drive leaves its events here.
            let (unused, returned) = returned;
            *step = unused;
            held = returned.or_else(|| ctx.reclaim_history());
            match drive {
                Ok(drive) if index == last_step => result = Ok(drive),
                Ok(drive) => {
                    let Some(workflow) = drive.resident.into_workflow() else {
                        result = Err(ResumeDeclined::SiblingStillParked);
                        break;
                    };
                    current = Some(workflow);
                }
                Err(declined) => {
                    result = Err(declined);
                    break;
                }
            }
        }
        // Put the history together again: what the workflow holds, then the
        // events of each step that did not run.
        let Some(mut whole) = held else {
            return Err(ResumeDeclined::HistoryNotReturned);
        };
        for step in steps {
            whole.extend(step);
        }
        *history = whole;
        result
    }

    /// Runs one cycle that delivers the results in `delta`.
    ///
    /// Each parked activity with no result waits again. Such a cycle runs
    /// as a speculation: it must only wait, and it must not read the replay
    /// position. A cold replay can read either one another way.
    ///
    /// Returns the events of a lent delta too, so that no path drops them.
    /// The first list holds the events that the step did not take, because
    /// it declined before its cycle began. The second holds the whole lent
    /// history after the drive. A copied delta returns neither.
    async fn step(
        self,
        delta: ResidentDelta<'_>,
        span_meta: Option<&WorkflowExecuteSpanMeta>,
    ) -> (
        (Vec<WorkflowEvent>, Option<Vec<WorkflowEvent>>),
        Result<DriveResult, ResumeDeclined>,
    ) {
        let resolved = match self.resolutions(delta.events()) {
            Ok(resolved) => resolved,
            Err(declined) => return ((delta.into_lent(), None), Err(declined)),
        };
        let Self {
            future,
            parked,
            key,
            ctx,
            ..
        } = self;
        let mut waits = Vec::new();
        let mut reparked = Vec::new();
        for (parked, at) in parked.into_iter().zip(resolved) {
            if let Some(at) = at {
                if let Err(declined) = parked.deliver(&delta.events()[at]) {
                    return ((delta.into_lent(), None), Err(declined));
                }
                continue;
            }
            // Only an activity can wait again. A lone timer or signal that
            // a redundant wake does not resolve declines here.
            let Some(wait) = parked.into_wait() else {
                return ((delta.into_lent(), None), Err(ResumeDeclined::NoResolution));
            };
            if let WorkflowCommand::WaitForActivity { activity_id, .. } = &wait {
                reparked.push(*activity_id);
            }
            waits.push(wait);
        }
        ctx.begin_resident_cycle(delta, waits);
        let speculation = Arc::clone(&ctx);
        let mut drive = crate::executor::drive_resumed(ctx, future, span_meta, key).await;
        let returned = drive.history.take();
        let read_position = speculation.end_speculation();
        // Dropping the drive drops the future. The worker then replays cold.
        if !reparked.is_empty() && (read_position || !only_waits_again(&drive, &reparked)) {
            return (
                (Vec::new(), returned),
                Err(ResumeDeclined::SiblingStillParked),
            );
        }
        ((Vec::new(), returned), Ok(drive))
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
    let (outcome, residency) = start_residency(exec_id, history, handler, input).await;
    (outcome, residency.into_workflow())
}

/// [`start`], with the history lent to the context as the worker lends it.
///
/// Returns the history too, with a parked workflow or with none. A workflow
/// that stays resident then holds no events, so the run has one history.
/// Resume it with [`ResidentWorkflow::resume_lending`], which lends the
/// history again. The history is `None` only when the context could not give
/// it back whole.
#[cfg(any(test, feature = "testing"))]
pub async fn start_lending(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> (
    WorkflowOutcome,
    Option<ResidentWorkflow>,
    Option<Vec<WorkflowEvent>>,
) {
    let ctx = crate::executor::default_task_context(exec_id, history);
    ctx.lend_history();
    let key = ResidentKey::new(handler, None, &HashMap::new());
    let drive = crate::executor::drive_workflow_keep(ctx, handler, input, None, Some(key)).await;
    (drive.outcome, drive.resident.into_workflow(), drive.history)
}

/// [`start`] that also returns why the suspension did not stay resident.
#[cfg(any(test, feature = "testing"))]
pub(crate) async fn start_residency(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> (WorkflowOutcome, Residency) {
    let ctx = crate::executor::default_task_context(exec_id, history);
    let key = ResidentKey::new(handler, None, &HashMap::new());
    let drive = crate::executor::drive_workflow_keep(ctx, handler, input, None, Some(key)).await;
    (drive.outcome, drive.resident)
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
        Ok((drive.outcome, drive.resident.into_workflow()))
    }

    /// [`Self::resume`] for a workflow from [`start_lending`], which shares
    /// the history of its caller.
    ///
    /// `history` is the whole history. Its events before `delta_start` are
    /// the ones that the last drive returned, and the rest were appended
    /// since. The call lends them all and gives them all back in `history`,
    /// with a resumed workflow or with a decline.
    ///
    /// # Errors
    ///
    /// Returns [`ResumeDeclined`] as [`Self::resume`] does. It also declines
    /// a history that the workflow did not give back.
    pub async fn resume_lending(
        self,
        history: &mut Vec<WorkflowEvent>,
        delta_start: usize,
    ) -> Result<(WorkflowOutcome, Option<Self>), ResumeDeclined> {
        let drive = self
            .resume_lending_with(history, delta_start, None, None)
            .await?;
        Ok((drive.outcome, drive.resident.into_workflow()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{WorkflowCommand, WorkflowContext};
    use crate::types::{ActivityExecId, TimerId};
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
            timestamp: crate::time::utc_ymd_hms(2026, 10, 3, 0, 0, 0).expect("valid timestamp"),
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
            WorkflowCommand::PublishDurableProgress { offset, chunk } => {
                format!("PublishDurableProgress({offset}, {chunk})")
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
        /// The reason of each declined resume, in order.
        declines: Vec<ResumeDeclined>,
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
                    declines: Vec::new(),
                };
            };
            history.extend(own_events(&commands));
            // A live worker closes each decision with a boundary (issue #1833).
            history.push(decision_boundary());
            history.push(resolution(&commands));
        }
        panic!("the run did not finish in 32 decisions");
    }

    /// The stored form of each event. `WorkflowEvent` has no `PartialEq`.
    fn stored_form(events: &[WorkflowEvent]) -> Vec<Value> {
        events
            .iter()
            .map(|event| serde_json::to_value(event).expect("an event serializes"))
            .collect()
    }

    /// A cold start that lends its history to the context, as the worker
    /// does. The drive must give the history back unchanged, with a parked
    /// workflow or with none.
    async fn start_lent(
        exec_id: ExecutionId,
        history: Vec<WorkflowEvent>,
        handler: WorkflowHandlerFn,
        input: Value,
    ) -> (WorkflowOutcome, Option<ResidentWorkflow>) {
        let lent = stored_form(&history);
        let (outcome, resident, returned) = start_lending(exec_id, history, handler, input).await;
        let returned = returned.expect("a cold drive returns its history");
        assert_eq!(
            stored_form(&returned),
            lent,
            "the history comes back as lent"
        );
        (outcome, resident)
    }

    /// A warm step that lends the whole history to the workflow, as the
    /// worker does. The step must give every event back in order, with a
    /// resumed workflow or with a decline.
    async fn resume_lent(
        live: ResidentWorkflow,
        history: &[WorkflowEvent],
        delta_start: usize,
    ) -> Option<(WorkflowOutcome, Option<ResidentWorkflow>)> {
        let mut lent = history.to_vec();
        let step = live.resume_lending(&mut lent, delta_start).await;
        assert_eq!(
            stored_form(&lent),
            stored_form(history),
            "a warm step returns the history as lent: {:?}",
            step.as_ref().err()
        );
        step.ok()
    }

    /// A cold start that gives the context a history of its own, or lends it.
    async fn cold_start(
        lend: bool,
        exec_id: ExecutionId,
        history: Vec<WorkflowEvent>,
        handler: WorkflowHandlerFn,
        input: Value,
    ) -> (WorkflowOutcome, Option<ResidentWorkflow>) {
        if lend {
            start_lent(exec_id, history, handler, input).await
        } else {
            start(exec_id, history, handler, input).await
        }
    }

    /// Drives a run that resumes the resident workflow when it can.
    async fn drive_warm(handler: WorkflowHandlerFn, input: Value) -> Trace {
        drive_warm_from(false, handler, input).await
    }

    /// [`drive_warm`], with each cold start lending its history when `lend`
    /// is set.
    async fn drive_warm_from(lend: bool, handler: WorkflowHandlerFn, input: Value) -> Trace {
        let exec_id = ExecutionId::new();
        let mut history = vec![started(input.clone())];
        let mut shapes = Vec::new();
        let mut resumes = 0;
        let (mut outcome, mut resident) =
            cold_start(lend, exec_id, history.clone(), handler, input.clone()).await;
        for _ in 0..32 {
            shapes.push(outcome_shape(&outcome));
            let WorkflowOutcome::Suspended { commands } = &outcome else {
                return Trace {
                    shapes,
                    history,
                    resumes,
                    declines: Vec::new(),
                };
            };
            let start_len = history.len();
            history.extend(own_events(commands));
            history.push(decision_boundary());
            history.push(resolution(commands));
            let warm_step = match resident.take() {
                Some(live) if lend => resume_lent(live, &history, start_len).await,
                Some(live) => live.resume(&history[start_len..]).await.ok(),
                None => None,
            };
            (outcome, resident) = match warm_step {
                Some(next) => {
                    resumes += 1;
                    next
                }
                None => cold_start(lend, exec_id, history.clone(), handler, input.clone()).await,
            };
        }
        panic!("the run did not finish in 32 decisions");
    }

    /// Asserts that the warm run decided like the cold run, and that its
    /// history replays cold to the same end.
    ///
    /// A warm run that lends its history to each drive must decide the same,
    /// and resume as often. Its parked workflow holds no events between two
    /// decisions.
    async fn assert_warm_matches_cold(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let cold = drive_cold(handler, input.clone()).await;
        let warm = drive_warm(handler, input.clone()).await;
        assert_eq!(warm.shapes, cold.shapes, "warm decisions must equal cold");
        let lent = drive_warm_from(true, handler, input.clone()).await;
        assert_eq!(lent.shapes, cold.shapes, "lent decisions must equal cold");
        assert_eq!(lent.resumes, warm.resumes, "a lent run resumes as often");
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

    /// Awaits an activity and a timer at once.
    fn mixed_join_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (a, t) = futures::join!(
                ctx.execute_activity_raw("a", json!({}), "default"),
                ctx.timer("t", 5),
            );
            t.map_err(|e| e.to_string())?;
            a.map_err(|e| e.to_string())
        })
    }

    fn race_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
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

    /// Parks on a payload-matching signal wait (issue #1985).
    ///
    /// A warm resume sends any same-name payload to the parked future. It
    /// does not run the predicate.
    fn predicate_wait_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.wait_for_signal_matching("order", |p| p["id"] == 42)
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Acquires a durable mutex, then awaits an activity.
    fn mutex_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let _guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn child_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            ctx.spawn_child_workflow_raw("child", json!({}))
                .await
                .map_err(|e| e.to_string())
        })
    }

    #[tokio::test]
    async fn capture_names_why_a_suspension_is_not_resident() {
        // Issue #2007: each miss reason is the `outcome` label of a decision.
        let cases: [(&str, WorkflowHandlerFn, ResidentOutcome); 7] = [
            (
                "mixed join",
                mixed_join_workflow,
                ResidentOutcome::MultiAwait,
            ),
            ("race", race_workflow, ResidentOutcome::Race),
            ("signal handler", handler_workflow, ResidentOutcome::Blocked),
            ("condition", condition_workflow, ResidentOutcome::Blocked),
            ("mutex", mutex_workflow, ResidentOutcome::Mutex),
            (
                "child workflow",
                child_workflow,
                ResidentOutcome::Unsupported,
            ),
            (
                "signal predicate",
                predicate_wait_workflow,
                ResidentOutcome::Blocked,
            ),
        ];
        for (name, handler, expected) in cases {
            let (outcome, residency) = start_residency(
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
            assert!(
                matches!(residency, Residency::Missed(reason) if reason == expected),
                "{name}: expected the miss reason {expected}, got {residency:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_race_in_flight_is_a_race_miss_on_a_cold_replay() {
        // A cold replay of a race in flight emits only waits. The open-race
        // count still names the miss.
        let (a, b) = (ActivityExecId::new(), ActivityExecId::new());
        let scheduled = |id, name: &str| WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: name.into(),
            input: json!({}),
            queue: "default".into(),
        };
        let history = vec![
            started(Value::Null),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".into(),
                details: json!(2),
            },
            scheduled(a, "a"),
            scheduled(b, "b"),
        ];
        let (outcome, residency) =
            start_residency(ExecutionId::new(), history, race_workflow, Value::Null).await;
        assert!(
            matches!(outcome, WorkflowOutcome::Suspended { .. }),
            "{outcome:?}"
        );
        assert!(
            matches!(residency, Residency::Missed(ResidentOutcome::Race)),
            "{residency:?}"
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

    /// An update that the edge admitted while the workflow was parked (issue #2035).
    fn admitted_update() -> WorkflowEvent {
        WorkflowEvent::UpdateAdmitted {
            update_id: crate::types::UpdateId::new(),
            name: "bump".into(),
            input: Value::Null,
            timestamp: jiff::Timestamp::now(),
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
                ResumeDeclined::UnexpectedEvent("WorkflowCancelled"),
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
                "an update admitted before the result",
                ResumeDeclined::UnexpectedEvent("UpdateAdmitted"),
                Box::new(|own, id| [own.to_vec(), vec![admitted_update(), completed(id)]].concat()),
            ),
            (
                "an update admitted after the result",
                ResumeDeclined::ExtraEvents,
                Box::new(|own, id| [own.to_vec(), vec![completed(id), admitted_update()]].concat()),
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
            Some(ResumeDeclined::UnexpectedEvent("ActivityStarted")),
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
            drive_clock: None,
            is_replay: true,
            link_traceparent: None,
            build_id: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            start_provenance: crate::types::StartProvenance::default(),
            cancellation_reason: None,
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
    async fn a_wake_with_nothing_new_waits_again() {
        // A second wake for results that the last decision already read
        // brings only the own events of the suspension.
        let (resident, own, id) = suspended_activity().await;
        let (outcome, next) = resident
            .resume(&own)
            .await
            .expect("a wake with nothing new must not decline");
        assert!(
            matches!(&outcome, WorkflowOutcome::Suspended { commands }
                if matches!(commands.as_slice(), [WorkflowCommand::WaitForActivity { activity_id, .. }] if *activity_id == id)),
            "the cycle must only wait again: {outcome:?}"
        );
        let next = next.expect("the workflow stays resident");
        let (outcome, _) = next
            .resume(&[decision_boundary(), completed(id)])
            .await
            .expect("the result resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Completed { output, .. } if *output == json!(1)),
            "{outcome:?}"
        );
    }

    /// [`suspended_activity`], with the history lent, as the worker lends
    /// it. Returns the history after the decision wrote its own events. The
    /// delta starts at index 1.
    async fn suspended_activity_lent() -> (ResidentWorkflow, Vec<WorkflowEvent>, ActivityExecId) {
        let (outcome, resident, returned) = start_lending(
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
        let mut history = returned.expect("a cold drive returns its history");
        assert_eq!(history.len(), 1, "the drive returns the event that it got");
        history.extend(own_events(&commands));
        (
            resident.expect("an activity suspension is resident"),
            history,
            id,
        )
    }

    #[tokio::test]
    async fn a_workflow_that_shares_its_history_declines_a_resume_with_none() {
        // The parked workflow holds no events. A resume that lends no
        // history would replay against an empty one.
        let (resident, history, id) = suspended_activity_lent().await;
        let mut delta = history[1..].to_vec();
        delta.extend([decision_boundary(), completed(id)]);
        assert_eq!(
            resident.resume(&delta).await.err(),
            Some(ResumeDeclined::HistoryMismatch)
        );
    }

    #[tokio::test]
    async fn a_lent_resume_declines_a_history_that_the_workflow_did_not_give_back() {
        // One event more before the delta than the workflow gave back.
        let (resident, history, id) = suspended_activity_lent().await;
        let mut longer = vec![started(Value::Null)];
        longer.extend(history);
        longer.extend([decision_boundary(), completed(id)]);
        let lent = stored_form(&longer);
        assert_eq!(
            resident.resume_lending(&mut longer, 2).await.err(),
            Some(ResumeDeclined::HistoryMismatch)
        );
        assert_eq!(stored_form(&longer), lent, "a decline returns every event");

        // A delta that starts past the end of the history.
        let (resident, mut history, _) = suspended_activity_lent().await;
        let lent = stored_form(&history);
        let past_the_end = history.len() + 1;
        assert_eq!(
            resident
                .resume_lending(&mut history, past_the_end)
                .await
                .err(),
            Some(ResumeDeclined::HistoryMismatch)
        );
        assert_eq!(stored_form(&history), lent, "a decline returns every event");
    }

    #[tokio::test]
    async fn a_lent_resume_that_declines_its_delta_returns_every_event() {
        // The delta lacks the own events of the suspension.
        let (resident, _, id) = suspended_activity_lent().await;
        let mut history = vec![started(Value::Null), decision_boundary(), completed(id)];
        let lent = stored_form(&history);
        assert_eq!(
            resident.resume_lending(&mut history, 1).await.err(),
            Some(ResumeDeclined::OwnEventsMismatch)
        );
        assert_eq!(stored_form(&history), lent, "a decline returns every event");
    }

    #[tokio::test]
    async fn a_lent_wake_with_nothing_new_waits_again_on_one_history() {
        // The redundant wake of `a_wake_with_nothing_new_waits_again`, with
        // the history lent to each decision.
        let (resident, mut history, id) = suspended_activity_lent().await;
        let lent = stored_form(&history);
        let (outcome, next) = resident
            .resume_lending(&mut history, 1)
            .await
            .expect("a wake with nothing new must not decline");
        assert!(
            matches!(&outcome, WorkflowOutcome::Suspended { commands }
                if matches!(commands.as_slice(), [WorkflowCommand::WaitForActivity { activity_id, .. }] if *activity_id == id)),
            "the cycle must only wait again: {outcome:?}"
        );
        assert_eq!(stored_form(&history), lent, "the wake returns every event");

        // The next delta starts after the events that the wake returned.
        let delta_start = history.len();
        history.extend([decision_boundary(), completed(id)]);
        let lent = stored_form(&history);
        let next = next.expect("the workflow stays resident");
        let (outcome, _) = next
            .resume_lending(&mut history, delta_start)
            .await
            .expect("the result resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Completed { output, .. } if *output == json!(1)),
            "{outcome:?}"
        );
        // The matched completion keeps its output in the one history.
        assert_eq!(
            stored_form(&history),
            lent,
            "the result stays in the history"
        );
    }

    #[tokio::test]
    async fn a_wake_with_nothing_new_declines_for_a_lone_timer() {
        let (outcome, resident) = start(
            ExecutionId::new(),
            vec![started(Value::Null)],
            timer_loop_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let own = own_events(&commands);
        let resident = resident.expect("a timer suspension is resident");
        assert_eq!(
            resident.resume(&own).await.err(),
            Some(ResumeDeclined::NoResolution),
            "only an activity can wait again"
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

    // ── Parallel activity awaits (issue #2008) ───────────────────────

    /// Which open awaits one decision resolves.
    #[derive(Debug, Clone, Copy)]
    enum Arrival {
        /// The oldest open await.
        Oldest,
        /// The newest open await.
        Newest,
        /// Every open await, in one delta.
        All,
        /// A random non-empty subset of the open awaits, in random order.
        /// The value seeds the choice.
        Shuffled(u64),
    }

    /// Every arrival order, each with and without progress events.
    const MODES: [(Arrival, bool); 12] = [
        (Arrival::Oldest, false),
        (Arrival::Newest, false),
        (Arrival::All, false),
        (Arrival::Shuffled(1), false),
        (Arrival::Shuffled(2), false),
        (Arrival::Shuffled(3), false),
        (Arrival::Oldest, true),
        (Arrival::Newest, true),
        (Arrival::All, true),
        (Arrival::Shuffled(4), true),
        (Arrival::Shuffled(5), true),
        (Arrival::Shuffled(6), true),
    ];

    /// A small deterministic generator for [`Arrival::Shuffled`].
    fn next_random(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state >> 33
    }

    /// Picks the resolving events of one decision from `open`.
    fn pick(open: Vec<WorkflowEvent>, arrival: Arrival, state: &mut u64) -> Vec<WorkflowEvent> {
        match arrival {
            Arrival::Oldest => open.into_iter().take(1).collect(),
            Arrival::Newest => open.into_iter().last().into_iter().collect(),
            Arrival::All => open,
            Arrival::Shuffled(_) => {
                let mut rest = open;
                let mut picked = Vec::new();
                while !rest.is_empty() {
                    let index = usize::try_from(next_random(state)).unwrap_or(0) % rest.len();
                    picked.push(rest.remove(index));
                    if next_random(state).is_multiple_of(2) {
                        break;
                    }
                }
                picked
            }
        }
    }

    /// The progress events a worker writes for the open activities: a start
    /// for each activity that has none yet.
    fn starts(history: &[WorkflowEvent], open: &[WorkflowEvent]) -> Vec<WorkflowEvent> {
        open.iter()
            .filter_map(|event| match event {
                WorkflowEvent::ActivityCompleted { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .filter(|id| {
                !history.iter().any(|e| {
                    matches!(e, WorkflowEvent::ActivityStarted { activity_id, .. } if activity_id == id)
                })
            })
            .map(|activity_id| WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: crate::types::WorkerId::new("w"),
            })
            .collect()
    }

    /// A heartbeat for each open activity that `picked` does not resolve.
    fn heartbeats(open: &[WorkflowEvent], picked: &[WorkflowEvent]) -> Vec<WorkflowEvent> {
        let activity = |event: &WorkflowEvent| match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. } => Some(*activity_id),
            _ => None,
        };
        let resolved: Vec<ActivityExecId> = picked.iter().filter_map(activity).collect();
        open.iter()
            .filter_map(activity)
            .filter(|id| !resolved.contains(id))
            .map(|activity_id| WorkflowEvent::ActivityHeartbeat {
                activity_id,
                details: Value::Null,
            })
            .collect()
    }

    /// Whether `history` holds an event after index `from` that matches.
    fn later(history: &[WorkflowEvent], from: usize, hit: impl Fn(&WorkflowEvent) -> bool) -> bool {
        history.iter().skip(from + 1).any(hit)
    }

    /// The resolving events of the awaits that `history` and the last
    /// suspension leave open, oldest first.
    fn open_resolutions(
        history: &[WorkflowEvent],
        commands: &[WorkflowCommand],
    ) -> Vec<WorkflowEvent> {
        let mut open = Vec::new();
        for (index, event) in history.iter().enumerate() {
            match event {
                WorkflowEvent::ActivityScheduled {
                    activity_id, input, ..
                } => {
                    let done = later(
                        history,
                        index,
                        |e| matches!(e, WorkflowEvent::ActivityCompleted { activity_id: id, .. } if id == activity_id),
                    );
                    if !done {
                        open.push(WorkflowEvent::ActivityCompleted {
                            activity_id: *activity_id,
                            output: json!({ "echo": input }),
                        });
                    }
                }
                WorkflowEvent::TimerStarted { timer_id, .. } => {
                    let fired = later(
                        history,
                        index,
                        |e| matches!(e, WorkflowEvent::TimerFired { timer_id: id } if id == timer_id),
                    );
                    if !fired {
                        open.push(WorkflowEvent::TimerFired {
                            timer_id: timer_id.clone(),
                        });
                    }
                }
                _ => {}
            }
        }
        for cmd in commands {
            if let WorkflowCommand::WaitForSignal { signal_name, .. } = cmd {
                open.push(WorkflowEvent::SignalReceived {
                    signal_name: signal_name.clone(),
                    payload: json!({ "signal": signal_name }),
                });
            }
        }
        open
    }

    /// The name and input of the activity with `id` in `history`.
    fn activity_name(history: &[WorkflowEvent], id: ActivityExecId) -> String {
        history
            .iter()
            .find_map(|event| match event {
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name,
                    input,
                    ..
                } if *activity_id == id => Some(format!("{name}, {input}")),
                _ => None,
            })
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// [`outcome_shape`] that names each waited activity, so two runs with
    /// other random ids compare equal.
    fn outcome_shape_in(outcome: &WorkflowOutcome, history: &[WorkflowEvent]) -> String {
        let WorkflowOutcome::Suspended { commands } = outcome else {
            return outcome_shape(outcome);
        };
        let shapes: Vec<String> = commands
            .iter()
            .map(|cmd| match cmd {
                WorkflowCommand::WaitForActivity { activity_id, .. } => {
                    format!("WaitForActivity({})", activity_name(history, *activity_id))
                }
                other => shape(other),
            })
            .collect();
        format!("Suspended[{}]", shapes.join(", "))
    }

    /// Drives a run. Each decision resolves the awaits that `arrival` picks.
    ///
    /// With `progress`, the delta also holds a start for each open activity
    /// before the results. It holds a heartbeat for each activity that stays
    /// open after them, as a worker writes them. With `warm`, a decision
    /// resumes the resident workflow when it can. Otherwise every decision
    /// replays cold.
    async fn drive_arrivals(
        handler: WorkflowHandlerFn,
        input: Value,
        mode: (Arrival, bool),
        warm: bool,
    ) -> Trace {
        drive_arrivals_from(false, handler, input, mode, warm).await
    }

    /// [`drive_arrivals`], with a warm run that shares one history when
    /// `lend` is set. Each cold start then lends its history, and each warm
    /// step lends it again, as the worker does.
    async fn drive_arrivals_from(
        lend: bool,
        handler: WorkflowHandlerFn,
        input: Value,
        (arrival, progress): (Arrival, bool),
        warm: bool,
    ) -> Trace {
        let exec_id = ExecutionId::new();
        let mut history = vec![started(input.clone())];
        let mut shapes = Vec::new();
        let mut resumes = 0;
        let mut declines = Vec::new();
        let mut state = match arrival {
            Arrival::Shuffled(seed) => seed,
            _ => 0,
        };
        let (mut outcome, mut resident) = if warm {
            cold_start(lend, exec_id, history.clone(), handler, input.clone()).await
        } else {
            let cold =
                crate::executor::run_workflow(exec_id, history.clone(), handler, input.clone())
                    .await;
            (cold, None)
        };
        for _ in 0..64 {
            shapes.push(outcome_shape_in(&outcome, &history));
            let WorkflowOutcome::Suspended { commands } = &outcome else {
                return Trace {
                    shapes,
                    history,
                    resumes,
                    declines,
                };
            };
            let start_len = history.len();
            history.extend(own_events(commands));
            history.push(decision_boundary());
            let open = open_resolutions(&history, commands);
            if progress {
                let starts = starts(&history, &open);
                history.extend(starts);
            }
            let picked = pick(open.clone(), arrival, &mut state);
            assert!(!picked.is_empty(), "a suspension must leave an await open");
            history.extend(picked.iter().cloned());
            if progress {
                history.extend(heartbeats(&open, &picked));
            }
            let delta = history[start_len..].to_vec();
            let stepped = match resident.take() {
                Some(live) if lend => {
                    let mut shared = history.clone();
                    let step = live.resume_lending(&mut shared, start_len).await;
                    assert_eq!(
                        stored_form(&shared),
                        stored_form(&history),
                        "a warm step returns the history as lent: {:?}",
                        step.as_ref().err()
                    );
                    Some(step)
                }
                Some(live) => Some(live.resume(&delta).await),
                None => None,
            };
            let warm_step = match stepped {
                Some(Ok(next)) => Some(next),
                Some(Err(reason)) => {
                    declines.push(reason);
                    None
                }
                None => None,
            };
            (outcome, resident) = match warm_step {
                Some(next) => {
                    resumes += 1;
                    next
                }
                None if warm => {
                    cold_start(lend, exec_id, history.clone(), handler, input.clone()).await
                }
                None => {
                    let cold = crate::executor::run_workflow(
                        exec_id,
                        history.clone(),
                        handler,
                        input.clone(),
                    )
                    .await;
                    (cold, None)
                }
            };
        }
        panic!("the run did not finish in 64 decisions");
    }

    /// Asserts that the warm run decides like the cold run for `arrival`,
    /// and that its history replays cold to the same end.
    async fn assert_arrivals_match_cold(
        handler: WorkflowHandlerFn,
        input: Value,
        mode: (Arrival, bool),
    ) -> Trace {
        let arrival = mode;
        let cold = drive_arrivals(handler, input.clone(), mode, false).await;
        let warm = drive_arrivals(handler, input.clone(), mode, true).await;
        assert_eq!(
            warm.shapes, cold.shapes,
            "{arrival:?}: warm decisions must equal cold"
        );
        // A warm run that shares one history with its driver decides the
        // same, resumes as often, and declines for the same reasons.
        let lent = drive_arrivals_from(true, handler, input.clone(), mode, true).await;
        assert_eq!(
            lent.shapes, cold.shapes,
            "{arrival:?}: lent decisions must equal cold"
        );
        assert_eq!(lent.resumes, warm.resumes, "{arrival:?}: lent resumes");
        assert_eq!(lent.declines, warm.declines, "{arrival:?}: lent declines");
        let replayed =
            crate::executor::run_workflow(ExecutionId::new(), warm.history.clone(), handler, input)
                .await;
        assert_eq!(
            Some(outcome_shape_in(&replayed, &warm.history)),
            warm.shapes.last().cloned(),
            "{arrival:?}: the warm history must replay cold to the same end"
        );
        warm
    }

    /// Asserts that every decision after the first resumed warm.
    fn assert_every_decision_resumes(warm: &Trace, mode: (Arrival, bool)) {
        assert!(warm.declines.is_empty(), "{mode:?}: {:?}", warm.declines);
        assert_eq!(
            warm.resumes,
            warm.shapes.len() - 1,
            "{mode:?}: every decision after the first must resume: {:?}",
            warm.shapes
        );
    }

    /// Joins two activities, then runs a third.
    fn pair_then_c_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (a, b) = futures::join!(
                ctx.execute_activity_raw("a", json!({ "n": 1 }), "default"),
                ctx.execute_activity_raw("b", json!({ "n": 2 }), "default"),
            );
            let a = a.map_err(|e| e.to_string())?;
            let b = b.map_err(|e| e.to_string())?;
            let c = ctx
                .execute_activity_raw("c", json!([a, b]), "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({ "c": c, "events": ctx.history_event_count() }))
        })
    }

    #[tokio::test]
    async fn an_activity_join_stays_resident() {
        let (outcome, residency) = start_residency(
            ExecutionId::new(),
            vec![started(Value::Null)],
            pair_then_c_workflow,
            Value::Null,
        )
        .await;
        assert!(
            matches!(&outcome, WorkflowOutcome::Suspended { commands } if commands.len() == 2),
            "{outcome:?}"
        );
        assert!(
            residency.is_parked(),
            "a join of two activities must stay resident: {residency:?}"
        );
    }

    #[tokio::test]
    async fn warm_activity_join_matches_cold_replay_in_every_arrival_order() {
        for mode in MODES {
            let warm = assert_arrivals_match_cold(pair_then_c_workflow, Value::Null, mode).await;
            assert_every_decision_resumes(&warm, mode);
        }
        // Two partial results, then the result of `c`.
        let warm = drive_arrivals(
            pair_then_c_workflow,
            Value::Null,
            (Arrival::Oldest, false),
            true,
        )
        .await;
        assert_eq!(warm.resumes, 3);
    }

    /// An agent loop: a model call, then parallel tool calls, per round.
    fn tool_loop_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let rounds = input["rounds"].as_u64().ok_or("missing rounds")?;
            let tools = input["tools"].as_u64().ok_or("missing tools")?;
            let mut transcript = Vec::new();
            for round in 0..rounds {
                let plan = ctx
                    .execute_activity_raw(
                        &format!("model{round}"),
                        json!({ "round": round }),
                        "default",
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                // With `reuse`, every call has one name, as an agent that
                // calls one tool twice.
                let names: Vec<String> = (0..tools)
                    .map(|tool| {
                        if input["reuse"] == json!(true) {
                            "tool".to_string()
                        } else {
                            format!("tool{round}.{tool}")
                        }
                    })
                    .collect();
                let calls = names.iter().zip(0..tools).map(|(name, tool)| {
                    ctx.execute_activity_raw(name, json!({ "plan": plan, "tool": tool }), "default")
                });
                let results = futures::future::try_join_all(calls)
                    .await
                    .map_err(|e| e.to_string())?;
                transcript.push(json!(results));
            }
            Ok(json!({ "transcript": transcript, "events": ctx.history_event_count() }))
        })
    }

    #[tokio::test]
    async fn warm_tool_loop_resumes_every_decision_in_every_arrival_order() {
        for reuse in [false, true] {
            let input = json!({ "rounds": 2, "tools": 3, "reuse": reuse });
            for mode in MODES {
                let warm =
                    assert_arrivals_match_cold(tool_loop_workflow, input.clone(), mode).await;
                assert_every_decision_resumes(&warm, mode);
            }
        }
        // Per round: one model result, then three tool results.
        let input = json!({ "rounds": 2, "tools": 3 });
        let warm = drive_arrivals(tool_loop_workflow, input, (Arrival::Newest, false), true).await;
        assert_eq!(warm.resumes, 8);
    }

    /// Body starts of `counted_tool_loop_workflow`. Only one test runs it.
    static TOOL_LOOP_BODY_STARTS: AtomicUsize = AtomicUsize::new(0);

    /// `tool_loop_workflow` with a body-start counter.
    fn counted_tool_loop_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
        TOOL_LOOP_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
        tool_loop_workflow(ctx, input)
    }

    #[tokio::test]
    async fn warm_tool_loop_runs_the_body_once() {
        let input = json!({ "rounds": 3, "tools": 4 });
        let warm = drive_arrivals(
            counted_tool_loop_workflow,
            input,
            (Arrival::Shuffled(7), true),
            true,
        )
        .await;
        assert!(
            warm.shapes
                .last()
                .is_some_and(|s| s.starts_with("Completed(")),
            "{:?}",
            warm.shapes.last()
        );
        assert_eq!(
            TOOL_LOOP_BODY_STARTS.load(Ordering::SeqCst),
            1,
            "a partial tool result must not replay the body"
        );
    }

    /// Fans out three activities with the fan-out helper.
    fn fan_out_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let calls = (0..3)
                .map(|i| {
                    (
                        format!("leg{i}"),
                        json!({ "leg": i }),
                        "default".to_string(),
                    )
                })
                .collect();
            let legs = ctx
                .execute_activity_fan_out_raw(calls)
                .await
                .map_err(|e| e.to_string())?;
            let total = ctx
                .execute_activity_raw("total", json!(legs), "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({ "total": total, "events": ctx.history_event_count() }))
        })
    }

    #[tokio::test]
    async fn warm_fan_out_matches_cold_replay_in_every_arrival_order() {
        for mode in MODES {
            let warm = assert_arrivals_match_cold(fan_out_workflow, Value::Null, mode).await;
            assert_every_decision_resumes(&warm, mode);
        }
    }

    /// Joins `a` with `b`. A bad result of `a` fails the join while `b`
    /// is still parked.
    fn failing_join_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let checked_a = async {
                let a = ctx
                    .execute_activity_raw("a", json!({}), "default")
                    .await
                    .map_err(|e| e.to_string())?;
                if a.get("echo").is_some() {
                    return Err("a is not valid".to_string());
                }
                Ok(a)
            };
            let b = async {
                ctx.execute_activity_raw("b", json!({}), "default")
                    .await
                    .map_err(|e| e.to_string())
            };
            let (a, b) = futures::try_join!(checked_a, b)?;
            Ok(json!([a, b]))
        })
    }

    /// Joins `a` with a branch that runs `c` after `b`.
    fn last_branch_runs_on_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (a, bc) =
                futures::join!(ctx.execute_activity_raw("a", json!({}), "default"), async {
                    ctx.execute_activity_raw("b", json!({}), "default").await?;
                    ctx.execute_activity_raw("c", json!({}), "default").await
                },);
            Ok(json!([
                a.map_err(|e| e.to_string())?,
                bc.map_err(|e| e.to_string())?
            ]))
        })
    }

    /// Joins a branch that runs `c` after `a` with `b`. A cold replay
    /// fails this shape once `a` and `b` are both done.
    fn first_branch_runs_on_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (ac, b) = futures::join!(
                async {
                    ctx.execute_activity_raw("a", json!({}), "default").await?;
                    ctx.execute_activity_raw("c", json!({}), "default").await
                },
                ctx.execute_activity_raw("b", json!({}), "default"),
            );
            Ok(json!([
                ac.map_err(|e| e.to_string())?,
                b.map_err(|e| e.to_string())?
            ]))
        })
    }

    #[tokio::test]
    async fn a_branch_that_runs_on_while_a_sibling_is_parked_replays_cold() {
        for (name, handler, arrival) in [
            (
                "failing join",
                failing_join_workflow as WorkflowHandlerFn,
                Arrival::Oldest,
            ),
            (
                "last branch runs on",
                last_branch_runs_on_workflow,
                Arrival::Newest,
            ),
            (
                "first branch runs on",
                first_branch_runs_on_workflow,
                Arrival::Oldest,
            ),
            // Both results in one delta: the first result runs first, as in
            // a cold replay, so its branch faces the check.
            (
                "first branch runs on, together",
                first_branch_runs_on_workflow,
                Arrival::All,
            ),
            (
                "position read",
                position_in_branch_workflow,
                Arrival::Oldest,
            ),
            (
                "position read, together",
                position_in_branch_workflow,
                Arrival::All,
            ),
            ("unread read", unread_in_branch_workflow, Arrival::Oldest),
        ] {
            for progress in [false, true] {
                let warm =
                    assert_arrivals_match_cold(handler, Value::Null, (arrival, progress)).await;
                assert!(
                    warm.declines.contains(&ResumeDeclined::SiblingStillParked),
                    "{name}: the resume must decline: {:?}",
                    warm.declines
                );
            }
        }
    }

    /// Reads the replay position in a branch while `b` is parked, then
    /// passes it to `c`.
    fn position_in_branch_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (seen, b) = futures::join!(
                async {
                    ctx.execute_activity_raw("a", json!({}), "default").await?;
                    Ok::<_, crate::error::HarvestError>(json!({
                        "info": ctx.info().history_event_count,
                        "position": ctx.replay_position(),
                        "replaying": ctx.is_replaying(),
                    }))
                },
                ctx.execute_activity_raw("b", json!({}), "default"),
            );
            let seen = seen.map_err(|e| e.to_string())?;
            b.map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("c", seen, "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Asks whether history is unread in a branch while `b` is parked, then
    /// passes the answer to `c`.
    fn unread_in_branch_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
        Box::pin(async move {
            let (seen, b) = futures::join!(
                async {
                    ctx.execute_activity_raw("a", json!({}), "default").await?;
                    Ok::<_, crate::error::HarvestError>(json!(ctx.history_has_unconsumed_events()))
                },
                ctx.execute_activity_raw("b", json!({}), "default"),
            );
            let seen = seen.map_err(|e| e.to_string())?;
            b.map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("c", seen, "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Suspends `pair_then_c_workflow` and returns its own events and the
    /// ids of `a` and `b`.
    async fn suspended_pair() -> (
        ResidentWorkflow,
        Vec<WorkflowEvent>,
        ActivityExecId,
        ActivityExecId,
    ) {
        let (outcome, resident) = start(
            ExecutionId::new(),
            vec![started(Value::Null)],
            pair_then_c_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let ids: Vec<ActivityExecId> = commands
            .iter()
            .filter_map(|c| match c {
                WorkflowCommand::ScheduleActivity { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .collect();
        let [a, b] = ids[..] else {
            panic!("the join schedules two activities: {ids:?}");
        };
        let own = own_events(&commands);
        (resident.expect("a join stays resident"), own, a, b)
    }

    #[tokio::test]
    async fn a_partial_delta_re_parks_the_sibling() {
        let (resident, own, a, b) = suspended_pair().await;
        let progress = WorkflowEvent::ActivityStarted {
            activity_id: a,
            worker_id: crate::types::WorkerId::new("w"),
        };
        let delta = [own, vec![progress.clone(), completed(b), progress]].concat();
        let (outcome, next) = resident
            .resume(&delta)
            .await
            .expect("a partial delta resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Suspended { commands }
                if matches!(commands.as_slice(), [WorkflowCommand::WaitForActivity { activity_id, .. }] if *activity_id == a)),
            "the cycle must wait for `a` only: {outcome:?}"
        );
        let next = next.expect("the sibling stays resident");
        let (outcome, _) = next
            .resume(&[decision_boundary(), completed(a)])
            .await
            .expect("the second result resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Suspended { commands }
                if matches!(commands.as_slice(), [WorkflowCommand::ScheduleActivity { name, .. }] if name == "c")),
            "the join completes and schedules `c`: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn join_deltas_that_replay_could_read_differently_decline() {
        type PairDelta = Box<dyn Fn(ActivityExecId, ActivityExecId) -> Vec<WorkflowEvent>>;
        let started_other = WorkflowEvent::ActivityStarted {
            activity_id: ActivityExecId::new(),
            worker_id: crate::types::WorkerId::new("w"),
        };
        let failed = |id| WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: "boom".into(),
            attempt: 1,
            error_type: "Error".into(),
            details: None,
            non_retryable: false,
        };
        let cases: Vec<(&str, ResumeDeclined, PairDelta)> = vec![
            (
                "a result twice",
                ResumeDeclined::ExtraEvents,
                Box::new(|a, _b| vec![completed(a), completed(a)]),
            ),
            (
                "a sibling failure",
                ResumeDeclined::InexactResolution("ActivityFailed"),
                Box::new(move |a, b| vec![completed(a), failed(b)]),
            ),
            (
                "progress of another activity",
                ResumeDeclined::UnexpectedEvent("ActivityStarted"),
                Box::new(move |a, _b| vec![started_other.clone(), completed(a)]),
            ),
            (
                "a sibling timeout",
                ResumeDeclined::InexactResolution("ActivityTimedOut"),
                Box::new(|a, b| {
                    vec![
                        completed(a),
                        WorkflowEvent::ActivityTimedOut {
                            activity_id: b,
                            timeout_type: crate::error::TimeoutType::StartToClose,
                        },
                    ]
                }),
            ),
            (
                "a result of another activity",
                ResumeDeclined::UnexpectedEvent("ActivityCompleted"),
                Box::new(|a, _b| vec![completed(a), completed(ActivityExecId::new())]),
            ),
            (
                "an event after every result",
                ResumeDeclined::ExtraEvents,
                Box::new(|a, b| {
                    vec![
                        completed(a),
                        completed(b),
                        WorkflowEvent::SignalReceived {
                            signal_name: "x".into(),
                            payload: Value::Null,
                        },
                    ]
                }),
            ),
        ];
        for (name, expected, build) in cases {
            let (resident, own, a, b) = suspended_pair().await;
            let delta = [own, build(a, b)].concat();
            assert_eq!(
                resident.resume(&delta).await.err(),
                Some(expected),
                "{name}: the resume must decline for this reason"
            );
        }
    }

    #[tokio::test]
    async fn a_join_declines_when_its_context_inputs_change() {
        let (resident, own, a, _b) = suspended_pair().await;
        let delta = [own, vec![completed(a)]].concat();
        let meta = crate::executor::WorkflowExecuteSpanMeta {
            workflow_name: "moved".into(),
            workflow_id: String::new(),
            shard_id: 7,
            queue_name: String::new(),
            drive_clock: None,
            is_replay: true,
            link_traceparent: None,
            build_id: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            start_provenance: crate::types::StartProvenance::default(),
            cancellation_reason: None,
        };
        let key = ResidentKey::new(pair_then_c_workflow, Some(&meta), &HashMap::new());
        let declined = resident.resume_with(&delta, Some(&key), Some(&meta)).await;
        assert_eq!(declined.err(), Some(ResumeDeclined::KeyChanged));
    }

    #[test]
    fn keys_differ_when_only_the_start_provenance_differs() {
        let meta = |provenance| crate::executor::WorkflowExecuteSpanMeta {
            workflow_name: "wf".into(),
            workflow_id: String::new(),
            shard_id: 0,
            queue_name: String::new(),
            drive_clock: None,
            is_replay: false,
            link_traceparent: None,
            build_id: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            start_provenance: provenance,
            cancellation_reason: None,
        };
        let key = |m: &crate::executor::WorkflowExecuteSpanMeta| {
            ResidentKey::new(pair_then_c_workflow, Some(m), &HashMap::new())
        };
        let api = meta(crate::types::StartProvenance::fresh(
            crate::types::StartSource::Api,
        ));
        let again = meta(crate::types::StartProvenance::fresh(
            crate::types::StartSource::Api,
        ));
        let can = meta(crate::types::StartProvenance::continued_from(
            ExecutionId::new(),
        ));
        assert_eq!(key(&api), key(&again));
        assert_ne!(key(&api), key(&can));
    }

    #[tokio::test]
    async fn a_sibling_left_parked_by_a_failed_branch_declines() {
        let (outcome, resident) = start(
            ExecutionId::new(),
            vec![started(Value::Null)],
            failing_join_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let resident = resident.expect("a join stays resident");
        let a = commands
            .iter()
            .find_map(|c| match c {
                WorkflowCommand::ScheduleActivity {
                    activity_id, name, ..
                } if name == "a" => Some(*activity_id),
                _ => None,
            })
            .expect("the join schedules `a`");
        let delta = [
            own_events(&commands),
            vec![WorkflowEvent::ActivityCompleted {
                activity_id: a,
                output: json!({ "echo": {} }),
            }],
        ]
        .concat();
        assert_eq!(
            resident.resume(&delta).await.err(),
            Some(ResumeDeclined::SiblingStillParked),
            "a failed join leaves `b` parked, so the cycle must replay cold"
        );
    }
}
