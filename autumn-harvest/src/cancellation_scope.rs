//! Cancellation scopes (issue #1984).
//!
//! A [`CancellationScope`] cancels a group of in-flight operations as a unit.
//! [`WorkflowContext::non_cancellable`] shields a block from a workflow
//! cancel. See `DESIGN-1984.md` for the design record.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::{LoserCancelReason, WorkflowCommand, WorkflowContext};
use crate::error::{HarvestError, HarvestResult};
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::types::{ActivityExecId, ExecutionId, TimerId};

/// The reason that a cancelled scope reports.
const SCOPE_CANCEL_REASON: &str = "cancellation scope cancelled";

/// The name prefix of a scope cancel marker.
const CANCEL_MARKER_PREFIX: &str = "cancel_scope:";

/// The marker that records the cancel decision of scope `seq`.
fn cancel_marker_name(seq: u32) -> String {
    format!("{CANCEL_MARKER_PREFIX}{seq}")
}

/// The marker that opens non-cancellable block `seq`.
pub(crate) fn shield_open_marker_name(seq: u32) -> String {
    format!("{SHIELD_OPEN_MARKER_PREFIX}{seq}")
}

/// The marker that closes non-cancellable block `seq`.
pub(crate) fn shield_close_marker_name(seq: u32) -> String {
    format!("{SHIELD_CLOSE_MARKER_PREFIX}{seq}")
}

/// Name prefix of the marker that opens a non-cancellable block.
pub(crate) const SHIELD_OPEN_MARKER_PREFIX: &str = "non_cancellable_open:";

/// Name prefix of the marker that closes a non-cancellable block.
pub(crate) const SHIELD_CLOSE_MARKER_PREFIX: &str = "non_cancellable_close:";

/// The operations that a scope cancels.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ScopeMembers {
    #[serde(default)]
    pub(crate) activities: Vec<ActivityExecId>,
    #[serde(default)]
    pub(crate) children: Vec<ExecutionId>,
    #[serde(default)]
    pub(crate) timers: Vec<TimerId>,
}

impl ScopeMembers {
    /// Add the activity, timer or child that `cmd` starts or waits on.
    fn add_from(&mut self, cmd: &WorkflowCommand) {
        match cmd {
            WorkflowCommand::ScheduleActivity { activity_id, .. }
            | WorkflowCommand::WaitForActivity { activity_id, .. }
            | WorkflowCommand::ScheduleExternalActivity { activity_id, .. }
                if !self.activities.contains(activity_id) =>
            {
                self.activities.push(*activity_id);
            }
            WorkflowCommand::StartChildWorkflow { child_id, .. }
                if !self.children.contains(child_id) =>
            {
                self.children.push(*child_id);
            }
            WorkflowCommand::StartTimer { timer_id, .. }
            | WorkflowCommand::ArmTimer {
                timer_id,
                for_await: true,
                ..
            } if !self.timers.contains(timer_id) => {
                self.timers.push(timer_id.clone());
            }
            _ => {}
        }
    }

    /// Drop the members that `event` resolves.
    fn drop_resolved_by(&mut self, event: &WorkflowEvent) {
        match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
            | WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
            | WorkflowEvent::ActivityFailedExternally { activity_id, .. } => {
                self.activities.retain(|id| id != activity_id);
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                self.children.retain(|id| id != child_id);
            }
            WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id } => {
                self.timers.retain(|id| id != timer_id);
            }
            _ => {}
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.activities.is_empty() && self.children.is_empty() && self.timers.is_empty()
    }
}

/// The details of a `cancel_scope:{seq}` marker.
#[derive(Debug, Serialize, Deserialize)]
struct CancelMarker {
    reason: String,
    /// The number of history events that the cancelling cycle saw.
    horizon: usize,
    #[serde(flatten)]
    members: ScopeMembers,
    /// The values that the body recorded in the cancelling cycle, in order.
    /// The cancel withdraws their commands, so replay reads them from here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    records: Vec<CapturedRecord>,
}

/// A recorded decision that a scope cancel withdraws (issue #1984).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(crate) enum CapturedRecord {
    /// A `RecordSideEffect`.
    SideEffect {
        /// The push order in the cancelling cycle.
        #[serde(default)]
        order: u64,
        kind: SideEffectKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        value: Value,
    },
    /// A `RecordMarker`, such as a version gate.
    Marker {
        /// The push order in the cancelling cycle.
        #[serde(default)]
        order: u64,
        name: String,
        details: Value,
    },
}

impl CapturedRecord {
    const fn order(&self) -> u64 {
        match self {
            Self::SideEffect { order, .. } | Self::Marker { order, .. } => *order,
        }
    }

    /// Names the record for a non-determinism diagnostic.
    fn label(&self) -> String {
        match self {
            Self::SideEffect { kind, name, .. } => side_effect_label(*kind, name.as_deref()),
            Self::Marker { name, .. } => format!("MarkerRecorded({name})"),
        }
    }
}

/// Names a side effect for a non-determinism diagnostic.
fn side_effect_label(kind: SideEffectKind, name: Option<&str>) -> String {
    format!(
        "SideEffectRecorded({})",
        name.unwrap_or_else(|| kind.as_str())
    )
}

/// The recorded decisions among `cmds`, in push order.
///
/// A nested scope that cancelled in the same cycle withdrew its own records
/// into its marker. That marker is withdrawn now, so its records move here.
/// The push order puts them back among the other records, as the body made
/// them. The nested cancel decision stays too, without its records, so a
/// replay that drops the nested cancel reports drift.
fn captured_records(cmds: &[(u64, WorkflowCommand)]) -> Vec<CapturedRecord> {
    let mut records = Vec::new();
    for (order, cmd) in cmds {
        let order = *order;
        match cmd {
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                records.push(CapturedRecord::SideEffect {
                    order,
                    kind: *kind,
                    name: name.clone(),
                    value: value.clone(),
                });
            }
            WorkflowCommand::RecordMarker { name, details }
                if name.starts_with(CANCEL_MARKER_PREFIX) =>
            {
                if let Ok(inner) = serde_json::from_value::<CancelMarker>(details.clone()) {
                    records.extend(inner.records);
                }
                records.push(CapturedRecord::Marker {
                    order,
                    name: name.clone(),
                    details: cancel_decision(details),
                });
            }
            WorkflowCommand::RecordMarker { name, details } => {
                records.push(CapturedRecord::Marker {
                    order,
                    name: name.clone(),
                    details: details.clone(),
                });
            }
            _ => {}
        }
    }
    records.sort_by_key(CapturedRecord::order);
    records
}

/// One entry of the context's scope stack.
#[derive(Clone)]
pub(crate) enum ScopeFrame {
    /// The body of a cancellable scope is being polled.
    Cancellable(Arc<ScopeShared>),
    /// The body of a non-cancellable block is being polled.
    Shield,
}

impl ScopeFrame {
    pub(crate) const fn is_cancellable(&self) -> bool {
        matches!(self, Self::Cancellable(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Running,
    Done,
}

/// State that a scope handle and its run future share.
pub(crate) struct ScopeShared {
    seq: u32,
    inner: Mutex<ScopeInner>,
}

struct ScopeInner {
    phase: Phase,
    cancel_requested: bool,
    waker: Option<Waker>,
    /// On replay of a cancelled scope, the body's commands stay here.
    holding: bool,
    held: Vec<WorkflowCommand>,
    members: ScopeMembers,
    /// On replay of a cancelled scope, the values that the body recorded.
    records: std::collections::VecDeque<CapturedRecord>,
    /// The handle timer arms that the body makes, matched or live, by epoch.
    /// A replay that matches an arm pushes no command, so `members` can miss it.
    armed_timers: Vec<(TimerId, u64)>,
}

impl ScopeShared {
    fn lock(&self) -> MutexGuard<'_, ScopeInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Drop the members that the events of a resident cycle resolved.
///
/// A resident future keeps its scopes across cycles. A resolved member must
/// leave the set. Otherwise a later operation that reuses its timer id would
/// look like a member, and a cancel would tear it down.
/// Give the other live scopes back the arm that an undone mutation replaced.
///
/// A nested scope that arms or resets a timer records the new epoch in each
/// enclosing scope. Its cancel restores the timer's prior epoch. An enclosing
/// scope that tracks the undone epoch must track the prior arm again, or a
/// later cancel of that scope would miss the arm. The cancelled scope keeps
/// its own entry, so its cancel does not claim the prior arm.
pub(crate) fn rewind_armed_timer(
    live: &[std::sync::Weak<ScopeShared>],
    undone_scope: u32,
    timer_id: &str,
    undone: u64,
    prior: Option<u64>,
) {
    for shared in live.iter().filter_map(std::sync::Weak::upgrade) {
        if shared.seq == undone_scope {
            continue;
        }
        let mut inner = shared.lock();
        let Some(at) = inner
            .armed_timers
            .iter()
            .position(|(id, epoch)| id.as_str() == timer_id && *epoch == undone)
        else {
            continue;
        };
        match prior {
            Some(epoch) => inner.armed_timers[at].1 = epoch,
            None => {
                inner.armed_timers.remove(at);
            }
        }
    }
}

/// Whether a scope that replays a cancel holds a command that parks.
///
/// The worker never gets a held command, so it is invisible to the park
/// check. A held park still means that the cycle waits on Harvest work.
pub(crate) fn holds_a_park(live: &[std::sync::Weak<ScopeShared>]) -> bool {
    live.iter()
        .filter_map(std::sync::Weak::upgrade)
        .any(|shared| {
            let inner = shared.lock();
            inner.holding && inner.held.iter().any(WorkflowCommand::awaits_result)
        })
}

pub(crate) fn drop_resolved_members(
    live: &mut Vec<std::sync::Weak<ScopeShared>>,
    delta: &[WorkflowEvent],
) {
    live.retain(|weak| {
        let Some(shared) = weak.upgrade() else {
            return false;
        };
        let mut inner = shared.lock();
        for event in delta {
            inner.members.drop_resolved_by(event);
        }
        true
    });
}

/// Route `cmd` through the scope stack, innermost scope first.
///
/// A shield stops the walk. A holding scope keeps the command and returns
/// `None`. Otherwise each scope records the command's operation as a member,
/// and the command goes to the buffer tagged with the scope numbers.
pub(crate) fn route_command(
    stack: &[ScopeFrame],
    cmd: WorkflowCommand,
) -> Option<(WorkflowCommand, Vec<u32>)> {
    let scopes: Vec<&Arc<ScopeShared>> = stack
        .iter()
        .rev()
        .map_while(|frame| match frame {
            ScopeFrame::Cancellable(shared) => Some(shared),
            ScopeFrame::Shield => None,
        })
        .collect();
    // The outermost holding scope lives longest, so it keeps the command.
    // Each scope inside it still records the member. Its re-derived cancel
    // marker then names the same members as the live one.
    if let Some(at) = scopes.iter().rposition(|shared| shared.lock().holding) {
        for shared in &scopes[..at] {
            shared.lock().members.add_from(&cmd);
        }
        scopes[at].lock().held.push(cmd);
        return None;
    }
    let mut tags = Vec::with_capacity(scopes.len());
    for shared in scopes {
        shared.lock().members.add_from(&cmd);
        tags.push(shared.seq);
    }
    Some((cmd, tags))
}

/// The result of a lookup for a captured decision (issue #1984).
pub(crate) enum Captured {
    /// No scope on the stack replays a cancel, so the primitive runs as usual.
    NotReplaying,
    /// The next captured decision, which the primitive returns.
    Value(Value),
    /// The replayed body asks for a decision out of order, or for one that
    /// the live cycle did not make. The text names both sides.
    Drift(String),
}

/// Take the next captured decision of the innermost replaying scope.
///
/// Replay must ask for the decisions in the order that the live cycle made
/// them, as a positional history match does. A record that does not match
/// the next one, or a request with no record left, is drift.
fn take_captured(
    stack: &[ScopeFrame],
    asked: &str,
    wanted: impl Fn(&CapturedRecord) -> Option<Value>,
) -> Captured {
    for frame in stack.iter().rev() {
        let ScopeFrame::Cancellable(shared) = frame else {
            break;
        };
        let mut inner = shared.lock();
        if !inner.holding {
            continue;
        }
        let next = inner
            .records
            .front()
            .map(|record| (wanted(record), record.label()));
        let taken = match next {
            Some((Some(value), _)) => {
                inner.records.pop_front();
                Captured::Value(value)
            }
            Some((None, expected)) => Captured::Drift(format!(
                "the replayed body asked for {asked}, but the next recorded decision is {expected}"
            )),
            None => Captured::Drift(format!(
                "the replayed body asked for {asked}, but no recorded decision is left"
            )),
        };
        drop(inner);
        return taken;
    }
    Captured::NotReplaying
}

/// The details of a cancel marker without its captured records.
fn cancel_decision(details: &Value) -> Value {
    let mut decision = details.clone();
    if let Some(fields) = decision.as_object_mut() {
        fields.remove("records");
    }
    decision
}

/// The drift of a replayed marker against the next captured record.
///
/// A doomed scope re-derives a withdrawn marker on replay, such as a race or
/// fan-out decision. Normal replay compares such a marker with history. Here
/// the captured record takes the place of history, so a deploy that changes
/// the decision reports drift instead of passing in silence. A version gate
/// takes its record when it is served, so it is not checked here. A nested
/// cancel marker is compared without its records, which moved to the outer
/// capture.
pub(crate) fn captured_marker_drift(stack: &[ScopeFrame], cmd: &WorkflowCommand) -> Option<String> {
    let WorkflowCommand::RecordMarker { name, details } = cmd else {
        return None;
    };
    if name.starts_with("version:") {
        return None;
    }
    let replayed = if name.starts_with(CANCEL_MARKER_PREFIX) {
        cancel_decision(details)
    } else {
        details.clone()
    };
    let asked = format!("MarkerRecorded({name})");
    // A `patched(id)` gate also matches a recorded `version:{id}` marker, as
    // the history matcher does. Its value does not matter then.
    let version_alias = name
        .strip_prefix("patch:")
        .map(crate::replay::version_marker_name);
    match take_captured(stack, &asked, |record| match record {
        CapturedRecord::Marker {
            name: n,
            details: captured,
            ..
        } if n == name => Some(captured.clone()),
        CapturedRecord::Marker { name: n, .. } if version_alias.as_deref() == Some(n.as_str()) => {
            Some(replayed.clone())
        }
        _ => None,
    }) {
        Captured::NotReplaying => None,
        Captured::Value(captured) if captured == replayed => None,
        Captured::Value(_) => Some(format!(
            "marker '{name}' differs from the one that a scope cancel recorded"
        )),
        Captured::Drift(drift) => Some(drift),
    }
}

/// The side-effect value that a cancelled scope recorded here, on replay.
pub(crate) fn take_captured_side_effect(
    stack: &[ScopeFrame],
    kind: SideEffectKind,
    name: Option<&str>,
) -> Captured {
    take_captured(
        stack,
        &side_effect_label(kind, name),
        |record| match record {
            CapturedRecord::SideEffect {
                kind: k,
                name: n,
                value,
                ..
            } if *k == kind && n.as_deref() == name => Some(value.clone()),
            _ => None,
        },
    )
}

/// The marker details that a cancelled scope recorded here, on replay.
pub(crate) fn take_captured_marker(stack: &[ScopeFrame], name: &str) -> Captured {
    take_captured(
        stack,
        &format!("MarkerRecorded({name})"),
        |record| match record {
            CapturedRecord::Marker {
                name: n, details, ..
            } if n == name => Some(details.clone()),
            _ => None,
        },
    )
}

/// The cancellable scopes on `stack` up to the nearest shield, innermost last.
/// Whether a scope on the stack replays a cancel.
///
/// Its body re-runs the cancelling cycle, whose side effects already fired.
pub(crate) fn replays_a_cancel(stack: &[ScopeFrame]) -> bool {
    stack.iter().any(|frame| match frame {
        ScopeFrame::Cancellable(shared) => shared.lock().holding,
        ScopeFrame::Shield => false,
    })
}

pub(crate) fn cancellable_seqs(stack: &[ScopeFrame]) -> Vec<u32> {
    let mut seqs: Vec<u32> = stack
        .iter()
        .rev()
        .map_while(|frame| match frame {
            ScopeFrame::Cancellable(shared) => Some(shared.seq),
            ScopeFrame::Shield => None,
        })
        .collect();
    seqs.reverse();
    seqs
}

/// Record that the body makes arm `epoch` of the handle timer `timer_id`.
///
/// Each cancellable scope up to the nearest shield records it. Live and
/// replay call this at the same point in the body. The epoch keeps a later
/// arm of the same id by other code out of the scope.
pub(crate) fn note_armed_timer(stack: &[ScopeFrame], timer_id: &str, epoch: u64) {
    for frame in stack.iter().rev() {
        let ScopeFrame::Cancellable(shared) = frame else {
            break;
        };
        let mut inner = shared.lock();
        inner.armed_timers.retain(|(id, _)| id.as_str() != timer_id);
        inner.armed_timers.push((TimerId::new(timer_id), epoch));
    }
}

/// A group of workflow operations that cancel together (issue #1984).
///
/// Create one with [`WorkflowContext::cancellation_scope`]. Run a body in
/// it with [`run`](Self::run). The scope tracks each activity, timer and
/// child workflow that the body starts. [`cancel`](Self::cancel) cancels all
/// of them. The run future then returns [`HarvestError::Cancelled`].
///
/// The cancel decision is a recorded marker, so a replay takes the same
/// decision at the same point.
///
/// ```rust,no_run
/// use autumn_harvest::{HarvestError, HarvestResult, WorkflowContext};
/// use serde_json::Value;
///
/// # async fn example(ctx: &WorkflowContext) -> HarvestResult<Value> {
/// let scope = ctx.cancellation_scope();
/// let (charged, ()) = tokio::join!(
///     scope.run(ctx.execute_activity_raw("charge", Value::Null, "default")),
///     async {
///         let _ = ctx.wait_for_signal("abort").await;
///         scope.cancel();
///     },
/// );
/// match charged {
///     Err(HarvestError::Cancelled(_)) => Ok(Value::from("aborted")),
///     other => other?,
/// }
/// # }
/// ```
///
/// Local activities, external activities and detached children are not
/// cancelled, but the body still stops. A scope runs one body only. A body
/// cannot acquire a durable mutex, signal, cancel or await an external
/// workflow: each has a durable result that a dropped body would strand.
/// A body cannot create a session either, because a dropped session is
/// never released.
#[derive(Clone)]
pub struct CancellationScope<'a> {
    ctx: &'a WorkflowContext,
    shared: Arc<ScopeShared>,
}

impl<'a> CancellationScope<'a> {
    fn new(ctx: &'a WorkflowContext) -> Self {
        let scope = Self {
            ctx,
            shared: Arc::new(ScopeShared {
                seq: ctx.next_scope_seq(),
                inner: Mutex::new(ScopeInner {
                    phase: Phase::Idle,
                    cancel_requested: false,
                    waker: None,
                    holding: false,
                    held: Vec::new(),
                    members: ScopeMembers::default(),
                    armed_timers: Vec::new(),
                    records: std::collections::VecDeque::new(),
                }),
            }),
        };
        ctx.register_scope(&scope.shared);
        scope
    }

    /// Run `body` inside this scope.
    ///
    /// The future returns `Ok` with the body's output when the body
    /// completes first. It returns `Err` when the scope is cancelled first.
    /// The body is then dropped.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Cancelled`] when the scope is cancelled first.
    /// - [`HarvestError::Config`] when the scope already ran a body.
    /// - [`HarvestError::NonDeterministic`] when replay cannot find the
    ///   recorded cancel.
    pub fn run<F: Future>(&self, body: F) -> ScopeRun<'a, F> {
        ScopeRun {
            ctx: self.ctx,
            shared: Arc::clone(&self.shared),
            body: Some(Box::pin(body)),
            started: false,
            finished: false,
            replay: None,
        }
    }

    /// Cancel every activity, timer and child workflow that the body started.
    ///
    /// The cancel takes effect at the next poll of the run future. A cancel
    /// after the body completes does nothing.
    pub fn cancel(&self) {
        let waker = {
            let mut inner = self.shared.lock();
            if inner.phase == Phase::Done {
                return;
            }
            inner.cancel_requested = true;
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether [`cancel`](Self::cancel) was called before the body completed.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.shared.lock().cancel_requested
    }
}

/// The future that [`CancellationScope::run`] returns.
#[must_use = "futures do nothing unless you .await or poll them"]
pub struct ScopeRun<'a, F: Future> {
    ctx: &'a WorkflowContext,
    shared: Arc<ScopeShared>,
    body: Option<Pin<Box<F>>>,
    started: bool,
    finished: bool,
    /// The recorded cancel, when this run replays a cancelled scope.
    replay: Option<CancelMarker>,
}

impl<F: Future> ScopeRun<'_, F> {
    /// Look for this scope's recorded cancel before the first poll.
    fn start(&mut self) -> HarvestResult<()> {
        {
            let mut inner = self.shared.lock();
            if inner.phase != Phase::Idle {
                return Err(HarvestError::Config(format!(
                    "cancellation scope {} already ran a body",
                    self.shared.seq
                )));
            }
            inner.phase = Phase::Running;
        }
        let name = cancel_marker_name(self.shared.seq);
        if let Some(details) = self.ctx.peek_marker_details(&name) {
            let marker = parse_cancel_marker(self.ctx, &name, details)?;
            {
                let mut inner = self.shared.lock();
                inner.holding = true;
                inner.records = marker.records.iter().cloned().collect();
            }
            self.replay = Some(marker);
        }
        Ok(())
    }

    /// Settle a cancelled scope: replay the recorded cancel or record it.
    fn settle_cancel(&mut self) -> HarvestResult<F::Output> {
        let ctx = self.ctx;
        let seq = self.shared.seq;
        let name = cancel_marker_name(seq);
        let mut withdrawn = Vec::new();
        let result = if let Some((index, details)) = ctx.take_marker(&name) {
            parse_cancel_marker(ctx, &name, details).and_then(|marker| {
                // Each captured decision stands in for a history event. One
                // that the replayed body did not ask for is drift.
                let unused = self.shared.lock().records.len();
                if unused > 0 {
                    return Err(ctx.scope_nd_error(
                        format!(
                            "cancellation scope {seq}: the replayed body skipped {unused} \
                             recorded decision(s) of the cancelling cycle"
                        ),
                        name.clone(),
                    ));
                }
                ctx.consume_scope_members(&marker.members, marker.horizon..index);
                // The held commands are the commands that the live cycle withdrew.
                let _ = ctx.undo_scope_timer_mutations(seq);
                ctx.mark_scope_timers_cancelled(&marker.members.timers);
                ctx.mark_scope_arms_cancelled(&self.armed_timers());
                Ok(marker.reason)
            })
        } else if self.replay.is_some() || ctx.has_unconsumed_marker(&name) {
            Err(ctx.scope_nd_error(
                format!(
                    "cancellation scope {seq}: the recorded cancel is not at this point on \
                     replay"
                ),
                name,
            ))
        } else {
            withdrawn = ctx.withdraw_scope_commands(seq);
            // A withdrawn reset or re-arm leaves an earlier arm in force. Its
            // durable row is not the scope's, so the cancel keeps it.
            let restored_arms = ctx.undo_scope_timer_mutations(seq);
            let mut members = ctx.open_scope_members(&self.shared.lock().members);
            members
                .timers
                .retain(|timer| !restored_arms.contains(timer));
            ctx.consume_cancelled_member_frontier(&members);
            let marker = CancelMarker {
                reason: SCOPE_CANCEL_REASON.to_string(),
                horizon: ctx.history_len(),
                members,
                records: captured_records(&withdrawn),
            };
            ctx.push_command(WorkflowCommand::RecordMarker {
                name,
                details: serde_json::to_value(&marker).unwrap_or(Value::Null),
            });
            ctx.mark_scope_timers_cancelled(&marker.members.timers);
            ctx.mark_scope_arms_cancelled(&self.armed_timers());
            if !marker.members.is_empty() {
                ctx.push_command(WorkflowCommand::CancelRaceLosers {
                    reason: LoserCancelReason::ScopeCancelled,
                    activities: marker.members.activities,
                    children: marker.members.children,
                    timers: marker.members.timers,
                });
            }
            Ok(marker.reason)
        };
        // Drop the body before the commands that hold its result channels.
        self.body = None;
        drop(withdrawn);
        result.and_then(|reason| Err(HarvestError::Cancelled(reason)))
    }

    fn armed_timers(&self) -> Vec<(TimerId, u64)> {
        self.shared.lock().armed_timers.clone()
    }

    fn finish(&mut self) {
        self.finished = true;
        self.body = None;
        let held = {
            let mut inner = self.shared.lock();
            inner.phase = Phase::Done;
            inner.holding = false;
            inner.waker = None;
            std::mem::take(&mut inner.held)
        };
        drop(held);
    }
}

fn parse_cancel_marker(
    ctx: &WorkflowContext,
    name: &str,
    details: Value,
) -> HarvestResult<CancelMarker> {
    serde_json::from_value(details).map_err(|e| {
        ctx.scope_nd_error(
            format!("{name}: the recorded cancel cannot be read: {e}"),
            name.to_string(),
        )
    })
}

impl<F: Future> Future for ScopeRun<'_, F> {
    type Output = HarvestResult<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(Err(HarvestError::Config(
                "cancellation scope run polled after it finished".to_string(),
            )));
        }
        if !this.started {
            this.started = true;
            if let Err(err) = this.start() {
                this.finished = true;
                this.body = None;
                return Poll::Ready(Err(err));
            }
        }
        this.shared.lock().waker = Some(cx.waker().clone());

        let ctx = this.ctx;
        let horizon = this.replay.as_ref().map(|marker| marker.horizon);
        if let Some(body) = this.body.as_mut() {
            let frame = ScopeFrame::Cancellable(Arc::clone(&this.shared));
            let polled = ctx.with_scope_frame(frame, || match horizon {
                Some(horizon) => ctx.with_history_horizon(horizon, || body.as_mut().poll(cx)),
                None => body.as_mut().poll(cx),
            });
            // A cancel that comes before or during this poll wins over a body
            // that completes in it. This includes a cancel by the body itself.
            // Replay sees the same flag, so both take one path.
            if let Poll::Ready(value) = polled {
                if !this.shared.lock().cancel_requested {
                    if horizon.is_none() {
                        this.finish();
                        return Poll::Ready(Ok(value));
                    }
                    // A body that completes without a cancel records no
                    // marker. So a replayed body that does so has lost the
                    // recorded cancel, and the scope would wait forever.
                    this.body = None;
                    let seq = this.shared.seq;
                    let err = ctx.scope_nd_error(
                        format!(
                            "cancellation scope {seq}: the recorded run cancelled the \
                             scope, but the replayed body completed without a cancel"
                        ),
                        cancel_marker_name(seq),
                    );
                    this.finish();
                    return Poll::Ready(Err(err));
                }
                // The cancel came first, live or on replay.
                this.body = None;
            }
        }

        if !this.shared.lock().cancel_requested {
            return Poll::Pending;
        }
        let result = this.settle_cancel();
        this.finish();
        Poll::Ready(result)
    }
}

/// The future that [`WorkflowContext::non_cancellable`] returns.
#[must_use = "futures do nothing unless you .await or poll them"]
pub struct NonCancellable<'a, F: Future> {
    ctx: &'a WorkflowContext,
    body: Option<Pin<Box<F>>>,
    seq: Option<u32>,
    open: bool,
    finished: bool,
}

/// Replay the shield marker `name`, or record it live.
fn record_shield_marker(ctx: &WorkflowContext, name: String) -> HarvestResult<()> {
    if ctx.take_marker(&name).is_some() {
        return Ok(());
    }
    if ctx.has_unconsumed_marker(&name) {
        return Err(ctx.scope_nd_error(
            format!("{name}: the recorded marker is not at this point on replay"),
            name,
        ));
    }
    ctx.push_command(WorkflowCommand::RecordMarker {
        name,
        details: Value::Null,
    });
    Ok(())
}

impl<F: Future> Future for NonCancellable<'_, F> {
    type Output = HarvestResult<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(Err(HarvestError::Config(
                "non-cancellable block polled after it finished".to_string(),
            )));
        }
        let ctx = this.ctx;
        let seq = if let Some(seq) = this.seq {
            seq
        } else {
            if ctx.in_cancellable_scope() {
                this.finished = true;
                this.body = None;
                return Poll::Ready(Err(HarvestError::Config(
                    "ctx.non_cancellable cannot run inside a cancellation scope".to_string(),
                )));
            }
            let seq = ctx.next_shield_seq();
            this.seq = Some(seq);
            if let Err(err) = record_shield_marker(ctx, shield_open_marker_name(seq)) {
                this.finished = true;
                this.body = None;
                return Poll::Ready(Err(err));
            }
            this.open = true;
            seq
        };
        let Some(body) = this.body.as_mut() else {
            return Poll::Pending;
        };
        let polled = ctx.with_scope_frame(ScopeFrame::Shield, || body.as_mut().poll(cx));
        let Poll::Ready(value) = polled else {
            return Poll::Pending;
        };
        this.body = None;
        this.finished = true;
        this.open = false;
        Poll::Ready(record_shield_marker(ctx, shield_close_marker_name(seq)).map(|()| value))
    }
}

impl<F: Future> Drop for NonCancellable<'_, F> {
    /// A block dropped before it completes still closes, so it cannot defer
    /// a workflow cancel for ever. A suspension drops every future, so it
    /// closes nothing (the `MutexGuard` rule).
    fn drop(&mut self) {
        if let Some(seq) = self.seq
            && self.open
            && !self.ctx.is_suspending()
        {
            self.body = None;
            let _ = record_shield_marker(self.ctx, shield_close_marker_name(seq));
        }
    }
}

impl WorkflowContext {
    /// Create a [`CancellationScope`] (issue #1984).
    #[must_use]
    pub fn cancellation_scope(&self) -> CancellationScope<'_> {
        CancellationScope::new(self)
    }

    /// Run `body` so that a workflow cancel cannot stop it (issue #1984).
    ///
    /// The block records a marker when it opens and when it closes. A
    /// workflow cancel that arrives while a block is open is deferred. The
    /// cancel request is recorded as `WorkflowCancelRequested`, and the run
    /// continues. The first suspended cycle with no open block cancels the
    /// run. A cycle that would fail or continue as new is cancelled instead.
    /// A cycle that completes keeps its result. A terminate, a paused run
    /// and a replace-on-start policy are never deferred.
    ///
    /// A block is open from the commit of the cycle that enters it. A cancel
    /// that commits first is terminal. The engine then discards that cycle,
    /// and the block's work with it.
    ///
    /// Workflow code does not see a deferred cancel: `is_cancelled` stays
    /// `false` until the cancel completes.
    ///
    /// ```rust,no_run
    /// use autumn_harvest::{HarvestResult, WorkflowContext};
    /// use serde_json::Value;
    ///
    /// # async fn example(ctx: &WorkflowContext) -> HarvestResult<Value> {
    /// let released = ctx
    ///     .non_cancellable(ctx.execute_activity_raw("release_hold", Value::Null, "default"))
    ///     .await??;
    /// # Ok(released)
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The future returns [`HarvestError::Config`] inside a cancellable
    /// scope, and [`HarvestError::NonDeterministic`] when replay cannot find
    /// a recorded marker.
    pub fn non_cancellable<F: Future>(&self, body: F) -> NonCancellable<'_, F> {
        NonCancellable {
            ctx: self,
            body: Some(Box::pin(body)),
            seq: None,
            open: false,
            finished: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use serde_json::{Value, json};

    use crate::context::{WorkflowCommand, WorkflowContext};
    use crate::error::{HarvestError, HarvestResult};
    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId, TimerId};

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(activity_id: ActivityExecId, name: &str) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: name.into(),
            input: Value::Null,
            queue: "default".into(),
        }
    }

    /// Fails the test when `fut` does not resolve.
    async fn bounded<F: std::future::Future>(fut: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .expect("the future must resolve")
    }

    /// Runs `body` in a new scope. A sibling branch cancels the scope after
    /// the first poll of the body.
    async fn run_then_cancel<F: std::future::Future>(
        ctx: &WorkflowContext,
        body: F,
    ) -> HarvestResult<F::Output> {
        let scope = ctx.cancellation_scope();
        let (result, ()) = tokio::join!(scope.run(body), async { scope.cancel() });
        result
    }

    struct Losers {
        activities: Vec<ActivityExecId>,
        children: Vec<ExecutionId>,
        timers: Vec<TimerId>,
    }

    fn losers(commands: &[WorkflowCommand]) -> Option<Losers> {
        commands.iter().find_map(|c| match c {
            WorkflowCommand::CancelRaceLosers {
                activities,
                children,
                timers,
                ..
            } => Some(Losers {
                activities: activities.clone(),
                children: children.clone(),
                timers: timers.clone(),
            }),
            _ => None,
        })
    }

    fn marker(commands: &[WorkflowCommand], wanted: &str) -> Option<Value> {
        commands.iter().find_map(|c| match c {
            WorkflowCommand::RecordMarker { name, details } if name == wanted => {
                Some(details.clone())
            }
            _ => None,
        })
    }

    fn count(commands: &[WorkflowCommand], pred: impl Fn(&WorkflowCommand) -> bool) -> usize {
        commands.iter().filter(|c| pred(c)).count()
    }

    #[tokio::test]
    async fn scope_cancel_cancels_an_in_flight_activity() {
        let a = ActivityExecId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![started(), scheduled(a, "charge")],
        );

        let result = bounded(run_then_cancel(
            &ctx,
            ctx.execute_activity_raw("charge", Value::Null, "default"),
        ))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = ctx.drain_commands();
        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.activities, vec![a]);
        assert!(
            marker(&commands, "cancel_scope:1").is_some(),
            "{commands:?}"
        );
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::WaitForActivity { .. }
            )),
            0,
            "a cancelled member must not park again: {commands:?}"
        );
    }

    #[tokio::test]
    async fn scope_cancel_cancels_an_in_flight_timer() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new("deadline"),
                    duration_secs: 60,
                },
            ],
        );

        let result = bounded(run_then_cancel(&ctx, ctx.timer("deadline", 60))).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = ctx.drain_commands();
        assert!(
            marker(&commands, "cancel_scope:1").is_some(),
            "{commands:?}"
        );
        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.timers, vec![TimerId::new("deadline")]);
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::StartTimer { .. }
            )),
            0,
            "a cancelled timer must not be armed again: {commands:?}"
        );
    }

    /// Runs a handle timer in a scope, cancels it, then arms the id again.
    async fn cancel_then_rearm(ctx: &WorkflowContext) -> Vec<WorkflowCommand> {
        let result = bounded(run_then_cancel(ctx, async {
            ctx.start_timer("deadline", 60).await_fire().await
        }))
        .await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let _handle = ctx.start_timer("deadline", 60);
        ctx.drain_commands()
    }

    fn arms(commands: &[WorkflowCommand]) -> usize {
        count(commands, |c| matches!(c, WorkflowCommand::ArmTimer { .. }))
    }

    /// A scope cancel withdraws a handle timer that the same cycle arms.
    /// A later arm of the id records a fresh arm, live and on replay.
    #[tokio::test]
    async fn scope_cancel_clears_the_armed_state_of_a_withdrawn_timer() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![started()]);

        let commands = cancel_then_rearm(&ctx).await;

        assert_eq!(
            arms(&commands),
            1,
            "the re-arm is not a no-op: {commands:?}"
        );
        let details = marker(&commands, "cancel_scope:1").expect("the cancel is recorded");
        let replay = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details,
                },
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new("deadline"),
                    duration_secs: 60,
                },
            ],
        );
        let replayed = cancel_then_rearm(&replay).await;
        assert_eq!(arms(&replayed), 0, "{replayed:?}");
        assert!(!replay.history_has_unconsumed_events());
    }

    /// A scope cancel deletes the row of an awaited handle timer.
    /// A later arm of the id records a fresh arm.
    #[tokio::test]
    async fn scope_cancel_clears_the_armed_state_of_a_member_timer() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new("deadline"),
                    duration_secs: 60,
                },
            ],
        );

        let commands = cancel_then_rearm(&ctx).await;

        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.timers, vec![TimerId::new("deadline")]);
        let rearm = commands.iter().rposition(|c| {
            matches!(
                c,
                WorkflowCommand::ArmTimer {
                    for_await: false,
                    ..
                }
            )
        });
        assert!(rearm.is_some(), "the re-arm is not a no-op: {commands:?}");
    }

    /// A handle timer that an earlier cycle armed and the body never awaits
    /// is not a member. The scope cancel still clears its armed state.
    #[tokio::test]
    async fn scope_cancel_clears_the_armed_state_of_an_unawaited_timer() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new("deadline"),
                    duration_secs: 60,
                },
            ],
        );

        let result = bounded(run_then_cancel(&ctx, async {
            let _handle = ctx.start_timer("deadline", 60);
            std::future::pending::<()>().await;
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let _handle = ctx.start_timer("deadline", 30);
        let commands = ctx.drain_commands();
        assert_eq!(
            arms(&commands),
            1,
            "the re-arm is not a no-op: {commands:?}"
        );
    }

    /// The body's timer fires. Other code then arms the same id. The scope
    /// cancel leaves that later arm armed.
    #[tokio::test]
    async fn scope_cancel_leaves_a_later_arm_of_the_same_id() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new("deadline"),
                    duration_secs: 60,
                },
                WorkflowEvent::TimerFired {
                    timer_id: TimerId::new("deadline"),
                },
            ],
        );
        let scope = ctx.cancellation_scope();
        let body = async {
            let fired = ctx.start_timer("deadline", 60).await_fire().await;
            assert!(fired.is_ok(), "{fired:?}");
            std::future::pending::<()>().await;
        };
        let sibling = async {
            tokio::task::yield_now().await;
            let _handle = ctx.start_timer("deadline", 30);
            scope.cancel();
        };

        let (result, ()) = bounded(async { tokio::join!(scope.run(body), sibling) }).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert_eq!(arms(&ctx.drain_commands()), 1, "the sibling arms once");
        let _handle = ctx.start_timer("deadline", 30);
        let commands = ctx.drain_commands();
        assert_eq!(
            arms(&commands),
            0,
            "the sibling arm stays armed: {commands:?}"
        );
    }

    /// Arms `deadline` outside a scope, then runs `mutate` in a scope that
    /// is cancelled in the same cycle. Returns the commands and the marker.
    async fn mutate_then_cancel(
        ctx: &WorkflowContext,
        mutate: impl FnOnce(&WorkflowContext),
    ) -> Vec<WorkflowCommand> {
        let _outer = ctx.start_timer("deadline", 60);
        let result = bounded(run_then_cancel(ctx, async {
            mutate(ctx);
            std::future::pending::<()>().await;
        }))
        .await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        ctx.drain_commands()
    }

    fn armed_deadline_history() -> Vec<WorkflowEvent> {
        vec![
            started(),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("deadline"),
                duration_secs: 60,
            },
        ]
    }

    /// The scope withdraws a `cancel_timer` of a timer armed outside it, so
    /// the timer stays armed, live and on replay.
    #[tokio::test]
    async fn a_withdrawn_cancel_timer_leaves_the_timer_armed() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), armed_deadline_history());

        let commands = mutate_then_cancel(&ctx, |ctx| {
            ctx.cancel_timer("deadline").expect("cancel_timer");
        })
        .await;

        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::CancelTimer { .. }
            )),
            0,
            "the cancel is withdrawn: {commands:?}"
        );
        let _again = ctx.start_timer("deadline", 60);
        let rearms = ctx.drain_commands();
        assert_eq!(arms(&rearms), 0, "the timer is still armed: {rearms:?}");

        let details = marker(&commands, "cancel_scope:1").expect("the cancel is recorded");
        let mut history = armed_deadline_history();
        history.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:1".into(),
            details,
        });
        let replay = WorkflowContext::for_replay(ExecutionId::new(), history);
        let replayed = mutate_then_cancel(&replay, |ctx| {
            ctx.cancel_timer("deadline").expect("cancel_timer");
        })
        .await;
        assert!(replayed.is_empty(), "{replayed:?}");
        let _again = replay.start_timer("deadline", 60);
        assert_eq!(arms(&replay.drain_commands()), 0);
        assert!(replay.take_nd_details().is_none());
        assert!(!replay.history_has_unconsumed_events());
    }

    /// The scope withdraws a `reset_timer` of a timer armed outside it, so
    /// the original arm stays armed.
    #[tokio::test]
    async fn a_withdrawn_reset_timer_keeps_the_original_arm() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), armed_deadline_history());

        let commands = mutate_then_cancel(&ctx, |ctx| {
            ctx.reset_timer("deadline", 30).expect("reset_timer");
        })
        .await;

        assert_eq!(arms(&commands), 0, "the re-arm is withdrawn: {commands:?}");
        let _again = ctx.start_timer("deadline", 60);
        let rearms = ctx.drain_commands();
        assert_eq!(arms(&rearms), 0, "the original arm stays: {rearms:?}");
    }

    #[tokio::test]
    async fn scope_cancel_cancels_an_in_flight_child_workflow() {
        let child = ExecutionId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::ChildWorkflowStarted {
                    child_id: child,
                    workflow_name: "child".into(),
                    input: Value::Null,
                },
            ],
        );

        let result = bounded(run_then_cancel(
            &ctx,
            ctx.spawn_child_workflow_raw("child", Value::Null),
        ))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = ctx.drain_commands();
        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.children, vec![child]);
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::StartChildWorkflow { .. }
            )),
            0,
            "a cancelled child must not be started again: {commands:?}"
        );
    }

    /// Reverse-brainstorm R1: an operation that starts and is cancelled in
    /// the same cycle never reaches the worker.
    #[tokio::test]
    async fn scope_cancel_withdraws_operations_started_in_the_same_cycle() {
        let ctx = WorkflowContext::new_test();

        let result = bounded(run_then_cancel(&ctx, async {
            tokio::join!(
                ctx.execute_activity_raw("charge", Value::Null, "default"),
                ctx.timer("deadline", 60),
                ctx.spawn_child_workflow_raw("child", Value::Null),
            )
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = ctx.drain_commands();
        assert!(
            marker(&commands, "cancel_scope:1").is_some(),
            "{commands:?}"
        );
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::ScheduleActivity { .. }
                    | WorkflowCommand::StartTimer { .. }
                    | WorkflowCommand::StartChildWorkflow { .. }
            )),
            0,
            "a withdrawn start must not reach the worker: {commands:?}"
        );
    }

    #[tokio::test]
    async fn scope_completes_normally_and_a_late_cancel_records_nothing() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let value = bounded(scope.run(async { 7 })).await;
        scope.cancel();

        assert!(matches!(value, Ok(7)), "{value:?}");
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn scope_cancelled_before_its_first_poll_returns_cancelled() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();
        scope.cancel();

        let result = bounded(scope.run(ctx.timer("deadline", 60))).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = ctx.drain_commands();
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::StartTimer { .. }
            )),
            0,
            "{commands:?}"
        );
    }

    /// A cancel before the first poll wins over a body that is ready at once.
    #[tokio::test]
    async fn scope_cancelled_before_its_first_poll_beats_a_ready_body() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();
        scope.cancel();

        let result = bounded(scope.run(async { 7 })).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert!(
            marker(&ctx.drain_commands(), "cancel_scope:1").is_some(),
            "the cancel is recorded, so replay takes the same path"
        );
    }

    /// Replay of the history above takes the same path.
    #[tokio::test]
    async fn replay_of_a_cancel_before_a_ready_body_is_deterministic() {
        let live = WorkflowContext::new_test();
        let scope = live.cancellation_scope();
        scope.cancel();
        let _ = bounded(scope.run(async { 7 })).await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details,
                },
            ],
        );
        let scope = ctx.cancellation_scope();
        scope.cancel();
        let result = bounded(scope.run(async { 7 })).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert!(ctx.take_nd_details().is_none());
        assert!(ctx.drain_commands().is_empty());
        assert!(!ctx.history_has_unconsumed_events());
    }

    /// A cancel by the body itself wins over the value it returns in that poll.
    #[tokio::test]
    async fn a_cancel_by_the_body_beats_its_ready_value() {
        let live = WorkflowContext::new_test();
        let scope = live.cancellation_scope();

        let result = bounded(scope.run(async {
            scope.cancel();
            7
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details,
                },
            ],
        );
        let scope = ctx.cancellation_scope();
        let replayed = bounded(scope.run(async {
            scope.cancel();
            7
        }))
        .await;

        assert!(
            matches!(replayed, Err(HarvestError::Cancelled(_))),
            "{replayed:?}"
        );
        assert!(ctx.take_nd_details().is_none());
        assert!(!ctx.history_has_unconsumed_events());
    }

    /// Records a value in a scope that is cancelled in the same cycle. The
    /// body leaks the value to `seen`, as a borrowed variable would.
    async fn record_then_cancel(
        ctx: &WorkflowContext,
        seen: &std::sync::Mutex<Option<Value>>,
        record: impl FnOnce(&WorkflowContext) -> Value + Send,
    ) {
        let result = bounded(run_then_cancel(ctx, async {
            *seen.lock().expect("seen lock") = Some(record(ctx));
            std::future::pending::<()>().await;
        }))
        .await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
    }

    fn replay_of(details: Value) -> WorkflowContext {
        WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details,
                },
            ],
        )
    }

    /// A cancel withdraws the body's `side_effect` record, so the marker
    /// keeps the value. Replay reads it and does not run the closure again.
    #[tokio::test]
    async fn replay_serves_a_side_effect_recorded_in_the_cancelling_cycle() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| {
            json!(ctx.side_effect("pick", || 41).expect("side effect"))
        })
        .await;
        let commands = live.drain_commands();
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::RecordSideEffect { .. }
            )),
            0,
            "the record is withdrawn: {commands:?}"
        );
        let details = marker(&commands, "cancel_scope:1").expect("recorded");
        assert_eq!(details["records"][0]["value"], json!(41), "{details}");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        let calls = std::sync::atomic::AtomicU32::new(0);
        record_then_cancel(&replay, &replayed, |ctx| {
            json!(
                ctx.side_effect("pick", || {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    99
                })
                .expect("side effect")
            )
        })
        .await;

        assert_eq!(
            *replayed.lock().expect("replayed lock"),
            *seen.lock().expect("seen lock")
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the closure must not run again"
        );
        assert!(replay.take_nd_details().is_none());
        assert!(!replay.history_has_unconsumed_events());
    }

    /// A built-in primitive gets the same treatment.
    #[tokio::test]
    async fn replay_serves_a_uuid_recorded_in_the_cancelling_cycle() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| json!(ctx.new_uuid().to_string())).await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| json!(ctx.new_uuid().to_string())).await;

        assert_eq!(
            *replayed.lock().expect("replayed lock"),
            *seen.lock().expect("seen lock")
        );
        assert!(replay.take_nd_details().is_none());
    }

    /// A version gate in the cancelling cycle keeps its live choice on
    /// replay, even after a deploy raises the maximum.
    #[tokio::test]
    async fn replay_keeps_a_version_chosen_in_the_cancelling_cycle() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| json!(ctx.version("gate", 1, 2))).await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| json!(ctx.version("gate", 1, 3))).await;

        assert_eq!(*seen.lock().expect("seen lock"), Some(json!(2)));
        assert_eq!(
            *replayed.lock().expect("replayed lock"),
            Some(json!(2)),
            "the live choice wins over the new maximum"
        );
        assert!(replay.take_nd_details().is_none());
    }

    /// An inner scope cancels after its body records a value, and the outer
    /// scope cancels in the same cycle. The outer marker keeps the value.
    #[tokio::test]
    async fn an_outer_cancel_keeps_the_records_of_a_nested_cancel() {
        async fn run(
            ctx: &WorkflowContext,
            seen: &std::sync::Mutex<Option<Value>>,
            pick: impl FnOnce() -> i32 + Send,
        ) {
            let outer = ctx.cancellation_scope();
            let inner = ctx.cancellation_scope();
            let body = async {
                let nested = inner
                    .run(async {
                        let value = ctx.side_effect("pick", pick).expect("side effect");
                        *seen.lock().expect("seen lock") = Some(json!(value));
                        inner.cancel();
                        std::future::pending::<()>().await;
                    })
                    .await;
                assert!(matches!(nested, Err(HarvestError::Cancelled(_))));
                std::future::pending::<()>().await;
            };
            let sibling = async {
                tokio::task::yield_now().await;
                outer.cancel();
            };
            let (result, ()) = bounded(async { tokio::join!(outer.run(body), sibling) }).await;
            assert!(
                matches!(result, Err(HarvestError::Cancelled(_))),
                "{result:?}"
            );
        }

        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        run(&live, &seen, || 41).await;
        let commands = live.drain_commands();
        assert!(
            marker(&commands, "cancel_scope:2").is_none(),
            "the outer cancel withdraws the inner marker: {commands:?}"
        );
        let details = marker(&commands, "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        let calls = std::sync::atomic::AtomicU32::new(0);
        run(&replay, &replayed, || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            99
        })
        .await;

        assert_eq!(
            *replayed.lock().expect("replayed lock"),
            *seen.lock().expect("seen lock")
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(replay.take_nd_details().is_none());
        assert!(!replay.history_has_unconsumed_events());
    }

    /// A scope cannot spawn a detached child. A cancel would withdraw the
    /// spawn, and replay would mint another child id.
    #[tokio::test]
    async fn a_scope_cannot_spawn_a_detached_child() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();
        let result = bounded(scope.run(async {
            ctx.spawn_child_workflow_detached_raw(
                "audit",
                Value::Null,
                crate::types::ParentClosePolicy::Abandon,
            )
        }))
        .await
        .expect("the scope completes");

        assert!(
            matches!(&result, Err(HarvestError::Config(m)) if m.contains("cancellation scope")),
            "{result:?}"
        );
        assert!(ctx.drain_commands().is_empty());
    }

    /// A replayed scope whose cancel the code no longer issues keeps its body
    /// parked on an in-flight activity. The cycle counts as parked, and the
    /// unconsumed cancel marker is the drift that the executor reports.
    #[tokio::test]
    async fn a_replay_that_drops_the_cancel_of_a_waiting_body_parks_on_drift() {
        let a = ActivityExecId::new();
        let live = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![started(), scheduled(a, "charge")],
        );
        let result = bounded(run_then_cancel(
            &live,
            live.execute_activity_raw("charge", Value::Null, "default"),
        ))
        .await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                scheduled(a, "charge"),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details,
                },
            ],
        );
        let scope = replay.cancellation_scope();
        let mut run =
            Box::pin(scope.run(replay.execute_activity_raw("charge", Value::Null, "default")));
        let polled = futures::poll!(run.as_mut());

        assert!(polled.is_pending());
        assert!(replay.parks_on_harvest_work(), "the held park counts");
        let skipped = replay.first_unconsumed_command_event();
        assert!(
            skipped
                .as_ref()
                .is_some_and(|(_, name)| name.contains("MarkerRecorded")),
            "{skipped:?}"
        );
    }

    /// A nested scope with an in-flight activity cancels in the same cycle as
    /// its outer scope. Replay re-derives the nested marker with the same
    /// members, so it matches the capture.
    #[tokio::test]
    async fn a_nested_cancel_with_a_member_replays_cleanly() {
        async fn run(ctx: &WorkflowContext) -> HarvestResult<()> {
            let outer = ctx.cancellation_scope();
            let inner = ctx.cancellation_scope();
            let body = async {
                let (_, ()) = tokio::join!(
                    inner.run(ctx.execute_activity_raw("charge", Value::Null, "default")),
                    async {
                        tokio::task::yield_now().await;
                        inner.cancel();
                    }
                );
                std::future::pending::<()>().await;
            };
            let sibling = async {
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;
                outer.cancel();
            };
            let (result, ()) = bounded(async { tokio::join!(outer.run(body), sibling) }).await;
            result
        }

        let a = ActivityExecId::new();
        let live = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![started(), scheduled(a, "charge")],
        );
        let result = run(&live).await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = live.drain_commands();
        assert!(
            marker(&commands, "cancel_scope:2").is_none(),
            "{commands:?}"
        );
        let details = marker(&commands, "cancel_scope:1").expect("recorded");

        let replay = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                scheduled(a, "charge"),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details,
                },
            ],
        );
        let result = run(&replay).await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let nd = replay.take_nd_details();
        assert!(nd.is_none(), "{nd:?}");
    }

    /// A replayed body that completes without the recorded cancel is drift.
    /// The scope does not wait forever.
    #[tokio::test]
    async fn a_replay_that_drops_the_cancel_call_is_drift() {
        let live = WorkflowContext::new_test();
        let result = bounded(run_then_cancel(&live, std::future::pending::<()>())).await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let scope = replay.cancellation_scope();
        let result = bounded(scope.run(async { 7 })).await;

        assert!(
            matches!(result, Err(HarvestError::NonDeterministic { .. })),
            "{result:?}"
        );
    }

    /// A nested cancel that the outer cancel withdrew stays in the capture.
    /// A replay that no longer cancels the nested scope is drift.
    #[tokio::test]
    async fn a_replay_that_drops_a_nested_cancel_is_drift() {
        async fn run(ctx: &WorkflowContext, cancel_inner: bool) -> HarvestResult<()> {
            let outer = ctx.cancellation_scope();
            let inner = ctx.cancellation_scope();
            let body = async {
                let _ = inner
                    .run(async {
                        if cancel_inner {
                            inner.cancel();
                        }
                        std::future::pending::<()>().await;
                    })
                    .await;
                std::future::pending::<()>().await;
            };
            let sibling = async {
                tokio::task::yield_now().await;
                outer.cancel();
            };
            let (result, ()) = bounded(async { tokio::join!(outer.run(body), sibling) }).await;
            result
        }

        let live = WorkflowContext::new_test();
        assert!(matches!(
            run(&live, true).await,
            Err(HarvestError::Cancelled(_))
        ));
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let same = replay_of(details.clone());
        let result = run(&same, true).await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert!(same.take_nd_details().is_none());

        let dropped = replay_of(details);
        let result = run(&dropped, false).await;
        assert!(
            matches!(result, Err(HarvestError::NonDeterministic { .. })),
            "{result:?}"
        );
    }

    /// A captured value that the deployed type cannot read fails as a
    /// recorded one does. The closure does not run again.
    #[tokio::test]
    async fn an_unreadable_captured_side_effect_fails_without_a_rerun() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| {
            json!(
                ctx.side_effect("pick", || "text".to_string())
                    .expect("side effect")
            )
        })
        .await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        let calls = std::sync::atomic::AtomicU32::new(0);
        record_then_cancel(&replay, &replayed, |ctx| {
            let result = ctx.side_effect("pick", || {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                7_i32
            });
            json!(matches!(result, Err(HarvestError::Serialization(_))))
        })
        .await;

        assert_eq!(
            *replayed.lock().expect("replayed lock"),
            Some(json!(true)),
            "the captured value fails to read"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A replayed marker that differs from the captured one is drift.
    #[tokio::test]
    async fn a_replayed_marker_that_differs_from_the_capture_is_drift() {
        fn push_winner(ctx: &WorkflowContext, winner: u64) -> Value {
            ctx.push_command(WorkflowCommand::RecordMarker {
                name: "race_winner:7".into(),
                details: json!(winner),
            });
            json!(winner)
        }
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| push_winner(ctx, 0)).await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let same = replay_of(details.clone());
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&same, &replayed, |ctx| push_winner(ctx, 0)).await;
        assert!(same.take_nd_details().is_none(), "the same decision passes");

        let changed = replay_of(details);
        record_then_cancel(&changed, &replayed, |ctx| push_winner(ctx, 1)).await;
        assert!(
            changed.take_nd_details().is_some(),
            "a changed decision is non-determinism"
        );
    }

    /// A replayed body that no longer makes a captured decision is drift.
    #[tokio::test]
    async fn a_replay_that_skips_a_captured_decision_is_drift() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| {
            json!(ctx.side_effect("pick", || 41).expect("side effect"))
        })
        .await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let result = bounded(run_then_cancel(&replay, async {
            std::future::pending::<()>().await;
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::NonDeterministic { .. })),
            "{result:?}"
        );
    }

    /// Records two named side effects, in the given order.
    fn pick_both(ctx: &WorkflowContext, first: &str, second: &str) -> Value {
        let picks = [first, second].map(|name| {
            ctx.side_effect(name, || name.len())
                .map_err(|e| matches!(e, HarvestError::NonDeterministic { .. }))
        });
        json!(picks.map(|pick| match pick {
            Ok(_) => "ok",
            Err(true) => "drift",
            Err(false) => "error",
        }))
    }

    /// Replay must ask for the captured decisions in the live order.
    #[tokio::test]
    async fn a_replay_that_reorders_captured_decisions_is_drift() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| pick_both(ctx, "a", "bb")).await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let same = replay_of(details.clone());
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&same, &replayed, |ctx| pick_both(ctx, "a", "bb")).await;
        assert_eq!(
            replayed.lock().expect("replayed lock").take(),
            Some(json!(["ok", "ok"]))
        );

        // The unused "bb" record also fails the cancel.
        let swapped = replay_of(details);
        let result = bounded(run_then_cancel(&swapped, async {
            *replayed.lock().expect("replayed lock") = Some(pick_both(&swapped, "bb", "a"));
            std::future::pending::<()>().await;
        }))
        .await;
        assert_eq!(
            replayed.lock().expect("replayed lock").take(),
            Some(json!(["drift", "ok"]))
        );
        assert!(
            matches!(result, Err(HarvestError::NonDeterministic { .. })),
            "{result:?}"
        );
    }

    /// A decision that the live cycle did not make is drift on replay.
    #[tokio::test]
    async fn a_replay_that_adds_a_decision_is_drift() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| {
            json!(ctx.side_effect("a", || 1).expect("side effect"))
        })
        .await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| pick_both(ctx, "a", "bb")).await;

        assert_eq!(
            replayed.lock().expect("replayed lock").take(),
            Some(json!(["ok", "drift"]))
        );
    }

    /// Counts the saga unwinds that a context reports.
    #[derive(Default)]
    struct SagaCount(std::sync::atomic::AtomicU32);

    impl crate::telemetry::MetricsRecorder for SagaCount {
        fn record_saga_compensated(&self, _workflow_name: &str, _queue: &str) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// The live cycle counts a saga unwind once. A replay of its cancel
    /// checks the withdrawn marker and does not count it again.
    #[tokio::test]
    async fn replay_of_a_cancel_does_not_count_a_saga_unwind_again() {
        let live_count = std::sync::Arc::new(SagaCount::default());
        let live = WorkflowContext::new_test().with_metrics(live_count.clone());
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| {
            json!(ctx.observe_saga_unwind_start(2).counted)
        })
        .await;
        assert_eq!(live_count.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay_count = std::sync::Arc::new(SagaCount::default());
        let replay = replay_of(details).with_metrics(replay_count.clone());
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| {
            json!(ctx.observe_saga_unwind_start(2).counted)
        })
        .await;

        assert_eq!(*replayed.lock().expect("replayed lock"), Some(json!(true)));
        assert_eq!(replay_count.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(replay.take_nd_details().is_none());
    }

    /// The body of a replayed cancel sees a replay, so logs and metrics
    /// do not fire again.
    #[tokio::test]
    async fn the_body_of_a_replayed_cancel_is_replaying() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| json!(ctx.is_replaying())).await;
        assert_eq!(*seen.lock().expect("seen lock"), Some(json!(false)));
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| json!(ctx.is_replaying())).await;

        assert_eq!(*replayed.lock().expect("replayed lock"), Some(json!(true)));
        assert!(!replay.is_replaying(), "the flag ends with the cancel");
    }

    /// A signal that lands between the horizon and the cancel marker reaches
    /// its handler after `run` returns. The cancelling cycle did not see it.
    #[tokio::test]
    async fn a_signal_after_the_horizon_waits_until_the_scope_returns() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "version:gate".into(),
                    details: json!(1),
                },
                WorkflowEvent::SignalReceived {
                    signal_name: "poke".into(),
                    payload: json!(1),
                },
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details: json!({
                        "reason": "cancellation scope cancelled",
                        "horizon": 2,
                    }),
                },
            ],
        );
        let poked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&poked);
        ctx.register_signal_handler_raw("poke", move |_| {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        assert!(!poked.load(std::sync::atomic::Ordering::SeqCst));

        let result = bounded(run_then_cancel(&ctx, async {
            assert_eq!(ctx.version("gate", 1, 1), 1);
            std::future::pending::<()>().await;
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert!(
            !poked.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run inside the replayed cancel"
        );
        let nd = ctx.take_nd_details();
        assert!(nd.is_none(), "{nd:?}");
        // The next history read delivers the signal.
        let _ = ctx.history_event_count();
        assert!(poked.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A timer armed before the scope, then reset and awaited in it. The
    /// cancel withdraws the reset, so the original arm and its durable row
    /// stay: the cancel does not tear the timer down.
    #[tokio::test]
    async fn a_withdrawn_reset_and_await_keeps_the_pre_scope_timer() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), armed_deadline_history());
        let _outer = ctx.start_timer("deadline", 60);
        let result = bounded(run_then_cancel(&ctx, async {
            ctx.reset_timer("deadline", 30).expect("reset_timer");
            let _ = ctx.start_timer("deadline", 30).await_fire().await;
        }))
        .await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );

        let commands = ctx.drain_commands();
        assert!(
            losers(&commands).is_none_or(|losers| losers.timers.is_empty()),
            "the pre-scope timer is not a member: {commands:?}"
        );
        let details = marker(&commands, "cancel_scope:1").expect("recorded");
        assert_eq!(details["timers"], json!([]), "{details}");
        let _again = ctx.start_timer("deadline", 60);
        assert_eq!(
            arms(&ctx.drain_commands()),
            0,
            "the original arm stays armed"
        );
    }

    /// A deploy moves a gate from `version` to `patched`, as documented. A
    /// captured `version:` marker still matches the replayed `patch:` gate.
    #[tokio::test]
    async fn a_patched_gate_matches_a_captured_version_marker() {
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| json!(ctx.version("rollout", 1, 2))).await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| json!(ctx.patched("rollout"))).await;

        assert_eq!(*replayed.lock().expect("replayed lock"), Some(json!(true)));
        let nd = replay.take_nd_details();
        assert!(nd.is_none(), "{nd:?}");
    }

    /// A pending cancel request (the SQLite cooperative cancel) is hidden
    /// inside a non-cancellable block, so its cleanup does not stop on it.
    #[tokio::test]
    async fn a_shield_hides_a_pending_cancel_request() {
        let ctx = WorkflowContext::new_test().with_pending_cancellation(Some("stop".into()));
        assert!(ctx.is_cancelled());

        let inside = bounded(ctx.non_cancellable(async {
            (
                ctx.is_cancelled(),
                ctx.check_cancellation().is_ok(),
                ctx.cancellation_reason().map(str::to_string),
            )
        }))
        .await
        .expect("the block completes");

        assert_eq!(inside, (false, true, None));
        assert!(
            ctx.is_cancelled(),
            "the request is visible again after the block"
        );
    }

    /// A withdrawn scope gives back the whole log budget, also for log calls
    /// over the line cap that queued nothing.
    #[tokio::test]
    async fn a_withdrawn_scope_gives_back_its_log_budget() {
        let ctx = WorkflowContext::new_test().with_log_policy(Some(
            crate::context::WorkflowLogPolicy::new().with_max_lines(1),
        ));
        let result = bounded(run_then_cancel(&ctx, async {
            for _ in 0..4 {
                ctx.log_info("in scope");
            }
            std::future::pending::<()>().await;
        }))
        .await;
        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );

        ctx.log_info("after");

        let commands = ctx.drain_commands();
        assert!(
            commands.iter().any(|c| matches!(
                c,
                WorkflowCommand::RecordLog { message, .. } if message == "after"
            )),
            "the log after the scope is kept: {commands:?}"
        );
    }

    /// A live scope cancel does not run the handler of a signal at the cursor.
    /// The replay of that cancel runs it only after the scope returns.
    #[tokio::test]
    async fn a_live_scope_cancel_does_not_run_a_signal_handler() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::SignalReceived {
                    signal_name: "poke".into(),
                    payload: json!(1),
                },
            ],
        );
        let poked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&poked);
        ctx.register_signal_handler_raw("poke", move |_| {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let result = bounded(run_then_cancel(&ctx, std::future::pending::<()>())).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert!(
            !poked.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run inside the scope cancel"
        );
    }

    /// An outer scope tracks a persisted arm. A nested scope resets the same
    /// timer and is cancelled. A later cancel of the outer scope still
    /// cancels the original arm, so a re-arm is not a no-op.
    #[tokio::test]
    async fn an_outer_cancel_still_cancels_an_arm_after_a_nested_reset_is_undone() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), armed_deadline_history());
        let outer = ctx.cancellation_scope();
        let inner = ctx.cancellation_scope();
        let body = async {
            let _arm = ctx.start_timer("deadline", 60);
            let nested = inner
                .run(async {
                    ctx.reset_timer("deadline", 30).expect("reset_timer");
                    inner.cancel();
                    std::future::pending::<()>().await;
                })
                .await;
            assert!(
                matches!(nested, Err(HarvestError::Cancelled(_))),
                "{nested:?}"
            );
            std::future::pending::<()>().await;
        };
        let sibling = async {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            outer.cancel();
        };

        let (result, ()) = bounded(async { tokio::join!(outer.run(body), sibling) }).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let _ = ctx.drain_commands();
        let _again = ctx.start_timer("deadline", 60);
        let commands = ctx.drain_commands();
        assert_eq!(
            arms(&commands),
            1,
            "the outer cancel cancelled the original arm: {commands:?}"
        );
    }

    /// A captured side-effect value replays as recorded. A cap lowered after
    /// the cancel does not fail it, as it does not fail a recorded value.
    #[tokio::test]
    async fn a_lowered_cap_does_not_fail_a_captured_side_effect() {
        let long = "x".repeat(200);
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        let value = long.clone();
        record_then_cancel(&live, &seen, move |ctx| {
            json!(
                ctx.side_effect("pick", move || value)
                    .expect("live side effect")
            )
        })
        .await;
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details).with_payload_caps(u64::MAX, u64::MAX, u64::MAX, 16);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| {
            json!(
                ctx.side_effect("pick", String::new)
                    .map_err(|e| e.to_string())
            )
        })
        .await;

        assert_eq!(
            *replayed.lock().expect("replayed lock"),
            Some(json!({ "Ok": long }))
        );
        let nd = replay.take_nd_details();
        assert!(nd.is_none(), "{nd:?}");
    }

    /// A signal right at the cursor when the scope starts lies past the
    /// horizon. The scope's peek at its marker does not run its handler.
    #[tokio::test]
    async fn the_marker_peek_does_not_run_a_late_signal_handler() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::SignalReceived {
                    signal_name: "poke".into(),
                    payload: json!(1),
                },
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details: json!({
                        "reason": "cancellation scope cancelled",
                        "horizon": 1,
                    }),
                },
            ],
        );
        let poked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&poked);
        ctx.register_signal_handler_raw("poke", move |_| {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let result = bounded(run_then_cancel(&ctx, std::future::pending::<()>())).await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        assert!(
            !poked.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run before the scope returns"
        );
        let nd = ctx.take_nd_details();
        assert!(nd.is_none(), "{nd:?}");
    }

    /// A version gate that matches its marker before the horizon takes no
    /// captured record, so a replayed cancel reports no drift.
    #[tokio::test]
    async fn a_version_gate_in_history_takes_no_captured_record() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "version:gate".into(),
                    details: json!(1),
                },
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details: json!({
                        "reason": "cancellation scope cancelled",
                        "horizon": 2,
                    }),
                },
            ],
        );

        let result = bounded(run_then_cancel(&ctx, async {
            assert_eq!(ctx.version("gate", 1, 2), 1);
            std::future::pending::<()>().await;
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let nd = ctx.take_nd_details();
        assert!(nd.is_none(), "{nd:?}");
    }

    /// An update inside a scope fails at once. Its terminal result stays
    /// queued when the scope is cancelled, so the caller does not wait forever.
    #[tokio::test]
    async fn an_update_inside_a_scope_fails_and_keeps_its_result() {
        let ctx = WorkflowContext::new_test();
        let update_id = crate::types::UpdateId::new();
        let seen = std::sync::Mutex::new(None);
        let result = bounded(run_then_cancel(&ctx, async {
            *seen.lock().expect("seen lock") = Some(
                ctx.execute_admitted_update(update_id, "rename", json!(1))
                    .await,
            );
            std::future::pending::<()>().await;
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let answer = seen.lock().expect("seen lock").take().expect("ran");
        assert!(
            answer
                .as_ref()
                .is_err_and(|e| e.contains("cancellation scope")),
            "{answer:?}"
        );
        let commands = ctx.drain_commands();
        assert!(
            commands.iter().any(|c| matches!(
                c,
                WorkflowCommand::RecordUpdateResult { update_id: id, result: Err(_) }
                    if *id == update_id
            )),
            "{commands:?}"
        );
    }

    /// Counts each serialization of a durable chunk.
    struct CountedChunk<'a>(&'a std::sync::atomic::AtomicU32);

    impl serde::Serialize for CountedChunk<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            serializer.serialize_u32(1)
        }
    }

    /// A withdrawn durable chunk leaves the queue count. A replay of the
    /// cancel does not serialize the chunk again.
    #[tokio::test]
    async fn a_replayed_cancel_does_not_serialize_a_durable_chunk() {
        let serialized = std::sync::atomic::AtomicU32::new(0);
        let live = WorkflowContext::new_test();
        let seen = std::sync::Mutex::new(None);
        record_then_cancel(&live, &seen, |ctx| {
            json!(
                ctx.publish_durable_progress(CountedChunk(&serialized))
                    .is_ok()
            )
        })
        .await;
        assert_eq!(serialized.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(live.durable_chunks_queued(), 0, "the chunk is withdrawn");
        let details = marker(&live.drain_commands(), "cancel_scope:1").expect("recorded");

        let replay = replay_of(details);
        let replayed = std::sync::Mutex::new(None);
        record_then_cancel(&replay, &replayed, |ctx| {
            json!(
                ctx.publish_durable_progress(CountedChunk(&serialized))
                    .is_ok()
            )
        })
        .await;

        assert_eq!(*replayed.lock().expect("replayed lock"), Some(json!(true)));
        assert_eq!(serialized.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// A member start that the replayed body no longer issues stops the
    /// scan for the cancel marker, so the removed command is drift.
    #[tokio::test]
    async fn a_removed_member_start_is_drift_at_the_cancel() {
        let a = ActivityExecId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                scheduled(a, "charge"),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details: json!({
                        "reason": "cancellation scope cancelled",
                        "horizon": 2,
                        "activities": [a],
                    }),
                },
            ],
        );

        let result = bounded(run_then_cancel(&ctx, async {
            std::future::pending::<()>().await;
        }))
        .await;

        assert!(
            matches!(result, Err(HarvestError::NonDeterministic { .. })),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_scope_runs_only_once() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let first = bounded(scope.run(async { 1 })).await;
        let second = bounded(scope.run(async { 2 })).await;

        assert!(matches!(first, Ok(1)), "{first:?}");
        assert!(matches!(second, Err(HarvestError::Config(_))), "{second:?}");
    }

    /// The workflow under the replay tests. Cycle 1 starts `charge` and
    /// waits for `abort`. The signal cancels the scope. The workflow then
    /// runs `cleanup`. The body runs `ship` after `charge`, so a replay that
    /// sees a completion the live cycle did not see would diverge.
    async fn charge_or_abort(ctx: &WorkflowContext) -> HarvestResult<Value> {
        let scope = ctx.cancellation_scope();
        let (charged, ()) = tokio::join!(
            scope.run(async {
                ctx.execute_activity_raw("charge", Value::Null, "default")
                    .await?;
                ctx.execute_activity_raw("ship", Value::Null, "default")
                    .await
            }),
            async {
                let _ = ctx.wait_for_signal("abort").await;
                scope.cancel();
            }
        );
        match charged {
            Err(HarvestError::Cancelled(_)) => {
                ctx.execute_activity_raw("cleanup", Value::Null, "default")
                    .await
            }
            other => other?,
        }
    }

    fn scheduled_id(commands: &[WorkflowCommand], wanted: &str) -> ActivityExecId {
        commands
            .iter()
            .find_map(|c| match c {
                WorkflowCommand::ScheduleActivity {
                    activity_id, name, ..
                } if name == wanted => Some(*activity_id),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no ScheduleActivity({wanted}) in {commands:?}"))
    }

    /// Drives `charge_or_abort` through its three cycles. `concurrent`
    /// inserts a completion of `charge` that the cancel cycle did not see.
    async fn replay_after_scope_cancel(concurrent: bool) {
        let exec_id = ExecutionId::new();
        let abort = WorkflowEvent::SignalReceived {
            signal_name: "abort".into(),
            payload: Value::Null,
        };

        // Cycle 1: start `charge`, park on the signal.
        let ctx = WorkflowContext::for_replay(exec_id, vec![started()]);
        let first = tokio::time::timeout(Duration::from_millis(100), charge_or_abort(&ctx)).await;
        assert!(first.is_err(), "cycle 1 must park");
        let charge = scheduled_id(&ctx.drain_commands(), "charge");

        // Cycle 2: the signal cancels the scope live.
        let mut history = vec![started(), scheduled(charge, "charge"), abort];
        let ctx = WorkflowContext::for_replay(exec_id, history.clone());
        let second = tokio::time::timeout(Duration::from_millis(100), charge_or_abort(&ctx)).await;
        assert!(second.is_err(), "cycle 2 must park on cleanup");
        let commands = ctx.drain_commands();
        let details = marker(&commands, "cancel_scope:1").expect("cycle 2 records the cancel");
        let cleanup = scheduled_id(&commands, "cleanup");

        // Persist cycle 2 the way the worker does.
        if concurrent {
            history.push(WorkflowEvent::ActivityCompleted {
                activity_id: charge,
                output: json!("charged"),
            });
        }
        history.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:1".into(),
            details,
        });
        history.push(scheduled(cleanup, "cleanup"));
        if !concurrent {
            history.push(WorkflowEvent::ActivityFailed {
                activity_id: charge,
                error: "cancelled by its cancellation scope".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: true,
                details: None,
            });
        }
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: cleanup,
            output: json!("cleaned"),
        });

        // Cycle 3: a full replay gives the same result and no new command.
        let ctx = WorkflowContext::for_replay(exec_id, history);
        let third = bounded(charge_or_abort(&ctx)).await;
        assert_eq!(third.ok(), Some(json!("cleaned")));
        assert!(ctx.take_nd_details().is_none());
        let commands = ctx.drain_commands();
        assert!(commands.is_empty(), "replay must add nothing: {commands:?}");
        assert!(
            !ctx.history_has_unconsumed_events(),
            "replay must consume the whole history"
        );
    }

    #[tokio::test]
    async fn replay_of_a_scope_cancel_is_deterministic() {
        replay_after_scope_cancel(false).await;
    }

    /// Reverse-brainstorm R2: a member completes during the cancel cycle.
    #[tokio::test]
    async fn replay_ignores_a_member_completion_the_cancel_cycle_did_not_see() {
        replay_after_scope_cancel(true).await;
    }

    /// A deferred cancel lands between a schedule and its completion.
    /// Replay skips it, and the next command lands at the frontier.
    #[tokio::test]
    async fn replay_skips_a_deferred_cancel_request() {
        let a = ActivityExecId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                scheduled(a, "cleanup"),
                WorkflowEvent::WorkflowCancelRequested {
                    reason: "operator abort".into(),
                },
                WorkflowEvent::ActivityCompleted {
                    activity_id: a,
                    output: json!("cleaned"),
                },
            ],
        );

        let cleaned = bounded(ctx.execute_activity_raw("cleanup", Value::Null, "default")).await;
        let next = tokio::time::timeout(
            Duration::from_millis(50),
            ctx.execute_activity_raw("next", Value::Null, "default"),
        )
        .await;

        assert_eq!(cleaned.ok(), Some(json!("cleaned")));
        assert!(next.is_err(), "the next activity parks at the frontier");
        assert!(ctx.take_nd_details().is_none());
        assert!(
            !ctx.is_cancelled(),
            "workflow code does not see the request"
        );
        let commands = ctx.drain_commands();
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::ScheduleActivity { .. }
            )),
            1,
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn non_cancellable_records_open_and_close_markers() {
        let ctx = WorkflowContext::new_test();

        let value = bounded(ctx.non_cancellable(async { 3 })).await;

        assert!(matches!(value, Ok(3)), "{value:?}");
        let commands = ctx.drain_commands();
        assert!(
            marker(&commands, "non_cancellable_open:1").is_some(),
            "{commands:?}"
        );
        assert!(
            marker(&commands, "non_cancellable_close:1").is_some(),
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn non_cancellable_replays_its_markers_without_new_commands() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "non_cancellable_open:1".into(),
                    details: Value::Null,
                },
                WorkflowEvent::MarkerRecorded {
                    name: "non_cancellable_close:1".into(),
                    details: Value::Null,
                },
            ],
        );

        let value = bounded(ctx.non_cancellable(async { 3 })).await;

        assert!(matches!(value, Ok(3)), "{value:?}");
        assert!(ctx.drain_commands().is_empty());
        assert!(!ctx.history_has_unconsumed_events());
    }

    /// Reverse-brainstorm R6.
    #[tokio::test]
    async fn non_cancellable_inside_a_cancellable_scope_is_rejected() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let result = bounded(scope.run(ctx.non_cancellable(async { 3 }))).await;

        assert!(
            matches!(result, Ok(Err(HarvestError::Config(_)))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_scope_inside_non_cancellable_cancels_normally() {
        let a = ActivityExecId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "non_cancellable_open:1".into(),
                    details: Value::Null,
                },
                scheduled(a, "charge"),
            ],
        );

        let result = bounded(ctx.non_cancellable(run_then_cancel(
            &ctx,
            ctx.execute_activity_raw("charge", Value::Null, "default"),
        )))
        .await;

        assert!(
            matches!(result, Ok(Err(HarvestError::Cancelled(_)))),
            "{result:?}"
        );
        let losers = losers(&ctx.drain_commands()).expect("the inner scope cancels");
        assert_eq!(losers.activities, vec![a]);
    }

    /// A member kind for the per-kind replay test.
    enum Kind {
        Activity,
        Timer,
        Child,
    }

    /// Cancels a scope over one in-flight member when `abort` arrives.
    async fn abort_member(ctx: &WorkflowContext, kind: &Kind) -> HarvestResult<()> {
        let scope = ctx.cancellation_scope();
        let body = async {
            match kind {
                Kind::Activity => ctx
                    .execute_activity_raw("charge", Value::Null, "default")
                    .await
                    .map(drop),
                Kind::Timer => ctx.timer("deadline", 60).await,
                Kind::Child => ctx
                    .spawn_child_workflow_raw("child", Value::Null)
                    .await
                    .map(drop),
            }
        };
        let (result, ()) = tokio::join!(scope.run(body), async {
            let _ = ctx.wait_for_signal("abort").await;
            scope.cancel();
        });
        result?
    }

    /// AC3 per kind: the cancel cycle records the marker. A replay of the
    /// persisted history returns the same result and adds nothing.
    async fn replay_member_kind(kind: Kind) {
        let exec_id = ExecutionId::new();
        let a = ActivityExecId::new();
        let child = ExecutionId::new();
        let start = match kind {
            Kind::Activity => scheduled(a, "charge"),
            Kind::Timer => WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("deadline"),
                duration_secs: 60,
            },
            Kind::Child => WorkflowEvent::ChildWorkflowStarted {
                child_id: child,
                workflow_name: "child".into(),
                input: Value::Null,
            },
        };
        let mut history = vec![
            started(),
            start,
            WorkflowEvent::SignalReceived {
                signal_name: "abort".into(),
                payload: Value::Null,
            },
        ];

        let ctx = WorkflowContext::for_replay(exec_id, history.clone());
        let live = bounded(abort_member(&ctx, &kind)).await;
        assert!(matches!(live, Err(HarvestError::Cancelled(_))), "{live:?}");
        let commands = ctx.drain_commands();
        let details = marker(&commands, "cancel_scope:1").expect("the cancel is recorded");
        assert!(losers(&commands).is_some(), "{commands:?}");

        history.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:1".into(),
            details,
        });
        match kind {
            Kind::Activity => history.push(WorkflowEvent::ActivityFailed {
                activity_id: a,
                error: "cancelled by its cancellation scope".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: true,
                details: None,
            }),
            Kind::Child => history.push(WorkflowEvent::child_workflow_failed(
                child,
                "cancelled by its cancellation scope",
            )),
            Kind::Timer => {}
        }

        let ctx = WorkflowContext::for_replay(exec_id, history);
        let replayed = bounded(abort_member(&ctx, &kind)).await;
        assert!(
            matches!(replayed, Err(HarvestError::Cancelled(_))),
            "{replayed:?}"
        );
        assert!(ctx.take_nd_details().is_none());
        let commands = ctx.drain_commands();
        assert!(commands.is_empty(), "replay must add nothing: {commands:?}");
        assert!(!ctx.history_has_unconsumed_events());
    }

    #[tokio::test]
    async fn replay_of_a_cancelled_activity_member_is_deterministic() {
        replay_member_kind(Kind::Activity).await;
    }

    #[tokio::test]
    async fn replay_of_a_cancelled_timer_member_is_deterministic() {
        replay_member_kind(Kind::Timer).await;
    }

    #[tokio::test]
    async fn replay_of_a_cancelled_child_member_is_deterministic() {
        replay_member_kind(Kind::Child).await;
    }

    #[tokio::test]
    async fn an_unreadable_cancel_marker_is_non_deterministic() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "cancel_scope:1".into(),
                    details: json!("not a cancel record"),
                },
            ],
        );

        let result = bounded(run_then_cancel(&ctx, async { 1 })).await;

        assert!(
            matches!(result, Err(HarvestError::NonDeterministic { .. })),
            "{result:?}"
        );
    }

    /// An outer cancel also cancels an operation of a nested scope.
    #[tokio::test]
    async fn an_outer_cancel_reaches_a_nested_scope_member() {
        let a = ActivityExecId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![started(), scheduled(a, "charge")],
        );
        let inner = ctx.cancellation_scope();

        let result = bounded(run_then_cancel(
            &ctx,
            inner.run(ctx.execute_activity_raw("charge", Value::Null, "default")),
        ))
        .await;

        assert!(
            matches!(result, Err(HarvestError::Cancelled(_))),
            "{result:?}"
        );
        let commands = ctx.drain_commands();
        assert!(
            marker(&commands, "cancel_scope:1").is_none(),
            "{commands:?}"
        );
        assert!(
            marker(&commands, "cancel_scope:2").is_some(),
            "{commands:?}"
        );
        let losers = losers(&commands).expect("the outer scope cancels");
        assert_eq!(losers.activities, vec![a]);
    }

    /// Reverse-brainstorm R5: a block dropped before it completes closes.
    #[tokio::test]
    async fn a_dropped_non_cancellable_block_still_closes() {
        let ctx = WorkflowContext::new_test();

        let parked = tokio::time::timeout(
            Duration::from_millis(50),
            ctx.non_cancellable(ctx.timer("cleanup", 60)),
        )
        .await;

        assert!(parked.is_err(), "the block parks on its timer");
        let commands = ctx.drain_commands();
        assert!(
            marker(&commands, "non_cancellable_open:1").is_some(),
            "{commands:?}"
        );
        assert!(
            marker(&commands, "non_cancellable_close:1").is_some(),
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn a_mutex_acquire_inside_a_scope_is_rejected() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let result = bounded(scope.run(async { ctx.mutex("key").acquire().await.map(drop) })).await;

        assert!(
            matches!(result, Ok(Err(HarvestError::Config(_)))),
            "{result:?}"
        );
    }

    /// An external activity in a cancelled scope delivers its result later.
    /// Replay consumes it, so the next command still matches.
    #[tokio::test]
    async fn a_late_external_result_of_a_cancelled_member_is_consumed() {
        async fn wf(ctx: &WorkflowContext) -> HarvestResult<Value> {
            let scope = ctx.cancellation_scope();
            let (approved, ()) = tokio::join!(
                scope.run(ctx.execute_activity_external("approve", Value::Null, "default", 3600)),
                async {
                    let _ = ctx.wait_for_signal("abort").await;
                    scope.cancel();
                }
            );
            assert!(matches!(approved, Err(HarvestError::Cancelled(_))));
            ctx.execute_activity_raw("after", Value::Null, "default")
                .await
        }
        let exec_id = ExecutionId::new();
        let a = ActivityExecId::new();
        let token = crate::types::ExternalActivityToken::new();
        let mut history = vec![
            started(),
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id: a,
                token,
                name: "approve".into(),
                input: Value::Null,
                queue: "default".into(),
                schedule_to_close_secs: 3600,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "abort".into(),
                payload: Value::Null,
            },
        ];
        let ctx = WorkflowContext::for_replay(exec_id, history.clone());
        let live = tokio::time::timeout(Duration::from_millis(50), wf(&ctx)).await;
        assert!(live.is_err(), "the workflow parks on `after`");
        let commands = ctx.drain_commands();
        let details = marker(&commands, "cancel_scope:1").expect("the cancel is recorded");
        let after = scheduled_id(&commands, "after");

        history.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:1".into(),
            details,
        });
        history.push(scheduled(after, "after"));
        history.push(WorkflowEvent::ActivityCompletedExternally {
            activity_id: a,
            token,
            output: json!("approved late"),
        });
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: after,
            output: json!("done"),
        });

        let ctx = WorkflowContext::for_replay(exec_id, history);
        let replayed = bounded(wf(&ctx)).await;
        assert_eq!(replayed.ok(), Some(json!("done")));
        assert!(ctx.take_nd_details().is_none());
        assert!(ctx.drain_commands().is_empty());
        assert!(!ctx.history_has_unconsumed_events());
    }

    /// The external workflow of the late-result tests.
    async fn approve_then_after(ctx: &WorkflowContext) -> HarvestResult<Value> {
        let scope = ctx.cancellation_scope();
        let (approved, ()) = tokio::join!(
            scope.run(ctx.execute_activity_external("approve", Value::Null, "default", 3600)),
            async {
                let _ = ctx.wait_for_signal("abort").await;
                scope.cancel();
            }
        );
        assert!(matches!(approved, Err(HarvestError::Cancelled(_))));
        ctx.execute_activity_raw("after", Value::Null, "default")
            .await
    }

    /// Other writers append between the horizon and the cancel marker: an
    /// external result and a signal. Replay still finds the marker.
    #[tokio::test]
    async fn replay_steps_over_events_between_the_horizon_and_the_marker() {
        let exec_id = ExecutionId::new();
        let a = ActivityExecId::new();
        let token = crate::types::ExternalActivityToken::new();
        let mut history = vec![
            started(),
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id: a,
                token,
                name: "approve".into(),
                input: Value::Null,
                queue: "default".into(),
                schedule_to_close_secs: 3600,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "abort".into(),
                payload: Value::Null,
            },
        ];
        let ctx = WorkflowContext::for_replay(exec_id, history.clone());
        let live = tokio::time::timeout(Duration::from_millis(50), approve_then_after(&ctx)).await;
        assert!(live.is_err(), "the workflow parks on `after`");
        let commands = ctx.drain_commands();
        let details = marker(&commands, "cancel_scope:1").expect("the cancel is recorded");
        let after = scheduled_id(&commands, "after");

        history.push(WorkflowEvent::ActivityCompletedExternally {
            activity_id: a,
            token,
            output: json!("approved late"),
        });
        history.push(WorkflowEvent::SignalReceived {
            signal_name: "unrelated".into(),
            payload: Value::Null,
        });
        history.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:1".into(),
            details,
        });
        history.push(scheduled(after, "after"));
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: after,
            output: json!("done"),
        });

        let ctx = WorkflowContext::for_replay(exec_id, history);
        let replayed = bounded(approve_then_after(&ctx)).await;
        assert_eq!(replayed.ok(), Some(json!("done")));
        assert!(ctx.take_nd_details().is_none());
        assert!(ctx.drain_commands().is_empty());
    }

    /// A resident cycle drops a member that resolved, so a later timer that
    /// reuses its id is not torn down by a cancel.
    #[tokio::test]
    async fn a_resident_cycle_drops_a_resolved_member() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();
        let parked =
            tokio::time::timeout(Duration::from_millis(50), scope.run(ctx.timer("t", 60))).await;
        assert!(parked.is_err(), "the body parks on its timer");
        assert_eq!(scope.shared.lock().members.timers, vec![TimerId::new("t")]);

        ctx.begin_resident_cycle(
            &[WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t"),
            }],
            Vec::new(),
        );

        assert!(scope.shared.lock().members.is_empty());
    }

    /// External workflow operations have a durable result that arrives
    /// later. A scope rejects them, so a cancel cannot strand that result.
    #[tokio::test]
    async fn external_workflow_operations_inside_a_scope_are_rejected() {
        let ctx = WorkflowContext::new_test();
        let target = ExecutionId::new();

        let signal = bounded(ctx.cancellation_scope().run(ctx.signal_external_workflow(
            target,
            "go",
            Value::Null,
        )))
        .await;
        let cancel = bounded(
            ctx.cancellation_scope()
                .run(ctx.request_cancel_external_workflow(target)),
        )
        .await;
        let awaited = bounded(
            ctx.cancellation_scope()
                .run(ctx.await_external_workflow_value(target)),
        )
        .await;

        assert!(
            matches!(signal, Ok(Err(HarvestError::Config(_)))),
            "{signal:?}"
        );
        assert!(
            matches!(cancel, Ok(Err(HarvestError::Config(_)))),
            "{cancel:?}"
        );
        assert!(
            matches!(awaited, Ok(Err(HarvestError::Config(_)))),
            "{awaited:?}"
        );
        assert!(
            ctx.drain_commands().is_empty(),
            "nothing reaches the worker"
        );
    }

    /// A dropped session is never released, so a scope rejects it.
    #[tokio::test]
    async fn a_session_inside_a_scope_is_rejected() {
        let ctx = WorkflowContext::new_test();

        let result = bounded(
            ctx.cancellation_scope()
                .run(ctx.create_session(crate::context::SessionOptions::new("gpu"))),
        )
        .await;

        assert!(
            matches!(result, Ok(Err(HarvestError::Config(_)))),
            "{:?}",
            result.as_ref().map(Result::is_ok)
        );
        assert!(
            ctx.drain_commands().is_empty(),
            "nothing reaches the worker"
        );
    }
}
