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
use crate::event::WorkflowEvent;
use crate::types::{ActivityExecId, ExecutionId, TimerId};

/// The reason that a cancelled scope reports.
const SCOPE_CANCEL_REASON: &str = "cancellation scope cancelled";

/// The marker that records the cancel decision of scope `seq`.
fn cancel_marker_name(seq: u32) -> String {
    format!("cancel_scope:{seq}")
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
    if let Some(holder) = scopes.iter().rev().find(|shared| shared.lock().holding) {
        holder.lock().held.push(cmd);
        return None;
    }
    let mut tags = Vec::with_capacity(scopes.len());
    for shared in scopes {
        shared.lock().members.add_from(&cmd);
        tags.push(shared.seq);
    }
    Some((cmd, tags))
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
            self.shared.lock().holding = true;
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
            parse_cancel_marker(ctx, &name, details).map(|marker| {
                ctx.consume_scope_members(&marker.members, marker.horizon..index);
                ctx.mark_scope_timers_cancelled(&marker.members.timers);
                // The held commands are the commands that the live cycle withdrew.
                ctx.mark_scope_timers_cancelled(&armed_timer_ids(&self.shared.lock().held));
                ctx.mark_scope_arms_cancelled(&self.armed_timers());
                marker.reason
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
            let members = ctx.open_scope_members(&self.shared.lock().members);
            ctx.consume_cancelled_member_frontier(&members);
            let marker = CancelMarker {
                reason: SCOPE_CANCEL_REASON.to_string(),
                horizon: ctx.history_len(),
                members,
            };
            ctx.push_command(WorkflowCommand::RecordMarker {
                name,
                details: serde_json::to_value(&marker).unwrap_or(Value::Null),
            });
            ctx.mark_scope_timers_cancelled(&marker.members.timers);
            ctx.mark_scope_timers_cancelled(&armed_timer_ids(&withdrawn));
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

/// The timer ids that `cmds` arm. A withdrawn arm never reaches the worker.
fn armed_timer_ids(cmds: &[WorkflowCommand]) -> Vec<TimerId> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::ArmTimer { timer_id, .. } => Some(timer_id.clone()),
            _ => None,
        })
        .collect()
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
                if horizon.is_none() && !this.shared.lock().cancel_requested {
                    this.finish();
                    return Poll::Ready(Ok(value));
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

        ctx.begin_resident_cycle(&[WorkflowEvent::TimerFired {
            timer_id: TimerId::new("t"),
        }]);

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
