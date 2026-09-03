//! Open-awaitables diagnostic projection (issue #615).
//!
//! Answers "what is this execution parked on right now?" by projecting the
//! executor's replay-derived pending [`WorkflowCommand`] set — plus recorded
//! history metadata — into a small, serializable list of *awaitables*: the
//! things the workflow is waiting for that have not happened yet.
//!
//! The six issue-#615 categories are covered:
//!
//! 1. **Pending activities** (regular, local, and external-handoff) —
//!    [`AwaitableKind::Activity`]
//! 2. **Unfired durable timers** — [`AwaitableKind::Timer`]
//! 3. **Awaited-but-unfulfilled signals (by name)** — [`AwaitableKind::Signal`].
//!    This is the category only a replayed view can see: an awaited-unsent
//!    signal exists solely as a parked `WaitForSignal` command inside the
//!    coroutine, never as a row in any side table.
//! 4. **Pending child workflows** — [`AwaitableKind::ChildWorkflow`]
//! 5. **Unresolved `await_condition` parks** — [`AwaitableKind::Condition`].
//!    `ctx.await_condition` parks command-less (and takes no site label), so a
//!    condition park is inferred: the replay suspended with zero open-awaitable
//!    commands and no terminal-transition command in the drained buffer.
//! 6. **Pending updates** (`UpdateAdmitted` with no recorded terminal) —
//!    [`AwaitableKind::Update`]
//!
//! Two bonus categories beyond the issue's six are reported when present:
//! a parked durable-mutex acquire ([`AwaitableKind::Mutex`], issue #691) and a
//! parked external-workflow operation ([`AwaitableKind::ExternalWorkflow`] —
//! `ctx.await_external_workflow` / in-flight external signal or cancel
//! delivery, issues #757/#244/#492).
//!
//! The projection is **pure and read-only**: it consumes an already-loaded
//! timestamped history slice plus (optionally) a drained command buffer, and
//! never touches the database, appends events, or mutates the execution. It
//! introduces **no new `WorkflowEvent` variant and no migration**.
//!
//! Degradation contract: when the replay drive is unavailable (unregistered
//! handler, classic DAG, replay divergence, drive timeout/panic), callers pass
//! [`WaitSetInput::HistoryOnly`] and the projection falls back to a best-effort
//! event-log scan. That mode covers activities, timers, children, updates, and
//! external awaits — it cannot see awaited-unsent signals (except a
//! signal-or-deadline race, whose reserved timer id encodes the signal name)
//! or `await_condition` parks, which is exactly the blindness the replayed
//! mode exists to remove. A history-only scan reports every unclosed
//! `TimerStarted`, so a caller with database access passes the live unfired
//! timer ids via [`WaitSetInput::HistoryOnly`]'s `fire_eligible_timers` field:
//! the projection then drops dormant cancellable `ctx.start_timer` arms (issue
//! #768) that recorded a `TimerStarted` but inserted no `harvest_timers` row,
//! filtered before the per-category cap so a fire-eligible timer is never
//! crowded out of the report by dormant arms ahead of it.
//!
//! Known approximations of the replayed mode (replay review, documented rather
//! than fixed — each is a best-effort triage bound, never a correctness
//! surface):
//!
//! - The #612 drive stops at the FIRST poll that emits a command, so a
//!   `tokio::join!` sibling whose wait would only be pushed on a later poll
//!   (after an intervening `yield_now`-style self-wake) can be absent from the
//!   drained buffer. In practice every ctx wait primitive pushes its command on
//!   its first poll, so joined siblings land in one batch; the gap needs a
//!   hand-rolled future that yields before awaiting.
//! - A [`AwaitableKind::Condition`] awaitable is inferred from a command-less
//!   suspension. A workflow that parks command-lessly for a *wrong* reason — an
//!   author determinism violation absorbed by an infallible primitive — could
//!   look identical; callers are expected to check the context's deferred
//!   non-determinism slot after the drive and degrade instead (the management
//!   endpoint does).
//! - A cancellable timer's (`ArmTimer { for_await: true }`) reported deadline
//!   is anchored at its recorded ARM time; the true deadline is anchored at
//!   `await_fire` time (issue #768), which history does not record, so the
//!   reported deadline can be conservatively early.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;

use crate::context::WorkflowCommand;
use crate::event::WorkflowEvent;
use crate::types::{ActivityExecId, ExecutionId};

/// Default per-category bound applied by the management endpoint.
///
/// At most this many awaitables of each [`AwaitableKind`] are reported (first
/// N in recorded order), with the overflow surfaced via
/// [`AwaitablesProjection::truncated_kinds`]. Keeps a pathological run (e.g. a
/// 10k-wide fan-out) from blowing up the response.
pub const AWAITABLE_CATEGORY_CAP: usize = 50;

/// Reserved timer-id prefix of the signal-or-deadline race (issue #476):
/// `__signal_timeout:{seq}:{signal_name}`.
const SIGNAL_TIMEOUT_TIMER_PREFIX: &str = "__signal_timeout:";

/// The category of a single open awaitable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AwaitableKind {
    /// A scheduled-but-unresolved activity (regular, local, or external).
    Activity,
    /// An armed-but-unfired durable timer.
    Timer,
    /// An awaited-but-unfulfilled signal (by name).
    Signal,
    /// An awaited child workflow that has not reached a terminal state.
    ChildWorkflow,
    /// A command-less `ctx.await_condition` park (no site label exists).
    Condition,
    /// An admitted update whose handler result has not been recorded.
    Update,
    /// A parked durable-mutex acquire (issue #691).
    Mutex,
    /// A parked external-workflow operation (await/signal/cancel of a sibling
    /// execution by id).
    ExternalWorkflow,
}

impl AwaitableKind {
    /// The `snake_case` wire label for this kind (matches the serde encoding).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Activity => "activity",
            Self::Timer => "timer",
            Self::Signal => "signal",
            Self::ChildWorkflow => "child_workflow",
            Self::Condition => "condition",
            Self::Update => "update",
            Self::Mutex => "mutex",
            Self::ExternalWorkflow => "external_workflow",
        }
    }
}

// serde's `skip_serializing_if` calls this with `&bool` (a reference to the
// field) — the reference signature is required by serde, not optional.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

/// One open awaitable the execution is currently parked on.
///
/// Deliberately payload-free: names, identifiers, and timestamps only — never
/// activity inputs, signal payloads, or update arguments — so the response
/// needs no payload-codec (issue #608) integration and cannot leak business
/// data beyond what the admin-gated side tables already expose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Awaitable {
    /// The awaitable's category.
    pub kind: AwaitableKind,
    /// Human-meaningful name where one exists: activity/child-workflow/update
    /// handler name, signal name, or mutex key. `None` for timers (see `id`)
    /// and `await_condition` parks (the API takes no site label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Stable identifier where one exists: activity exec id, timer id, child
    /// execution id, update id, or external-workflow target execution id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// When the wait began, derived from recorded history: the awaitable's own
    /// scheduling/arming event where one exists, else the timestamp of the last
    /// recorded event (the suspension point — when the run last made progress).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
    /// A deadline where one applies: timer fire time (armed-at + duration),
    /// external-activity schedule-to-close, or the deadline of a
    /// signal-or-deadline / child-or-deadline race. Callers may enrich activity
    /// awaitables with the task row's cross-retry `schedule_to_close_at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline: Option<DateTime<Utc>>,
    /// `true` for a local activity (runs inline on the workflow worker).
    #[serde(skip_serializing_if = "is_false")]
    pub local: bool,
    /// `true` for an external-handoff activity (completed via task token).
    #[serde(skip_serializing_if = "is_false")]
    pub external: bool,
}

impl Awaitable {
    const fn new(kind: AwaitableKind) -> Self {
        Self {
            kind,
            name: None,
            id: None,
            since: None,
            deadline: None,
            local: false,
            external: false,
        }
    }
}

/// How the open wait-set was derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitSetSource {
    /// The wait-set is the replay-derived pending command buffer — all six
    /// categories observable.
    Replayed,
    /// Best-effort event-log scan only — awaited-unsent signals and
    /// `await_condition` parks are not observable.
    HistoryOnly,
}

/// The wait-set input to [`project_awaitables`].
#[derive(Clone, Copy)]
pub enum WaitSetInput<'a> {
    /// The read-only replay drive reached a genuine suspension and `commands`
    /// is the drained command buffer at that point.
    Replayed {
        /// The drained pending commands (consumed via
        /// `WorkflowContext::drain_commands` after a `Suspended` drive).
        commands: &'a [WorkflowCommand],
    },
    /// The replay drive was unavailable; scan recorded history alone.
    HistoryOnly {
        /// The live *unfired* `harvest_timers` ids, when the caller (the
        /// management endpoint) has database access. A history-only scan
        /// reports every unclosed `TimerStarted`, but a cancellable
        /// `ctx.start_timer` arm (issue #768, `ArmTimer { for_await: false }`)
        /// records a `TimerStarted` with **no** `harvest_timers` row and cannot
        /// fire until `await_fire()`. When `Some`, a projected `Timer`
        /// awaitable whose id is absent from the set is dropped as a dormant
        /// arm — filtered **inside** the projection, before the per-category
        /// cap, so a fire-eligible timer is never crowded out of the report by
        /// dormant arms ahead of it (issue #615 Codex P2). `None` (a pure
        /// caller with no DB, or a failed live-timer read) skips the filter and
        /// reports every unclosed `TimerStarted`. Fire-eligibility is
        /// unrepresentable in the `Replayed` mode, which already excludes
        /// dormant arms by construction (it matches only
        /// `ArmTimer { for_await: true }`).
        fire_eligible_timers: Option<&'a HashSet<String>>,
    },
}

/// The projected open-awaitables report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AwaitablesProjection {
    /// How the wait-set was derived.
    pub source: WaitSetSource,
    /// The open awaitables, in recorded/command order, bounded per category.
    pub awaitables: Vec<Awaitable>,
    /// `true` when any category overflowed its cap.
    pub truncated: bool,
    /// The categories that overflowed (sorted, deduplicated).
    pub truncated_kinds: Vec<AwaitableKind>,
    /// The per-category cap that was applied.
    pub category_cap: usize,
}

/// External-handoff activity metadata: (name, awaiting-since, deadline).
type ExternalActivityMeta = (String, DateTime<Utc>, Option<DateTime<Utc>>);

/// Metadata about one open (unpaired) durable-timer arm.
struct OpenTimerArm {
    since: DateTime<Utc>,
    duration_secs: u64,
}

/// History-derived indexes consulted by both projection modes. Built in one
/// O(n) pass over the timestamped rows.
#[derive(Default)]
struct HistoryIndex {
    /// `activity_id` → (name, scheduled-at) for regular activities.
    ///
    /// Keyed by the native `ActivityExecId` (a `Copy` newtype over `Uuid`,
    /// not `String`): every insert/lookup on this history-derived index sees
    /// each activity id at least twice (open, then closed), so keying by the
    /// 16-byte `Copy` id instead of a formatted 36-byte hyphenated string
    /// avoids a `Uuid::to_string()` allocation-plus-format AND a
    /// variable-length `SipHash` pass on every touch — a wide fan-out's
    /// history scan is the whole cost of this index, so both add up.
    activities: HashMap<ActivityExecId, (String, DateTime<Utc>)>,
    /// activity ids with a recorded terminal (completed/failed/timed out/
    /// externally resolved).
    closed_activities: HashSet<ActivityExecId>,
    /// `activity_id` → (name, scheduled-at) for local activities.
    local_activities: HashMap<ActivityExecId, (String, DateTime<Utc>)>,
    /// local activity ids with a recorded terminal (completed/exhausted).
    closed_local_activities: HashSet<ActivityExecId>,
    /// `activity_id` → (name, awaiting-since, deadline) for external handoffs.
    external_activities: HashMap<ActivityExecId, ExternalActivityMeta>,
    /// Per-timer-id FIFO of open (unpaired) arms: `TimerFired`/`TimerCancelled`
    /// closes the oldest open arm (the poll-loop re-arm idiom).
    open_timer_arms: HashMap<String, VecDeque<OpenTimerArm>>,
    /// Insertion order of timer ids (stable reporting order).
    timer_order: Vec<String>,
    /// Child exec id → (workflow name, started-at) for open awaited children.
    ///
    /// Keyed by the native `ExecutionId` (`Copy`, same rationale as
    /// `activities` above).
    children: HashMap<ExecutionId, (String, DateTime<Utc>)>,
    /// Insertion order of open children.
    child_order: Vec<ExecutionId>,
    /// Update id → (handler name, admitted-at) for unresolved updates,
    /// in admission order.
    pending_updates: Vec<(String, String, DateTime<Utc>)>,
    /// `await_id` → (target exec id, requested-at) for unresolved external
    /// workflow awaits, in request order.
    open_external_awaits: Vec<(String, String, DateTime<Utc>)>,
    /// Unresolved external signal (issue #244) / cancel (issue #492) requests,
    /// each `(target exec id, optional signal name, requested-at)`, in request
    /// order. A signal request carries the signal name; a cancel request does
    /// not. These are the in-flight external operations a workflow is parked on
    /// after appending `External{Signal,Cancel}Requested` but before the
    /// (possibly cross-shard, outbox-delivered) terminal event — so the
    /// history-only fallback still reports them in crash-recovery / degraded
    /// scans, matching the replayed mode's `SignalExternalWorkflow` /
    /// `RequestCancelExternalWorkflow` command projection.
    open_external_ops: Vec<(String, Option<String>, DateTime<Utc>)>,
    /// Timestamp of the last recorded event (the suspension point).
    last_event_at: Option<DateTime<Utc>>,
}

#[allow(clippy::too_many_lines)]
fn build_history_index(rows: &[(DateTime<Utc>, WorkflowEvent)]) -> HistoryIndex {
    let mut index = HistoryIndex::default();
    let mut resolved_updates: HashSet<String> = HashSet::new();
    let mut admitted_updates: Vec<(String, String, DateTime<Utc>)> = Vec::new();
    let mut external_awaits: Vec<(String, String, DateTime<Utc>)> = Vec::new();
    let mut resolved_awaits: HashSet<String> = HashSet::new();
    // External signal/cancel requests keyed by their correlation id. Signal and
    // cancel ids are distinct UUIDs, so one resolved set cannot collide across
    // the two. Tuple: (correlation id, target, optional signal name, at).
    let mut external_ops: Vec<(String, String, Option<String>, DateTime<Utc>)> = Vec::new();
    let mut resolved_external_ops: HashSet<String> = HashSet::new();

    for (at, event) in rows {
        index.last_event_at = Some(*at);
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } => {
                index.activities.insert(*activity_id, (name.clone(), *at));
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
            | WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
            | WorkflowEvent::ActivityFailedExternally { activity_id, .. } => {
                index.closed_activities.insert(*activity_id);
            }
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                name,
                schedule_to_close_secs,
                ..
            } => {
                let deadline = i64::try_from(*schedule_to_close_secs)
                    .ok()
                    .and_then(|secs| at.checked_add_signed(ChronoDuration::seconds(secs)));
                index
                    .external_activities
                    .insert(*activity_id, (name.clone(), *at, deadline));
            }
            WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, .. } => {
                // The extension event carries no new deadline value (the real
                // deadline lives only in the `harvest_external_tasks` row), so
                // clear the recorded one: reporting the ORIGINAL
                // schedule-to-close as still due after an operator extended it
                // would be actively misleading (replay review).
                if let Some(meta) = index.external_activities.get_mut(activity_id) {
                    meta.2 = None;
                }
            }
            WorkflowEvent::LocalActivityScheduled {
                activity_id, name, ..
            } => {
                index
                    .local_activities
                    .insert(*activity_id, (name.clone(), *at));
            }
            WorkflowEvent::LocalActivityCompleted { activity_id, .. }
            | WorkflowEvent::LocalActivityExhausted { activity_id, .. } => {
                index.closed_local_activities.insert(*activity_id);
            }
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => {
                let id = timer_id.to_string();
                if !index.open_timer_arms.contains_key(&id) {
                    index.timer_order.push(id.clone());
                }
                index
                    .open_timer_arms
                    .entry(id)
                    .or_default()
                    .push_back(OpenTimerArm {
                        since: *at,
                        duration_secs: *duration_secs,
                    });
            }
            WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id } => {
                if let Some(arms) = index.open_timer_arms.get_mut(&timer_id.to_string()) {
                    arms.pop_front();
                }
            }
            // A signal-win of a signal-or-deadline race (issue #476) tears its
            // reserved deadline timer down via an EVENT-LESS
            // `CancelRaceLosers` row delete — no `TimerFired`/`TimerCancelled`
            // is ever recorded for the losing arm. Treat the oldest open
            // reserved arm for this signal name as closed at the
            // `SignalReceived`, so history-only mode does not report a
            // resolved race as a still-open signal awaitable (replay review
            // R3). A timer-win recorded `TimerFired` first (arm already
            // closed), so a genuinely LATE signal is a no-op here.
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                close_race_arm_for(&mut index, reserved_signal_race_name, signal_name);
            }
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => {
                index.child_order.push(*child_id);
                index
                    .children
                    .insert(*child_id, (workflow_name.clone(), *at));
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                // Mirror of the SignalReceived arm above for the
                // child-or-deadline race (issue #779): a child-win tears the
                // reserved deadline timer down event-lessly, so close the
                // oldest open reserved arm matching the child's workflow name
                // at its terminal. On a timer-win the fired arm was already
                // closed and the loser's synthetic terminal is a no-op here.
                if let Some((child_name, _)) = index.children.remove(child_id) {
                    close_race_arm_for(&mut index, reserved_child_race_name, &child_name);
                }
            }
            WorkflowEvent::UpdateAdmitted {
                update_id, name, ..
            } => {
                admitted_updates.push((update_id.to_string(), name.clone(), *at));
            }
            WorkflowEvent::UpdateCompleted { update_id, .. }
            | WorkflowEvent::UpdateFailed { update_id, .. } => {
                resolved_updates.insert(update_id.to_string());
            }
            WorkflowEvent::ExternalAwaitRequested { await_id, target } => {
                external_awaits.push((await_id.to_string(), target.to_string(), *at));
            }
            WorkflowEvent::ExternalAwaitResolved { await_id, .. }
            | WorkflowEvent::ExternalAwaitFailed { await_id, .. } => {
                resolved_awaits.insert(await_id.to_string());
            }
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name,
                ..
            } => {
                external_ops.push((
                    signal_id.to_string(),
                    target.to_string(),
                    Some(signal_name.clone()),
                    *at,
                ));
            }
            WorkflowEvent::ExternalSignalDelivered { signal_id }
            | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                resolved_external_ops.insert(signal_id.to_string());
            }
            WorkflowEvent::ExternalCancelRequested { cancel_id, target } => {
                external_ops.push((cancel_id.to_string(), target.to_string(), None, *at));
            }
            WorkflowEvent::ExternalCancelDelivered { cancel_id }
            | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                resolved_external_ops.insert(cancel_id.to_string());
            }
            _ => {}
        }
    }

    index.pending_updates = admitted_updates
        .into_iter()
        .filter(|(id, _, _)| !resolved_updates.contains(id))
        .collect();
    index.open_external_awaits = external_awaits
        .into_iter()
        .filter(|(id, _, _)| !resolved_awaits.contains(id))
        .collect();
    index.open_external_ops = external_ops
        .into_iter()
        .filter(|(id, _, _, _)| !resolved_external_ops.contains(id))
        .map(|(_, target, name, at)| (target, name, at))
        .collect();
    index
}

/// Closes the oldest open reserved race arm whose id parses (via `parse`) to
/// `name`.
///
/// A resolved signal-or-deadline / child-or-deadline race tears its reserved
/// deadline timer down via an event-less `CancelRaceLosers` row delete, so the
/// arm never records a closing `TimerFired`/`TimerCancelled`. Called from the
/// history-index pass at each race-resolving event so history-only mode does
/// not report the resolved race as still open. No-op when no open matching arm
/// exists (timer-win already closed it via its recorded `TimerFired`).
fn close_race_arm_for(index: &mut HistoryIndex, parse: fn(&str) -> Option<&str>, name: &str) {
    // Close the OLDEST open reserved arm for `name` in RECORDED timer order,
    // NOT by `arm.since` timestamp: two same-name race timers armed in one
    // suspension are persisted in a single transaction and share a
    // `DEFAULT NOW()` `TimerStarted.timestamp`, so a timestamp tie-break pops an
    // arbitrary arm across the two `open_timer_arms` HashMap entries (HashMap
    // iteration order is nondeterministic), leaving the surviving waiter paired
    // with the wrong deadline. `index.timer_order` is the authoritative recorded
    // order (insertion == command == increasing reserved seq) and is exactly
    // what `project_history_only` walks to pair surviving arms with their
    // deadlines, so closing the front-most `timer_order` arm keeps the two
    // consistent — the K-th race resolution FIFO-closes the K-th oldest reserved
    // arm (issue #615 Codex P2). A timer-won arm was already closed by its
    // recorded `TimerFired` (empty deque), so it is skipped here.
    let candidate = index
        .timer_order
        .iter()
        .find(|id| {
            parse(id.as_str()) == Some(name)
                && index
                    .open_timer_arms
                    .get(id.as_str())
                    .is_some_and(|arms| !arms.is_empty())
        })
        .cloned();
    if let Some(id) = candidate
        && let Some(arms) = index.open_timer_arms.get_mut(&id)
    {
        arms.pop_front();
    }
}

/// Parses the signal name out of a reserved signal-or-deadline race timer id
/// (`__signal_timeout:{seq}:{signal_name}`, issue #476).
pub(crate) fn reserved_signal_race_name(timer_id: &str) -> Option<&str> {
    timer_id
        .strip_prefix(SIGNAL_TIMEOUT_TIMER_PREFIX)?
        .split_once(':')
        .map(|(_seq, name)| name)
}

/// Parses the child workflow name out of a reserved child-or-deadline race
/// timer id (`__child_timeout:{seq}:{workflow_name}`, issue #779).
pub(crate) fn reserved_child_race_name(timer_id: &str) -> Option<&str> {
    timer_id
        .strip_prefix(crate::context::CHILD_TIMEOUT_TIMER_PREFIX)?
        .split_once(':')
        .map(|(_seq, name)| name)
}

/// A race deadline parsed from a reserved race timer, to be folded into the
/// matching signal/child awaitable rather than reported as an internal timer.
struct RaceDeadline {
    since: Option<DateTime<Utc>>,
    deadline: Option<DateTime<Utc>>,
}

fn timer_arm_metadata(
    index: &HistoryIndex,
    timer_id: &str,
    duration_secs: u64,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let since = index
        .open_timer_arms
        .get(timer_id)
        .and_then(|arms| arms.front())
        .map(|arm| arm.since);
    let deadline = since.and_then(|at| {
        i64::try_from(duration_secs)
            .ok()
            .and_then(|secs| at.checked_add_signed(ChronoDuration::seconds(secs)))
    });
    (since, deadline)
}

fn activity_awaitable_by_id(index: &HistoryIndex, activity_id: ActivityExecId) -> Awaitable {
    let mut awaitable = Awaitable::new(AwaitableKind::Activity);
    awaitable.id = Some(activity_id.to_string());
    if let Some((name, since)) = index.activities.get(&activity_id) {
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
    } else if let Some((name, since, deadline)) = index.external_activities.get(&activity_id) {
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.deadline = *deadline;
        awaitable.external = true;
    } else if let Some((name, since)) = index.local_activities.get(&activity_id) {
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.local = true;
    }
    awaitable
}

/// Projects the wait-set drained from a suspended replay drive.
#[allow(clippy::too_many_lines)]
fn project_replayed(index: &HistoryIndex, commands: &[WorkflowCommand]) -> Vec<Awaitable> {
    let mut awaitables: Vec<Awaitable> = Vec::new();
    // Reserved race deadlines, per name, in command order. A `VecDeque` (not a
    // single `RaceDeadline`) so two concurrent same-name waiters — a `join!` of
    // two `receive_signal_timeout("approval", ...)` with distinct deadlines —
    // do not overwrite each other; the K-th wait for a name is paired with the
    // K-th reserved race for that name in the fold below (issue #615 Codex P2).
    let mut signal_races: HashMap<String, VecDeque<RaceDeadline>> = HashMap::new();
    let mut child_races: HashMap<String, VecDeque<RaceDeadline>> = HashMap::new();
    let mut transitioning = false;
    let mut resolved_update_ids: HashSet<String> = HashSet::new();

    for command in commands {
        match command {
            WorkflowCommand::ScheduleActivity {
                activity_id, name, ..
            } => {
                let mut awaitable = activity_awaitable_by_id(index, *activity_id);
                if awaitable.name.is_none() {
                    awaitable.name = Some(name.clone());
                }
                awaitables.push(awaitable);
            }
            WorkflowCommand::WaitForActivity { activity_id, .. } => {
                awaitables.push(activity_awaitable_by_id(index, *activity_id));
            }
            WorkflowCommand::RunLocalActivity {
                activity_id, name, ..
            } => {
                let mut awaitable = activity_awaitable_by_id(index, *activity_id);
                awaitable.local = true;
                if awaitable.name.is_none() {
                    awaitable.name = Some(name.clone());
                }
                awaitables.push(awaitable);
            }
            WorkflowCommand::ScheduleExternalActivity {
                activity_id,
                name,
                schedule_to_close_secs,
                ..
            } => {
                let mut awaitable = activity_awaitable_by_id(index, *activity_id);
                awaitable.external = true;
                if awaitable.name.is_none() {
                    awaitable.name = Some(name.clone());
                }
                if awaitable.deadline.is_none() {
                    awaitable.deadline = awaitable.since.and_then(|at| {
                        i64::try_from(*schedule_to_close_secs)
                            .ok()
                            .and_then(|secs| at.checked_add_signed(ChronoDuration::seconds(secs)))
                    });
                }
                awaitables.push(awaitable);
            }
            // Known approximation: a cancellable timer's real deadline is
            // anchored at `await_fire` time (issue #768 round 4), but the
            // recorded `TimerStarted` is at ARM time — so an
            // `ArmTimer { for_await: true }` deadline computed from the arm's
            // recorded `since` can UNDERSTATE the true fire time by the
            // arm-to-await gap. The await instant is not recoverable from
            // history, so the earlier (conservative) deadline is reported.
            WorkflowCommand::StartTimer {
                timer_id,
                duration_secs,
                ..
            }
            | WorkflowCommand::ArmTimer {
                timer_id,
                duration_secs,
                for_await: true,
            } => {
                let id = timer_id.to_string();
                let (since, deadline) = timer_arm_metadata(index, &id, *duration_secs);
                if let Some(signal_name) = reserved_signal_race_name(&id) {
                    signal_races
                        .entry(signal_name.to_string())
                        .or_default()
                        .push_back(RaceDeadline { since, deadline });
                } else if let Some(child_name) = reserved_child_race_name(&id) {
                    child_races
                        .entry(child_name.to_string())
                        .or_default()
                        .push_back(RaceDeadline { since, deadline });
                } else {
                    let mut awaitable = Awaitable::new(AwaitableKind::Timer);
                    awaitable.id = Some(id);
                    awaitable.since = since;
                    awaitable.deadline = deadline;
                    awaitables.push(awaitable);
                }
            }
            WorkflowCommand::WaitForSignal { signal_name, .. } => {
                let mut awaitable = Awaitable::new(AwaitableKind::Signal);
                awaitable.name = Some(signal_name.clone());
                awaitable.since = index.last_event_at;
                awaitables.push(awaitable);
            }
            WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name,
                ..
            } => {
                let mut awaitable = Awaitable::new(AwaitableKind::ChildWorkflow);
                if let Some((name, since)) = index.children.get(child_id) {
                    awaitable.name = Some(name.clone());
                    awaitable.since = Some(*since);
                } else {
                    awaitable.name = Some(workflow_name.clone());
                }
                awaitable.id = Some(child_id.to_string());
                awaitables.push(awaitable);
            }
            WorkflowCommand::AwaitExternalWorkflow { target, .. } => {
                let mut awaitable = Awaitable::new(AwaitableKind::ExternalWorkflow);
                let target_id = target.to_string();
                awaitable.since = index
                    .open_external_awaits
                    .iter()
                    .find(|(_, open_target, _)| *open_target == target_id)
                    .map(|(_, _, at)| *at)
                    .or(index.last_event_at);
                awaitable.id = Some(target_id);
                awaitables.push(awaitable);
            }
            WorkflowCommand::SignalExternalWorkflow {
                target,
                signal_name,
                ..
            } => {
                let mut awaitable = Awaitable::new(AwaitableKind::ExternalWorkflow);
                awaitable.id = Some(target.to_string());
                awaitable.name = Some(signal_name.clone());
                awaitable.since = index.last_event_at;
                awaitables.push(awaitable);
            }
            WorkflowCommand::RequestCancelExternalWorkflow { target, .. } => {
                let mut awaitable = Awaitable::new(AwaitableKind::ExternalWorkflow);
                awaitable.id = Some(target.to_string());
                awaitable.since = index.last_event_at;
                awaitables.push(awaitable);
            }
            WorkflowCommand::AcquireMutex { key, .. } => {
                let mut awaitable = Awaitable::new(AwaitableKind::Mutex);
                awaitable.name = Some(key.clone());
                awaitable.since = index.last_event_at;
                awaitables.push(awaitable);
            }
            WorkflowCommand::RecordUpdateResult { update_id, .. } => {
                resolved_update_ids.insert(update_id.to_string());
            }
            WorkflowCommand::Complete { .. }
            | WorkflowCommand::Fail { .. }
            | WorkflowCommand::ContinueAsNew { .. } => {
                transitioning = true;
            }
            _ => {}
        }
    }

    // Fold race deadlines into their matching signal/child awaitables, pairing
    // per name in COMMAND ORDER so two concurrent same-name waiters each keep
    // their own deadline. Both the reserved race deadlines and the
    // Signal/ChildWorkflow awaitables are collected in command order, and the
    // K-th `receive_signal_timeout`/`execute_child_workflow_timeout` call for a
    // name contributes both the K-th reserved race and the K-th matching
    // awaitable for that name — so popping the front of a per-name queue as
    // each awaitable is visited pairs them correctly (issue #615 Codex P2). A
    // single pass over the awaitables (M) with O(1) queue ops keeps this
    // O(N + M), not O(N·M). Any race deadline left unpaired is reported as a
    // plain timer below so nothing is silently dropped.
    for awaitable in &mut awaitables {
        match awaitable.kind {
            AwaitableKind::Signal => {
                if let Some(name) = &awaitable.name
                    && let Some(race) = signal_races.get_mut(name).and_then(VecDeque::pop_front)
                {
                    if let Some(since) = race.since {
                        awaitable.since = Some(since);
                    }
                    awaitable.deadline = race.deadline;
                }
            }
            AwaitableKind::ChildWorkflow => {
                if let Some(name) = &awaitable.name
                    && let Some(race) = child_races.get_mut(name).and_then(VecDeque::pop_front)
                {
                    awaitable.deadline = race.deadline;
                }
            }
            _ => {}
        }
    }
    // A reserved race timer with no matching wait (defensive — should not occur
    // in a coherent suspension) degrades to a plain timer so nothing is dropped.
    for (signal_name, races) in signal_races {
        for race in races {
            awaitables.push(orphan_race_timer(
                SIGNAL_TIMEOUT_TIMER_PREFIX,
                &signal_name,
                &race,
            ));
        }
    }
    for (child_name, races) in child_races {
        for race in races {
            awaitables.push(orphan_race_timer(
                crate::context::CHILD_TIMEOUT_TIMER_PREFIX,
                &child_name,
                &race,
            ));
        }
    }

    // Condition park: suspended with nothing awaitable and no terminal
    // transition in flight — the only command-less cold park the engine has is
    // `ctx.await_condition` (issue #612's case-3 discriminator). A drained
    // `RecordUpdateResult` disqualifies the inference: it produces no awaitable
    // and no transition, but it means an admitted update's result is committing
    // this cycle (its `UpdateCompleted` not yet persisted) — an in-flight
    // update-result commit, NOT a condition park. Pure-bookkeeping commands
    // (`SetCurrentDetails`, `RecordSideEffect`, `RecordMarker`,
    // `SpawnDetachedChildWorkflow`, `ReleaseMutex`, …) legitimately precede a
    // cold `await_condition` park and are left alone, so keying on the empty
    // `resolved_update_ids` set — rather than an empty command buffer — keeps
    // the inference precise (a real cold park after a `set_current_details`
    // breadcrumb still reports `Condition`).
    if awaitables.is_empty() && !transitioning && resolved_update_ids.is_empty() {
        let mut awaitable = Awaitable::new(AwaitableKind::Condition);
        awaitable.since = index.last_event_at;
        awaitables.push(awaitable);
    }

    // Pending updates from history, minus any update resolved in the drained
    // cycle (its terminal event has not persisted yet, but it is no longer
    // awaited).
    for (update_id, name, since) in &index.pending_updates {
        if resolved_update_ids.contains(update_id) {
            continue;
        }
        awaitables.push(update_awaitable(update_id, name, *since));
    }

    awaitables
}

fn orphan_race_timer(prefix: &str, name: &str, race: &RaceDeadline) -> Awaitable {
    let mut awaitable = Awaitable::new(AwaitableKind::Timer);
    awaitable.id = Some(format!("{prefix}{name}"));
    awaitable.since = race.since;
    awaitable.deadline = race.deadline;
    awaitable
}

fn update_awaitable(update_id: &str, name: &str, since: DateTime<Utc>) -> Awaitable {
    let mut awaitable = Awaitable::new(AwaitableKind::Update);
    awaitable.id = Some(update_id.to_string());
    awaitable.name = Some(name.to_string());
    awaitable.since = Some(since);
    awaitable
}

/// Projects the best-effort wait-set from recorded history alone.
///
/// `fire_eligible_timers`, when `Some`, is the set of live *unfired*
/// `harvest_timers` ids used to drop dormant cancellable `ctx.start_timer`
/// arms (issue #768) — a `TimerStarted` with no `harvest_timers` row that
/// cannot fire until `await_fire()`. The filter is applied HERE, before the
/// caller ([`project_awaitables`]) caps each category, so a fire-eligible
/// timer beyond the cap position is never crowded out by dormant arms ahead of
/// it (issue #615 Codex P2). `None` reports every unclosed `TimerStarted`.
#[allow(clippy::too_many_lines)]
fn project_history_only(
    index: &HistoryIndex,
    fire_eligible_timers: Option<&HashSet<String>>,
) -> Vec<Awaitable> {
    let mut awaitables: Vec<Awaitable> = Vec::new();

    // Open regular activities, in scheduling order.
    let mut open_activities: Vec<(&ActivityExecId, &(String, DateTime<Utc>))> = index
        .activities
        .iter()
        .filter(|(id, _)| !index.closed_activities.contains(*id))
        .collect();
    open_activities.sort_by_key(|(_, (_, at))| *at);
    for (id, (name, since)) in open_activities {
        let mut awaitable = Awaitable::new(AwaitableKind::Activity);
        awaitable.id = Some(id.to_string());
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitables.push(awaitable);
    }

    // Open external-handoff activities.
    let mut open_external: Vec<(&ActivityExecId, &ExternalActivityMeta)> = index
        .external_activities
        .iter()
        .filter(|(id, _)| !index.closed_activities.contains(*id))
        .collect();
    open_external.sort_by_key(|(_, (_, at, _))| *at);
    for (id, (name, since, deadline)) in open_external {
        let mut awaitable = Awaitable::new(AwaitableKind::Activity);
        awaitable.id = Some(id.to_string());
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.deadline = *deadline;
        awaitable.external = true;
        awaitables.push(awaitable);
    }

    // Open local activities (scheduled, possibly mid-retry, no terminal).
    let mut open_local: Vec<(&ActivityExecId, &(String, DateTime<Utc>))> = index
        .local_activities
        .iter()
        .filter(|(id, _)| !index.closed_local_activities.contains(*id))
        .collect();
    open_local.sort_by_key(|(_, (_, at))| *at);
    for (id, (name, since)) in open_local {
        let mut awaitable = Awaitable::new(AwaitableKind::Activity);
        awaitable.id = Some(id.to_string());
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.local = true;
        awaitables.push(awaitable);
    }

    // Open timer arms. A reserved signal-race timer id encodes the awaited
    // signal's name, so even without a replay we can name the signal and its
    // deadline; a reserved child-race timer folds into its child below.
    //
    // Per-child-name FIFO (not a single deadline per name): two concurrent
    // same-type child-or-deadline waiters (issue #779) with distinct deadlines
    // must each keep their own, paired in recorded order — otherwise a later
    // reserved `TimerStarted` overwrites the earlier one and both children read
    // the same deadline. `timer_order` and `child_order` both preserve
    // recorded order and both derive from the same command order, so popping
    // the front of a name's queue as each child is visited pairs them
    // correctly, mirroring the replayed path's per-name `VecDeque` (issue #615
    // Codex P2).
    let mut child_race_deadlines: HashMap<String, VecDeque<DateTime<Utc>>> = HashMap::new();
    for timer_id in &index.timer_order {
        let Some(arms) = index.open_timer_arms.get(timer_id) else {
            continue;
        };
        for arm in arms {
            let deadline = i64::try_from(arm.duration_secs)
                .ok()
                .and_then(|secs| arm.since.checked_add_signed(ChronoDuration::seconds(secs)));
            if let Some(signal_name) = reserved_signal_race_name(timer_id) {
                let mut awaitable = Awaitable::new(AwaitableKind::Signal);
                awaitable.name = Some(signal_name.to_string());
                awaitable.since = Some(arm.since);
                awaitable.deadline = deadline;
                awaitables.push(awaitable);
            } else if let Some(child_name) = reserved_child_race_name(timer_id) {
                if let Some(deadline) = deadline {
                    child_race_deadlines
                        .entry(child_name.to_string())
                        .or_default()
                        .push_back(deadline);
                }
            } else {
                // Drop a dormant cancellable `ctx.start_timer` arm (issue #768):
                // a `TimerStarted` with no live `harvest_timers` row cannot fire
                // until `await_fire()`. Filtering here — before the caller caps
                // each category — keeps a fire-eligible timer that sits beyond
                // the cap position from being crowded out by dormant arms ahead
                // of it (issue #615 Codex P2). A reserved race timer (handled
                // above) always arms a real durable timer, so it is never
                // subject to this filter.
                if let Some(live) = fire_eligible_timers
                    && !live.contains(timer_id)
                {
                    continue;
                }
                let mut awaitable = Awaitable::new(AwaitableKind::Timer);
                awaitable.id = Some(timer_id.clone());
                awaitable.since = Some(arm.since);
                awaitable.deadline = deadline;
                awaitables.push(awaitable);
            }
        }
    }

    // Open awaited children, in start order. Pop the front of the child's
    // per-name deadline FIFO so two concurrent same-type child-or-deadline
    // waiters each keep their own deadline (issue #615 Codex P2).
    for child_id in &index.child_order {
        if let Some((name, since)) = index.children.get(child_id) {
            let mut awaitable = Awaitable::new(AwaitableKind::ChildWorkflow);
            awaitable.id = Some(child_id.to_string());
            awaitable.name = Some(name.clone());
            awaitable.since = Some(*since);
            awaitable.deadline = child_race_deadlines
                .get_mut(name)
                .and_then(VecDeque::pop_front);
            awaitables.push(awaitable);
        }
    }

    // Unresolved external-workflow awaits.
    for (_, target, since) in &index.open_external_awaits {
        let mut awaitable = Awaitable::new(AwaitableKind::ExternalWorkflow);
        awaitable.id = Some(target.clone());
        awaitable.since = Some(*since);
        awaitables.push(awaitable);
    }

    // Unresolved external signal/cancel requests (issue #244/#492): an
    // in-flight external operation whose terminal event has not been recorded
    // yet — the crash-recovery / cross-shard-outbox window the replayed mode
    // sees via the drained command, and which the fallback must not drop.
    for (target, name, since) in &index.open_external_ops {
        let mut awaitable = Awaitable::new(AwaitableKind::ExternalWorkflow);
        awaitable.id = Some(target.clone());
        awaitable.name.clone_from(name);
        awaitable.since = Some(*since);
        awaitables.push(awaitable);
    }

    // Unresolved updates.
    for (update_id, name, since) in &index.pending_updates {
        awaitables.push(update_awaitable(update_id, name, *since));
    }

    awaitables
}

/// Projects an execution's open awaitables from its recorded history and (when
/// available) the replay-derived pending command buffer.
///
/// `rows` is the timestamped event history (e.g.
/// `store::load_history_with_timestamps`), in recorded order. `cap` bounds
/// each category to its first N entries — see [`AWAITABLE_CATEGORY_CAP`].
///
/// Pure and read-only: no side effects, no mutation, no new event variants.
#[must_use]
pub fn project_awaitables(
    rows: &[(DateTime<Utc>, WorkflowEvent)],
    wait_set: WaitSetInput<'_>,
    cap: usize,
) -> AwaitablesProjection {
    let index = build_history_index(rows);
    let (source, awaitables) = match wait_set {
        WaitSetInput::Replayed { commands } => {
            (WaitSetSource::Replayed, project_replayed(&index, commands))
        }
        WaitSetInput::HistoryOnly {
            fire_eligible_timers,
        } => (
            WaitSetSource::HistoryOnly,
            project_history_only(&index, fire_eligible_timers),
        ),
    };

    let cap = cap.max(1);
    let mut kept: Vec<Awaitable> = Vec::with_capacity(awaitables.len().min(cap * 4));
    let mut per_kind: HashMap<AwaitableKind, usize> = HashMap::new();
    let mut truncated_kinds: Vec<AwaitableKind> = Vec::new();
    for awaitable in awaitables {
        let count = per_kind.entry(awaitable.kind).or_insert(0);
        if *count < cap {
            *count += 1;
            kept.push(awaitable);
        } else if !truncated_kinds.contains(&awaitable.kind) {
            truncated_kinds.push(awaitable.kind);
        }
    }
    truncated_kinds.sort();

    AwaitablesProjection {
        source,
        truncated: !truncated_kinds.is_empty(),
        truncated_kinds,
        awaitables: kept,
        category_cap: cap,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_prefix_parsers_extract_names() {
        assert_eq!(
            reserved_signal_race_name("__signal_timeout:3:approval"),
            Some("approval")
        );
        assert_eq!(
            reserved_signal_race_name("__signal_timeout:0:has:colons"),
            Some("has:colons")
        );
        assert_eq!(reserved_signal_race_name("cooldown"), None);
        assert_eq!(reserved_signal_race_name("__signal_timeout:"), None);
        assert_eq!(
            reserved_child_race_name("__child_timeout:1:fulfillment_flow"),
            Some("fulfillment_flow")
        );
        assert_eq!(reserved_child_race_name("__signal_timeout:1:x"), None);
    }

    #[test]
    fn kind_as_str_matches_serde_encoding() {
        for kind in [
            AwaitableKind::Activity,
            AwaitableKind::Timer,
            AwaitableKind::Signal,
            AwaitableKind::ChildWorkflow,
            AwaitableKind::Condition,
            AwaitableKind::Update,
            AwaitableKind::Mutex,
            AwaitableKind::ExternalWorkflow,
        ] {
            let encoded = serde_json::to_value(kind).expect("serialize kind");
            assert_eq!(
                encoded,
                serde_json::Value::String(kind.as_str().to_string())
            );
        }
    }

    #[test]
    fn zero_cap_is_clamped_to_one() {
        let projection = project_awaitables(
            &[],
            WaitSetInput::HistoryOnly {
                fire_eligible_timers: None,
            },
            0,
        );
        assert_eq!(projection.category_cap, 1);
    }
}
