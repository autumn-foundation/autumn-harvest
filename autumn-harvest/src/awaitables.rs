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
//! mode exists to remove.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;

use crate::context::WorkflowCommand;
use crate::event::WorkflowEvent;

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
    HistoryOnly,
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
    activities: HashMap<String, (String, DateTime<Utc>)>,
    /// activity ids with a recorded terminal (completed/failed/timed out/
    /// externally resolved).
    closed_activities: HashSet<String>,
    /// `activity_id` → (name, scheduled-at) for local activities.
    local_activities: HashMap<String, (String, DateTime<Utc>)>,
    /// local activity ids with a recorded terminal (completed/exhausted).
    closed_local_activities: HashSet<String>,
    /// `activity_id` → (name, awaiting-since, deadline) for external handoffs.
    external_activities: HashMap<String, ExternalActivityMeta>,
    /// Per-timer-id FIFO of open (unpaired) arms: `TimerFired`/`TimerCancelled`
    /// closes the oldest open arm (the poll-loop re-arm idiom).
    open_timer_arms: HashMap<String, VecDeque<OpenTimerArm>>,
    /// Insertion order of timer ids (stable reporting order).
    timer_order: Vec<String>,
    /// Child exec id → (workflow name, started-at) for open awaited children.
    children: HashMap<String, (String, DateTime<Utc>)>,
    /// Insertion order of open children.
    child_order: Vec<String>,
    /// Update id → (handler name, admitted-at) for unresolved updates,
    /// in admission order.
    pending_updates: Vec<(String, String, DateTime<Utc>)>,
    /// `await_id` → (target exec id, requested-at) for unresolved external
    /// workflow awaits, in request order.
    open_external_awaits: Vec<(String, String, DateTime<Utc>)>,
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

    for (at, event) in rows {
        index.last_event_at = Some(*at);
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } => {
                index
                    .activities
                    .insert(activity_id.to_string(), (name.clone(), *at));
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
            | WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
            | WorkflowEvent::ActivityFailedExternally { activity_id, .. } => {
                index.closed_activities.insert(activity_id.to_string());
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
                    .insert(activity_id.to_string(), (name.clone(), *at, deadline));
            }
            WorkflowEvent::LocalActivityScheduled {
                activity_id, name, ..
            } => {
                index
                    .local_activities
                    .insert(activity_id.to_string(), (name.clone(), *at));
            }
            WorkflowEvent::LocalActivityCompleted { activity_id, .. }
            | WorkflowEvent::LocalActivityExhausted { activity_id, .. } => {
                index
                    .closed_local_activities
                    .insert(activity_id.to_string());
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
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => {
                let id = child_id.to_string();
                index.child_order.push(id.clone());
                index.children.insert(id, (workflow_name.clone(), *at));
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                index.children.remove(&child_id.to_string());
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
    index
}

/// Parses the signal name out of a reserved signal-or-deadline race timer id
/// (`__signal_timeout:{seq}:{signal_name}`, issue #476).
fn reserved_signal_race_name(timer_id: &str) -> Option<&str> {
    timer_id
        .strip_prefix(SIGNAL_TIMEOUT_TIMER_PREFIX)?
        .split_once(':')
        .map(|(_seq, name)| name)
}

/// Parses the child workflow name out of a reserved child-or-deadline race
/// timer id (`__child_timeout:{seq}:{workflow_name}`, issue #779).
fn reserved_child_race_name(timer_id: &str) -> Option<&str> {
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

fn activity_awaitable_by_id(index: &HistoryIndex, activity_id: &str) -> Awaitable {
    let mut awaitable = Awaitable::new(AwaitableKind::Activity);
    awaitable.id = Some(activity_id.to_string());
    if let Some((name, since)) = index.activities.get(activity_id) {
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
    } else if let Some((name, since, deadline)) = index.external_activities.get(activity_id) {
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.deadline = *deadline;
        awaitable.external = true;
    } else if let Some((name, since)) = index.local_activities.get(activity_id) {
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
    let mut signal_races: HashMap<String, RaceDeadline> = HashMap::new();
    let mut child_races: HashMap<String, RaceDeadline> = HashMap::new();
    let mut transitioning = false;
    let mut resolved_update_ids: HashSet<String> = HashSet::new();

    for command in commands {
        match command {
            WorkflowCommand::ScheduleActivity {
                activity_id, name, ..
            } => {
                let mut awaitable = activity_awaitable_by_id(index, &activity_id.to_string());
                if awaitable.name.is_none() {
                    awaitable.name = Some(name.clone());
                }
                awaitables.push(awaitable);
            }
            WorkflowCommand::WaitForActivity { activity_id, .. } => {
                awaitables.push(activity_awaitable_by_id(index, &activity_id.to_string()));
            }
            WorkflowCommand::RunLocalActivity {
                activity_id, name, ..
            } => {
                let mut awaitable = activity_awaitable_by_id(index, &activity_id.to_string());
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
                let mut awaitable = activity_awaitable_by_id(index, &activity_id.to_string());
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
                    signal_races.insert(signal_name.to_string(), RaceDeadline { since, deadline });
                } else if let Some(child_name) = reserved_child_race_name(&id) {
                    child_races.insert(child_name.to_string(), RaceDeadline { since, deadline });
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
                let id = child_id.to_string();
                if let Some((name, since)) = index.children.get(&id) {
                    awaitable.name = Some(name.clone());
                    awaitable.since = Some(*since);
                } else {
                    awaitable.name = Some(workflow_name.clone());
                }
                awaitable.id = Some(id);
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

    // Fold race deadlines into their matching signal/child awaitables. A race
    // timer with no matching wait (defensive — should not occur in a coherent
    // suspension) is reported as a plain timer so nothing is silently dropped.
    for (signal_name, race) in signal_races {
        if let Some(awaitable) = awaitables
            .iter_mut()
            .find(|a| a.kind == AwaitableKind::Signal && a.name.as_deref() == Some(&signal_name))
        {
            if let Some(since) = race.since {
                awaitable.since = Some(since);
            }
            awaitable.deadline = race.deadline;
        } else {
            awaitables.push(orphan_race_timer(
                SIGNAL_TIMEOUT_TIMER_PREFIX,
                &signal_name,
                &race,
            ));
        }
    }
    for (child_name, race) in child_races {
        if let Some(awaitable) = awaitables.iter_mut().find(|a| {
            a.kind == AwaitableKind::ChildWorkflow && a.name.as_deref() == Some(&child_name)
        }) {
            awaitable.deadline = race.deadline;
        } else {
            awaitables.push(orphan_race_timer(
                crate::context::CHILD_TIMEOUT_TIMER_PREFIX,
                &child_name,
                &race,
            ));
        }
    }

    // Condition park: suspended with nothing awaitable and no terminal
    // transition in flight — the only command-less cold park the engine has is
    // `ctx.await_condition` (issue #612's case-3 discriminator).
    if awaitables.is_empty() && !transitioning {
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
fn project_history_only(index: &HistoryIndex) -> Vec<Awaitable> {
    let mut awaitables: Vec<Awaitable> = Vec::new();

    // Open regular activities, in scheduling order.
    let mut open_activities: Vec<(&String, &(String, DateTime<Utc>))> = index
        .activities
        .iter()
        .filter(|(id, _)| !index.closed_activities.contains(*id))
        .collect();
    open_activities.sort_by_key(|(_, (_, at))| *at);
    for (id, (name, since)) in open_activities {
        let mut awaitable = Awaitable::new(AwaitableKind::Activity);
        awaitable.id = Some(id.clone());
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitables.push(awaitable);
    }

    // Open external-handoff activities.
    let mut open_external: Vec<(&String, &ExternalActivityMeta)> = index
        .external_activities
        .iter()
        .filter(|(id, _)| !index.closed_activities.contains(*id))
        .collect();
    open_external.sort_by_key(|(_, (_, at, _))| *at);
    for (id, (name, since, deadline)) in open_external {
        let mut awaitable = Awaitable::new(AwaitableKind::Activity);
        awaitable.id = Some(id.clone());
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.deadline = *deadline;
        awaitable.external = true;
        awaitables.push(awaitable);
    }

    // Open local activities (scheduled, possibly mid-retry, no terminal).
    let mut open_local: Vec<(&String, &(String, DateTime<Utc>))> = index
        .local_activities
        .iter()
        .filter(|(id, _)| !index.closed_local_activities.contains(*id))
        .collect();
    open_local.sort_by_key(|(_, (_, at))| *at);
    for (id, (name, since)) in open_local {
        let mut awaitable = Awaitable::new(AwaitableKind::Activity);
        awaitable.id = Some(id.clone());
        awaitable.name = Some(name.clone());
        awaitable.since = Some(*since);
        awaitable.local = true;
        awaitables.push(awaitable);
    }

    // Open timer arms. A reserved signal-race timer id encodes the awaited
    // signal's name, so even without a replay we can name the signal and its
    // deadline; a reserved child-race timer folds into its child below.
    let mut child_race_deadlines: HashMap<String, DateTime<Utc>> = HashMap::new();
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
                    child_race_deadlines.insert(child_name.to_string(), deadline);
                }
            } else {
                let mut awaitable = Awaitable::new(AwaitableKind::Timer);
                awaitable.id = Some(timer_id.clone());
                awaitable.since = Some(arm.since);
                awaitable.deadline = deadline;
                awaitables.push(awaitable);
            }
        }
    }

    // Open awaited children, in start order.
    for child_id in &index.child_order {
        if let Some((name, since)) = index.children.get(child_id) {
            let mut awaitable = Awaitable::new(AwaitableKind::ChildWorkflow);
            awaitable.id = Some(child_id.clone());
            awaitable.name = Some(name.clone());
            awaitable.since = Some(*since);
            awaitable.deadline = child_race_deadlines.get(name).copied();
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
        WaitSetInput::HistoryOnly => (WaitSetSource::HistoryOnly, project_history_only(&index)),
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
        let projection = project_awaitables(&[], WaitSetInput::HistoryOnly, 0);
        assert_eq!(projection.category_cap, 1);
    }
}
