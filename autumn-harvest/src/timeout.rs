//! Timeout enforcement for tasks in the work queue.
//!
//! This module provides a background scanner that periodically checks for tasks
//! that have exceeded their timeout limits:
//!
//! - **Heartbeat timeout**: RUNNING tasks whose `last_heartbeat_at` is older than
//!   their `heartbeat_timeout` interval.
//! - **Start-to-close timeout**: RUNNING tasks whose `started_at` plus
//!   `start_to_close` interval has elapsed.
//! - **Schedule-to-start timeout**: PENDING tasks whose `scheduled_at` plus
//!   `schedule_to_start` interval has elapsed.

use std::collections::HashSet;
use std::time::Duration;

use chrono::Utc;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult, TimeoutType};
use crate::event::WorkflowEvent;
use crate::execution::{
    apply_parent_close_cascade, cancel_workflow_execution_collect,
    check_and_report_unfinished_handlers,
};
use crate::models::{ExternalTask, TaskQueueItem, WorkflowExecution};
use crate::schema::{harvest_external_tasks, harvest_task_queue, harvest_workflow_executions};
use crate::telemetry::MetricsRecorder;
use crate::types::{ActivityExecId, ExecutionId, ExternalTarget};
use crate::{queue, store};

/// The reason a task was identified as timed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeoutReason {
    /// Activity stopped sending heartbeats within the configured interval.
    Heartbeat,
    /// Task has been RUNNING longer than its `start_to_close` limit.
    StartToClose,
    /// Task has been PENDING longer than its `schedule_to_start` limit.
    ScheduleToStart,
    /// The cross-retry wall-clock deadline (`schedule_to_close_at`) has elapsed.
    ///
    /// Fires for both RUNNING tasks (mid-execution) and PENDING tasks (queued
    /// but deadline already expired before a worker could claim the task).
    ScheduleToClose,
}

impl std::fmt::Display for TimeoutReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Heartbeat => write!(f, "Heartbeat"),
            Self::StartToClose => write!(f, "StartToClose"),
            Self::ScheduleToStart => write!(f, "ScheduleToStart"),
            Self::ScheduleToClose => write!(f, "ScheduleToClose"),
        }
    }
}

impl TimeoutReason {
    const fn timeout_type(&self) -> TimeoutType {
        match self {
            Self::Heartbeat => TimeoutType::Heartbeat,
            Self::StartToClose => TimeoutType::StartToClose,
            Self::ScheduleToStart => TimeoutType::ScheduleToStart,
            Self::ScheduleToClose => TimeoutType::ScheduleToClose,
        }
    }
}

// ---------------------------------------------------------------------------
// SQL query builders
// ---------------------------------------------------------------------------

/// SQL query to find RUNNING tasks with expired heartbeat timeout.
///
/// A task is considered heartbeat-timed-out when:
/// - `state = 'RUNNING'`
/// - `heartbeat_timeout IS NOT NULL`
/// - `last_heartbeat_at + heartbeat_timeout < NOW()` (or `started_at` if no heartbeat yet)
#[must_use]
pub const fn heartbeat_timeout_query() -> &'static str {
    "SELECT * FROM harvest_task_queue \
     WHERE state = 'RUNNING' \
     AND heartbeat_timeout IS NOT NULL \
     AND COALESCE(last_heartbeat_at, started_at) + heartbeat_timeout < NOW()"
}

/// SQL query to find RUNNING tasks that exceeded their start-to-close timeout.
///
/// A task is considered start-to-close-timed-out when:
/// - `state = 'RUNNING'`
/// - `start_to_close IS NOT NULL`
/// - `started_at + start_to_close < NOW()`
#[must_use]
pub const fn start_to_close_timeout_query() -> &'static str {
    "SELECT * FROM harvest_task_queue \
     WHERE state = 'RUNNING' \
     AND start_to_close IS NOT NULL \
     AND started_at + start_to_close < NOW()"
}

/// SQL query to find PENDING tasks that exceeded their schedule-to-start timeout.
///
/// A task is considered schedule-to-start-timed-out when:
/// - `state = 'PENDING'`
/// - `schedule_to_start IS NOT NULL`
/// - `scheduled_at + schedule_to_start < NOW()`
/// - the task is not **frozen** by a pause (see below)
///
/// Frozen-row carve-out (issue #609 post-review hardening): the pause ×
/// `schedule_to_close` interaction created a new state this reason must not
/// destroy — a PENDING row of a `PAUSED` execution whose (not-yet-shifted)
/// `schedule_to_close_at` has elapsed. Such a row is unclaimable by
/// construction (the claim query requires `schedule_to_close_at > NOW()`)
/// and is deliberately spared by the pause-aware `ScheduleToClose` scanner,
/// so it sits frozen until resume shifts its deadline forward. Its
/// schedule-to-start clock is therefore *not* a genuine worker-capacity
/// signal while frozen, and enforcing it would terminally kill a task the
/// pause machinery explicitly preserved. The exclusion is scoped to exactly
/// those frozen rows — a PENDING activity of a paused execution whose
/// `schedule_to_close_at` is NULL or still ahead remains claimable by design
/// (activities are not pause-gated), so its schedule-to-start signal stays
/// genuine and stays enforced. On resume, `scheduled_at` is shifted forward
/// by the pause span for exactly the frozen rows
/// ([`crate::execution::shift_schedule_to_close_on_resume_query`]) so the
/// row does not get instantly killed post-resume with a schedule-to-start
/// budget the pause consumed.
///
/// Queue-pause carve-out (issue #619): a task held by a **queue** pause is
/// likewise not a genuine worker-capacity signal — it is unclaimable because an
/// operator deliberately froze its queue, not because the fleet is short of
/// capacity. Enforcing schedule-to-start against it would fail exactly the work
/// the hold exists to protect (AC3/AC4). Unlike the frozen-row carve-out above,
/// this exclusion is unconditional for the queue: every PENDING row on a paused
/// queue is unclaimable by construction, whatever its `schedule_to_close_at`.
/// The absolute `schedule_to_close` deadline keeps ticking during a queue pause
/// (an explicit out-of-scope decision in #619), so that scanner is deliberately
/// *not* given a matching carve-out.
///
/// Activity-pause carve-out (issue #807): identical reasoning one level down. A
/// task held because an operator paused its *activity type* is unclaimable by
/// construction, so its schedule-to-start clock is not a genuine
/// worker-capacity signal, and enforcing it would terminally fail exactly the
/// work the hold exists to preserve — violating AC3 ("remain `PENDING` (not
/// failed, not DLQ'd)"). On resume,
/// [`crate::activity_pause::resume_shift_scheduled_at_query`] credits the held
/// time back so the thaw does not immediately kill the released backlog
/// instead.
///
/// The anti-join carries its own `task_type = 'activity'` scope, so a workflow
/// task's schedule-to-start stays enforced even though such a row can carry a
/// non-NULL `activity_name` (the `'mixed_signal_suspension'` sentinel). See
/// [`crate::activity_pause::activity_pause_anti_join`] for why that scope is
/// structural rather than a NULL-semantics coincidence.
#[must_use]
pub const fn schedule_to_start_timeout_query() -> &'static str {
    "SELECT t.* FROM harvest_task_queue t \
     WHERE t.state = 'PENDING' \
     AND t.schedule_to_start IS NOT NULL \
     AND t.scheduled_at + t.schedule_to_start < NOW() \
     AND NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp \
         WHERE qp.queue_name = t.queue_name) \
     AND NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap \
         WHERE ap.activity_name = t.activity_name \
           AND t.task_type = 'activity') \
     AND NOT (\
         t.schedule_to_close_at IS NOT NULL \
         AND t.schedule_to_close_at <= NOW() \
         AND EXISTS (\
             SELECT 1 FROM harvest_workflow_executions e \
             WHERE e.id = t.workflow_exec_id \
             AND e.state = 'PAUSED'))"
}

/// SQL query to find RUNNING or PENDING tasks that exceeded their total
/// schedule-to-close wall-clock deadline (issue #378).
///
/// A task is considered schedule-to-close-timed-out when:
/// - `state IN ('RUNNING', 'PENDING')`
/// - `schedule_to_close_at IS NOT NULL`
/// - `schedule_to_close_at < NOW()`
/// - the owning execution is not `PAUSED`
///
/// Fires for in-flight executions (RUNNING) and tasks queued past their
/// deadline (PENDING). The partial index on `schedule_to_close_at` makes
/// this scan cheap.
///
/// Pause suspends the cross-retry deadline clock (issue #609, AC5): a task
/// whose owning execution is `PAUSED` is excluded here — resume shifts its
/// `schedule_to_close_at` forward by the pause span
/// ([`crate::execution::shift_schedule_to_close_on_resume_query`]). Heartbeat
/// and start-to-close enforcement stay pause-blind because already-dispatched
/// work runs to completion under pause (issue #383), so a hung in-flight
/// activity of a paused execution must still time out. Schedule-to-start
/// enforcement stays pause-blind except for the narrow frozen-row carve-out
/// documented on [`schedule_to_start_timeout_query`].
#[must_use]
pub const fn schedule_to_close_timeout_query() -> &'static str {
    "SELECT t.* FROM harvest_task_queue t \
     WHERE t.state IN ('RUNNING', 'PENDING') \
     AND t.schedule_to_close_at IS NOT NULL \
     AND t.schedule_to_close_at < NOW() \
     AND NOT EXISTS (\
         SELECT 1 FROM harvest_workflow_executions e \
         WHERE e.id = t.workflow_exec_id \
         AND e.state = 'PAUSED')"
}

/// SQL query to find RUNNING workflow executions that have exceeded either their
/// per-run `execution_timeout` deadline (issue #243) OR their chain-scoped
/// lifetime cap deadline (issue #617).
///
/// A workflow execution is considered timed out when `state = 'RUNNING'` and
/// EITHER deadline has fired:
/// - `deadline_at IS NOT NULL AND deadline_at < NOW()` (per-run), OR
/// - `chain_deadline_at IS NOT NULL AND chain_deadline_at < NOW()` (chain).
///
/// Both deadlines are computed at start time (`started_at + timeout`), so this is
/// an indexed range scan served by `idx_harvest_executions_deadline` and
/// `idx_harvest_executions_chain_deadline`.
///
/// This `const` renders `NOW()` illustratively for the SQL-shape unit test; the
/// executed scanner (`enforce_workflow_execution_timeouts`) uses the equivalent
/// Diesel DSL with a Rust-captured `now` (see below), not this string.
#[must_use]
pub const fn workflow_execution_timeout_query() -> &'static str {
    "SELECT * FROM harvest_workflow_executions \
     WHERE state = 'RUNNING' \
     AND ((deadline_at IS NOT NULL AND deadline_at < NOW()) \
       OR (chain_deadline_at IS NOT NULL AND chain_deadline_at < NOW()))"
}

/// Which timeout deadline fired for a scanned, expired workflow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutKind {
    /// The per-run `execution_timeout` deadline (`deadline_at`) fired (issue #243).
    Run,
    /// The chain-scoped lifetime cap deadline (`chain_deadline_at`) fired (issue #617).
    Chain,
}

/// Resolve the effective chain-scoped lifetime cap from the workflow-declared
/// value and the fleet-wide builder ceiling (issue #617, AC4).
///
/// The effective cap is the MINIMUM of any present values, and `None` only when
/// BOTH are absent. This DIVERGES deliberately from #243's per-run ceiling: there
/// the ceiling only caps a *specified* value (`(None, Some) => None`), while here
/// the ceiling ALSO acts as a fleet-wide DEFAULT (`(None, Some) => Some(ceiling)`)
/// so an operator can cap every chain even when a workflow under-specifies.
#[must_use]
pub fn effective_chain_timeout(
    workflow: Option<chrono::Duration>,
    ceiling: Option<chrono::Duration>,
) -> Option<chrono::Duration> {
    match (workflow, ceiling) {
        (Some(w), Some(c)) => Some(w.min(c)),
        (Some(w), None) => Some(w),
        // The #243 divergence: ceiling doubles as a fleet-wide default here.
        (None, Some(c)) => Some(c),
        (None, None) => None,
    }
}

/// Given the two candidate deadlines on an expired scanned row and the scan
/// instant, decide which deadline fired and return it (issue #617).
///
/// The chain deadline takes PRECEDENCE: if both fired, the run has exceeded its
/// whole-chain lifetime and is terminated as a chain timeout. A chain-only expiry
/// (no per-run deadline configured) is handled without panicking — the scanner
/// selects rows on either disjunct, so `deadline_at` may be `None` here.
#[must_use]
pub fn classify_workflow_timeout(
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    chain_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> (chrono::DateTime<chrono::Utc>, TimeoutKind) {
    let chain_fired = chain_deadline_at.is_some_and(|d| d < now);
    if chain_fired {
        (
            chain_deadline_at.expect("chain_fired implies chain_deadline_at is Some"),
            TimeoutKind::Chain,
        )
    } else {
        (
            deadline_at
                .expect("selected row without a fired chain deadline implies deadline_at < NOW()"),
            TimeoutKind::Run,
        )
    }
}

/// Find all tasks that have exceeded their timeout limits.
///
/// Runs all three timeout queries and returns the matched tasks along with
/// their timeout reason.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn find_timed_out_tasks(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<(TaskQueueItem, TimeoutReason)>> {
    let mut results = Vec::new();
    let mut seen = HashSet::new();

    // Heartbeat timeouts
    let heartbeat_tasks: Vec<TaskQueueItem> = diesel::sql_query(heartbeat_timeout_query())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    for task in heartbeat_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::Heartbeat));
        }
    }

    // Start-to-close timeouts
    let start_close_tasks: Vec<TaskQueueItem> = diesel::sql_query(start_to_close_timeout_query())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    for task in start_close_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::StartToClose));
        }
    }

    // Schedule-to-start timeouts
    let sched_start_tasks: Vec<TaskQueueItem> =
        diesel::sql_query(schedule_to_start_timeout_query())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
    for task in sched_start_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::ScheduleToStart));
        }
    }

    // Schedule-to-close timeouts (cross-retry wall-clock deadline, issue #378)
    let sched_close_tasks: Vec<TaskQueueItem> =
        diesel::sql_query(schedule_to_close_timeout_query())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
    for task in sched_close_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::ScheduleToClose));
        }
    }

    Ok(results)
}

fn execution_id_from_uuid(id: uuid::Uuid) -> crate::types::ExecutionId {
    id.to_string()
        .parse()
        .expect("database UUIDs must round-trip into ExecutionId")
}

fn timeout_error(task_name: &str, reason: &TimeoutReason) -> String {
    HarvestError::Timeout {
        timeout_type: reason.timeout_type(),
        task_name: task_name.to_string(),
    }
    .to_string()
}

fn find_pending_scheduled_activity(
    history: &[WorkflowEvent],
    activity_name: &str,
) -> HarvestResult<crate::types::ActivityExecId> {
    let terminal_ids = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect::<HashSet<_>>();

    let mut pending = None;
    for event in history {
        if let WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } = event
            && name == activity_name
            && !terminal_ids.contains(activity_id)
        {
            if pending.is_some() {
                return Err(HarvestError::non_deterministic_simple(format!(
                    "multiple pending scheduled activities named '{activity_name}' found in history"
                )));
            }
            pending = Some(*activity_id);
        }
    }

    pending.ok_or_else(|| {
        HarvestError::NotFound(format!(
            "no pending scheduled activity '{activity_name}' in workflow history"
        ))
    })
}

fn find_pending_scheduled_activity_by_id(
    history: &[WorkflowEvent],
    requested_activity_id: crate::types::ActivityExecId,
    activity_name: &str,
) -> HarvestResult<crate::types::ActivityExecId> {
    let mut scheduled = false;
    let mut terminal = false;

    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if *activity_id == requested_activity_id => {
                if name != activity_name {
                    return Err(HarvestError::non_deterministic_simple(format!(
                        "activity task id '{}' was scheduled for '{name}', not '{activity_name}'",
                        requested_activity_id.as_uuid()
                    )));
                }
                scheduled = true;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
                if *activity_id == requested_activity_id =>
            {
                terminal = true;
            }
            _ => {}
        }
    }

    if scheduled && !terminal {
        Ok(requested_activity_id)
    } else if terminal {
        Err(HarvestError::NotFound(format!(
            "activity '{activity_name}' with id '{}' already has a terminal event",
            requested_activity_id.as_uuid()
        )))
    } else {
        Err(HarvestError::NotFound(format!(
            "no scheduled activity '{activity_name}' with id '{}' in workflow history",
            requested_activity_id.as_uuid()
        )))
    }
}

/// `true` when any `ActivityScheduled` event named `activity_name` already
/// has a terminal event (`ActivityCompleted`/`ActivityFailed`/
/// `ActivityTimedOut`) recorded in `history`.
///
/// Used by [`force_fail_activity`]'s legacy-row branch (`activity_id = NULL`
/// on the task row): the name-based fallback in
/// [`pending_activity_id_for_task`] reports "no pending activity" as
/// `NotFound` without distinguishing "never scheduled" from "already
/// terminal", and only the latter is the documented `409` conflict.
fn named_activity_has_terminal_event(history: &[WorkflowEvent], activity_name: &str) -> bool {
    history.iter().any(|event| {
        matches!(
            event,
            WorkflowEvent::ActivityScheduled { activity_id, name, .. }
                if name == activity_name && has_activity_terminal_event(history, *activity_id)
        )
    })
}

fn has_activity_terminal_event(
    history: &[WorkflowEvent],
    activity_id: crate::types::ActivityExecId,
) -> bool {
    history.iter().any(|event| {
        matches!(
            event,
            WorkflowEvent::ActivityCompleted { activity_id: id, .. }
                | WorkflowEvent::ActivityFailed { activity_id: id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id: id, .. }
                if *id == activity_id
        )
    })
}

/// Resolve the pending (non-terminal) `ActivityScheduled` id a task row
/// corresponds to, if any.
///
/// `pub(crate)` so [`crate::sessions::enforce_broken_sessions`] (issue #606)
/// can reuse the exact same resolution logic when failing a hard-pinned
/// session-member task, rather than a fourth hand-rolled copy (this and
/// `worker.rs`'s already-duplicated sibling are the established precedent
/// for this small helper trio in this codebase).
pub(crate) fn pending_activity_id_for_task(
    history: &[WorkflowEvent],
    task: &TaskQueueItem,
    activity_name: &str,
) -> HarvestResult<Option<crate::types::ActivityExecId>> {
    if let Some(activity_id) = task.activity_id {
        let activity_id = crate::types::ActivityExecId::from_uuid(activity_id);
        if has_activity_terminal_event(history, activity_id) {
            return Ok(None);
        }
        return find_pending_scheduled_activity_by_id(history, activity_id, activity_name)
            .map(Some);
    }

    find_pending_scheduled_activity(history, activity_name).map(Some)
}

pub(crate) async fn lock_workflow_execution_and_load_history(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<store::EventHistory> {
    Ok(
        lock_workflow_execution_row_and_load_history(conn, exec_id, codecs)
            .await?
            .1,
    )
}

/// Like [`lock_workflow_execution_and_load_history`], but also returns the
/// locked execution row itself — the `SELECT ... FOR UPDATE` loads the full
/// row anyway, so a caller that needs row-current execution metadata under
/// the lock (e.g. [`enforce_activity_timeout`]'s PAUSED re-check, issue #609
/// post-review hardening, second bot-review round) gets it without a second
/// query. Mirrors `worker.rs`'s sibling of the same name.
async fn lock_workflow_execution_row_and_load_history(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<(WorkflowExecution, store::EventHistory)> {
    let execution = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    // Codec-aware, NOT `store::load_history` -- the same reason as `worker.rs`'s
    // sibling: the engine's writes encode through the configured registry, so an
    // identity read raises `UnknownCodecKey` on the first keyed envelope.
    let history = store::load_history_with_codecs(conn, exec_id, codecs).await?;
    Ok((execution, history))
}

pub(crate) async fn task_state_for_update(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<Option<String>> {
    use crate::schema::harvest_task_queue::dsl;

    dsl::harvest_task_queue
        .find(task_id)
        .for_update()
        .select(dsl::state)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

/// Locked (`FOR UPDATE`) read of a task row's current state **and**
/// row-current `schedule_to_close_at`. [`enforce_activity_timeout`] needs the
/// deadline in addition to the state so the frozen-row half of the PAUSED
/// re-check ([`pause_suppresses_timeout_enforcement`]) can be evaluated
/// against the row's current value under the execution row lock, not the
/// scan-time snapshot.
async fn task_state_and_deadline_for_update(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<Option<(String, Option<chrono::DateTime<Utc>>)>> {
    use crate::schema::harvest_task_queue::dsl;

    dsl::harvest_task_queue
        .find(task_id)
        .for_update()
        .select((dsl::state, dsl::schedule_to_close_at))
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

/// SQL for [`schedule_to_start_still_expired`], exposed for shape tests.
///
/// `FOR UPDATE` so the read serializes against `resume_queue`'s `scheduled_at`
/// shift, and `clock_timestamp()` rather than `NOW()` because `NOW()` is frozen
/// at transaction start — i.e. before this transaction waited on the queue
/// advisory lock — which would judge the deadline against a stale instant.
///
/// **Lock ordering (issue #619 round-22 review):** this locks a `harvest_task_queue`
/// row, so it may only run *after* the execution row is locked — see
/// [`schedule_to_start_still_expired_unlocked_query`] for the fast path that
/// runs before it and why the split exists.
#[cfg(feature = "db")]
#[must_use]
const fn schedule_to_start_still_expired_query() -> &'static str {
    "SELECT COALESCE(scheduled_at + schedule_to_start < clock_timestamp(), false) AS expired \
     FROM harvest_task_queue \
     WHERE id = $1 AND schedule_to_start IS NOT NULL \
     FOR UPDATE"
}

/// SQL for [`schedule_to_start_still_expired_unlocked`], exposed for shape tests.
///
/// Byte-identical to [`schedule_to_start_still_expired_query`] minus the
/// `FOR UPDATE`, so the two can never disagree about the deadline predicate.
///
/// # Why the split exists (issue #619 round-22 review)
///
/// The documented `harvest_task_queue` lock order is **execution row → task
/// row**, and `resume_workflow_execution` follows it: it holds the execution
/// row `FOR UPDATE` and then shifts this execution's task rows with a plain,
/// *waiting* `UPDATE` (unlike its external-task sibling, that shift has no
/// `SKIP LOCKED` escape). Round 18 placed the locked re-read *before* the
/// execution lock, inverting the order for `ScheduleToStart` — enforcement
/// holding a task row and waiting on the execution row while a resume holds the
/// execution row and waits on that task row. Postgres would abort one of them,
/// failing either the timeout sweep or the operator's resume.
///
/// So the authoritative locked re-read moved *after* the execution lock, and
/// this unlocked variant took its place as a **fast path**: it bails out of the
/// common held-task case before the execution-row lock and history load,
/// without taking any lock of its own. It is advisory only — a concurrent
/// resume can still commit between it and the authoritative check — which is
/// exactly the fast-path/authoritative split `worker::process_workflow_task`
/// already uses for its PAUSED re-check.
#[cfg(feature = "db")]
#[must_use]
const fn schedule_to_start_still_expired_unlocked_query() -> &'static str {
    "SELECT COALESCE(scheduled_at + schedule_to_start < clock_timestamp(), false) AS expired \
     FROM harvest_task_queue \
     WHERE id = $1 AND schedule_to_start IS NOT NULL"
}

/// Locked re-read of a task's *row-current* schedule-to-start deadline (issue
/// #619 round-18 review).
///
/// [`find_timed_out_tasks`] produces a snapshot, and a whole pause/resume cycle
/// can complete before a given row in a large batch is reached. Resume credits
/// the held time back by shifting `scheduled_at` forward and then **deletes**
/// the pause row, so by the time enforcement runs the pause re-check finds
/// nothing to suppress on — and, without this, enforcement proceeds on the
/// scan-time deadline and times the task out immediately after its deadline was
/// credited. On the workflow path that seals the entire execution `TIMED_OUT`,
/// which is exactly what AC3/AC4 forbid.
///
/// Returns `false` — meaning *do not enforce* — when the row is gone, when it
/// carries no `schedule_to_start` at all, or when the row-current deadline is no
/// longer in the past. Safe in the conservative direction: a task that is
/// genuinely still expired is caught on this or any later scan.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
async fn schedule_to_start_still_expired(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct ExpiredRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        expired: bool,
    }

    let row: Option<ExpiredRow> = diesel::sql_query(schedule_to_start_still_expired_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(row.is_some_and(|r| r.expired))
}

/// Non-locking fast-path twin of [`schedule_to_start_still_expired`].
///
/// Takes **no** row lock, so it is safe to run before the execution row is
/// locked. Advisory only: a resume can commit between this and the
/// authoritative locked check, so a `true` here must always be re-confirmed
/// under the execution lock. A `false` is safe to act on immediately — the
/// deadline can only move *forward* (a resume credits held time), so a task
/// this reports as un-expired cannot become expired by a concurrent resume.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
async fn schedule_to_start_still_expired_unlocked(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct ExpiredRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        expired: bool,
    }

    let row: Option<ExpiredRow> =
        diesel::sql_query(schedule_to_start_still_expired_unlocked_query())
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;
    Ok(row.is_some_and(|r| r.expired))
}

/// Returns the set of task states that are valid for a given timeout reason.
///
/// `enforce_activity_timeout` skips rows whose current state is not in this
/// set, guarding against double-enforcement races (e.g. two scanner ticks
/// racing on the same row, or a worker completing a task between the scan
/// and enforcement).
const fn expected_task_states_for_timeout(reason: &TimeoutReason) -> &'static [&'static str] {
    match reason {
        TimeoutReason::Heartbeat | TimeoutReason::StartToClose => &["RUNNING"],
        TimeoutReason::ScheduleToStart => &["PENDING"],
        // ScheduleToClose fires for both: mid-execution (RUNNING) and queued
        // past the deadline before any worker claimed it (PENDING).
        TimeoutReason::ScheduleToClose => &["RUNNING", "PENDING"],
    }
}

/// Pure decision rule for the PAUSED re-check the timeout enforcers perform
/// under the execution row lock (issue #609 post-review hardening, second
/// bot-review round). `true` means "skip enforcement for this task without
/// mutating anything" — the row stays `PENDING`/`RUNNING`, no
/// `ActivityTimedOut` is appended, and the pause machinery (claim-gate
/// freeze + resume-time deadline shift) covers it instead.
///
/// The scan-time PAUSED exclusions ([`schedule_to_close_timeout_query`]'s
/// blanket `NOT EXISTS`, [`schedule_to_start_timeout_query`]'s frozen-row
/// carve-out, and [`enforce_external_task_timeouts`]'s Diesel filter) protect
/// only a non-locking snapshot: a pause committing after the scan — or while
/// enforcement waits on the execution row lock — was previously enforced
/// anyway, appending a timeout event mid-pause. This re-check, evaluated
/// against the state observed *under* the execution row lock (the same lock
/// `pause_workflow_execution` holds, so the two serialize), is the guarantee.
///
/// Per-reason scoping mirrors the scan queries exactly:
/// - `ScheduleToClose`: pause suspends the cross-retry deadline clock
///   outright (issue #609, AC5) — always skip while paused.
/// - `ScheduleToStart`: stays pause-blind **except** for a row that is now
///   frozen (`schedule_to_close_at` set and elapsed): a row unfrozen at scan
///   time can become frozen before enforcement locks the row (pause commits
///   plus deadline elapses in the gap), and terminally
///   schedule-to-start-failing it mid-pause would destroy exactly the state
///   the frozen-row carve-out preserves. An *unfrozen* pending row of a
///   paused execution remains claimable by design (activities are not
///   pause-gated), so its schedule-to-start signal (worker capacity) stays
///   genuine and stays enforced.
/// - `Heartbeat`/`StartToClose`: deliberately pause-blind — already-
///   dispatched work runs to completion under pause (issue #383), so a hung
///   in-flight activity of a paused execution still times out on its own
///   merits.
fn pause_suppresses_timeout_enforcement(
    reason: &TimeoutReason,
    execution_state: &str,
    row_schedule_to_close_at: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> bool {
    if execution_state != "PAUSED" {
        return false;
    }
    match reason {
        TimeoutReason::ScheduleToClose => true,
        TimeoutReason::ScheduleToStart => {
            row_schedule_to_close_at.is_some_and(|deadline| deadline <= now)
        }
        TimeoutReason::Heartbeat | TimeoutReason::StartToClose => false,
    }
}

/// Pure verdict for the locked re-read of an external-task row inside
/// [`enforce_external_task_timeouts`]'s per-task transaction (issue #609
/// post-review hardening, third bot-review round). `true` means "the row is
/// still an enforceable schedule-to-close timeout": it is still open
/// (`PENDING`) and its deadline is still elapsed. `false` means a concurrent
/// writer won the race after the scan snapshot — a completion/failure flipped
/// the state, or `extend_deadline`/a resume's pause-span shift pushed
/// `schedule_to_close_at` back into the future — and the scanner must skip
/// the row entirely: no state flip, no `ActivityTimedOut` event, not counted.
///
/// The locked re-read this feeds replaces trusting the scan snapshot (which
/// the pre-fix code re-verified via filters on the claiming `UPDATE`
/// instead); it exists so the row lock can be taken *before* the execution
/// row lock — see the lock-ordering convention comment inside
/// [`enforce_external_task_timeouts`].
fn external_task_timeout_still_due(
    state: &str,
    schedule_to_close_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> bool {
    state == "PENDING" && schedule_to_close_at < now
}

async fn update_workflow_execution_timed_out(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    // Code-review fix (issue #603): see
    // `worker::update_workflow_execution_completed` for the rationale --
    // read the pre-update block state so the search_attrs clear below can be
    // gated on it instead of running unconditionally on every timeout.
    let was_nd_blocked = dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::nd_blocked_at.is_not_null())
        .first::<bool>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .unwrap_or(false);

    let updated = diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set((
            dsl::state.eq("TIMED_OUT"),
            dsl::output.eq(None::<serde_json::Value>),
            dsl::error.eq(Some(error.to_string())),
            dsl::completed_at.eq(Some(Utc::now())),
            // Belt-and-braces ND-block reset (code-review fix, issue #603):
            // a TIMED_OUT execution closes out permanently — a stale block
            // marker must not survive on a terminal row, matching the
            // precedent already applied to the two worker.rs terminal
            // writers.
            dsl::nd_blocked_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::nd_block_reason.eq(None::<String>),
            dsl::nd_block_count.eq(0),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(HarvestError::NotFound(format!(
            "workflow execution {exec_id}"
        )));
    }

    // Gated on `was_nd_blocked` (PR review fix): an unconditional clear here
    // would silently delete pre-existing user search_attrs of the same name
    // on rows created before these keys became reserved.
    if was_nd_blocked {
        crate::store::update_search_attrs(
            conn,
            exec_id,
            &crate::worker::nd_search_attrs_clear_patch(),
        )
        .await?;
    }

    Ok(())
}

async fn wake_parent_for_child_timeout(
    conn: &mut AsyncPgConnection,
    parent_exec_id: crate::types::ExecutionId,
    child_exec_id: crate::types::ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    // #779 (Codex P2): order any DUE child-timeout deadline BEFORE the child
    // terminal (mirrors worker::wake_parent_for_child_completion/_failure) so an
    // over-deadline child that hits its OWN execution timeout resolves the
    // parent's `spawn_child_workflow_timeout` to the timeout branch (None), not
    // Err. Appends the `TimerFired` under the same parent-row FOR UPDATE + MAX
    // discipline as the child terminal below (see
    // worker::materialize_due_child_timeout_deadlines).
    crate::worker::materialize_due_child_timeout_deadlines(conn, parent_exec_id).await?;
    // Use append_single_event so concurrent sibling timeout/completion paths
    // serialise around the parent execution row and cannot collide on the
    // (workflow_exec_id, event_id) unique constraint.
    let event = WorkflowEvent::child_workflow_failed(child_exec_id, error.to_string());
    store::append_single_event(conn, parent_exec_id, event).await?;
    queue::wake_workflow_task(conn, parent_exec_id).await
}

async fn commit_workflow_execution_timeout(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    parent_uuid: Option<uuid::Uuid>,
    timeout_event: &WorkflowEvent,
    error_msg: &str,
    metrics: Option<&(dyn MetricsRecorder + Send + Sync)>,
) -> HarvestResult<(
    bool,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(ExecutionId, String)>,
)> {
    Box::pin(conn.transaction::<(
        bool,
        Vec<crate::completion_trigger::DeferredTriggerStart>,
        Vec<(ExecutionId, String)>,
    ), HarvestError, _>(async |conn| {
        let timeout_event = timeout_event.clone();
        let error_msg = error_msg.to_owned();
        // Re-check state under lock to guard against concurrent completion.
        let current_state: Option<String> = harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .for_update()
            .select(harvest_workflow_executions::state)
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        match current_state.as_deref() {
            Some("RUNNING") => {}
            _ => return Ok((false, Vec::new(), Vec::new())),
        }

        store::append_single_event(conn, exec_id, timeout_event).await?;
        update_workflow_execution_timed_out(conn, exec_id, &error_msg).await?;

        let _rows = diesel::update(
            harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
                .filter(
                    harvest_task_queue::state
                        .eq("PENDING")
                        .or(harvest_task_queue::state.eq("RUNNING")),
                ),
        )
        .set((
            harvest_task_queue::state.eq("FAILED"),
            harvest_task_queue::error.eq(Some(&error_msg)),
            harvest_task_queue::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

        if let Some(parent_uuid) = parent_uuid {
            wake_parent_for_child_timeout(
                conn,
                execution_id_from_uuid(parent_uuid),
                exec_id,
                &error_msg,
            )
            .await?;
        }

        let (mut deferred, closed_children) = apply_parent_close_cascade(conn, exec_id).await?;
        let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
            conn,
            exec_id,
            crate::completion_trigger::TerminalState::TimedOut,
            metrics,
        )
        .await?;
        deferred.extend(triggers);
        Ok((true, deferred, closed_children))
    }))
    .await
}

async fn enforce_activity_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: crate::types::ExecutionId,
    reason: &TimeoutReason,
    circuit_breakers: Option<&crate::circuit_breaker::CircuitBreakerRegistry>,
    metrics: &(dyn MetricsRecorder + Send + Sync),

    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<()> {
    let Some(activity_name) = task.activity_name.as_deref() else {
        return queue::fail_task(conn, task.id, &timeout_error("activity", reason)).await;
    };
    let error = timeout_error(activity_name, reason);

    // Did we actually append a timeout (vs. a no-op because the task already
    // moved on)? Only a real enforcement should count toward the breaker.
    //
    // Pinned `READ COMMITTED`, not the session default (issue #619 round-24
    // review). The queue advisory lock taken below is a `SELECT`, so it fixes
    // this transaction's snapshot and *then* blocks waiting for an in-flight
    // pause. Under `REPEATABLE READ` that snapshot is the transaction's only
    // one, so after the pause commits and releases the lock, `is_queue_paused`
    // still reads pre-pause state, returns false, and this enforcer fails the
    // task *after the hold was acknowledged* — defeating the guarantee the
    // whole queue-pause feature exists to provide. `queue::claim_task` pins the
    // same level for the same reason; inheriting it would let a
    // `default_transaction_isolation = repeatable read` on the database or the
    // role disable the guarantee from outside this code.
    let mut tx = conn.build_transaction().read_committed();
    let enforced = Box::pin(tx.run::<bool, HarvestError, _>(async |conn| {
        let error = error.clone();

        // Authoritative QUEUE-pause re-check (issue #619). The scan predicate
        // is a non-locking snapshot, so a queue pause committing after the scan
        // would otherwise let this transaction schedule-to-start-fail the very
        // task the hold was meant to protect.
        //
        // Unlike the execution-pause re-check further down — which is
        // authoritative because both sides lock the *execution row* —
        // `pause_queue` shares no row with this transaction, so a bare re-read
        // would not serialize: a pause could commit in the window between the
        // read and this transaction's own commit. Both sides therefore take the
        // same queue-scoped advisory lock, so a pause is either fully visible
        // here or blocked until this enforcement commits.
        //
        // LOCK ORDERING (load-bearing): this runs FIRST, before the execution
        // and task row locks below, because `resume_queue` takes the very same
        // advisory lock and *then* row-locks every PENDING task on the queue via
        // its `scheduled_at` shift. Taking the rows first here and the advisory
        // lock after would invert that order — enforcement holding a task row
        // and waiting on the advisory lock while resume holds the advisory lock
        // and waits on that task row — and Postgres would abort one of them,
        // failing either the timeout pass or the operator's resume. Both paths
        // now take advisory-then-rows. (Same convention as the
        // `harvest_external_tasks` task-row -> execution-row ordering documented
        // in `enforce_external_task_timeouts`.)
        //
        // Scoped by `queue_pause_suppresses_timeout` to `ScheduleToStart` only
        // — the absolute `schedule_to_close` deadline keeps ticking during a
        // queue pause, and heartbeat/start-to-close apply to RUNNING rows that a
        // pause never touches — so the lock is taken only for that reason and
        // never on the far more common in-flight timeout paths. Bailing here
        // also skips the row locks and history load entirely for a held task.
        //
        // SHARED mode (round-21 review): exclusive would block every claim's
        // `try_lock_queue_for_claim` for this whole transaction, stalling
        // dispatch on a queue that is not paused at all. Shared still mutually
        // excludes the exclusive pause/resume, which is the only ordering this
        // re-check needs. See `lock_queue_for_timeout_recheck`.
        if matches!(reason, TimeoutReason::ScheduleToStart) {
            crate::queue_pause::lock_queue_for_timeout_recheck(conn, &task.queue_name).await?;
            if crate::queue_pause::queue_pause_suppresses_timeout(
                reason,
                crate::queue_pause::is_queue_paused(conn, &task.queue_name).await?,
            ) {
                return Ok(false);
            }
            // Authoritative ACTIVITY-pause re-check (issue #807), the
            // per-activity-type sibling of the queue re-check above. Same
            // staleness problem, same fix: the scan predicate is a non-locking
            // snapshot, so a pause committing after the scan would otherwise let
            // this transaction schedule-to-start-fail a task the hold protects.
            //
            // No advisory lock (unlike the queue path). This copy is an
            // ADVISORY FAST PATH: it lets a held task bail before paying for the
            // execution row lock and history load below. It is NOT the
            // guarantee -- the very next statement,
            // `lock_workflow_execution_row_and_load_history`, can block for an
            // UNBOUNDED period behind any other holder of that row, and
            // `pause_activity` shares no row with this transaction so it commits
            // freely during that wait (round-17 review, P1). The authoritative
            // re-check therefore runs AFTER the row locks; see it below.
            // Placed after the queue re-check so the two read coarse-to-fine,
            // and before `schedule_to_start_still_expired_unlocked` so a held
            // task short-circuits on the cheaper condition.
            //
            // Scoped by `task_type = 'activity'` to match the scan predicate
            // exactly. A *workflow* task can carry a non-NULL `activity_name` —
            // the engine stamps the `'mixed_signal_suspension'` sentinel there —
            // so without this an activity paused under that name would suppress
            // a workflow task's schedule-to-start timeout. That is unreachable
            // today only because workflow tasks are enqueued with
            // `schedule_to_start: None` and so never reach this scan; relying on
            // that would leave the enforcer silently disagreeing with its own
            // scan predicate the moment a workflow task gains one.
            if task.task_type == crate::activity_pause::ACTIVITY_TASK_TYPE
                && let Some(activity_name) = task.activity_name.as_deref()
                && crate::activity_pause::activity_pause_suppresses_timeout(
                    reason,
                    crate::activity_pause::is_activity_paused(conn, activity_name).await?,
                )
            {
                return Ok(false);
            }
            // A *completed* pause/resume cycle leaves nothing for either check
            // above to suppress on, but resume has already credited the held
            // time back into `scheduled_at`. Re-read the row-current deadline
            // before trusting the scan snapshot, or a task is timed out the
            // instant its deadline was extended. Covers both the queue (#619)
            // and activity (#807) resume paths, which shift the same column.
            //
            // Unlocked here (round-22 review): locking the task row before the
            // execution row below would invert the documented
            // execution-row -> task-row order and deadlock against
            // `resume_workflow_execution`. This is a fast path that skips the
            // execution lock and history load for the common held-task case;
            // the authoritative locked re-read runs after the row locks below.
            if !schedule_to_start_still_expired_unlocked(conn, task.id).await? {
                return Ok(false);
            }
        }

        let (execution, history) =
            lock_workflow_execution_row_and_load_history(conn, exec_id, codecs).await?;
        let Some((state, row_schedule_to_close_at)) =
            task_state_and_deadline_for_update(conn, task.id).await?
        else {
            return Ok(false);
        };
        if !expected_task_states_for_timeout(reason).contains(&state.as_str()) {
            return Ok(false);
        }
        // Authoritative `schedule_to_start` deadline re-read (round-18 review),
        // now placed here — after the execution row lock above and while this
        // transaction already holds the task row from
        // `task_state_and_deadline_for_update` — so it preserves the
        // execution-row -> task-row order (round-22 review). The unlocked
        // fast-path check near the top of this transaction is advisory; this is
        // the one that must be trusted, because only a lock held across the
        // resume's own `scheduled_at` shift can serialize against it.
        if matches!(reason, TimeoutReason::ScheduleToStart)
            && !schedule_to_start_still_expired(conn, task.id).await?
        {
            return Ok(false);
        }
        // Authoritative ACTIVITY-pause re-check, AFTER the blocking row
        // acquisitions above (issue #807, round-17 review, P1).
        //
        // The copy near the top of this transaction is a fast path only. Between
        // it and here sits `lock_workflow_execution_row_and_load_history`, a
        // `FOR UPDATE` that can block for an UNBOUNDED period behind any other
        // transaction holding the execution row. `pause_activity` touches
        // neither that row nor this task's, so an operator pause commits
        // immediately during that wait and returns success -- and without this
        // re-check the enforcer went on to append `ActivityTimedOut` and
        // terminally fail the very task the acknowledged hold was placed to
        // protect, seconds after the operator was told the brake had taken.
        //
        // Why this is not the queue path's problem: `lock_queue_for_timeout_recheck`
        // takes a shared advisory lock at the TOP of this transaction and holds
        // it through COMMIT, so `pause_queue` cannot interleave at all. The
        // activity path deliberately takes no advisory lock -- a new keyspace
        // would impose a fleet-wide queue-before-activity ordering rule and an
        // ABBA hazard on two paths that today take none -- so the equivalent
        // guarantee is bought by re-reading after the last blocking acquisition
        // instead. That is what makes the documented residual genuinely
        // "bounded by this transaction's own remaining work": every statement
        // from here to COMMIT touches only rows this transaction already holds.
        //
        // Cheap by construction: one indexed `EXISTS` on `harvest_activity_pauses`,
        // and only on the `ScheduleToStart` path (`activity_pause_suppresses_timeout`
        // is false for every other reason), which is the rarest of the four.
        if task.task_type == crate::activity_pause::ACTIVITY_TASK_TYPE
            && let Some(activity_name) = task.activity_name.as_deref()
            && crate::activity_pause::activity_pause_suppresses_timeout(
                reason,
                crate::activity_pause::is_activity_paused(conn, activity_name).await?,
            )
        {
            return Ok(false);
        }
        // Authoritative PAUSED re-check under the execution row lock
        // (issue #609 post-review hardening, second bot-review round):
        // the scan snapshot's PAUSED exclusions are non-locking, so a
        // pause committing after the scan — or while this transaction
        // waited on the lock `pause_workflow_execution` itself holds —
        // must be honoured here or the timeout lands mid-pause. See
        // `pause_suppresses_timeout_enforcement` for the per-reason
        // scoping (schedule_to_close always; schedule_to_start only
        // for a now-frozen row; heartbeat/start-to-close pause-blind).
        if pause_suppresses_timeout_enforcement(
            reason,
            &execution.state,
            row_schedule_to_close_at,
            Utc::now(),
        ) {
            return Ok(false);
        }
        // (The queue-pause re-check runs at the TOP of this transaction, before
        // the row locks above — see the lock-ordering note there.)
        let activity_id = match pending_activity_id_for_task(&history.events, task, activity_name) {
            Ok(Some(activity_id)) => activity_id,
            Ok(None) => return Ok(false),
            Err(missing_error) => {
                let fallback = missing_error.to_string();
                queue::fail_task(conn, task.id, &fallback).await?;
                return Ok(false);
            }
        };
        let timeout_event = WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type: reason.timeout_type(),
        };
        store::append_events_with_codecs(
            conn,
            exec_id,
            &[timeout_event],
            history.next_event_id,
            codecs,
        )
        .await?;
        queue::fail_task(conn, task.id, &error).await?;
        queue::wake_workflow_task(conn, exec_id).await?;
        Ok(true)
    }))
    .await?;

    // Circuit breaker (issue #369): a start-to-close / heartbeat timeout against
    // a protected downstream is a retryable, downstream-style failure that the
    // handler-result path never sees (the worker may be gone). Record it
    // out-of-band so a hanging downstream trips the breaker just like an
    // explicit error would.
    //
    // ScheduleToClose on a PENDING task means the activity sat in the queue
    // until its total deadline elapsed without any handler being dispatched.
    // No downstream call was made, so the breaker must NOT be fed — a backlog
    // spike would otherwise incorrectly open the circuit.
    // ScheduleToStart: task sat in the queue until the schedule-to-start window
    // expired without ever being dispatched — no downstream call was made.
    // ScheduleToClose on a PENDING task: total deadline expired before dispatch.
    // Neither case represents a downstream failure; suppress the breaker so a
    // queue backlog does not incorrectly open the circuit.
    let downstream_call_made = matches!(reason, TimeoutReason::ScheduleToStart)
        || (matches!(reason, TimeoutReason::ScheduleToClose) && task.state == "PENDING");
    if enforced
        && !downstream_call_made
        && let Some(breakers) = circuit_breakers
        && breakers.on_external_failure(activity_name, std::time::Instant::now())
            == Some(crate::circuit_breaker::CircuitTransition::Tripped)
    {
        metrics.record_circuit_tripped(activity_name);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Operator force-fail for a single hung in-flight activity (issue #765)
// ---------------------------------------------------------------------------

/// Outcome of a [`force_fail_activity`] call (issue #765).
#[derive(Debug, Clone)]
pub struct ForceFailActivityOutcome {
    /// The task-queue row PK that was (or already had been) force-failed.
    pub task_id: uuid::Uuid,
    /// The queue this task belongs to.
    pub queue_name: String,
    /// The registered activity handler name.
    pub activity_name: String,
    /// `true` when this call performed the force-fail.
    pub forced: bool,
    /// `true` when a prior force-fail was detected — idempotent no-op, zero
    /// writes were performed by this call.
    pub already_forced: bool,
}

/// Pure classification of a task row for [`force_fail_activity`] (issue
/// #765). Decides, from the row's `task_type`, `state`, and stored `error`,
/// which of the endpoint's outcomes applies:
///
/// - `NotAnActivityTask` → `409` (only activity tasks are force-failable —
///   a workflow task existing at the id is a conflict, not a 404).
/// - `AlreadyForced` → idempotent no-op success (`FAILED` row whose stored
///   error is the typed `harvest_activity_failure_v1` envelope with
///   `error_type == OperatorForceFailed`).
/// - `NotRunning` → `409` (PENDING backing-off, COMPLETED, CANCELLED, or
///   FAILED with a *genuine* error — re-forcing a genuinely failed task is a
///   conflict, never silently reinterpreted as the idempotent case).
/// - `Forceable` → the RUNNING happy path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForceFailClassification {
    /// RUNNING activity task — force-failable.
    Forceable,
    /// FAILED with a prior operator force-fail envelope — idempotent no-op.
    AlreadyForced,
    /// Row exists but is not an activity task (`task_type != 'activity'`).
    NotAnActivityTask,
    /// Any other non-RUNNING state (or FAILED with a genuine error).
    NotRunning,
}

/// The `409` returned when a force-fail target's history already carries a
/// terminal event for the activity. Shared by [`force_fail_activity`]'s
/// id-resolved (`Ok(None)`) and legacy name-resolved branches so the two
/// paths cannot drift apart.
fn terminal_history_conflict(activity_name: &str, task_id: uuid::Uuid) -> HarvestError {
    HarvestError::Config(format!(
        "activity '{activity_name}' (task {task_id}) already has a terminal \
         event recorded in history; refusing to append a second terminal event"
    ))
}

fn classify_force_fail_target(
    task_type: &str,
    state: &str,
    stored_error: Option<&str>,
) -> ForceFailClassification {
    if task_type != "activity" {
        return ForceFailClassification::NotAnActivityTask;
    }
    match state {
        "RUNNING" => ForceFailClassification::Forceable,
        "FAILED" => {
            let already_forced = stored_error
                .and_then(crate::failure::parse_typed_payload)
                .is_some_and(|f| f.error_type == crate::failure::ERROR_TYPE_OPERATOR_FORCE_FAILED);
            if already_forced {
                ForceFailClassification::AlreadyForced
            } else {
                ForceFailClassification::NotRunning
            }
        }
        _ => ForceFailClassification::NotRunning,
    }
}

/// Force-fail exactly one in-flight (RUNNING) activity task (issue #765).
///
/// Appends an `ActivityFailed` event carrying the distinct
/// [`crate::failure::ERROR_TYPE_OPERATOR_FORCE_FAILED`] error type (via the
/// **existing** recording path — no new `WorkflowEvent` variant), marks the
/// task row `FAILED` with the typed wire envelope, and wakes the parked
/// workflow task so the owning workflow advances to its own
/// failure/compensation path (it is *not* terminated) within one worker poll
/// cycle. The forced failure is non-retryable by construction, so every
/// remaining retry attempt is skipped regardless of the activity's retry
/// policy.
///
/// # Semantics
///
/// - Everything runs inside **one transaction**, obeying the documented
///   lock-ordering convention for `harvest_task_queue` (execution row FIRST
///   via `FOR UPDATE`, then the task row — the same order
///   [`enforce_activity_timeout`] and the worker's `finalize_activity_*`
///   paths use, so a concurrent late worker result serializes with this call
///   rather than racing it).
/// - **Late results are ignored**: after this commits, a late `Ok` hits
///   `complete_task`'s `state = 'RUNNING'` filter (no-op error), a late
///   retryable `Err` hits `requeue_for_retry`'s `state = 'RUNNING'` filter
///   (cannot resurrect the FAILED row), and a late non-retryable `Err` no-ops
///   in `finalize_activity_failure`'s terminal-history/row-state guards.
/// - **Idempotent**: re-issuing the call on an already-forced task returns
///   `Ok` with `already_forced: true` and performs zero writes — including
///   after the owning run has since sealed (the woken workflow consuming the
///   forced failure and reaching its own terminal state is the *expected*
///   aftermath of a successful force-fail, so a lost-response retry must not
///   flip to the terminal 409 below).
/// - **Pause is deliberately NOT a blocker**: in-flight enforcement is
///   pause-blind, mirroring the Heartbeat/StartToClose posture in
///   [`pause_suppresses_timeout_enforcement`] — a hung activity of a paused
///   execution can still be force-failed. The wake is recorded durably and
///   safely deferred by the PAUSED claim gate until resume.
/// - **No DLQ row** is inserted, matching `finalize_activity_failure`'s
///   deliberate non-DLQ posture for activity failures (an activity DLQ entry
///   would be un-replayable once a terminal `ActivityFailed` event exists).
/// - **Terminal executions are guarded**: if the owning execution is already
///   in a terminal state per [`crate::erase::is_terminal_state`]
///   (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`CONTINUED_AS_NEW`/
///   `TERMINATED`), this returns a `409` conflict without
///   touching the task row — a sealed run's history must never grow another
///   `ActivityFailed` after its terminal event. A stray `RUNNING` activity
///   row on a terminal execution is reachable (a plain workflow failure does
///   NOT fail open activity rows), so this guard is load-bearing, not
///   theoretical. The idempotent already-forced short-circuit wins over
///   this guard: a retried fail-now whose first call succeeded and whose
///   forced failure has since sealed the run still returns the documented
///   no-op success (`already_forced: true`, zero writes) — only a
///   non-already-forced row on a terminal execution reports the conflict.
/// - If the row is still `RUNNING` but history already carries a terminal
///   event for the activity (a state [`enforce_activity_timeout`] treats as
///   a no-op), this returns a `409` conflict instead of appending — the
///   invariant is that a second terminal event for the same `activity_id` is
///   never appended. This state is believed unreachable via engine paths
///   (every terminal-event appender flips the row inside the same locked
///   transaction), so the `409` is a defensive invariant guard; the stuck
///   `RUNNING` row only reconciles if the in-flight worker attempt
///   eventually returns.
/// - **Workflow-level retry (issue #523) interaction**: if the owning
///   workflow has a workflow-level retry policy and propagates this error to
///   workflow failure, the run is retried fresh and the activity
///   re-dispatched — use cancel/terminate instead if the goal is to stop the
///   run.
///
/// # Errors
///
/// - [`HarvestError::NotFound`] (→ 404) — unknown execution, unknown task
///   id, or a task belonging to a different workflow.
/// - [`HarvestError::Config`] (→ 409 via `conflict_from`) — the owning
///   execution is already terminal, a workflow task rather than an activity
///   task, or any non-RUNNING state that is not the idempotent
///   already-forced case.
/// - [`HarvestError::Database`] — Postgres error.
#[allow(clippy::too_many_lines)]
pub async fn force_fail_activity(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: uuid::Uuid,
    task_id: uuid::Uuid,
    reason: Option<&str>,

    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<ForceFailActivityOutcome> {
    use crate::failure::IntoActivityErrorString;
    use crate::schema::harvest_task_queue::dsl;

    let exec_id = execution_id_from_uuid(workflow_exec_id);
    let reason = reason.map(str::to_owned);

    Box::pin(
        conn.transaction::<ForceFailActivityOutcome, HarvestError, _>(async |conn| {
            // Lock ordering (harvest_task_queue convention, see the comment in
            // `enforce_external_task_timeouts`): execution row FIRST, then the
            // task row.
            let (execution, history) =
                lock_workflow_execution_row_and_load_history(conn, exec_id, codecs).await?;

            let task: Option<TaskQueueItem> = dsl::harvest_task_queue
                .filter(dsl::id.eq(task_id))
                .filter(dsl::workflow_exec_id.eq(Some(workflow_exec_id)))
                .for_update()
                .select(TaskQueueItem::as_select())
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            let Some(task) = task else {
                return Err(HarvestError::NotFound(format!(
                    "activity task {task_id} not found for workflow {workflow_exec_id}"
                )));
            };

            let classification =
                classify_force_fail_target(&task.task_type, &task.state, task.error.as_deref());

            // Idempotent short-circuit — deliberately checked BEFORE the
            // terminal-execution guard below. The common lifecycle of a
            // successful force-fail is that the woken workflow consumes the
            // forced `ActivityFailed` and seals its own run (often FAILED)
            // within one poll cycle, so an operator/script retry after a
            // lost response would otherwise flip from the documented
            // idempotent no-op to the terminal 409. Both paths are
            // zero-writes, so honouring idempotency here never grows a
            // sealed run's history (PR #974 Codex review).
            if classification == ForceFailClassification::AlreadyForced {
                return Ok(ForceFailActivityOutcome {
                    task_id,
                    queue_name: task.queue_name.clone(),
                    activity_name: task.activity_name.clone().unwrap_or_default(),
                    forced: false,
                    already_forced: true,
                });
            }

            // Terminal-execution guard: a sealed run's history must never
            // grow another `ActivityFailed` after its terminal event (a plain
            // workflow failure does NOT fail open activity rows, so a stray
            // RUNNING row on a FAILED execution is reachable). Checked after
            // the task-existence check so an unknown task id still reports
            // `404` first, and after the zero-write idempotent already-forced
            // short-circuit above so a retried fail-now stays idempotent even
            // once the run seals.
            if crate::erase::is_terminal_state(&execution.state) {
                return Err(HarvestError::Config(format!(
                    "workflow execution {workflow_exec_id} is already terminal ({}); \
                 its activities cannot be force-failed",
                    execution.state
                )));
            }

            match classification {
                ForceFailClassification::AlreadyForced => {
                    unreachable!(
                        "AlreadyForced is short-circuited above, before the \
                     terminal-execution guard"
                    );
                }
                ForceFailClassification::NotAnActivityTask => {
                    return Err(HarvestError::Config(format!(
                        "task {task_id} is a '{}' task, not an activity task — only \
                     in-flight activity tasks can be force-failed",
                        task.task_type
                    )));
                }
                ForceFailClassification::NotRunning => {
                    return Err(HarvestError::Config(format!(
                        "activity task {task_id} is in state '{}', not RUNNING — only \
                     in-flight (RUNNING) activity tasks can be force-failed",
                        task.state
                    )));
                }
                ForceFailClassification::Forceable => {}
            }

            let Some(activity_name) = task.activity_name.clone() else {
                return Err(HarvestError::Config(format!(
                    "activity task {task_id} carries no activity_name; its pending \
                 history event cannot be resolved for a force-fail"
                )));
            };

            // History already carrying a terminal event for this activity
            // while the row is still RUNNING is believed unreachable via
            // engine paths (every terminal-event appender flips the row in
            // the same locked transaction), but if it ever occurs — e.g. via
            // manual surgery — appending here could double-terminal the
            // activity. Refuse with a defensive-invariant `409` conflict,
            // mirroring `enforce_activity_timeout`'s no-op treatment of the
            // same edge; the stuck RUNNING row only reconciles if the
            // in-flight worker attempt eventually returns.
            let activity_id =
                match pending_activity_id_for_task(&history.events, &task, &activity_name) {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        return Err(terminal_history_conflict(&activity_name, task_id));
                    }
                    // Legacy rows (`activity_id = NULL`) resolve through the
                    // name-based fallback, whose `NotFound` cannot distinguish
                    // "never scheduled" from "already terminal". Map the
                    // terminal-in-history case onto the same documented `409` as
                    // the `Ok(None)` branch above instead of letting it
                    // `?`-propagate as a `404`. `pending_activity_id_for_task`
                    // itself is deliberately unchanged — its other callers
                    // (worker finalize, broken-session reclaim) depend on the
                    // current contract.
                    Err(HarvestError::NotFound(_))
                        if task.activity_id.is_none()
                            && named_activity_has_terminal_event(
                                &history.events,
                                &activity_name,
                            ) =>
                    {
                        return Err(terminal_history_conflict(&activity_name, task_id));
                    }
                    Err(e) => return Err(e),
                };

            // Build the typed failure once and derive both persisted forms
            // from it: the wire envelope stored on the task row, and the
            // event fields decoded through `parse_error_payload_full` — the
            // exact decoder `worker::finalize_activity_failure` uses, so the
            // two recording paths cannot diverge.
            let envelope =
                crate::failure::ActivityFailure::operator_force_failed(reason.as_deref())
                    .into_error_payload();
            let parsed = crate::failure::parse_error_payload_full(&envelope);
            let failed_event = WorkflowEvent::ActivityFailed {
                activity_id,
                error: parsed.message,
                attempt: crate::worker::task_attempt(&task),
                error_type: parsed.error_type,
                non_retryable: parsed.non_retryable,
                details: parsed.details,
            };
            store::append_events_with_codecs(
                conn,
                exec_id,
                &[failed_event],
                history.next_event_id,
                codecs,
            )
            .await?;
            queue::fail_task(conn, task.id, &envelope).await?;
            queue::wake_workflow_task(conn, exec_id).await?;

            Ok(ForceFailActivityOutcome {
                task_id,
                queue_name: task.queue_name,
                activity_name,
                forced: true,
                already_forced: false,
            })
        }),
    )
    .await
}

async fn enforce_workflow_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: crate::types::ExecutionId,
    reason: &TimeoutReason,
    metrics: &(dyn MetricsRecorder + Send + Sync),

    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<()> {
    // Pinned `READ COMMITTED` for the same fresh-snapshot reason as
    // `enforce_activity_timeout` — see the note there (issue #619 round-24
    // review). Without it, a `repeatable read` session default lets this
    // enforcer time out the whole execution after a pause was acknowledged.
    let mut tx = conn.build_transaction().read_committed();
    let enforced = Box::pin(tx.run::<_, HarvestError, _>(async |conn| {
        // Authoritative QUEUE-pause re-check (issue #619), the exact mirror of
        // the one in `enforce_activity_timeout` — see that function for the full
        // rationale on why an advisory lock (not a bare re-read) is required and
        // why the lock must be taken BEFORE any row is touched.
        //
        // This path needs it for the same reason: `find_timed_out_tasks` does
        // not filter on `task_type`, so a PENDING **workflow** task carrying
        // `schedule_to_start` reaches here exactly as an activity task does, and
        // the scan's queue-pause carve-out is only a non-locking snapshot. A
        // pause committing after that snapshot would otherwise let this
        // transaction append `WorkflowFailed` and seal the whole execution
        // `TIMED_OUT` — strictly worse than the activity case, which fails one
        // task, and precisely the outcome AC3/AC4 forbid.
        //
        // Bailing here also skips the execution/history loads below, so the
        // advisory-lock wait cannot widen the window between reading
        // `history.next_event_id` and appending at it (those loads used to sit
        // outside this transaction; they are inside it now for that reason).
        //
        // Returning `None` — rather than proceeding with no writes — is
        // load-bearing beyond the appends: it also skips
        // `maybe_increment_schedule_failure_counter` below, so a deliberately
        // held task can never count toward the schedule auto-pause threshold
        // (issue #360). A hold is not a schedule failure.
        if matches!(reason, TimeoutReason::ScheduleToStart) {
            // Shared mode, exactly as in `enforce_activity_timeout` — and it
            // matters more here, because this transaction holds the lock across
            // `append_events`, the parent-close cascade and trigger evaluation,
            // so an exclusive lock would stall dispatch on an unpaused queue for
            // that entire span. See `lock_queue_for_timeout_recheck`.
            crate::queue_pause::lock_queue_for_timeout_recheck(conn, &task.queue_name).await?;
            if crate::queue_pause::queue_pause_suppresses_timeout(
                reason,
                crate::queue_pause::is_queue_paused(conn, &task.queue_name).await?,
            ) {
                return Ok(None);
            }
            // Same stale-scan guard as the activity path, and it matters more
            // here: this path seals the whole execution `TIMED_OUT` rather than
            // failing one task, so acting on a deadline a completed resume has
            // already credited forward destroys the run the hold was protecting.
            //
            // Unlocked fast path, for the same lock-ordering reason as the
            // activity path (round-22 review); the authoritative locked re-read
            // runs below, once the execution row is held.
            if !schedule_to_start_still_expired_unlocked(conn, task.id).await? {
                return Ok(None);
            }
        }

        // Lock the execution row BEFORE any task row (round-22 review). This
        // path used to read the execution unlocked and only take the row lock
        // implicitly, inside `store::append_events` below — which left the
        // locked `schedule_to_start` re-read above it, inverting the documented
        // execution-row -> task-row order. Locking here also loads the history
        // in the same call, replacing a separate `store::load_history`.
        let (execution, history) =
            lock_workflow_execution_row_and_load_history(conn, exec_id, codecs).await?;
        // Authoritative deadline re-read, now correctly ordered after the
        // execution lock. See the activity path for why the unlocked check
        // above cannot be trusted on its own.
        if matches!(reason, TimeoutReason::ScheduleToStart)
            && !schedule_to_start_still_expired(conn, task.id).await?
        {
            return Ok(None);
        }
        let error = timeout_error(&execution.workflow_name, reason);
        let workflow_event = WorkflowEvent::workflow_failed(error.clone());

        store::append_events_with_codecs(
            conn,
            exec_id,
            &[workflow_event],
            history.next_event_id,
            codecs,
        )
        .await?;
        update_workflow_execution_timed_out(conn, exec_id, &error).await?;
        queue::fail_task(conn, task.id, &error).await?;
        let (mut deferred, closed_children) = apply_parent_close_cascade(conn, exec_id).await?;
        let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
            conn,
            exec_id,
            crate::completion_trigger::TerminalState::TimedOut,
            Some(metrics),
        )
        .await?;
        deferred.extend(triggers);
        if execution.parent_close_policy.is_none()
            && let Some(parent_uuid) = execution.parent_id
        {
            wake_parent_for_child_timeout(
                conn,
                execution_id_from_uuid(parent_uuid),
                exec_id,
                &error,
            )
            .await?;
        }
        Ok(Some((execution, deferred, closed_children)))
    }))
    .await?;

    // Suppressed by a queue pause: nothing was written, so there is nothing to
    // cascade, no handler check to run, and no schedule failure to count.
    let Some((execution, deferred_starts, closed_children)) = enforced else {
        return Ok(());
    };

    for start in deferred_starts {
        start.spawn();
    }

    for (child_id, child_name) in closed_children {
        if let Err(e) =
            check_and_report_unfinished_handlers(conn, child_id, &child_name, Some(metrics)).await
        {
            tracing::error!(
                child_id = %child_id,
                err = %e,
                "Failed to check and report unfinished handlers on cascaded child in workflow timeout"
            );
        }
    }

    if let Err(e) =
        check_and_report_unfinished_handlers(conn, exec_id, &execution.workflow_name, Some(metrics))
            .await
    {
        tracing::error!(
            exec_id = %exec_id,
            err = %e,
            "Failed to check and report unfinished handlers on workflow timeout"
        );
    }

    // Best-effort: count task-level timeouts toward the schedule auto-pause threshold.
    // Called AFTER the transaction commits to avoid aborting the transition on a
    // counter query failure.
    crate::scheduler::maybe_increment_schedule_failure_counter(
        conn,
        &execution.workflow_id,
        &execution.workflow_name,
        execution.schedule_id,
        execution.origin.as_deref(),
        metrics,
    )
    .await;

    Ok(())
}

/// Enforce schedule-to-close timeouts for pending external activity tasks.
///
/// Scans `harvest_external_tasks` for rows that are still `PENDING` but whose
/// `schedule_to_close_at` has elapsed.  For each expired row the function:
///
/// 1. Marks the row `TIMED_OUT`.
/// 2. Appends `ActivityTimedOut { timeout_type: ScheduleToClose }` to the
///    owning workflow's event history.
/// 3. Wakes the parked workflow task so it can process the timeout.
///
/// Returns the number of external tasks that were timed out.
///
/// Pause suspends this deadline clock too (issue #609 post-review
/// hardening): external tasks whose owning execution is `PAUSED` are
/// excluded, mirroring the task-queue `ScheduleToClose` treatment. This scan
/// enforces *only* the `schedule_to_close_at` wall-clock deadline — external
/// tasks have no heartbeat/start-to-close reason whose in-flight semantics
/// would argue for pause-blind enforcement — so a blanket `PAUSED` exclusion
/// is correct here. On resume, `resume_workflow_execution` shifts the
/// execution's still-`PENDING` external tasks' `schedule_to_close_at`
/// forward by the clamped pause span
/// ([`crate::execution::shift_external_schedule_to_close_on_resume_query`])
/// so paused wall-clock is never charged against the external deadline.
///
/// The scan filter above is a non-locking snapshot only — it does not
/// serialize with `pause_workflow_execution` (which locks only the execution
/// row), so a pause committing after the scan must not be enforced anyway
/// (issue #609 post-review hardening, second bot-review round). The guarantee
/// is the per-task transaction below: it locks the external-task row, then
/// the execution row `FOR UPDATE`, re-checks `PAUSED` under that execution
/// lock, and skips the task entirely (left `PENDING`, no event) when the
/// pause won the race — so the state flip and the event append always happen
/// under the same lock the pause path holds. The lock *order* (task row
/// first, execution row second) matches the external-task completion paths —
/// see the lock-ordering convention comment inside the transaction (issue
/// #609 post-review hardening, third bot-review round).
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
pub async fn enforce_external_task_timeouts(conn: &mut AsyncPgConnection) -> HarvestResult<usize> {
    let expired: Vec<ExternalTask> = harvest_external_tasks::table
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .filter(harvest_external_tasks::schedule_to_close_at.lt(Utc::now()))
        .filter(diesel::dsl::not(diesel::dsl::exists(
            harvest_workflow_executions::table
                .filter(
                    harvest_workflow_executions::id.eq(harvest_external_tasks::workflow_exec_id),
                )
                .filter(harvest_workflow_executions::state.eq("PAUSED")),
        )))
        .select(ExternalTask::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut count = 0usize;

    for task in &expired {
        let exec_id = execution_id_from_uuid(task.workflow_exec_id);
        let exec_uuid = task.workflow_exec_id;
        let activity_id = ActivityExecId::from_uuid(task.activity_id);
        let task_id = task.id;

        let timeout_event = WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type: TimeoutType::ScheduleToClose,
        };

        let result = Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
            // Per-table lock-ordering convention (issue #609
            // post-review hardening, third bot-review round):
            //
            //   harvest_external_tasks: task row → execution row
            //   harvest_task_queue:     execution row → task row
            //
            // The external-task completion paths (`external_task.rs`'s
            // `complete_externally`/`fail_externally`/`extend_deadline`)
            // lock the task row first via `lock_task`, then lock the
            // execution row inside `store::append_single_event` — so
            // this scanner MUST lock the task row first too. An
            // earlier revision took the execution row lock first,
            // which was an ABBA inversion against a concurrent
            // completion: Postgres deadlock-detects and aborts one of
            // the two transactions, surfacing spurious errors to
            // valid external-completion callers. (The task-queue
            // enforcers — `enforce_activity_timeout`,
            // `worker::record_schedule_to_close_activity_timeout` —
            // follow the *opposite*, execution-first convention for
            // `harvest_task_queue` rows; that is safe because no
            // task-queue writer locks the task row and then the
            // execution row, e.g. `queue::requeue_for_retry` touches
            // only the task row. The one external-task writer that
            // must run execution-first — resume's pause-span shift,
            // `execution::shift_external_schedule_to_close_on_resume_query`,
            // which lives inside the execution-locked resume
            // transaction — uses `FOR UPDATE SKIP LOCKED` so it never
            // waits on a task row and cannot join a lock cycle.)
            //
            // The locked re-read below replaces trusting the scan
            // snapshot (the pre-fix code re-verified it via filters
            // on the claiming UPDATE instead).
            let locked_row: Option<(String, chrono::DateTime<Utc>)> = harvest_external_tasks::table
                .find(task_id)
                .for_update()
                .select((
                    harvest_external_tasks::state,
                    harvest_external_tasks::schedule_to_close_at,
                ))
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            let Some((task_state, deadline)) = locked_row else {
                // Row vanished after the scan (e.g. retention
                // cascade-deleted the owning execution): skip.
                return Ok(false);
            };
            // Guard against two races the scan snapshot cannot see:
            // 1. complete/fail landed after our scan → state != PENDING
            // 2. heartbeat (or a resume's pause-span shift, issue
            //    #609) extended the deadline after our scan →
            //    schedule_to_close_at is now in the future
            // Either way: skip — no flip, no event, not counted.
            if !external_task_timeout_still_due(&task_state, deadline, Utc::now()) {
                return Ok(false);
            }

            // THEN the execution row lock — the same lock
            // `pause_workflow_execution`/`resume_workflow_execution`
            // hold — so the PAUSED re-check, the external-task state
            // flip, and the event append below all serialize with the
            // pause path (issue #609 post-review hardening, second
            // bot-review round): the pause-suppression guarantee is
            // unchanged by the task-first reordering. A vanished
            // execution row (None) proceeds and surfaces as
            // `append_single_event`'s NotFound, matching the
            // pre-existing behaviour.
            let execution_state: Option<String> = harvest_workflow_executions::table
                .find(exec_uuid)
                .for_update()
                .select(harvest_workflow_executions::state)
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            if execution_state.as_deref().is_some_and(|state| {
                pause_suppresses_timeout_enforcement(
                    &TimeoutReason::ScheduleToClose,
                    state,
                    None,
                    Utc::now(),
                )
            }) {
                // Pause won the race: leave the row PENDING and
                // untouched — the resume-time deadline shift covers it.
                return Ok(false);
            }

            // The task row is locked and verified above, so a plain
            // flip suffices — no re-filters needed.
            diesel::update(harvest_external_tasks::table.find(task_id))
                .set((
                    harvest_external_tasks::state.eq("TIMED_OUT"),
                    harvest_external_tasks::updated_at.eq(Utc::now()),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            store::append_single_event(conn, exec_id, timeout_event).await?;
            queue::wake_workflow_task(conn, exec_id).await?;
            Ok(true)
        }))
        .await;

        match result {
            Ok(true) => count += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    task_id = %task.id,
                    exec_id = %exec_id,
                    error = %error,
                    "failed to enforce external task schedule-to-close timeout"
                );
                return Err(error);
            }
        }
    }

    Ok(count)
}

/// Enforce execution-level timeouts for RUNNING workflow executions whose
/// `deadline_at` has elapsed (issue #243).
///
/// For each expired execution this function:
/// 1. Appends `WorkflowEvent::WorkflowExecutionTimedOut` to the execution history.
/// 2. Transitions the execution row to `TIMED_OUT` state.
/// 3. Cancels/fails the outstanding workflow task in `harvest_task_queue`.
/// 4. Notifies the parent workflow (for awaited children) via `ChildWorkflowFailed`.
/// 5. Applies parent-close policy to any running detached children.
///
/// Returns the number of executions that were timed out.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
#[allow(clippy::too_many_lines)]
pub async fn enforce_workflow_execution_timeouts(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    let now = Utc::now();
    // Select rows where EITHER the per-run deadline (issue #243) OR the chain
    // deadline (issue #617) has fired. The chain cap is carried verbatim across
    // continue-as-new, so a runaway loop cannot escape it by continuing.
    let expired: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .filter(
            harvest_workflow_executions::deadline_at
                .is_not_null()
                .and(harvest_workflow_executions::deadline_at.lt(Some(now)))
                .or(harvest_workflow_executions::chain_deadline_at
                    .is_not_null()
                    .and(harvest_workflow_executions::chain_deadline_at.lt(Some(now)))),
        )
        .select(WorkflowExecution::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let count = expired.len();

    for execution in &expired {
        let exec_id = execution_id_from_uuid(execution.id);
        // Chain deadline takes precedence when both fired (issue #617). A
        // chain-only expiry has no per-run `deadline_at`, so classification must
        // not `.expect()` `deadline_at`.
        let (deadline, timeout_kind) =
            classify_workflow_timeout(execution.deadline_at, execution.chain_deadline_at, now);
        let timed_out_at = Utc::now();

        let timeout_event = WorkflowEvent::WorkflowExecutionTimedOut {
            deadline,
            timed_out_at,
        };
        let timeout_type = match timeout_kind {
            TimeoutKind::Chain => TimeoutType::WorkflowChain,
            TimeoutKind::Run => TimeoutType::WorkflowExecution,
        };
        let error_msg = HarvestError::Timeout {
            timeout_type,
            task_name: execution.workflow_name.clone(),
        }
        .to_string();

        let parent_uuid = if execution.parent_close_policy.is_none() {
            execution.parent_id
        } else {
            None
        };
        let workflow_name = execution.workflow_name.clone();

        // true  = timeout transition was committed
        // false = row was already non-RUNNING; cascade must not run
        let result = commit_workflow_execution_timeout(
            conn,
            exec_id,
            parent_uuid,
            &timeout_event,
            &error_msg,
            Some(metrics),
        )
        .await;

        let (timed_out_applied, deferred_starts, closed_children) = match result {
            Ok((applied, deferred, closed)) => (applied, deferred, closed),
            Err(error) => {
                tracing::error!(
                    exec_id = %exec_id,
                    workflow_name = %workflow_name,
                    error = %error,
                    "failed to enforce workflow execution timeout"
                );
                return Err(error);
            }
        };

        if !timed_out_applied {
            // Row was already non-RUNNING; nothing to do.
            continue;
        }

        for start in deferred_starts {
            start.spawn();
        }

        for (child_id, child_name) in closed_children {
            if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
                conn,
                child_id,
                &child_name,
                Some(metrics),
            )
            .await
            {
                tracing::error!(
                    child_id = %child_id,
                    err = %e,
                    "Failed to check and report unfinished handlers on cascaded child in timeout"
                );
            }
        }

        if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
            conn,
            exec_id,
            &workflow_name,
            Some(metrics),
        )
        .await
        {
            tracing::error!(exec_id = %exec_id, err = %e, "Failed to check and report unfinished handlers");
        }

        tracing::warn!(
            exec_id = %exec_id,
            workflow_name = %workflow_name,
            deadline = %deadline,
            "workflow execution timed out"
        );

        // Synthetic liveness canary (issue #796, AC6): a probe that does not
        // reach terminal completion within its per-probe timeout (its
        // execution deadline) is a `harvest.canary.failure`, not a business
        // timeout/terminal. Excluded from `harvest.workflow.timeout` and
        // `harvest.workflow.terminal` (AC8). Distinct from the #512 replay
        // canary. Labels: `queue` + `shard` only.
        if crate::canary::is_canary_workflow(&workflow_name) {
            let canary_shard = u16::try_from(execution.shard_id).unwrap_or(0);
            metrics.record_canary_failure(&execution.queue_name, canary_shard);
        } else {
            // The chain-vs-run distinction lives in the two timeout counters
            // (issue #617, AC6), not in the terminal outcome — both emit
            // `WorkflowStatus::TimedOut`.
            match timeout_kind {
                TimeoutKind::Chain => {
                    metrics.record_workflow_chain_timeout(&workflow_name, &execution.queue_name);
                }
                TimeoutKind::Run => {
                    metrics.record_workflow_timeout(&workflow_name, &execution.queue_name);
                }
            }
            crate::telemetry::emit_workflow_terminal(
                metrics,
                &workflow_name,
                &execution.queue_name,
                crate::telemetry::WorkflowStatus::TimedOut,
            );
        }

        // Best-effort: count execution timeouts toward the auto-pause threshold.
        // `workflow_id` encodes the schedule UUID so the update is scoped to the
        // triggering schedule and does not cross-contaminate sibling schedules.
        crate::scheduler::maybe_increment_schedule_failure_counter(
            conn,
            &execution.workflow_id,
            &workflow_name,
            execution.schedule_id,
            execution.origin.as_deref(),
            metrics,
        )
        .await;
    }

    Ok(count)
}

/// Detect and mark workflow executions that have exceeded their declared soft
/// SLA budget (issue #487).
///
/// The scanner atomically flips `sla_breached = true` and sets `sla_breached_at`
/// for every non-PAUSED execution whose `sla_deadline_at` has elapsed — measured
/// against `COALESCE(completed_at, NOW())` — and that has not yet been marked.
///
/// - RUNNING rows (no `completed_at`) are compared against the current time, as
///   before.
/// - Already-terminal rows (`COMPLETED` / `FAILED` / `CANCELLED` / `TIMED_OUT` /
///   `TERMINATED` / `CONTINUED_AS_NEW`) are compared against their **actual
///   terminal timestamp** (`completed_at`). This means a run that finished
///   *before* its
///   deadline never breaches (no false positive), while a run that crossed the
///   deadline and then went terminal within one scan interval is still caught
///   after the fact — covering completion, failure, cancel, terminate, timeout,
///   and continue-as-new uniformly without a separate per-path marker.
/// - PAUSED rows are excluded: pause suspends the SLA clock.
///
/// It emits `harvest.workflow.sla_breached{workflow, queue}` **exactly once per
/// run**.
///
/// **This function never terminates, cancels, fails, or otherwise alters the
/// lifecycle of the run.**  A breaching run that later completes still reaches
/// COMPLETED with its normal result.  No `WorkflowEvent` is appended and
/// `harvest_events` is left untouched (zero replay footprint, same posture as
/// query handlers).
///
/// Idempotency is guaranteed by the `WHERE sla_breached = false` guard combined
/// with the `RETURNING` clause: repeated scans and concurrent workers on the
/// same shard cannot double-count a breach.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn enforce_workflow_sla_breaches(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    use crate::schema::harvest_workflow_executions;
    use diesel::dsl::sql;
    use diesel::sql_types::Bool;

    let now = Utc::now();
    let breached: Vec<(uuid::Uuid, String, String)> =
        diesel::update(harvest_workflow_executions::table)
            // Exclude only PAUSED (pause suspends the SLA clock); RUNNING and all
            // terminal states are eligible.
            .filter(harvest_workflow_executions::state.ne("PAUSED"))
            .filter(harvest_workflow_executions::sla_deadline_at.is_not_null())
            .filter(harvest_workflow_executions::sla_breached.eq(false))
            // RUNNING rows compare against NOW(); terminal rows against their
            // actual completion instant, so finishing before the deadline never
            // counts as a breach.
            .filter(sql::<Bool>(
                "sla_deadline_at < COALESCE(completed_at, NOW())",
            ))
            .set((
                harvest_workflow_executions::sla_breached.eq(true),
                harvest_workflow_executions::sla_breached_at.eq(Some(now)),
            ))
            .returning((
                harvest_workflow_executions::id,
                harvest_workflow_executions::workflow_name,
                harvest_workflow_executions::queue_name,
            ))
            .get_results(conn)
            .await
            .map_err(crate::error::database_error)?;

    let count = breached.len();

    for (exec_uuid, workflow_name, queue_name) in &breached {
        tracing::warn!(
            exec_id = %exec_uuid,
            workflow_name = %workflow_name,
            queue = %queue_name,
            "soft SLA deadline exceeded — run continues to completion"
        );
        metrics.record_workflow_sla_breach(workflow_name, queue_name);
    }

    Ok(count)
}

/// Attempt one signal delivery to `target` on `conn` (issue #751).
///
/// Shared by `enforce_external_signals_outbox`'s same-pool and cross-pool
/// branches so the `ExecutionId`-vs-`WorkflowId` dispatch logic exists in
/// exactly one place. A database error is logged and reported as `Ok(None)`
/// ("leave pending, retry on the next sweep") rather than propagated — a
/// transient failure delivering to ONE row must not abort the scan of every
/// other pending row.
async fn attempt_signal_delivery(
    conn: &mut AsyncPgConnection,
    target: &ExternalTarget,
    signal_id: crate::types::ExternalSignalId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
    not_found_terminal: impl Fn() -> Option<WorkflowEvent>,
) -> Option<WorkflowEvent> {
    match target {
        ExternalTarget::ExecutionId(target_id) => {
            match crate::signal::send_signal_idempotent(
                conn,
                *target_id,
                signal_name,
                payload,
                idempotency_key,
            )
            .await
            {
                // Delivered or deduped (idempotency-key collision): both
                // mean the signal landed exactly once.
                Ok(_delivered_or_deduped) => {
                    Some(WorkflowEvent::ExternalSignalDelivered { signal_id })
                }
                Err(HarvestError::NotFound(_)) => not_found_terminal(),
                Err(HarvestError::Database(e)) => {
                    tracing::error!(error = %e, "outbox sweep: db error during signal delivery");
                    None
                }
                Err(_) => Some(WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code: "target_terminal".to_string(),
                }),
            }
        }
        ExternalTarget::WorkflowId {
            workflow_name,
            workflow_id,
        } => match crate::signal::resolve_and_signal_by_workflow_id(
            conn,
            workflow_name,
            workflow_id,
            signal_name,
            payload,
            idempotency_key,
        )
        .await
        {
            Ok(crate::signal::ByIdSignalOutcome::Delivered) => {
                Some(WorkflowEvent::ExternalSignalDelivered { signal_id })
            }
            // No run has ever existed for this business key — subject to
            // the same grace window as an `ExecutionId`'s `NotFound`.
            Ok(crate::signal::ByIdSignalOutcome::NoRunFound) => not_found_terminal(),
            // The current run for this business key is already terminal — a
            // genuine, immediate failure (unlike cancel, a signal's goal is
            // never already met by a terminal target), never gated by the
            // grace window (issue #751 AC4).
            Ok(crate::signal::ByIdSignalOutcome::NotRunning) => {
                Some(WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code: "not_running".to_string(),
                })
            }
            Err(HarvestError::Database(e)) => {
                tracing::error!(error = %e, "outbox sweep: db error during by-id signal delivery");
                None
            }
            // Raced to a CONTINUED_AS_NEW successor between resolution and
            // this delivery attempt: leave pending — NEVER converted to a
            // grace-window failure, since the target is definitely alive,
            // just momentarily unresolvable (issue #751 AC2/AC3). Any other
            // error is likewise left unresolved.
            Ok(crate::signal::ByIdSignalOutcome::RacedToSuccessor) | Err(_) => None,
        },
    }
}

/// Enforce all currently expired task timeouts against the database state.
///
/// This mutates queue rows and workflow history so timed-out tasks are not
/// retried indefinitely in the logs while the rest of the runtime remains
/// oblivious.
///
/// # Errors
///
/// Background outbox scanner that polls pending external signal requests,
/// attempts same-shard and cross-shard delivery via `GLOBAL_SHARDED_POOL`,
/// fails with `"target_unknown"` after `unknown_target_grace_window` has elapsed,
/// and wakes up the caller workflow.
#[allow(clippy::too_many_lines)]
pub async fn enforce_external_signals_outbox(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],

    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<usize> {
    let mut count = 0;
    // The configured registry, NOT `PayloadCodecs::default()`. This decodes
    // stored `ExternalSignalRequested` / cancel / await rows and reloads the
    // caller's history to append the terminal event; with the identity
    // registry both raise `UnknownCodecKey` the moment a keyed codec is
    // configured, stranding every outbox row on the shard.
    let codecs = codecs.clone();

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    let mut excluded_event_ids: Vec<i64> = Vec::new();

    loop {
        let shards_clone = shards.clone();
        let codecs_clone = codecs.clone();
        let excluded_clone = excluded_event_ids.clone();

        let step_res: Result<Option<(bool, Option<i64>)>, HarvestError> = Box::pin(conn
            .transaction::<Option<(bool, Option<i64>)>, HarvestError, _>(async |conn| {
                let shards = shards_clone;
                let codecs = codecs_clone;
                let excluded = excluded_clone;
                let sql = "SELECT e.* FROM harvest_events e \
                           INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id \
                           WHERE e.event_type = 'ExternalSignalRequested' \
                             AND execs.state = 'RUNNING' \
                             AND execs.shard_id = ANY($1) \
                             AND (e.event_data->'data'->>'signal_id') IS NOT NULL \
                             AND NOT (e.id = ANY($2)) \
                             AND NOT EXISTS ( \
                                 SELECT 1 FROM harvest_events res \
                                 WHERE res.workflow_exec_id = e.workflow_exec_id \
                                   AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed') \
                                   AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id' \
                             ) \
                           LIMIT 1 \
                           FOR UPDATE OF e SKIP LOCKED";

                let row_opt: Option<crate::models::HarvestEvent> = diesel::sql_query(sql)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&shards)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&excluded)
                    .get_result(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;

                let Some(row) = row_opt else {
                    return Ok(None);
                };

                let caller_exec_id = crate::types::ExecutionId::from_uuid(row.workflow_exec_id);

                let event = match codecs.decode_event(row.event_data.clone()) {
                    Ok(WorkflowEvent::ExternalSignalRequested {
                        signal_id,
                        target,
                        signal_name,
                        payload,
                        idempotency_key,
                    }) => (signal_id, target, signal_name, payload, idempotency_key),
                    Ok(other) => {
                        tracing::error!(event = ?other, "outbox sweep: query returned non-ExternalSignalRequested event");
                        return Ok(Some((false, Some(row.id))));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "outbox sweep: failed to decode event_data");
                        return Ok(Some((false, Some(row.id))));
                    }
                };

                let (signal_id, target, signal_name, payload, idempotency_key) = event;

                let age = Utc::now() - row.timestamp;
                let grace_chrono = chrono::Duration::from_std(unknown_target_grace_window)
                    .unwrap_or(chrono::Duration::MAX);
                let grace_expired = age > grace_chrono;

                // A NotFound delivery attempt only becomes a permanent
                // `target_unknown` failure once the grace window has elapsed.
                // Within the window we leave the row pending (retried next
                // sweep) so a target that starts slightly after the request —
                // or that the outbox first sees after worker downtime/backlog —
                // is still signalled rather than wrongly reported unknown.
                let not_found_terminal = || {
                    grace_expired.then(|| WorkflowEvent::ExternalSignalFailed {
                        signal_id,
                        reason_code: "target_unknown".to_string(),
                    })
                };

                // Try to route target using the config's sharded pool if configured
                let active_sharded_pool = sharded_pool
                    .clone()
                    .or_else(|| {
                        crate::shard::GLOBAL_SHARDED_POOL.read().ok()
                            .and_then(|lock| lock.clone())
                    });

                let caller_shard = caller_exec_id.shard();

                let same_pool = active_sharded_pool.as_ref().is_none_or(|pool| {
                    if let (Some(t_pool), Some(c_pool)) = (
                        pool.exact_pool_for_target(&target, caller_shard),
                        pool.exact_pool_for_execution(caller_exec_id),
                    ) {
                        std::ptr::eq(t_pool, c_pool)
                    } else {
                        false
                    }
                });

                let terminal_opt = if same_pool {
                    attempt_signal_delivery(
                        conn,
                        &target,
                        signal_id,
                        &signal_name,
                        payload.clone(),
                        idempotency_key.as_deref(),
                        not_found_terminal,
                    )
                    .await
                } else {
                    // Different pools, so we must have a target_pool resolved
                    let Some(pool) = active_sharded_pool
                        .as_ref()
                        .and_then(|p| p.exact_pool_for_target(&target, caller_shard))
                    else {
                        tracing::warn!(
                            target_shard = ?crate::shard::external_target_owning_shard(&target),
                            "outbox sweep: target shard is not configured locally; leaving row locked/pending for other workers"
                        );
                        return Ok(Some((false, Some(row.id))));
                    };

                    let mut target_conn = match pool.get().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(error = %e, "outbox sweep: failed to acquire target connection");
                            return Ok(Some((false, Some(row.id))));
                        }
                    };

                    attempt_signal_delivery(
                        &mut target_conn,
                        &target,
                        signal_id,
                        &signal_name,
                        payload.clone(),
                        idempotency_key.as_deref(),
                        not_found_terminal,
                    )
                    .await
                };

                if let Some(terminal_event) = terminal_opt {
                    let outcome = match &terminal_event {
                        WorkflowEvent::ExternalSignalDelivered { .. } => "delivered",
                        _ => "failed",
                    };
                    let reason_code = match &terminal_event {
                        WorkflowEvent::ExternalSignalFailed {
                            reason_code, ..
                        } => Some(reason_code.clone()),
                        _ => None,
                    };

                    let history = lock_workflow_execution_and_load_history(conn, caller_exec_id, &codecs).await?;
                    store::append_events_with_codecs(
                        conn,
                        caller_exec_id,
                        &[terminal_event],
                        history.next_event_id,
                        &codecs,
                    )
                    .await?;
                    queue::wake_workflow_task(conn, caller_exec_id).await?;

                    metrics.record_external_signal_sent(outcome, reason_code.as_deref());
                    Ok(Some((true, None)))
                } else {
                    Ok(Some((false, Some(row.id))))
                }
            }))
            .await;

        match step_res {
            Ok(Some((processed, skipped_id))) => {
                if processed {
                    count += 1;
                }
                if let Some(id) = skipped_id {
                    excluded_event_ids.push(id);
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                tracing::error!(error = %e, "outbox sweep error in transaction step");
                return Err(e);
            }
        }
    }

    Ok(count)
}

/// Deferred trigger starts / unfinished-handler checks / terminal metrics
/// collected while attempting a cancel delivery, bundled into one out-param
/// so [`attempt_cancel_delivery`] stays under clippy's argument-count
/// ceiling (issue #751).
struct CancelDeliveryAccumulators {
    deferred_starts: Vec<crate::completion_trigger::DeferredTriggerStart>,
    deferred_checks: Vec<(ExecutionId, String)>,
    cancel_metrics: Vec<(String, String)>,
}

/// Attempt one cancel delivery to `target` on `conn` (issue #751).
///
/// Shared by `enforce_external_cancels_outbox`'s same-pool and cross-pool
/// branches so the `ExecutionId`-vs-`WorkflowId` dispatch logic exists in
/// exactly one place. Always resolves through the `_collect`-style deferred
/// path (never spawns triggers itself) — the caller is responsible for
/// spawning `acc.deferred_starts`/`acc.deferred_checks`/`acc.cancel_metrics`
/// once its own step transaction commits. This is correct for BOTH
/// same-pool delivery (where deferral is required — the underlying work is
/// a savepoint nested inside the caller's still-open transaction) and
/// cross-pool delivery (where the underlying work already committed
/// independently on its own connection by the time this function returns,
/// so deferring a few microseconds longer costs nothing but keeps one
/// uniform code path).
///
/// A database error is logged and reported as `None` ("leave pending, retry
/// on the next sweep") rather than propagated — matching
/// `attempt_signal_delivery`'s "one row's transient failure must not abort
/// the scan" contract.
async fn attempt_cancel_delivery(
    conn: &mut AsyncPgConnection,
    target: &ExternalTarget,
    cancel_id: crate::types::ExternalCancelId,
    reason: &str,
    not_found_terminal: impl Fn() -> Option<WorkflowEvent>,
    acc: &mut CancelDeliveryAccumulators,
) -> Option<WorkflowEvent> {
    match target {
        ExternalTarget::ExecutionId(target_id) => {
            match cancel_workflow_execution_collect(conn, *target_id, reason).await {
                Ok((_cancelled, deferred, checks, metrics_opt)) => {
                    acc.deferred_starts.extend(deferred);
                    acc.deferred_checks.extend(checks);
                    if let Some(m) = metrics_opt {
                        acc.cancel_metrics.push(m);
                    }
                    Some(WorkflowEvent::ExternalCancelDelivered { cancel_id })
                }
                Err(HarvestError::NotFound(_)) => not_found_terminal(),
                Err(HarvestError::Database(e)) => {
                    tracing::error!(error = %e, "cancel outbox sweep: db error");
                    None
                }
                // Other Err (already terminal) = no-op success.
                Err(_) => Some(WorkflowEvent::ExternalCancelDelivered { cancel_id }),
            }
        }
        ExternalTarget::WorkflowId {
            workflow_name,
            workflow_id,
        } => {
            match crate::execution::resolve_and_cancel_by_workflow_id(
                conn,
                workflow_name,
                workflow_id,
                reason,
            )
            .await
            {
                Ok(crate::execution::ByIdCancelOutcome::Cancelled {
                    deferred,
                    closed_children,
                    metrics,
                    ..
                }) => {
                    acc.deferred_starts.extend(deferred);
                    acc.deferred_checks.extend(closed_children);
                    if let Some(m) = metrics {
                        acc.cancel_metrics.push(m);
                    }
                    Some(WorkflowEvent::ExternalCancelDelivered { cancel_id })
                }
                // Already terminal = no-op success (goal already met).
                Ok(crate::execution::ByIdCancelOutcome::AlreadyTerminal) => {
                    Some(WorkflowEvent::ExternalCancelDelivered { cancel_id })
                }
                Ok(crate::execution::ByIdCancelOutcome::NoRunFound) => not_found_terminal(),
                Err(HarvestError::Database(e)) => {
                    tracing::error!(
                        error = %e,
                        "cancel outbox sweep: db error during by-id cancel delivery"
                    );
                    None
                }
                // A live successor now exists; leave pending so a later sweep
                // re-resolves and cancels the successor instead. Any other
                // error is likewise left unresolved.
                Ok(crate::execution::ByIdCancelOutcome::RacedToSuccessor) | Err(_) => None,
            }
        }
    }
}

/// Scan for `ExternalCancelRequested` events without a matching terminal event
/// and attempt cancel delivery (issue #492).
///
/// Mirrors `enforce_external_signals_outbox` but calls `cancel_workflow_execution`
/// instead of `signal::send_signal`. Already-terminal targets resolve as
/// `ExternalCancelDelivered` (no-op success); missing targets after the grace
/// window resolve as `ExternalCancelFailed { reason_code: "target_unknown" }`.
///
/// Per-step outcome: (processed, `skipped_event_id`, deferred trigger starts,
/// (`workflow_name`, `queue_name`) of targets newly cancelled, deferred
/// unfinished-update-handler checks, the shard `conn` (the caller-shard
/// connection) is bound to). The deferred starts and terminal metrics are
/// spawned/recorded only after the step transaction commits so trigger
/// workflows never start for a cancellation that later rolls back (issue
/// #492). The trailing shard lets the post-commit loop route each deferred
/// handler check to the connection that actually owns its execution id
/// (issue #751 review, round 2): for a cross-shard cancellation the target
/// (and any cascade-closed children) live on the TARGET shard, never on
/// `conn`, so routing every check through `conn` unconditionally would
/// silently see zero events and never report a genuinely unfinished update
/// handler.
type CancelStepOutcome = (
    bool,
    Option<i64>,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(String, String)>,
    Vec<(ExecutionId, String)>,
    crate::types::ShardId,
);

#[allow(clippy::too_many_lines)]
pub async fn enforce_external_cancels_outbox(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],

    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<usize> {
    let mut count = 0;
    // The configured registry, NOT `PayloadCodecs::default()`. This decodes
    // stored `ExternalSignalRequested` / cancel / await rows and reloads the
    // caller's history to append the terminal event; with the identity
    // registry both raise `UnknownCodecKey` the moment a keyed codec is
    // configured, stranding every outbox row on the shard.
    let codecs = codecs.clone();

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    // Resolved once, up front, for routing deferred unfinished-handler
    // checks to the correct shard's connection after each step commits
    // (issue #751 review, round 2). Mirrors the identical resolution
    // performed per-attempt inside the step transaction below.
    let outer_sharded_pool = sharded_pool.clone().or_else(|| {
        crate::shard::GLOBAL_SHARDED_POOL
            .read()
            .ok()
            .and_then(|lock| lock.clone())
    });

    let mut excluded_event_ids: Vec<i64> = Vec::new();

    loop {
        let shards_clone = shards.clone();
        let codecs_clone = codecs.clone();
        let excluded_clone = excluded_event_ids.clone();

        let step_res: Result<Option<CancelStepOutcome>, HarvestError> = Box::pin(conn
            .transaction::<Option<CancelStepOutcome>, HarvestError, _>(async |conn| {
                let shards = shards_clone;
                let codecs = codecs_clone;
                let excluded = excluded_clone;
                let sql = "SELECT e.* FROM harvest_events e \
                           INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id \
                           WHERE e.event_type = 'ExternalCancelRequested' \
                             AND execs.state = 'RUNNING' \
                             AND execs.shard_id = ANY($1) \
                             AND (e.event_data->'data'->>'cancel_id') IS NOT NULL \
                             AND NOT (e.id = ANY($2)) \
                             AND NOT EXISTS ( \
                                 SELECT 1 FROM harvest_events res \
                                 WHERE res.workflow_exec_id = e.workflow_exec_id \
                                   AND res.event_type IN ('ExternalCancelDelivered', 'ExternalCancelFailed') \
                                   AND res.event_data->'data'->>'cancel_id' = e.event_data->'data'->>'cancel_id' \
                             ) \
                           LIMIT 1 \
                           FOR UPDATE OF e SKIP LOCKED";

                let row_opt: Option<crate::models::HarvestEvent> = diesel::sql_query(sql)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&shards)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&excluded)
                    .get_result(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;

                let Some(row) = row_opt else {
                    return Ok(None);
                };

                let caller_exec_id = crate::types::ExecutionId::from_uuid(row.workflow_exec_id);
                // Computed once, up front, so every early return below (and
                // the deferred unfinished-handler-check routing after this
                // step commits) can tell a same-shard check apart from a
                // cross-shard one without re-deriving it (issue #751 review).
                let caller_shard = caller_exec_id.shard();

                let (cancel_id, target) = match codecs.decode_event(row.event_data.clone()) {
                    Ok(WorkflowEvent::ExternalCancelRequested { cancel_id, target }) => {
                        (cancel_id, target)
                    }
                    Ok(other) => {
                        tracing::error!(event = ?other, "cancel outbox sweep: query returned non-ExternalCancelRequested event");
                        return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new(), caller_shard)));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "cancel outbox sweep: failed to decode event_data");
                        return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new(), caller_shard)));
                    }
                };

                let age = Utc::now() - row.timestamp;
                let grace_chrono = chrono::Duration::from_std(unknown_target_grace_window)
                    .unwrap_or(chrono::Duration::MAX);
                let grace_expired = age > grace_chrono;

                // A NotFound delivery attempt only becomes a permanent
                // `target_unknown` failure once the grace window has elapsed.
                // Within the window we leave the row pending (retried next
                // sweep) so a target that starts slightly after the request —
                // or that the outbox first sees after worker downtime/backlog —
                // is still cancelled rather than wrongly reported unknown.
                // (issue #492)
                let not_found_terminal = || {
                    grace_expired.then(|| WorkflowEvent::ExternalCancelFailed {
                        cancel_id,
                        reason_code: "target_unknown".to_string(),
                    })
                };

                let active_sharded_pool = sharded_pool
                    .clone()
                    .or_else(|| {
                        crate::shard::GLOBAL_SHARDED_POOL.read().ok()
                            .and_then(|lock| lock.clone())
                    });

                let same_pool = active_sharded_pool.as_ref().is_none_or(|pool| {
                    if let (Some(t_pool), Some(c_pool)) = (
                        pool.exact_pool_for_target(&target, caller_shard),
                        pool.exact_pool_for_execution(caller_exec_id),
                    ) {
                        std::ptr::eq(t_pool, c_pool)
                    } else {
                        false
                    }
                });

                // Completion-trigger / cascade follow-up starts + terminal
                // metrics, spawned/recorded only after this step transaction
                // commits for SAME-POOL delivery (issue #492): that work runs
                // as a nested savepoint on `conn` itself, so it genuinely
                // rolls back with the outer transaction and must not be
                // acted on before that transaction commits. `attempt_cancel_delivery`
                // routes through the `_collect`-style deferred path (issue
                // #751) uniformly for both branches, but CROSS-POOL delivery
                // is handled differently below -- see the comment there.
                let mut acc = CancelDeliveryAccumulators {
                    deferred_starts: Vec::new(),
                    deferred_checks: Vec::new(),
                    cancel_metrics: Vec::new(),
                };

                let mut target_conn_opt = None;
                let terminal_opt = if same_pool {
                    attempt_cancel_delivery(
                        conn,
                        &target,
                        cancel_id,
                        "cancelled by external request",
                        not_found_terminal,
                        &mut acc,
                    )
                    .await
                } else {
                    let Some(pool) = active_sharded_pool
                        .as_ref()
                        .and_then(|p| p.exact_pool_for_target(&target, caller_shard))
                    else {
                        tracing::warn!(
                            target_shard = ?crate::shard::external_target_owning_shard(&target),
                            "cancel outbox sweep: target shard not configured locally; skipping"
                        );
                        return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new(), caller_shard)));
                    };

                    let mut target_conn = match pool.get().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(error = %e, "cancel outbox sweep: failed to acquire target connection");
                            return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new(), caller_shard)));
                        }
                    };

                    let terminal = attempt_cancel_delivery(
                        &mut target_conn,
                        &target,
                        cancel_id,
                        "cancelled by external request",
                        not_found_terminal,
                        &mut acc,
                    )
                    .await;
                    // Keep the target connection open past this branch: for
                    // cross-pool delivery, `attempt_cancel_delivery`'s work
                    // already committed independently on `target_conn` by
                    // the time it returned above (a real, top-level Postgres
                    // commit, unlike the same-pool savepoint) -- nothing
                    // that happens afterward on the unrelated `conn`
                    // transaction can roll it back. Its follow-ups are
                    // therefore handled immediately below, using this same
                    // connection, rather than deferred past `conn`'s commit
                    // (issue #751 review, round 3).
                    target_conn_opt = Some(target_conn);
                    terminal
                };

                let CancelDeliveryAccumulators {
                    deferred_starts,
                    deferred_checks,
                    cancel_metrics,
                } = acc;

                // Deferring a cross-pool cancellation's follow-ups past the
                // caller-side outer transaction's commit would lose them
                // permanently if that (unrelated) transaction subsequently
                // failed: a retry re-attempts delivery against an
                // already-terminal target, whose `AlreadyTerminal` outcome
                // contributes no new checks/metrics of its own. Acting on
                // them right here -- while `target_conn` is still open --
                // means they can never be dropped this way (issue #751
                // review, round 3).
                let (deferred_starts, deferred_checks, cancel_metrics) =
                    if let Some(mut target_conn) = target_conn_opt {
                        for start in deferred_starts {
                            start.spawn();
                        }
                        for (exec_id, workflow_name) in deferred_checks {
                            if let Err(e) = check_and_report_unfinished_handlers(
                                &mut target_conn,
                                exec_id,
                                &workflow_name,
                                Some(metrics),
                            )
                            .await
                            {
                                tracing::error!(
                                    exec_id = %exec_id,
                                    err = %e,
                                    "cancel outbox sweep: failed to check unfinished update handlers on target shard"
                                );
                            }
                        }
                        for (workflow_name, queue_name) in cancel_metrics {
                            crate::telemetry::emit_workflow_terminal(
                                metrics,
                                &workflow_name,
                                &queue_name,
                                crate::telemetry::WorkflowStatus::Cancelled,
                            );
                        }
                        (Vec::new(), Vec::new(), Vec::new())
                    } else {
                        (deferred_starts, deferred_checks, cancel_metrics)
                    };

                if let Some(terminal_event) = terminal_opt {
                    let outcome = match &terminal_event {
                        WorkflowEvent::ExternalCancelDelivered { .. } => "delivered",
                        _ => "failed",
                    };
                    let reason_code = match &terminal_event {
                        WorkflowEvent::ExternalCancelFailed { reason_code, .. } => {
                            Some(reason_code.clone())
                        }
                        _ => None,
                    };

                    let history = lock_workflow_execution_and_load_history(conn, caller_exec_id, &codecs).await?;
                    store::append_events_with_codecs(
                        conn,
                        caller_exec_id,
                        &[terminal_event],
                        history.next_event_id,
                        &codecs,
                    )
                    .await?;
                    queue::wake_workflow_task(conn, caller_exec_id).await?;
                    metrics.record_external_cancel_sent(outcome, reason_code.as_deref());
                    Ok(Some((true, None, deferred_starts, cancel_metrics, deferred_checks, caller_shard)))
                } else {
                    Ok(Some((false, Some(row.id), deferred_starts, cancel_metrics, deferred_checks, caller_shard)))
                }
            }))
            .await;

        match step_res {
            Ok(Some((
                processed,
                skipped_id,
                deferred_starts,
                cancel_metrics,
                deferred_checks,
                caller_shard,
            ))) => {
                // The step transaction has committed: now spawn trigger/cascade
                // follow-up starts and record terminal metrics for same-pool
                // cancellations (issue #492).
                for start in deferred_starts {
                    start.spawn();
                }
                // Route each deferred unfinished-handler check to whichever
                // connection actually owns its execution id (issue #751
                // review, round 2). A same-shard check reuses the
                // already-committed `conn` directly (and must — re-acquiring
                // a second connection from that same pool while `conn` is
                // still held would self-deadlock a pool of size 1, matching
                // issue #688's precedent). A cross-shard check (the target
                // and any cascade-closed children of a cross-shard
                // cancellation, which live only on the target shard) gets a
                // fresh connection to that shard's own pool.
                //
                // "Same shard" here means "resolves to the same *pool*", not
                // a raw `ShardId` equality check (issue #751 review, round
                // 5): a legacy/pre-sharding caller execution carries
                // `ShardId::UNENCODED`, which `exact_pool_for_execution`
                // correctly resolves to the default shard's pool via its own
                // fallback -- but a raw `exec_id.shard() == caller_shard`
                // comparison would treat that as cross-shard even against an
                // *encoded* default-shard execution physically served by the
                // identical pool, taking the `pool.get()` branch below and
                // self-deadlocking under the same pool-size-1 configuration
                // this whole routing exists to protect. `pool_for(shard)`
                // (with its own default-shard fallback) applied to
                // `caller_shard` is a faithful, `ShardId`-only stand-in for
                // `exact_pool_for_execution(caller_exec_id)` -- both consult
                // only `.shard()` internally, so the two are provably
                // equivalent without needing `caller_exec_id` itself here.
                for (exec_id, workflow_name) in deferred_checks {
                    let same_pool_as_caller = outer_sharded_pool.as_ref().is_none_or(|pool| {
                        pool.exact_pool_for_execution(exec_id)
                            .is_some_and(|e_pool| std::ptr::eq(e_pool, pool.pool_for(caller_shard)))
                    });
                    if same_pool_as_caller {
                        if let Err(e) = check_and_report_unfinished_handlers(
                            conn,
                            exec_id,
                            &workflow_name,
                            Some(metrics),
                        )
                        .await
                        {
                            tracing::error!(
                                exec_id = %exec_id,
                                err = %e,
                                "cancel outbox sweep: failed to check unfinished update handlers"
                            );
                        }
                    } else if let Some(pool) = outer_sharded_pool
                        .as_ref()
                        .and_then(|p| p.exact_pool_for_execution(exec_id))
                    {
                        match pool.get().await {
                            Ok(mut target_conn) => {
                                if let Err(e) = check_and_report_unfinished_handlers(
                                    &mut target_conn,
                                    exec_id,
                                    &workflow_name,
                                    Some(metrics),
                                )
                                .await
                                {
                                    tracing::error!(
                                        exec_id = %exec_id,
                                        err = %e,
                                        "cancel outbox sweep: failed to check unfinished update handlers on target shard"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    exec_id = %exec_id,
                                    error = %e,
                                    "cancel outbox sweep: failed to acquire target-shard connection for unfinished-handler check"
                                );
                            }
                        }
                    } else {
                        tracing::warn!(
                            exec_id = %exec_id,
                            "cancel outbox sweep: target shard for unfinished-handler check not configured locally; skipping"
                        );
                    }
                }
                for (workflow_name, queue_name) in cancel_metrics {
                    crate::telemetry::emit_workflow_terminal(
                        metrics,
                        &workflow_name,
                        &queue_name,
                        crate::telemetry::WorkflowStatus::Cancelled,
                    );
                }
                if processed {
                    count += 1;
                }
                if let Some(id) = skipped_id {
                    excluded_event_ids.push(id);
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!(error = %e, "cancel outbox sweep error in transaction step");
                return Err(e);
            }
        }
    }

    Ok(count)
}

/// Scan for `ExternalAwaitRequested` events without a matching terminal event
/// and attempt to resolve the awaited target's terminal outcome (issue #757).
///
/// Observe-only: reads the target's terminal outcome via
/// [`crate::execution::read_external_await_outcome`] and, when terminal, appends
/// `ExternalAwaitResolved`/`ExternalAwaitFailed` to the **awaiter's** own history
/// (inflated) and wakes it. A still-`RUNNING`/`PAUSED` target is left pending
/// (retried next sweep — this is how "resolves within one poll interval after
/// the target reaches terminal" is met). A `NotFound` target only becomes a
/// permanent `target_unknown` failure once the grace window has elapsed.
///
/// Per-step outcome: `(processed, skipped_event_id)`.
#[allow(clippy::too_many_lines)]
pub async fn enforce_external_awaits_outbox(
    conn: &mut AsyncPgConnection,
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],

    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<usize> {
    let mut count = 0;
    // The configured registry, NOT `PayloadCodecs::default()`. This decodes
    // stored `ExternalSignalRequested` / cancel / await rows and reloads the
    // caller's history to append the terminal event; with the identity
    // registry both raise `UnknownCodecKey` the moment a keyed codec is
    // configured, stranding every outbox row on the shard.
    let codecs = codecs.clone();

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    let mut excluded_event_ids: Vec<i64> = Vec::new();

    loop {
        let shards_clone = shards.clone();
        let codecs_clone = codecs.clone();
        let excluded_clone = excluded_event_ids.clone();

        let step_res: Result<Option<(bool, Option<i64>)>, HarvestError> = Box::pin(conn
            .transaction::<Option<(bool, Option<i64>)>, HarvestError, _>(async |conn| {
                let shards = shards_clone;
                let codecs = codecs_clone;
                let excluded = excluded_clone;
                let sql = "SELECT e.* FROM harvest_events e \
                           INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id \
                           WHERE e.event_type = 'ExternalAwaitRequested' \
                             AND execs.state = 'RUNNING' \
                             AND execs.shard_id = ANY($1) \
                             AND (e.event_data->'data'->>'await_id') IS NOT NULL \
                             AND NOT (e.id = ANY($2)) \
                             AND NOT EXISTS ( \
                                 SELECT 1 FROM harvest_events res \
                                 WHERE res.workflow_exec_id = e.workflow_exec_id \
                                   AND res.event_type IN ('ExternalAwaitResolved', 'ExternalAwaitFailed') \
                                   AND res.event_data->'data'->>'await_id' = e.event_data->'data'->>'await_id' \
                             ) \
                           LIMIT 1 \
                           FOR UPDATE OF e SKIP LOCKED";

                let row_opt: Option<crate::models::HarvestEvent> = diesel::sql_query(sql)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&shards)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&excluded)
                    .get_result(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;

                let Some(row) = row_opt else {
                    return Ok(None);
                };

                let caller_exec_id = crate::types::ExecutionId::from_uuid(row.workflow_exec_id);

                let (await_id, target) = match codecs.decode_event(row.event_data.clone()) {
                    Ok(WorkflowEvent::ExternalAwaitRequested { await_id, target }) => {
                        (await_id, target)
                    }
                    Ok(other) => {
                        tracing::error!(event = ?other, "await outbox sweep: query returned non-ExternalAwaitRequested event");
                        return Ok(Some((false, Some(row.id))));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "await outbox sweep: failed to decode event_data");
                        return Ok(Some((false, Some(row.id))));
                    }
                };

                let age = Utc::now() - row.timestamp;
                let grace_chrono = chrono::Duration::from_std(unknown_target_grace_window)
                    .unwrap_or(chrono::Duration::MAX);
                let grace_expired = age > grace_chrono;

                let active_sharded_pool = sharded_pool
                    .clone()
                    .or_else(|| {
                        crate::shard::GLOBAL_SHARDED_POOL.read().ok()
                            .and_then(|lock| lock.clone())
                    });

                let same_pool = active_sharded_pool.as_ref().is_none_or(|pool| {
                    if let (Some(t_pool), Some(c_pool)) = (
                        pool.exact_pool_for_execution(target),
                        pool.exact_pool_for_execution(caller_exec_id),
                    ) {
                        std::ptr::eq(t_pool, c_pool)
                    } else {
                        false
                    }
                });

                // Map the reader's 3-state result to the awaiter's terminal
                // event, if any. The reader distinguishes NotYetTerminal from
                // NotFound directly, so no separate existence probe is needed:
                // NotFound only becomes a permanent `target_unknown` once the
                // grace window elapses (matching the cancel outbox, issue #492).
                let resolve =
                    |result: crate::execution::ExternalAwaitReadResult| -> Option<WorkflowEvent> {
                        use crate::execution::{ExternalAwaitOutcome, ExternalAwaitReadResult};
                        match result {
                            ExternalAwaitReadResult::Terminal(
                                ExternalAwaitOutcome::Completed(output),
                            ) => Some(WorkflowEvent::ExternalAwaitResolved { await_id, output }),
                            ExternalAwaitReadResult::Terminal(
                                ExternalAwaitOutcome::Terminal {
                                    reason_code,
                                    message,
                                    error_type,
                                    details,
                                    non_retryable,
                                },
                            ) => Some(WorkflowEvent::ExternalAwaitFailed {
                                await_id,
                                reason_code,
                                message,
                                error_type,
                                details,
                                non_retryable,
                            }),
                            // Still running/paused → pending (retry next sweep).
                            ExternalAwaitReadResult::NotYetTerminal => None,
                            // Not found → `target_unknown` only after grace.
                            ExternalAwaitReadResult::NotFound => {
                                grace_expired.then(|| WorkflowEvent::ExternalAwaitFailed {
                                    await_id,
                                    reason_code: "target_unknown".to_string(),
                                    message: None,
                                    error_type: None,
                                    details: None,
                                    non_retryable: None,
                                })
                            }
                        }
                    };

                // Read the target's outcome on the correct connection. A
                // transient DB read error must NOT propagate — that would
                // abort the whole `enforce_timeouts_once` sweep for this tick
                // (starving completion-triggers/debounce/throttle, which run
                // after this pass). Mirror the cancel outbox: log and leave
                // the row pending so the sweep continues and retries next
                // tick (issue #757 review, P2-b).
                let read_result = if same_pool {
                    match crate::execution::read_external_await_outcome(conn, target).await {
                        Ok(r) => r,
                        Err(HarvestError::Database(e)) => {
                            tracing::error!(error = %e, "await outbox sweep: db error reading target; leaving pending");
                            return Ok(Some((false, Some(row.id))));
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    let Some(pool) = active_sharded_pool
                        .as_ref()
                        .and_then(|p| p.exact_pool_for_execution(target))
                    else {
                        tracing::warn!(
                            target_shard = %target.shard(),
                            "await outbox sweep: target shard not configured locally; skipping"
                        );
                        return Ok(Some((false, Some(row.id))));
                    };
                    let mut target_conn = match pool.get().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(error = %e, "await outbox sweep: failed to acquire target connection");
                            return Ok(Some((false, Some(row.id))));
                        }
                    };
                    match crate::execution::read_external_await_outcome(&mut target_conn, target)
                        .await
                    {
                        Ok(r) => r,
                        Err(HarvestError::Database(e)) => {
                            tracing::error!(error = %e, "await outbox sweep: db error reading target (remote shard); leaving pending");
                            return Ok(Some((false, Some(row.id))));
                        }
                        Err(e) => return Err(e),
                    }
                };

                let terminal_opt = resolve(read_result);

                if let Some(terminal_event) = terminal_opt {
                    // Take the awaiter row lock — the SAME serialization point
                    // the inline path uses — then re-check history for a
                    // terminal already recorded for this await_id (issue #757
                    // review, P1): the inline re-park path may have resolved it
                    // between our claim-time NOT EXISTS filter and this lock.
                    // If present, skip the duplicate append (the inline path
                    // owns the awaiter's own wake/resolution).
                    let history =
                        lock_workflow_execution_and_load_history(conn, caller_exec_id, &codecs).await?;
                    let already_resolved = history.events.iter().any(|e| match e {
                        WorkflowEvent::ExternalAwaitResolved { await_id: a, .. }
                        | WorkflowEvent::ExternalAwaitFailed { await_id: a, .. } => *a == await_id,
                        _ => false,
                    });
                    if already_resolved {
                        return Ok(Some((false, Some(row.id))));
                    }
                    store::append_events_with_codecs(
                        conn,
                        caller_exec_id,
                        &[terminal_event],
                        history.next_event_id,
                        &codecs,
                    )
                    .await?;
                    queue::wake_workflow_task(conn, caller_exec_id).await?;
                    Ok(Some((true, None)))
                } else {
                    Ok(Some((false, Some(row.id))))
                }
            }))
            .await;

        match step_res {
            Ok(Some((processed, skipped_id))) => {
                if processed {
                    count += 1;
                }
                if let Some(id) = skipped_id {
                    excluded_event_ids.push(id);
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!(error = %e, "await outbox sweep error in transaction step");
                return Err(e);
            }
        }
    }

    Ok(count)
}

/// Sweep every timeout-related enforcement pass in one connection.
///
/// `session_worker_stale_secs` bounds the worker-session broken-session scan
/// (issue #606): a session's host is considered dead once its
/// `harvest_workers` heartbeat is older than this many seconds. Mirrors the
/// poison-pill reclaimer's `worker_stale_secs` convention (`2 ×
/// worker_heartbeat_interval`, computed once by the caller).
///
/// `payload_codecs` / `codec_rotation_batch_size` drive the lazy payload-codec
/// re-encryption sweep (issue #948). The sweep is a no-op — not one statement
/// issued — unless the registry holds a keyed codec, so it costs nothing on
/// every deployment that has not adopted key rotation. A batch size of `0`
/// disables it outright.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn enforce_timeouts_once(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    circuit_breakers: Option<&crate::circuit_breaker::CircuitBreakerRegistry>,
    max_workflow_history_events: Option<u64>,
    session_worker_stale_secs: i64,
    payload_codecs: &crate::payload_codec::PayloadCodecs,
    codec_rotation_batch_size: i64,
) -> HarvestResult<usize> {
    // Off-box audit-record export (issue #953) runs FIRST, and its failures are
    // logged rather than propagated. Both halves are deliberate, and both were
    // wrong in an earlier revision that simply appended the call to the end of
    // this function (Codex review round 26 P1).
    //
    // First, because a resident BEFORE it can `return Err` on every tick -- a
    // task whose history will not decode, say, because its payload codec is
    // unavailable -- and audit export would then never run on that shard again.
    // Records would accumulate unexported, and because the lag gauge is written
    // inside the export pass it would go stale rather than climb, so neither
    // the threshold alert nor the absent-series alert could fire. A compliance
    // gap that hides its own signal.
    //
    // Logged rather than propagated, because the mirror of that hazard is just
    // as bad: an export failure must not stop timeout enforcement, SLA checks,
    // session cleanup or codec rotation. This is the same "one failure must
    // never stop the others" rule already applied per-shard inside
    // `fire_due_audit_exports`, lifted to the residents of this loop.
    let mut count = 0usize;
    match crate::audit_export::fire_due_audit_exports(
        conn,
        sharded_pool,
        shard_assignments,
        metrics,
    )
    .await
    {
        Ok(exported) => count += exported,
        Err(error) => tracing::error!(
            error = %error,
            "[audit_export] export pass failed; continuing with the rest of the scanner"
        ),
    }

    let timed_out = find_timed_out_tasks(conn).await?;
    count += timed_out.len();

    for (task, reason) in timed_out {
        let result = match (task.task_type.as_str(), task.workflow_exec_id) {
            ("activity", Some(exec_uuid)) => {
                enforce_activity_timeout(
                    conn,
                    &task,
                    execution_id_from_uuid(exec_uuid),
                    &reason,
                    circuit_breakers,
                    metrics,
                    payload_codecs,
                )
                .await
            }
            ("workflow", Some(exec_uuid)) => {
                enforce_workflow_timeout(
                    conn,
                    &task,
                    execution_id_from_uuid(exec_uuid),
                    &reason,
                    metrics,
                    payload_codecs,
                )
                .await
            }
            _ => queue::fail_task(conn, task.id, &timeout_error(&task.task_type, &reason)).await,
        };

        if let Err(error) = result {
            tracing::error!(
                task_id = %task.id,
                queue = %task.queue_name,
                reason = %reason,
                error = %error,
                "failed to enforce timed-out task"
            );
            return Err(error);
        }
    }

    count += enforce_external_task_timeouts(conn).await?;
    // Run the soft-SLA scan *before* the hard execution-timeout pass: if a run
    // crosses both its SLA and hard deadline within one tick, the breach must be
    // recorded while the row is still RUNNING — the hard-timeout pass then
    // transitions it to TIMED_OUT and the SLA scan would otherwise skip it.
    count += enforce_workflow_sla_breaches(conn, metrics).await?;
    count += enforce_workflow_execution_timeouts(conn, metrics).await?;
    count += enforce_external_signals_outbox(
        conn,
        metrics,
        unknown_target_grace_window,
        sharded_pool,
        shard_assignments,
        payload_codecs,
    )
    .await?;
    count += enforce_external_cancels_outbox(
        conn,
        metrics,
        unknown_target_grace_window,
        sharded_pool,
        shard_assignments,
        payload_codecs,
    )
    .await?;
    count += enforce_external_awaits_outbox(
        conn,
        unknown_target_grace_window,
        sharded_pool,
        shard_assignments,
        payload_codecs,
    )
    .await?;
    count += crate::completion_trigger::enforce_completion_triggers_outbox(
        conn,
        metrics,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    count +=
        crate::debounce::fire_due_debounced_starts(conn, sharded_pool, shard_assignments, metrics)
            .await?;
    count +=
        crate::throttle::fire_due_throttled_starts(conn, sharded_pool, shard_assignments, metrics)
            .await?;
    count +=
        crate::event_batch::fire_due_event_batches(conn, sharded_pool, shard_assignments, metrics)
            .await?;
    count += crate::completion_callback::fire_due_completion_deliveries(
        conn,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    if let Some(ceiling) = max_workflow_history_events {
        count += enforce_workflow_history_ceiling(conn, ceiling, metrics).await?;
    }
    count +=
        crate::sessions::enforce_broken_sessions(conn, session_worker_stale_secs, payload_codecs)
            .await?;
    // Sweep expired request-scoped start-idempotency claims (issue #808). Best
    // effort table growth control; the reserve upsert overwrites an expired row
    // in place regardless, so correctness does not depend on this running.
    count += crate::start_idempotency::sweep_expired_start_idempotency(
        conn,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    // Lazy payload-codec re-encryption (issue #948): convert a bounded batch of
    // stored rows per shard from a retired key id onto the active one.
    //
    // ⚠️ This is the ONLY resident of this pass that mutates
    // `harvest_events.event_data` in place — sanctioned exception #3, see
    // `crate::codec_rotation` and CLAUDE.md. Only the ciphertext bytes inside
    // payload fields change; decoded plaintext, event `type`, ids, ordering and
    // timestamps are untouched, so replay is unaffected by construction.
    //
    // Returns without issuing a statement unless a keyed codec is registered,
    // so a deployment that has never rotated pays nothing for it.
    // Shard-local by design: it sweeps THIS connection's shard through THIS
    // connection. Reaching back into the same shard pool for a second
    // connection would park forever on a single-connection pool (deadpool is
    // configured with no acquisition timeout), wedging every later resident of
    // this tick as well as rotation itself.
    //
    // Isolated from the rest of the pass on purpose: a rotation failure is
    // logged and skipped, never propagated. Rotation is new and optional; the
    // durable-mutex lease reclamation below is neither, and a workflow waiting
    // on a lease this pass would have freed stays stuck for as long as the
    // sweep keeps failing. Missing grants on `harvest_codec_rotation_cursor`
    // or on `UPDATE harvest_events` are exactly the kind of persistent,
    // deployment-shaped failure that repeats identically every tick, so
    // propagating would not be a transient blip — it would take mutex
    // reclamation down on that shard indefinitely. A new feature must not be
    // able to break an old one by failing.
    match crate::codec_rotation::sweep_codec_reencryption(
        conn,
        shard_assignments,
        payload_codecs,
        codec_rotation_batch_size,
        metrics,
    )
    .await
    {
        // Deliberately NOT folded into `count`. That value is the
        // timeout-enforcement total: the caller logs `warn!("enforced timed-out
        // tasks")` whenever it is non-zero, and embedders read it as "this many
        // tasks timed out". Adding rotation rewrites to it makes every
        // productive sweep tick claim a timeout that never happened. Rotation
        // reports itself through `harvest.codec.reencrypted` instead.
        Ok(_swept) => {}
        Err(e) => tracing::warn!(
            error = %e,
            "codec re-encryption sweep failed; continuing with the remaining \
             timeout-pass residents"
        ),
    }
    // Reclaim expired durable-mutex leases (crash recovery, issue #691) and wake
    // each freed key's new head of line. Shard-local: it runs against this
    // connection's own database (like `enforce_broken_sessions`), and is a no-op
    // when the mutex tables are absent (guarded internally).
    //
    // MUST run inside an explicit transaction. The reclaim takes a per-key
    // `pg_advisory_xact_lock` before re-checking the lease and fencing the
    // delete/wake — the mutex-vs-acquire race guard depends on that advisory
    // being HELD across the whole per-key section. `enforce_timeouts_once` runs
    // in autocommit (its body is not wrapped in a `conn.transaction`), so
    // calling the reclaim directly would let each `pg_advisory_xact_lock` drop
    // the instant its statement returns, defeating the guard. Wrapping the call
    // in `conn.transaction(...)` keeps every advisory-xact lock alive until the
    // transaction commits.
    count += conn
        .transaction::<usize, HarvestError, _>(async |conn| {
            crate::mutex::reclaim_expired_leases_and_wake(conn).await
        })
        .await?;
    Ok(count)
}

/// Spawn a background task that periodically checks for timed-out tasks.
///
/// The checker runs every `interval` duration and enforces any timed-out tasks
/// it finds by mutating queue state and workflow history.
///
/// Stops when the cancellation token is triggered.
///
/// Equivalent to [`spawn_timeout_checker_for_shard`] with no shard attributed.
/// The shard is only used to label this loop in the `scanner_liveness`
/// health check (issue #797); it never affects which shards the loop
/// enforces against — that is `shard_assignments`.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn spawn_timeout_checker(
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    interval: Duration,
    telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
    unknown_target_grace_window: Duration,
    sharded_pool: Option<crate::shard::ShardedDbPool>,
    shard_assignments: Vec<crate::types::ShardId>,
    circuit_breakers: std::sync::Arc<crate::circuit_breaker::CircuitBreakerRegistry>,
    max_workflow_history_events: Option<u64>,
    session_worker_stale_secs: i64,
) -> tokio::task::JoinHandle<()> {
    spawn_timeout_checker_for_shard(
        pool,
        cancel,
        interval,
        telemetry,
        unknown_target_grace_window,
        sharded_pool,
        shard_assignments,
        circuit_breakers,
        max_workflow_history_events,
        session_worker_stale_secs,
        None,
        // Issue #948: this legacy single-shard entry point carries no codec
        // registry, so it drives the sweep with an empty one — which is an
        // unconditional no-op. A deployment that has adopted key rotation
        // reaches the sweep through `spawn_timeout_checker_for_shard`, wired
        // from `WorkerConfig::payload_codecs` by `HarvestBuilder::build`.
        crate::payload_codec::PayloadCodecs::default(),
        0,
    )
}

/// [`spawn_timeout_checker`], attributing this loop instance to `shard` in the
/// `scanner_liveness` health check (issue #797).
///
/// A multi-shard worker spawns one checker per assigned shard, all registered
/// under the same `timeout` scanner label. Passing the shard here is what lets
/// the health check say *which* shard's loop is wedged — the metric carries no
/// shard label, so this is the only surface that can localize it.
///
/// Pass `None` for a process-wide loop or a single-shard deployment.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn spawn_timeout_checker_for_shard(
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    interval: Duration,
    telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
    unknown_target_grace_window: Duration,
    sharded_pool: Option<crate::shard::ShardedDbPool>,
    shard_assignments: Vec<crate::types::ShardId>,
    circuit_breakers: std::sync::Arc<crate::circuit_breaker::CircuitBreakerRegistry>,
    max_workflow_history_events: Option<u64>,
    session_worker_stale_secs: i64,
    shard: Option<crate::types::ShardId>,
    payload_codecs: crate::payload_codec::PayloadCodecs,
    codec_rotation_batch_size: i64,
) -> tokio::task::JoinHandle<()> {
    // Issue #797: declare this loop (and the sub-passes it drives) before the
    // first iteration, so the `scanner_liveness` health check knows they are
    // expected in this process and grants them their boot grace window.
    //
    // `sla` and `external_outbox` are enforcement responsibilities of THIS
    // loop, not separately spawned tasks, so they are registered and ticked
    // here rather than inside `enforce_timeouts_once`. That keeps all three
    // ticks genuinely unconditional (a tick inside the pass would sit behind
    // a `?` and be skipped on a transient DB error — a wedge signal must have
    // exactly one meaning: the code stopped reaching this point) and keeps
    // scanner bookkeeping out of a `pub` primitive an embedder may drive by
    // hand. The cost is that the three labels share one liveness fate; see
    // `Scanner`'s docs.
    let owners: Vec<crate::scanner_health::ScannerOwner> = [
        crate::scanner_health::Scanner::Timeout,
        crate::scanner_health::Scanner::Sla,
        crate::scanner_health::Scanner::ExternalOutbox,
    ]
    .into_iter()
    .map(|scanner| {
        crate::scanner_health::register_scanner_for_shard(
            &*telemetry.metrics,
            scanner,
            interval,
            shard,
        )
    })
    .collect();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::debug!("timeout checker cancelled");
                    break;
                }
                () = tokio::time::sleep(interval) => {
                    // Check for timed out tasks
                }
            }

            match pool.get().await {
                Ok(mut conn) => match enforce_timeouts_once(
                    &mut conn,
                    &*telemetry.metrics,
                    unknown_target_grace_window,
                    &sharded_pool,
                    &shard_assignments,
                    Some(&circuit_breakers),
                    max_workflow_history_events,
                    session_worker_stale_secs,
                    &payload_codecs,
                    codec_rotation_batch_size,
                )
                .await
                {
                    Ok(enforced_count) if enforced_count > 0 => {
                        tracing::warn!(enforced_count, "enforced timed-out tasks");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "failed to enforce timed-out tasks");
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "failed to acquire DB connection for timeout check");
                }
            }

            // Issue #797: unconditional end-of-iteration liveness tick — a
            // no-work pass, an enforcement error, and a failed connection
            // checkout all still prove the loop itself is alive. Only a
            // panicked, deadlocked, or permanently hung loop stops ticking.
            for owner in &owners {
                crate::scanner_health::record_scanner_tick(&*telemetry.metrics, *owner);
            }

            if cancel.is_cancelled() {
                break;
            }
        }

        // Issue #797: a *graceful* stop retires this loop from the expected
        // scanner set, so draining a worker while keeping the API up does not
        // leave phantom scanners aging into `Wedged`. Deliberately after the
        // loop rather than in a guard: a panic unwinds past this point, so a
        // panicked loop stays registered and correctly goes stale.
        for owner in owners {
            crate::scanner_health::deregister_scanner(owner);
        }
    })
}

/// Terminate RUNNING workflow executions whose durable event count has reached
/// or exceeded the operator-configured hard ceiling (issue #493).
///
/// The ceiling is set via `HarvestBuilder::max_workflow_history_events`.  No
/// new `WorkflowEvent` variant is introduced: each affected execution receives
/// an ordinary `WorkflowFailed` event with a machine-readable error string of
/// the form `"history_ceiling_exceeded: event count {n} >= ceiling {c}"`, and
/// its state transitions to `FAILED`.  Outstanding task-queue rows are
/// cancelled and any awaiting parent is notified.
///
/// Idempotency: the inner transaction re-checks `state = 'RUNNING'` under a
/// `FOR UPDATE` lock, so a concurrent completion or a duplicate scanner tick
/// can never double-append the failure event.
///
/// Returns the number of executions that were terminated.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
pub async fn enforce_workflow_history_ceiling(
    conn: &mut AsyncPgConnection,
    ceiling: u64,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    use diesel::sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid};

    // Find RUNNING executions whose recorded event count >= ceiling.
    // The event_count is selected inline to avoid an N+1 query pattern.
    #[derive(diesel::QueryableByName)]
    struct OversizedRow {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = Text)]
        workflow_id: String,
        #[diesel(sql_type = Text)]
        workflow_name: String,
        #[diesel(sql_type = Text)]
        queue_name: String,
        #[diesel(sql_type = Nullable<SqlUuid>)]
        parent_id: Option<uuid::Uuid>,
        #[diesel(sql_type = Nullable<Text>)]
        parent_close_policy: Option<String>,
        #[diesel(sql_type = BigInt)]
        event_count: i64,
    }

    let ceiling_i64 = i64::try_from(ceiling).unwrap_or(i64::MAX);
    let oversized: Vec<OversizedRow> = diesel::sql_query(
        "SELECT id, workflow_id, workflow_name, queue_name, parent_id, parent_close_policy, \
         (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id)::bigint AS event_count \
         FROM harvest_workflow_executions \
         WHERE state = 'RUNNING' \
         AND (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= $1",
    )
    .bind::<BigInt, _>(ceiling_i64)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    let count = oversized.len();

    for row in &oversized {
        let exec_id = execution_id_from_uuid(row.id);
        let event_count = row.event_count;

        let error_msg =
            format!("history_ceiling_exceeded: event count {event_count} >= ceiling {ceiling}");
        let fail_event = WorkflowEvent::workflow_failed(error_msg.clone());

        let parent_uuid = if row.parent_close_policy.is_none() {
            row.parent_id
        } else {
            None
        };
        let workflow_name = row.workflow_name.clone();
        let queue_name = row.queue_name.clone();

        let (applied, deferred_starts, closed_children) =
            Box::pin(conn.transaction::<(
                bool,
                Vec<crate::completion_trigger::DeferredTriggerStart>,
                Vec<(ExecutionId, String)>,
            ), HarvestError, _>(async |conn| {
                let fail_event = fail_event.clone();
                let error_msg = error_msg.clone();
                // Re-check state under FOR UPDATE to guard against concurrent
                // completion or a duplicate scanner tick.
                let current_state: Option<String> = harvest_workflow_executions::table
                    .find(row.id)
                    .for_update()
                    .select(harvest_workflow_executions::state)
                    .first(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;

                if current_state.as_deref() != Some("RUNNING") {
                    return Ok((false, Vec::new(), Vec::new()));
                }

                store::append_single_event(conn, exec_id, fail_event).await?;

                // Transition to FAILED state.
                diesel::update(harvest_workflow_executions::table.find(row.id))
                    .set((
                        harvest_workflow_executions::state.eq("FAILED"),
                        harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                        harvest_workflow_executions::error.eq(Some(error_msg.clone())),
                        harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                // Cancel outstanding task queue rows.
                diesel::update(
                    harvest_task_queue::table
                        .filter(harvest_task_queue::workflow_exec_id.eq(row.id))
                        .filter(
                            harvest_task_queue::state
                                .eq("PENDING")
                                .or(harvest_task_queue::state.eq("RUNNING")),
                        ),
                )
                .set((
                    harvest_task_queue::state.eq("FAILED"),
                    harvest_task_queue::error.eq(Some(&error_msg)),
                    harvest_task_queue::completed_at.eq(Some(Utc::now())),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

                if let Some(parent_uuid) = parent_uuid {
                    wake_parent_for_child_timeout(
                        conn,
                        execution_id_from_uuid(parent_uuid),
                        exec_id,
                        &error_msg,
                    )
                    .await?;
                }

                let (mut deferred, closed_children) =
                    apply_parent_close_cascade(conn, exec_id).await?;
                let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::Failed,
                    Some(metrics),
                )
                .await?;
                deferred.extend(triggers);
                Ok((true, deferred, closed_children))
            }))
            .await?;

        if !applied {
            continue;
        }

        for start in deferred_starts {
            start.spawn();
        }

        tracing::warn!(
            exec_id = %exec_id,
            workflow_name = %workflow_name,
            ceiling = ceiling,
            "workflow execution terminated: history ceiling exceeded"
        );

        crate::telemetry::emit_workflow_terminal(
            metrics,
            &workflow_name,
            &queue_name,
            crate::telemetry::WorkflowStatus::Failed,
        );

        if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
            conn,
            exec_id,
            &workflow_name,
            Some(metrics),
        )
        .await
        {
            tracing::error!(
                exec_id = %exec_id,
                err = %e,
                "Failed to check and report unfinished handlers on history ceiling enforcement"
            );
        }

        for (child_id, child_name) in closed_children {
            if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
                conn,
                child_id,
                &child_name,
                Some(metrics),
            )
            .await
            {
                tracing::error!(
                    child_id = %child_id,
                    err = %e,
                    "Failed to check and report unfinished handlers on cascaded child execution in history ceiling"
                );
            }
        }

        // Best-effort: count ceiling failures toward the schedule auto-pause threshold.
        // Called after the transaction commits so a counter query failure cannot
        // roll back the terminal transition.
        crate::scheduler::maybe_increment_schedule_failure_counter(
            conn,
            &row.workflow_id,
            &workflow_name,
            None, // schedule_id not available in OversizedRow
            None, // origin not available in OversizedRow; NULL treated as 'scheduled' (backward-compat)
            metrics,
        )
        .await;
    }

    Ok(count)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── force_fail_activity classification truth table (issue #765) ────────

    fn forced_envelope() -> String {
        use crate::failure::IntoActivityErrorString;
        crate::failure::ActivityFailure::operator_force_failed(Some("ops")).into_error_payload()
    }

    #[test]
    fn classify_force_fail_workflow_task_is_never_forceable() {
        // task_type wins over every state, including RUNNING and an
        // envelope-shaped stored error.
        for state in ["RUNNING", "PENDING", "COMPLETED", "FAILED", "CANCELLED"] {
            assert!(
                matches!(
                    classify_force_fail_target("workflow", state, None),
                    ForceFailClassification::NotAnActivityTask
                ),
                "workflow task in state {state} must classify NotAnActivityTask"
            );
        }
        assert!(matches!(
            classify_force_fail_target("workflow", "FAILED", Some(&forced_envelope())),
            ForceFailClassification::NotAnActivityTask
        ));
    }

    #[test]
    fn classify_force_fail_running_activity_is_forceable() {
        assert!(matches!(
            classify_force_fail_target("activity", "RUNNING", None),
            ForceFailClassification::Forceable
        ));
        // A stale stored error from an earlier retry attempt does not change
        // the verdict for a currently-RUNNING row.
        assert!(matches!(
            classify_force_fail_target("activity", "RUNNING", Some("earlier transient error")),
            ForceFailClassification::Forceable
        ));
    }

    #[test]
    fn classify_force_fail_failed_with_forced_envelope_is_already_forced() {
        assert!(matches!(
            classify_force_fail_target("activity", "FAILED", Some(&forced_envelope())),
            ForceFailClassification::AlreadyForced
        ));
    }

    #[test]
    fn classify_force_fail_failed_with_genuine_error_is_not_running() {
        use crate::failure::IntoActivityErrorString;

        // A genuinely failed task (plain string error) is NOT the idempotent
        // no-op case — re-forcing it must conflict.
        assert!(matches!(
            classify_force_fail_target("activity", "FAILED", Some("connection refused")),
            ForceFailClassification::NotRunning
        ));
        // A typed-but-differently-caused failure is also a conflict.
        let other_typed = crate::failure::ActivityFailure::non_retryable("InvalidInput", "bad")
            .into_error_payload();
        assert!(matches!(
            classify_force_fail_target("activity", "FAILED", Some(&other_typed)),
            ForceFailClassification::NotRunning
        ));
        // FAILED with no stored error at all is a conflict, not idempotent.
        assert!(matches!(
            classify_force_fail_target("activity", "FAILED", None),
            ForceFailClassification::NotRunning
        ));
    }

    #[test]
    fn classify_force_fail_other_states_are_not_running() {
        for state in ["PENDING", "COMPLETED", "CANCELLED", "TIMED_OUT"] {
            assert!(
                matches!(
                    classify_force_fail_target("activity", state, None),
                    ForceFailClassification::NotRunning
                ),
                "activity task in state {state} must classify NotRunning"
            );
        }
    }

    // ── named_activity_has_terminal_event (issue #765, legacy-row 409) ─────

    fn scheduled(name: &str, activity_id: crate::types::ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: name.to_string(),
            input: serde_json::Value::Null,
            queue: "default".to_string(),
        }
    }

    #[test]
    fn named_terminal_true_when_scheduled_activity_completed() {
        let id = crate::types::ActivityExecId::new();
        let history = vec![
            scheduled("hung_activity", id),
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output: serde_json::Value::Null,
            },
        ];
        assert!(named_activity_has_terminal_event(&history, "hung_activity"));
    }

    #[test]
    fn named_terminal_false_when_activity_still_pending() {
        let id = crate::types::ActivityExecId::new();
        let history = vec![scheduled("hung_activity", id)];
        assert!(!named_activity_has_terminal_event(
            &history,
            "hung_activity"
        ));
    }

    #[test]
    fn named_terminal_false_when_never_scheduled() {
        let other = crate::types::ActivityExecId::new();
        let history = vec![
            scheduled("other_activity", other),
            WorkflowEvent::ActivityFailed {
                activity_id: other,
                error: "boom".to_string(),
                attempt: 1,
                error_type: "Error".to_string(),
                non_retryable: false,
                details: None,
            },
        ];
        assert!(!named_activity_has_terminal_event(
            &history,
            "hung_activity"
        ));
    }

    #[test]
    fn named_terminal_true_when_any_same_named_schedule_is_terminal() {
        // Two scheduled activities share a name; one terminal is enough —
        // this mirrors the name-based fallback's inability to tell which one
        // a legacy (activity_id = NULL) task row corresponds to.
        let a = crate::types::ActivityExecId::new();
        let b = crate::types::ActivityExecId::new();
        let history = vec![
            scheduled("hung_activity", a),
            scheduled("hung_activity", b),
            WorkflowEvent::ActivityTimedOut {
                activity_id: a,
                timeout_type: crate::error::TimeoutType::StartToClose,
            },
        ];
        assert!(named_activity_has_terminal_event(&history, "hung_activity"));
    }

    #[test]
    fn heartbeat_timeout_query_references_correct_table() {
        let sql = heartbeat_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(sql.contains("RUNNING"), "should filter for RUNNING state");
        assert!(
            sql.contains("heartbeat_timeout"),
            "should reference heartbeat_timeout column"
        );
        assert!(
            sql.contains("last_heartbeat_at"),
            "should reference last_heartbeat_at column"
        );
    }

    #[test]
    fn start_to_close_timeout_query_references_correct_columns() {
        let sql = start_to_close_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(sql.contains("RUNNING"), "should filter for RUNNING state");
        assert!(
            sql.contains("start_to_close"),
            "should reference start_to_close column"
        );
        assert!(
            sql.contains("started_at"),
            "should reference started_at column"
        );
    }

    /// Round-18 P2: the stale-scan guard must re-read the row under a lock and
    /// judge it against *real* current time.
    ///
    /// `FOR UPDATE` is what serializes this read against `resume_queue`'s
    /// `scheduled_at` shift; `clock_timestamp()` rather than `NOW()` is what
    /// keeps it honest, since `NOW()` is frozen at transaction start — before
    /// this transaction waited on the queue advisory lock — and would judge the
    /// deadline against a stale instant.
    #[cfg(feature = "db")]
    #[test]
    fn stale_scan_guard_relocks_the_row_and_uses_real_time() {
        let sql = schedule_to_start_still_expired_query();
        assert!(
            sql.contains("FOR UPDATE"),
            "the re-read must lock the row so it serializes against the resume \
             shift; got:\n{sql}"
        );
        assert!(
            sql.contains("clock_timestamp()"),
            "must compare against real current time, not the transaction's frozen \
             NOW(); got:\n{sql}"
        );
        assert!(
            !sql.contains("NOW()"),
            "NOW() is frozen at transaction start, i.e. before the advisory-lock \
             wait, so it would judge a credited-forward deadline as still expired; \
             got:\n{sql}"
        );
        assert!(
            sql.contains("scheduled_at + schedule_to_start"),
            "must recompute the deadline from the row's current columns; got:\n{sql}"
        );
        assert!(
            sql.contains("schedule_to_start IS NOT NULL"),
            "a row with no schedule-to-start has no such deadline to enforce; got:\n{sql}"
        );
    }

    /// Round-22 P2: the locked deadline re-read must never precede the
    /// execution-row lock.
    ///
    /// The documented `harvest_task_queue` order is execution row -> task row,
    /// and `resume_workflow_execution` follows it (execution `FOR UPDATE`, then
    /// a plain *waiting* `UPDATE` of this execution's task rows — that shift has
    /// no `SKIP LOCKED` escape, unlike its external-task sibling). Round 18 put
    /// the locked re-read before the execution lock and inverted it, so a resume
    /// racing a `schedule_to_start` sweep could deadlock and Postgres would abort
    /// one of them.
    ///
    /// Source-level because the hazard is *statement order*, which no SQL-shape
    /// assertion can see. Checked per enforcement function so a later edit that
    /// reintroduces the locked call early in either one fails here.
    #[test]
    fn locked_deadline_reread_never_precedes_the_execution_lock() {
        let src = include_str!("timeout.rs");
        for func in [
            "async fn enforce_activity_timeout(",
            "async fn enforce_workflow_timeout(",
        ] {
            let Some(start) = src.find(func) else {
                continue;
            };
            let body = &src[start..];
            // Bound the search to this function: the next `\nasync fn ` at column 0.
            let end = body[1..].find("\nasync fn ").map_or(body.len(), |o| o + 1);
            let body = &body[..end];

            let locked = body
                .find("schedule_to_start_still_expired(conn")
                .expect("each schedule_to_start path must keep the authoritative locked re-read");
            let exec_lock = body
                .find("lock_workflow_execution_row_and_load_history(conn")
                .expect("each schedule_to_start path must lock the execution row explicitly");
            assert!(
                exec_lock < locked,
                "{func}: the locked schedule_to_start re-read must come AFTER the \
                 execution-row lock, or it takes a task row first and deadlocks \
                 against resume_workflow_execution (execution row -> task rows)"
            );

            let unlocked = body
                .find("schedule_to_start_still_expired_unlocked(conn")
                .expect("the pre-execution-lock fast path must stay unlocked");
            assert!(
                unlocked < exec_lock,
                "{func}: the unlocked fast path exists to bail before the \
                 execution lock and history load; after it, it is pointless"
            );
        }
    }

    /// Audit export must run before the first `?` in the pass, and its own
    /// failure must never abort the pass (issue #953).
    ///
    /// Two directions, one hazard each:
    ///
    /// - **Export before the first `?`.** Every resident after the export call
    ///   can return `Err` and end the tick. If export moved below one of them, a
    ///   single permanently-failing resident — a task whose enforcement errors on
    ///   every pass — would stop compliance delivery for the whole shard
    ///   indefinitely, and the backlog would grow silently.
    /// - **Export's own error swallowed.** Conversely, if the export call grew a
    ///   `?`, a sink outage would abort timeout enforcement, SLA checks and
    ///   session cleanup — letting a compliance feature take down the scanner.
    ///
    /// Source-level for the same reason as
    /// `locked_deadline_reread_never_precedes_the_execution_lock` above: the
    /// hazard is *statement order*, which no behavioural assertion can see.
    ///
    /// It is also the only form this guard can take. The natural dynamic test —
    /// seed a due task whose enforcement fails, assert the export still happened
    /// — cannot be written: `harvest_task_queue.workflow_exec_id` is a foreign
    /// key, so a task naming a nonexistent execution cannot be inserted, and an
    /// unregistered payload codec decodes to the `undecodable_marker` rather
    /// than erroring (issue #608). There is no supported way to seed a
    /// deterministically-failing resident.
    #[test]
    fn audit_export_runs_before_the_first_fallible_resident() {
        let src = include_str!("timeout.rs");
        let start = src
            .find("pub async fn enforce_timeouts_once(")
            .expect("enforce_timeouts_once must exist");
        let body = &src[start..];
        let end = body[1..]
            .find("\npub async fn ")
            .map_or(body.len(), |o| o + 1);
        let body = &body[..end];

        let export = body
            .find("fire_due_audit_exports(")
            .expect("the pass must fire due audit exports");
        let first_fallible = body
            .find("find_timed_out_tasks(conn).await?")
            .expect("the timed-out-task scan must stay the first fallible resident");
        assert!(
            export < first_fallible,
            "audit export must run BEFORE the first resident that can `?` out of              the pass; below it, one permanently-failing task stops compliance              delivery for the whole shard"
        );

        let handled = &body[export..first_fallible];
        assert!(
            handled.contains("Err(error) => tracing::error!"),
            "the export call must log and continue on error, never `?`; a sink              outage must not abort timeout enforcement for the shard"
        );
        assert!(
            !handled.contains("fire_due_audit_exports(conn, sharded_pool, shard_assignments, metrics).await?"),
            "the export call must not propagate its error with `?`"
        );
    }

    /// The two deadline queries must differ only by `FOR UPDATE`, so the fast
    /// path can never disagree with the authoritative check about expiry.
    #[test]
    fn the_two_deadline_queries_differ_only_by_for_update() {
        let locked = schedule_to_start_still_expired_query();
        let unlocked = schedule_to_start_still_expired_unlocked_query();
        assert!(
            locked.ends_with(" FOR UPDATE"),
            "the authoritative re-read must lock the row; got:\n{locked}"
        );
        assert!(
            !unlocked.contains("FOR UPDATE"),
            "the fast path must take no lock, or it reintroduces the inverted \
             lock order it exists to avoid; got:\n{unlocked}"
        );
        assert_eq!(
            locked.trim_end_matches(" FOR UPDATE"),
            unlocked,
            "the two deadline queries must be identical apart from FOR UPDATE"
        );
    }

    /// This query is a `const fn` and so must hardcode its anti-join rather
    /// than calling the renderer. Pin the two together, or a fix applied to
    /// [`crate::activity_pause::activity_pause_anti_join`] silently misses the
    /// scan that decides whether a held task is terminally failed (issue #807
    /// AC3) — the highest-consequence of the three copies.
    #[test]
    fn schedule_to_start_scan_matches_the_shared_activity_pause_renderer() {
        let rendered = crate::activity_pause::activity_pause_anti_join("t");
        assert!(
            schedule_to_start_timeout_query().contains(&rendered),
            "the hardcoded activity-pause anti-join has drifted from the shared \
             renderer.\nexpected to find:\n  {rendered}\nin:\n{}",
            schedule_to_start_timeout_query()
        );
    }

    #[test]
    fn schedule_to_start_timeout_query_references_correct_columns() {
        let sql = schedule_to_start_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(sql.contains("PENDING"), "should filter for PENDING state");
        assert!(
            sql.contains("schedule_to_start"),
            "should reference schedule_to_start column"
        );
        assert!(
            sql.contains("scheduled_at"),
            "should reference scheduled_at column"
        );
    }

    #[test]
    fn schedule_to_close_timeout_query_references_correct_columns() {
        let sql = schedule_to_close_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(
            sql.contains("RUNNING") && sql.contains("PENDING"),
            "should filter for both RUNNING and PENDING states"
        );
        assert!(
            sql.contains("schedule_to_close_at"),
            "should reference schedule_to_close_at column"
        );
        assert!(sql.contains("NOW()"), "should compare against NOW()");
    }

    #[test]
    fn schedule_to_close_timeout_query_excludes_paused_executions() {
        // AC5 (issue #609): the cross-retry wall-clock deadline is suspended
        // while the owning execution is PAUSED — the scanner must skip those
        // tasks; resume shifts `schedule_to_close_at` forward by the pause
        // span instead.
        let sql = schedule_to_close_timeout_query();
        assert!(
            sql.contains("PAUSED"),
            "must exclude tasks whose owning execution is PAUSED"
        );
        assert!(
            sql.contains("harvest_workflow_executions"),
            "must consult the owning execution's state"
        );
        assert!(
            sql.contains("NOT EXISTS"),
            "an orphan task (NULL workflow_exec_id) must remain enforceable"
        );
        // The in-flight enforcement reasons deliberately stay pause-blind:
        // already-dispatched work runs to completion (issue #383), so a hung
        // in-flight activity of a paused execution must still time out.
        // (schedule_to_start has its own narrowly-scoped frozen-row
        // carve-out — see the dedicated test below.)
        assert!(!heartbeat_timeout_query().contains("PAUSED"));
        assert!(!start_to_close_timeout_query().contains("PAUSED"));
    }

    #[test]
    fn schedule_to_start_timeout_query_excludes_only_frozen_paused_rows() {
        // Finding 3 (issue #609 post-review hardening): the pause-aware
        // ScheduleToClose treatment created a frozen state — a PENDING row of
        // a PAUSED execution past its (unshifted) schedule_to_close deadline,
        // unclaimable until resume shifts it. The ScheduleToStart scanner
        // must spare exactly those frozen rows, and ONLY those: an unfrozen
        // pending activity of a paused execution is still claimable by
        // design (activities are not pause-gated), so its schedule_to_start
        // signal (worker capacity) remains genuine and stays enforced.
        let sql = schedule_to_start_timeout_query();
        assert!(
            sql.contains("PAUSED"),
            "frozen rows of paused executions must be spared"
        );
        assert!(
            sql.contains("schedule_to_close_at IS NOT NULL")
                && sql.contains("schedule_to_close_at <= NOW()"),
            "the exclusion must be scoped to rows past their (unshifted) \
             cross-retry deadline — never a blanket paused-execution exclusion"
        );
        assert!(
            sql.contains("NOT ("),
            "the frozen-row predicate must negate the full conjunction so a \
             claimable (unfrozen) row of a paused execution stays enforceable"
        );
        assert!(
            sql.contains("EXISTS"),
            "the freeze requires the owning execution to actually be PAUSED"
        );
    }

    // ── locked PAUSED re-check (issue #609, second bot-review round) ────────

    #[test]
    fn pause_recheck_never_suppresses_a_running_execution() {
        let now = Utc::now();
        let elapsed = Some(now - chrono::Duration::minutes(1));
        for reason in [
            TimeoutReason::Heartbeat,
            TimeoutReason::StartToClose,
            TimeoutReason::ScheduleToStart,
            TimeoutReason::ScheduleToClose,
        ] {
            assert!(
                !pause_suppresses_timeout_enforcement(&reason, "RUNNING", elapsed, now),
                "{reason} must enforce normally against a RUNNING execution"
            );
        }
    }

    #[test]
    fn pause_recheck_suppresses_schedule_to_close_while_paused() {
        // The P2 race this round closes for the scanner path: a pause
        // committing after the scan snapshot (or while enforcement waits on
        // the execution row lock) suspends the cross-retry deadline clock,
        // so ScheduleToClose enforcement must always yield to it.
        let now = Utc::now();
        assert!(pause_suppresses_timeout_enforcement(
            &TimeoutReason::ScheduleToClose,
            "PAUSED",
            Some(now - chrono::Duration::minutes(1)),
            now
        ));
        // The external-task path carries no separate row deadline argument —
        // the blanket ScheduleToClose suppression must not depend on it.
        assert!(pause_suppresses_timeout_enforcement(
            &TimeoutReason::ScheduleToClose,
            "PAUSED",
            None,
            now
        ));
    }

    #[test]
    fn pause_recheck_suppresses_schedule_to_start_only_for_frozen_rows() {
        // A row unfrozen at scan time (execution RUNNING) can become frozen
        // (pause commits + deadline elapses) before enforcement locks the
        // row — the re-check must spare exactly the now-frozen row, and only
        // it: an unfrozen pending row of a paused execution stays claimable
        // by design, so its schedule-to-start signal stays enforced.
        let now = Utc::now();
        assert!(
            pause_suppresses_timeout_enforcement(
                &TimeoutReason::ScheduleToStart,
                "PAUSED",
                Some(now - chrono::Duration::seconds(1)),
                now
            ),
            "frozen (deadline elapsed) rows must be spared"
        );
        assert!(
            !pause_suppresses_timeout_enforcement(
                &TimeoutReason::ScheduleToStart,
                "PAUSED",
                Some(now + chrono::Duration::minutes(10)),
                now
            ),
            "an unfrozen row (deadline still ahead) stays enforced"
        );
        assert!(
            !pause_suppresses_timeout_enforcement(
                &TimeoutReason::ScheduleToStart,
                "PAUSED",
                None,
                now
            ),
            "a row with no cross-retry deadline can never be frozen"
        );
    }

    #[test]
    fn pause_recheck_keeps_in_flight_reasons_pause_blind() {
        // Already-dispatched work runs to completion under pause (issue
        // #383): a hung in-flight activity of a paused execution still times
        // out on its own merits.
        let now = Utc::now();
        let elapsed = Some(now - chrono::Duration::minutes(1));
        assert!(!pause_suppresses_timeout_enforcement(
            &TimeoutReason::Heartbeat,
            "PAUSED",
            elapsed,
            now
        ));
        assert!(!pause_suppresses_timeout_enforcement(
            &TimeoutReason::StartToClose,
            "PAUSED",
            elapsed,
            now
        ));
    }

    // ── external-task locked re-read verdict (issue #609, third bot-review
    //    round: task-row-first lock ordering) ─────────────────────────────

    #[test]
    fn external_task_recheck_enforces_a_still_open_expired_row() {
        let now = Utc::now();
        assert!(
            external_task_timeout_still_due("PENDING", now - chrono::Duration::seconds(1), now),
            "a still-PENDING row past its deadline is an enforceable timeout"
        );
    }

    #[test]
    fn external_task_recheck_skips_a_concurrently_resolved_row() {
        // A completion/failure that won the race after the scan snapshot must
        // make the scanner skip: no flip, no ActivityTimedOut, not counted.
        let now = Utc::now();
        let elapsed = now - chrono::Duration::seconds(1);
        for state in ["COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"] {
            assert!(
                !external_task_timeout_still_due(state, elapsed, now),
                "a {state} row must not be timed out a second time"
            );
        }
    }

    #[test]
    fn external_task_recheck_skips_a_deadline_shifted_into_the_future() {
        // extend_deadline — or a resume's pause-span shift (issue #609) —
        // pushing schedule_to_close_at past NOW() after the scan snapshot
        // must make the scanner skip the row.
        let now = Utc::now();
        assert!(!external_task_timeout_still_due(
            "PENDING",
            now + chrono::Duration::minutes(5),
            now
        ));
        // Boundary: a deadline exactly at NOW() is not yet elapsed (the scan
        // itself uses a strict `<`).
        assert!(!external_task_timeout_still_due("PENDING", now, now));
    }

    #[test]
    fn timeout_reason_schedule_to_close_maps_correctly() {
        assert_eq!(
            TimeoutReason::ScheduleToClose.to_string(),
            "ScheduleToClose"
        );
        assert_eq!(
            TimeoutReason::ScheduleToClose.timeout_type(),
            TimeoutType::ScheduleToClose
        );
        // Both RUNNING and PENDING are valid states for ScheduleToClose
        let states = expected_task_states_for_timeout(&TimeoutReason::ScheduleToClose);
        assert!(states.contains(&"RUNNING"), "RUNNING must be a valid state");
        assert!(states.contains(&"PENDING"), "PENDING must be a valid state");
        // Other reasons must NOT include both states
        let heartbeat_states = expected_task_states_for_timeout(&TimeoutReason::Heartbeat);
        assert_eq!(heartbeat_states, &["RUNNING"]);
        let sched_start_states = expected_task_states_for_timeout(&TimeoutReason::ScheduleToStart);
        assert_eq!(sched_start_states, &["PENDING"]);
    }

    #[test]
    fn timeout_reason_display() {
        assert_eq!(TimeoutReason::Heartbeat.to_string(), "Heartbeat");
        assert_eq!(TimeoutReason::StartToClose.to_string(), "StartToClose");
        assert_eq!(
            TimeoutReason::ScheduleToStart.to_string(),
            "ScheduleToStart"
        );
    }

    #[test]
    fn timeout_reason_equality() {
        assert_eq!(TimeoutReason::Heartbeat, TimeoutReason::Heartbeat);
        assert_ne!(TimeoutReason::Heartbeat, TimeoutReason::StartToClose);
    }

    // ── Workflow execution timeout query tests (issue #243) ──────────────────

    #[test]
    fn workflow_execution_timeout_query_references_correct_table_and_columns() {
        let sql = workflow_execution_timeout_query();
        assert!(
            sql.contains("harvest_workflow_executions"),
            "must query harvest_workflow_executions"
        );
        assert!(sql.contains("RUNNING"), "must filter for RUNNING state");
        assert!(
            sql.contains("deadline_at"),
            "must reference deadline_at column"
        );
        assert!(sql.contains("NOW()"), "must compare against NOW()");
    }

    // ── Chain-scoped lifetime cap tests (issue #617) ─────────────────────────

    #[test]
    fn chain_timeout_query_includes_the_chain_deadline_disjunct() {
        let sql = workflow_execution_timeout_query();
        // Must still enforce the per-run deadline (issue #243)…
        assert!(
            sql.contains("deadline_at IS NOT NULL"),
            "must still reference the per-run deadline_at"
        );
        // …AND the chain deadline disjunct (issue #617).
        assert!(
            sql.contains("chain_deadline_at IS NOT NULL"),
            "must reference chain_deadline_at IS NOT NULL"
        );
        assert!(
            sql.contains("chain_deadline_at < NOW()"),
            "must compare chain_deadline_at against NOW()"
        );
        assert!(
            sql.contains(" OR "),
            "the two deadlines must be OR'd so either can fire"
        );
    }

    #[test]
    fn effective_chain_timeout_truth_table() {
        use chrono::Duration;
        let w = Duration::hours(10);
        let c = Duration::hours(3);
        // both → min
        assert_eq!(effective_chain_timeout(Some(w), Some(c)), Some(c));
        assert_eq!(
            effective_chain_timeout(Some(c), Some(w)),
            Some(c),
            "min is symmetric"
        );
        // only workflow → workflow
        assert_eq!(effective_chain_timeout(Some(w), None), Some(w));
        // only ceiling → ceiling (the #243 divergence: ceiling doubles as a default)
        assert_eq!(effective_chain_timeout(None, Some(c)), Some(c));
        // neither → None
        assert_eq!(effective_chain_timeout(None, None), None);
    }

    #[test]
    fn classify_workflow_timeout_chain_takes_precedence() {
        use chrono::Duration;
        let now = Utc::now();
        let run = now - Duration::seconds(10);
        let chain = now - Duration::seconds(5);
        // Both fired → chain wins (precedence).
        let (fired, kind) = classify_workflow_timeout(Some(run), Some(chain), now);
        assert_eq!(kind, TimeoutKind::Chain);
        assert_eq!(fired, chain);
    }

    #[test]
    fn classify_workflow_timeout_run_only() {
        use chrono::Duration;
        let now = Utc::now();
        let run = now - Duration::seconds(10);
        // Chain deadline in the future (not fired) → run wins.
        let future_chain = now + Duration::hours(1);
        let (fired, kind) = classify_workflow_timeout(Some(run), Some(future_chain), now);
        assert_eq!(kind, TimeoutKind::Run);
        assert_eq!(fired, run);
        // No chain deadline at all → run wins.
        let (fired2, kind2) = classify_workflow_timeout(Some(run), None, now);
        assert_eq!(kind2, TimeoutKind::Run);
        assert_eq!(fired2, run);
    }

    #[test]
    fn classify_workflow_timeout_chain_only_does_not_panic() {
        use chrono::Duration;
        let now = Utc::now();
        let chain = now - Duration::seconds(5);
        // A chain-only expiry has no per-run deadline_at — must NOT panic.
        let (fired, kind) = classify_workflow_timeout(None, Some(chain), now);
        assert_eq!(kind, TimeoutKind::Chain);
        assert_eq!(fired, chain);
    }

    #[test]
    fn chain_deadline_checked_add_overflow_yields_none_not_panic() {
        use chrono::Duration;
        // Because the chain ceiling doubles as the effective value (AC4), an
        // absurd operator ceiling can reach `Duration::MAX`, and
        // `effective_chain_timeout(None, ceiling)` returns it verbatim.
        let effective = effective_chain_timeout(None, Some(Duration::MAX));
        assert_eq!(effective, Some(Duration::MAX));
        // The start path adds it to the start instant with `checked_add_signed`
        // so an overflow yields `None` (no chain cap) rather than panicking, which
        // is exactly what the fresh-start / reset / child sites now do.
        let start = Utc::now();
        let chain_deadline_at = effective.and_then(|d| start.checked_add_signed(d));
        assert_eq!(
            chain_deadline_at, None,
            "Duration::MAX must overflow to None, never panic"
        );
        // A sane duration must still produce a deadline.
        let sane = effective_chain_timeout(None, Some(Duration::hours(1)));
        let ok = sane.and_then(|d| start.checked_add_signed(d));
        assert!(
            ok.is_some(),
            "a representable chain cap must yield a deadline"
        );
    }

    #[test]
    fn chain_timeout_metric_has_correct_name() {
        assert_eq!(
            crate::telemetry::METRIC_WORKFLOW_CHAIN_TIMEOUT,
            "harvest.workflow.chain_timeout"
        );
    }

    #[test]
    fn record_workflow_chain_timeout_is_callable_on_no_op_recorder() {
        use crate::telemetry::MetricsRecorder;
        struct NoOp;
        impl MetricsRecorder for NoOp {}
        NoOp.record_workflow_chain_timeout("my_workflow", "default");
    }

    #[test]
    fn timeout_type_workflow_chain_has_distinct_display() {
        use crate::error::TimeoutType;
        // Consistent with every sibling variant, Display renders the variant name.
        assert_eq!(TimeoutType::WorkflowChain.to_string(), "WorkflowChain");
        assert_ne!(
            TimeoutType::WorkflowChain.to_string(),
            TimeoutType::WorkflowExecution.to_string()
        );
    }

    // ── Soft SLA breach scanner tests (issue #487) ────────────────────────────

    #[test]
    fn sla_breached_metric_has_correct_name() {
        assert_eq!(
            crate::telemetry::METRIC_WORKFLOW_SLA_BREACHED,
            "harvest.workflow.sla_breached"
        );
    }

    #[test]
    fn record_workflow_sla_breach_is_callable_on_no_op_recorder() {
        use crate::telemetry::MetricsRecorder;
        struct NoOp;
        impl MetricsRecorder for NoOp {}
        // Must not panic; default no-op implementation.
        NoOp.record_workflow_sla_breach("my_workflow", "default");
    }

    #[test]
    fn sla_breach_spy_records_one_call_per_breach() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct SpyRecorder {
            breaches: Mutex<Vec<(String, String)>>,
        }
        impl crate::telemetry::MetricsRecorder for SpyRecorder {
            fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
                self.breaches
                    .lock()
                    .unwrap()
                    .push((workflow_name.to_owned(), queue.to_owned()));
            }
        }

        let spy = SpyRecorder::default();
        spy.record_workflow_sla_breach("slow_workflow", "priority-queue");
        spy.record_workflow_sla_breach("slow_workflow", "priority-queue");

        let b = spy.breaches.lock().unwrap().clone();
        assert_eq!(b.len(), 2, "spy records every call; idempotency is DB-side");
        assert_eq!(
            b[0],
            ("slow_workflow".to_owned(), "priority-queue".to_owned())
        );
    }
}
