//! External (task-token) activities: the handoff table and its writes.
//!
//! `ctx.execute_activity_external` emits `ScheduleExternalActivity` with a
//! fresh token. The decision cycle records the handoff in one transaction:
//! the `ActivityAwaitingExternal` event, a `harvest_external_tasks` row and a
//! deadline row in `harvest_timers`. A dispatcher reads the `PENDING` rows and
//! settles each token through
//! [`SqliteRuntime::complete_external`](crate::SqliteRuntime::complete_external)
//! or [`SqliteRuntime::fail_external`](crate::SqliteRuntime::fail_external).
//!
//! The deadline fires through the timer path. A timer id with the prefix
//! [`DEADLINE_TIMER_PREFIX`] writes `ActivityTimedOut { ScheduleToClose }`,
//! not `TimerFired`. This is the event that the Postgres timeout scanner
//! writes, so a history replays on both backends.
//!
//! A row leaves `PENDING` once. A settle deletes the deadline timer, and a
//! fire checks the state first, so a token never gets a second result. Each
//! terminal setter of a run sets its `PENDING` rows `CANCELLED`. A dispatcher
//! therefore never reads a handoff that no run waits on.

use std::fmt;

use autumn_harvest::{
    ActivityExecId, ExecutionId, ExternalActivityToken, TimeoutType, WorkflowEvent,
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};
use crate::{queue, store};

/// The timer id prefix of an external activity deadline. The token follows it.
pub const DEADLINE_TIMER_PREFIX: &str = "__external_deadline:";

/// The default row limit of [`ExternalHandoffFilter`], as on Postgres.
pub const DEFAULT_HANDOFF_LIMIT: u32 = 100;

/// The largest row limit of [`ExternalHandoffFilter`]. A larger limit is cut
/// to it.
pub const MAX_HANDOFF_LIMIT: u32 = 1000;

/// The state of one external activity handoff.
///
/// The names match the Postgres `harvest_external_tasks.state` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalTaskState {
    /// The run waits for a result.
    Pending,
    /// A caller completed the token.
    Completed,
    /// A caller failed the token.
    Failed,
    /// The schedule-to-close deadline fired first.
    TimedOut,
    /// The run ended before a result arrived.
    Cancelled,
}

impl ExternalTaskState {
    /// The stored name of the state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::TimedOut => "TIMED_OUT",
            Self::Cancelled => "CANCELLED",
        }
    }
}

impl fmt::Display for ExternalTaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ExternalTaskState {
    /// The state of a stored name. Any other name is a damaged row.
    fn parse(stored: &str) -> SqliteResult<Self> {
        match stored {
            "PENDING" => Ok(Self::Pending),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "TIMED_OUT" => Ok(Self::TimedOut),
            "CANCELLED" => Ok(Self::Cancelled),
            _ => Err(SqliteError::corrupt("harvest_external_tasks.state")),
        }
    }
}

/// The filter of [`SqliteRuntime::list_external_handoffs`](crate::SqliteRuntime::list_external_handoffs).
///
/// Each set field narrows the read. [`Self::new`] reads every state, up to
/// [`DEFAULT_HANDOFF_LIMIT`] rows. [`Self::pending`] reads the `PENDING`
/// rows, which is the dispatcher read. The struct is `#[non_exhaustive]`, so
/// build it with these functions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExternalHandoffFilter {
    /// Only rows in this state.
    pub state: Option<ExternalTaskState>,
    /// Only rows of this activity name.
    pub activity_name: Option<String>,
    /// Only rows of this run.
    pub execution_id: Option<ExecutionId>,
    /// Only the row of this token.
    pub token: Option<ExternalActivityToken>,
    /// The most rows to return. `None` reads [`DEFAULT_HANDOFF_LIMIT`]
    /// rows. The read cuts a value to the range 1 to [`MAX_HANDOFF_LIMIT`].
    pub limit: Option<u32>,
}

impl ExternalHandoffFilter {
    /// A filter that reads every state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The `PENDING` rows: the handoffs that wait for a result.
    #[must_use]
    pub fn pending() -> Self {
        Self::new().state(ExternalTaskState::Pending)
    }

    /// Keep only the rows in `state`.
    #[must_use]
    pub const fn state(mut self, state: ExternalTaskState) -> Self {
        self.state = Some(state);
        self
    }

    /// Keep only the rows of the activity `name`.
    #[must_use]
    pub fn activity_name(mut self, name: impl Into<String>) -> Self {
        self.activity_name = Some(name.into());
        self
    }

    /// Keep only the rows of the run `exec`.
    #[must_use]
    pub const fn execution_id(mut self, exec: ExecutionId) -> Self {
        self.execution_id = Some(exec);
        self
    }

    /// Keep only the row of `token`.
    #[must_use]
    pub const fn token(mut self, token: ExternalActivityToken) -> Self {
        self.token = Some(token);
        self
    }

    /// Return at most `limit` rows. The read cuts it to the range 1 to
    /// [`MAX_HANDOFF_LIMIT`].
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// The row limit of the read.
    fn page_size(&self) -> u32 {
        self.limit
            .unwrap_or(DEFAULT_HANDOFF_LIMIT)
            .clamp(1, MAX_HANDOFF_LIMIT)
    }
}

/// One external activity handoff.
///
/// A dispatcher sends `input` to the external system, and settles `token`
/// with the reply. Unlike the Postgres operator row, this row carries the
/// input, because a dispatcher on one file has no other read of it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExternalHandoff {
    /// The single-use token that settles the activity.
    pub token: ExternalActivityToken,
    /// The run that waits.
    pub execution_id: ExecutionId,
    /// The business id of that run.
    pub workflow_id: String,
    /// The workflow type of that run.
    pub workflow_name: String,
    /// The activity id in the history of that run.
    pub activity_id: ActivityExecId,
    /// The activity name that the workflow gave.
    pub activity_name: String,
    /// The queue that the workflow gave. Routing is a no-op on this backend.
    pub queue: String,
    /// The activity input.
    pub input: Value,
    /// The state of the handoff.
    pub state: ExternalTaskState,
    /// When the decision cycle recorded the handoff.
    pub created_at: DateTime<Utc>,
    /// When the state or the deadline last changed.
    pub updated_at: DateTime<Utc>,
    /// The schedule-to-close deadline.
    pub deadline_at: DateTime<Utc>,
}

/// The fields of one `ScheduleExternalActivity` command.
pub struct Handoff<'a> {
    pub activity_id: ActivityExecId,
    pub token: ExternalActivityToken,
    pub name: &'a str,
    pub input: &'a Value,
    pub queue: &'a str,
    pub schedule_to_close_secs: u64,
}

/// The stored state of one token, as a settle reads it.
pub struct Task {
    pub exec: ExecutionId,
    pub activity_id: ActivityExecId,
    pub state: ExternalTaskState,
    /// The deadline in epoch milliseconds.
    pub deadline_at: i64,
}

/// The latest deadline that a handoff stores, in epoch milliseconds.
///
/// A longer deadline saturates to it. A later instant has no `DateTime`
/// form, so the read of its row would fail.
const fn latest_deadline() -> i64 {
    DateTime::<Utc>::MAX_UTC.timestamp_millis()
}

/// `deadline_at`, cut to [`latest_deadline`].
pub fn bounded_deadline(deadline_at: i64) -> i64 {
    deadline_at.min(latest_deadline())
}

/// The timer id of the deadline of `token`.
#[must_use]
pub fn deadline_timer_id(token: ExternalActivityToken) -> String {
    format!("{DEADLINE_TIMER_PREFIX}{token}")
}

/// The token of a deadline timer id, or `None` for any other timer.
#[must_use]
pub fn deadline_token(timer_id: &str) -> Option<ExternalActivityToken> {
    timer_id
        .strip_prefix(DEADLINE_TIMER_PREFIX)
        .and_then(|token| token.parse().ok())
}

/// Record a new handoff in the caller's decision transaction. Returns `true`
/// if it wrote the handoff.
///
/// A parked body emits the command again on each replay, with the same token.
/// A token that `history` already holds writes nothing. So a re-park adds no
/// event, and its deadline still counts from the first record.
pub fn record(
    conn: &Connection,
    exec: ExecutionId,
    history: &[WorkflowEvent],
    handoff: &Handoff<'_>,
    now: i64,
    deadline_at: i64,
) -> SqliteResult<bool> {
    let deadline_at = bounded_deadline(deadline_at);
    let recorded = history.iter().any(|event| {
        matches!(
            event,
            WorkflowEvent::ActivityAwaitingExternal { token, .. } if *token == handoff.token
        )
    });
    if recorded {
        return Ok(false);
    }
    store::append_event(
        conn,
        exec,
        &WorkflowEvent::ActivityAwaitingExternal {
            activity_id: handoff.activity_id,
            token: handoff.token,
            name: handoff.name.to_string(),
            input: handoff.input.clone(),
            queue: handoff.queue.to_string(),
            schedule_to_close_secs: handoff.schedule_to_close_secs,
        },
    )?;
    conn.execute(
        INSERT_SQL,
        params![
            handoff.token.to_string(),
            exec.to_string(),
            handoff.activity_id.to_string(),
            handoff.name,
            handoff.queue,
            serde_json::to_string(handoff.input)?,
            now,
            deadline_at,
        ],
    )?;
    queue::enqueue_timer(conn, exec, &deadline_timer_id(handoff.token), deadline_at)?;
    Ok(true)
}

/// A token is unique, so a second insert of the same token changes nothing.
const INSERT_SQL: &str = "INSERT OR IGNORE INTO harvest_external_tasks \
     (token, exec_id, activity_id, name, queue, input_json, state, created_at, updated_at, \
      deadline_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'PENDING', ?7, ?7, ?8)";

/// The stored state of `token`, or `None` for an unknown token.
pub fn find(conn: &Connection, token: ExternalActivityToken) -> SqliteResult<Option<Task>> {
    let row: Option<(String, String, String, i64)> = conn
        .query_row(
            "SELECT exec_id, activity_id, state, deadline_at FROM harvest_external_tasks \
             WHERE token = ?1",
            params![token.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    row.map(|(exec, activity_id, state, deadline_at)| {
        Ok(Task {
            exec: exec
                .parse()
                .map_err(|_| SqliteError::corrupt("harvest_external_tasks.exec_id"))?,
            activity_id: activity_id
                .parse()
                .map_err(|_| SqliteError::corrupt("harvest_external_tasks.activity_id"))?,
            state: ExternalTaskState::parse(&state)?,
            deadline_at,
        })
    })
    .transpose()
}

/// Move a `PENDING` token to `state`, append `event` to its run, and delete
/// its deadline timer, in the caller's transaction.
///
/// The caller checks that the token is `PENDING` and that its run is
/// `RUNNING`. The state update repeats the `PENDING` check, so a stale read
/// cannot write a second result.
pub fn settle(
    conn: &Connection,
    token: ExternalActivityToken,
    task: &Task,
    state: ExternalTaskState,
    event: &WorkflowEvent,
    now: i64,
) -> SqliteResult<bool> {
    let changed = conn.execute(
        "UPDATE harvest_external_tasks SET state = ?2, updated_at = ?3 \
         WHERE token = ?1 AND state = 'PENDING'",
        params![token.to_string(), state.as_str(), now],
    )?;
    if changed == 0 {
        return Ok(false);
    }
    store::append_event(conn, task.exec, event)?;
    queue::delete_pending_timer(conn, task.exec, &deadline_timer_id(token))?;
    Ok(true)
}

/// Move the deadline of a `PENDING` token to `deadline_at`, in the caller's
/// transaction. The deadline timer moves with it, and the run records
/// `ActivityExternalDeadlineExtended`, as on Postgres.
pub fn extend(
    conn: &Connection,
    token: ExternalActivityToken,
    task: &Task,
    deadline_at: i64,
    now: i64,
) -> SqliteResult<()> {
    let deadline_at = bounded_deadline(deadline_at);
    conn.execute(
        "UPDATE harvest_external_tasks SET deadline_at = ?2, updated_at = ?3 \
         WHERE token = ?1 AND state = 'PENDING'",
        params![token.to_string(), deadline_at, now],
    )?;
    conn.execute(
        "UPDATE harvest_timers SET fire_at = ?3 \
         WHERE exec_id = ?1 AND timer_id = ?2 AND fired = 0",
        params![task.exec.to_string(), deadline_timer_id(token), deadline_at],
    )?;
    store::append_event(conn, task.exec, &extended_event(token, task))
}

/// The event that an extension of `token` appends.
pub const fn extended_event(token: ExternalActivityToken, task: &Task) -> WorkflowEvent {
    WorkflowEvent::ActivityExternalDeadlineExtended {
        activity_id: task.activity_id,
        token,
    }
}

/// Fire the deadline timer `timer_id` of `exec`, in the caller's transaction.
///
/// A `PENDING` token times out: the run records
/// `ActivityTimedOut { ScheduleToClose }`, and the row becomes `TIMED_OUT`.
/// A settled token keeps its result, and the fire writes no event. In both
/// cases, the fire marks the timer row fired, so it never fires again.
pub fn fire_deadline(conn: &Connection, exec: ExecutionId, timer_id: &str) -> SqliteResult<()> {
    if let Some(token) = deadline_token(timer_id)
        && let Some(task) = find(conn, token)?
        && task.state == ExternalTaskState::Pending
        && task.exec == exec
    {
        // The row times out at its own deadline. The timer row holds the same
        // instant.
        conn.execute(
            "UPDATE harvest_external_tasks SET state = 'TIMED_OUT', updated_at = deadline_at \
             WHERE token = ?1 AND state = 'PENDING'",
            params![token.to_string()],
        )?;
        store::append_event(
            conn,
            exec,
            &WorkflowEvent::ActivityTimedOut {
                activity_id: task.activity_id,
                timeout_type: TimeoutType::ScheduleToClose,
            },
        )?;
    }
    queue::mark_timer_fired(conn, exec, timer_id)
}

/// Time out `token` if its deadline has passed at `now`, in the caller's
/// transaction. Returns `true` if the token timed out.
///
/// The deadline fires only when a drive runs the worker pass. A settle or an
/// extension calls this first, so a reply after the deadline loses, as on
/// Postgres, where a scanner times the row out.
pub fn time_out_if_due(
    conn: &Connection,
    token: ExternalActivityToken,
    task: &Task,
    now: i64,
) -> SqliteResult<bool> {
    if task.state != ExternalTaskState::Pending || task.deadline_at > now {
        return Ok(false);
    }
    fire_deadline(conn, task.exec, &deadline_timer_id(token))?;
    Ok(true)
}

/// Set every `PENDING` token of `exec` `CANCELLED`, in the caller's
/// transaction. Each terminal setter of a run calls it. A later settle of
/// such a token returns `false`.
pub fn cancel_pending(conn: &Connection, exec: ExecutionId) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_external_tasks SET state = 'CANCELLED', updated_at = ?2 \
         WHERE exec_id = ?1 AND state = 'PENDING'",
        params![exec.to_string(), Utc::now().timestamp_millis()],
    )?;
    Ok(())
}

/// The handoffs that `filter` selects, by deadline, then by the newest
/// update, then by token. This is the Postgres order.
pub fn list(
    conn: &Connection,
    filter: &ExternalHandoffFilter,
) -> SqliteResult<Vec<ExternalHandoff>> {
    let limit = i64::from(filter.page_size());
    let mut stmt = conn.prepare_cached(LIST_SQL)?;
    let rows = stmt.query_map(
        params![
            filter.state.map(ExternalTaskState::as_str),
            filter.activity_name,
            filter.execution_id.map(|e| e.to_string()),
            filter.token.map(|t| t.to_string()),
            limit,
        ],
        |row| {
            Ok(RawHandoff {
                token: row.get(0)?,
                exec_id: row.get(1)?,
                workflow_id: row.get(2)?,
                workflow_name: row.get(3)?,
                activity_id: row.get(4)?,
                name: row.get(5)?,
                queue: row.get(6)?,
                input_json: row.get(7)?,
                state: row.get(8)?,
                created_at: row.get(9)?,
                updated_at: row.get(10)?,
                deadline_at: row.get(11)?,
            })
        },
    )?;
    rows.map(|row| row?.decode()).collect()
}

/// A `NULL` parameter matches every row.
const LIST_SQL: &str = "SELECT x.token, x.exec_id, e.workflow_id, e.workflow_name, \
       x.activity_id, x.name, x.queue, x.input_json, x.state, x.created_at, x.updated_at, \
       x.deadline_at \
     FROM harvest_external_tasks x \
     JOIN harvest_executions e ON e.exec_id = x.exec_id \
     WHERE (?1 IS NULL OR x.state = ?1) \
       AND (?2 IS NULL OR x.name = ?2) \
       AND (?3 IS NULL OR x.exec_id = ?3) \
       AND (?4 IS NULL OR x.token = ?4) \
     ORDER BY x.deadline_at ASC, x.updated_at DESC, x.token ASC \
     LIMIT ?5";

/// One stored row before the decode.
struct RawHandoff {
    token: String,
    exec_id: String,
    workflow_id: String,
    workflow_name: String,
    activity_id: String,
    name: String,
    queue: String,
    input_json: String,
    state: String,
    created_at: i64,
    updated_at: i64,
    deadline_at: i64,
}

impl RawHandoff {
    fn decode(self) -> SqliteResult<ExternalHandoff> {
        let time = |ms: i64, field: &str| {
            DateTime::from_timestamp_millis(ms).ok_or_else(|| SqliteError::corrupt(field))
        };
        Ok(ExternalHandoff {
            token: self
                .token
                .parse()
                .map_err(|_| SqliteError::corrupt("harvest_external_tasks.token"))?,
            execution_id: self
                .exec_id
                .parse()
                .map_err(|_| SqliteError::corrupt("harvest_external_tasks.exec_id"))?,
            workflow_id: self.workflow_id,
            workflow_name: self.workflow_name,
            activity_id: self
                .activity_id
                .parse()
                .map_err(|_| SqliteError::corrupt("harvest_external_tasks.activity_id"))?,
            activity_name: self.name,
            queue: self.queue,
            input: serde_json::from_str(&self.input_json)?,
            state: ExternalTaskState::parse(&self.state)?,
            created_at: time(self.created_at, "harvest_external_tasks.created_at")?,
            updated_at: time(self.updated_at, "harvest_external_tasks.updated_at")?,
            deadline_at: time(self.deadline_at, "harvest_external_tasks.deadline_at")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEADLINE_TIMER_PREFIX, DEFAULT_HANDOFF_LIMIT, ExternalHandoffFilter, ExternalTaskState,
        MAX_HANDOFF_LIMIT, bounded_deadline, deadline_timer_id, deadline_token, latest_deadline,
    };
    use autumn_harvest::ExternalActivityToken;

    #[test]
    fn the_page_size_stays_in_its_range() {
        let size = |filter: ExternalHandoffFilter| filter.page_size();
        assert_eq!(size(ExternalHandoffFilter::new()), DEFAULT_HANDOFF_LIMIT);
        assert_eq!(size(ExternalHandoffFilter::new().limit(0)), 1);
        assert_eq!(size(ExternalHandoffFilter::new().limit(7)), 7);
        assert_eq!(
            size(ExternalHandoffFilter::new().limit(u32::MAX)),
            MAX_HANDOFF_LIMIT
        );
    }

    #[test]
    fn a_deadline_stays_in_the_datetime_range() {
        assert_eq!(bounded_deadline(i64::MAX), latest_deadline());
        assert_eq!(bounded_deadline(5), 5);
        assert!(chrono::DateTime::from_timestamp_millis(latest_deadline()).is_some());
    }

    #[test]
    fn a_deadline_timer_id_round_trips_its_token() {
        let token = ExternalActivityToken::new();
        let id = deadline_timer_id(token);
        assert!(id.starts_with(DEADLINE_TIMER_PREFIX));
        assert_eq!(deadline_token(&id), Some(token));
    }

    #[test]
    fn another_timer_id_has_no_token() {
        assert_eq!(deadline_token("__signal_timeout:1:go"), None);
        assert_eq!(deadline_token("tick"), None);
        assert_eq!(deadline_token("__external_deadline:not-a-token"), None);
    }

    #[test]
    fn each_state_round_trips_its_stored_name() {
        for state in [
            ExternalTaskState::Pending,
            ExternalTaskState::Completed,
            ExternalTaskState::Failed,
            ExternalTaskState::TimedOut,
            ExternalTaskState::Cancelled,
        ] {
            assert_eq!(ExternalTaskState::parse(state.as_str()).unwrap(), state);
            assert_eq!(state.to_string(), state.as_str());
        }
        assert!(ExternalTaskState::parse("RUNNING").is_err());
    }
}
