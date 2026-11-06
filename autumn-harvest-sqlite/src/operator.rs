//! The operator reads of the management API (issue #2468).
//!
//! The SQLite plugin serves `count`, `children`, `tree`, `timeline` and
//! `stack` from these reads. Each read is one query on the file. The plugin
//! then calls the same pure step as the Postgres route, so both backends
//! answer with one shape.
//!
//! These reads expect the current schema. The plugin opens the file with a
//! runtime first, and the runtime migrates it.

use autumn_harvest::{ExecutionId, WorkflowEvent};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};

/// Which runs a count reads. All set filters combine with AND.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CountFilter {
    /// Only runs of this workflow.
    pub workflow_name: Option<String>,
    /// Only runs in one of these states. An empty list matches every state.
    pub states: Vec<String>,
    /// Only runs that started at or after this time.
    pub started_after: Option<DateTime<Utc>>,
    /// Only runs that started at or before this time.
    pub started_before: Option<DateTime<Utc>>,
}

/// The number of runs of one `(state, workflow_name)` group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCount {
    /// The state of the group.
    pub state: String,
    /// The workflow name of the group.
    pub workflow_name: String,
    /// The number of runs.
    pub count: i64,
}

/// One child run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRun {
    /// The child's execution id.
    pub exec_id: ExecutionId,
    /// The parent that spawned it.
    pub parent: ExecutionId,
    /// The child's workflow name.
    pub workflow_name: String,
    /// The child's business id.
    pub workflow_id: String,
    /// The child's state.
    pub state: String,
    /// When the child started. A row from before issue #2027 has none.
    pub started_at: Option<DateTime<Utc>>,
    /// When the child closed. An open child has none.
    pub closed_at: Option<DateTime<Utc>>,
    /// The failure, cancel or terminate reason.
    pub error: Option<String>,
    /// `true` for a detached child.
    pub detached: bool,
    /// The parent-close policy of a detached child, such as `abandon`.
    pub parent_close_policy: Option<String>,
}

/// One event with its append time.
#[derive(Debug, Clone)]
pub struct TimedEvent {
    /// When the event was appended. An event from before issue #2489 has
    /// none.
    pub recorded_at: Option<DateTime<Utc>>,
    /// The event.
    pub event: WorkflowEvent,
}

/// One activity task that has not finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTask {
    /// The scheduled activity id.
    pub activity_id: String,
    /// The activity name.
    pub name: String,
    /// The task queue.
    pub queue: String,
    /// `PENDING` or `RUNNING`.
    pub state: String,
    /// The attempts consumed so far, as on Postgres.
    pub attempt: i64,
    /// The attempt cap. `None` until the first claim resolves it.
    pub max_attempts: Option<i64>,
    /// The earliest time that the next attempt may run.
    pub run_at: DateTime<Utc>,
    /// When the workflow scheduled the activity.
    pub scheduled_at: DateTime<Utc>,
    /// The start-to-close budget of one attempt.
    pub start_to_close: Option<chrono::Duration>,
    /// The deadline over all attempts.
    pub schedule_to_close_at: Option<DateTime<Utc>>,
    /// The latest heartbeat payload.
    pub heartbeat_details: Option<Value>,
    /// The error of the previous attempt.
    pub last_error: Option<String>,
}

/// One durable timer that has not fired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTimer {
    /// The timer id.
    pub timer_id: String,
    /// When the timer fires.
    pub fire_at: DateTime<Utc>,
}

/// The undelivered signals of one name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedSignal {
    /// The signal name.
    pub name: String,
    /// The number of undelivered signals.
    pub count: i64,
    /// When the oldest one arrived.
    pub oldest_received_at: DateTime<Utc>,
}

/// The work that a run waits on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingWork {
    /// The open activity tasks, in schedule order.
    pub tasks: Vec<PendingTask>,
    /// The unfired timers, in arm order.
    pub timers: Vec<PendingTimer>,
    /// The undelivered signals, by name.
    pub signals: Vec<BufferedSignal>,
}

/// A time column in epoch microseconds.
fn micros(value: Option<i64>, field: &str) -> SqliteResult<Option<DateTime<Utc>>> {
    value
        .map(|us| DateTime::from_timestamp_micros(us).ok_or_else(|| SqliteError::corrupt(field)))
        .transpose()
}

/// A time column in epoch milliseconds.
fn millis(value: i64, field: &str) -> SqliteResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| SqliteError::corrupt(field))
}

/// See [`SqliteReader::count_executions`](crate::SqliteReader::count_executions).
///
/// The filters compare as the listing does: by bytes, and a time bound
/// matches only an integer time. A damaged row then counts as it lists.
pub fn count(conn: &Connection, filter: &CountFilter) -> SqliteResult<Vec<ExecutionCount>> {
    let mut statement = conn.prepare_cached(
        "SELECT state, workflow_name, count(*) FROM harvest_executions \
         WHERE (?1 IS NULL OR cast(workflow_name as blob) = cast(?1 as blob)) \
           AND (json_array_length(?2) = 0 \
                OR cast(state as blob) IN (SELECT cast(value as blob) FROM json_each(?2))) \
           AND (?3 IS NULL OR (typeof(started_at) = 'integer' AND started_at >= ?3)) \
           AND (?4 IS NULL OR (typeof(started_at) = 'integer' AND started_at <= ?4)) \
         GROUP BY state, workflow_name",
    )?;
    let rows = statement.query_map(
        params![
            filter.workflow_name,
            serde_json::to_string(&filter.states)?,
            filter.started_after.map(|at| at.timestamp_micros()),
            filter.started_before.map(|at| at.timestamp_micros()),
        ],
        |row| {
            Ok(ExecutionCount {
                state: row.get(0)?,
                workflow_name: row.get(1)?,
                count: row.get(2)?,
            })
        },
    )?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// A JSON array of execution ids, bound as one parameter.
fn id_array(ids: &[ExecutionId]) -> SqliteResult<String> {
    let ids: Vec<String> = ids.iter().map(ToString::to_string).collect();
    Ok(serde_json::to_string(&ids)?)
}

/// See [`SqliteReader::children`](crate::SqliteReader::children).
pub fn children(
    conn: &Connection,
    parents: &[ExecutionId],
    limit: Option<u32>,
) -> SqliteResult<Vec<ChildRun>> {
    if parents.is_empty() {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare_cached(
        "SELECT exec_id, parent_exec_id, workflow_name, workflow_id, state, started_at, \
                closed_at, error, parent_close_policy \
         FROM harvest_executions \
         WHERE parent_exec_id IN (SELECT value FROM json_each(?1)) \
         ORDER BY started_at, rowid LIMIT ?2",
    )?;
    let limit = limit.map_or(-1, i64::from);
    let rows = statement.query_map(params![id_array(parents)?, limit], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
        ))
    })?;
    rows.map(|row| {
        let (exec, parent, workflow_name, workflow_id, state, started, closed, error, policy) =
            row?;
        Ok(ChildRun {
            exec_id: exec.parse().map_err(|_| SqliteError::corrupt("exec_id"))?,
            parent: parent
                .parse()
                .map_err(|_| SqliteError::corrupt("parent_exec_id"))?,
            workflow_name,
            workflow_id,
            state,
            started_at: micros(started, "started_at")?,
            closed_at: micros(closed, "closed_at")?,
            error,
            detached: policy.is_some(),
            parent_close_policy: policy,
        })
    })
    .collect()
}

/// See [`SqliteReader::parents_with_children`](crate::SqliteReader::parents_with_children).
pub fn parents_with_children(
    conn: &Connection,
    parents: &[ExecutionId],
) -> SqliteResult<Vec<ExecutionId>> {
    if parents.is_empty() {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare_cached(
        "SELECT DISTINCT parent_exec_id FROM harvest_executions \
         WHERE parent_exec_id IN (SELECT value FROM json_each(?1))",
    )?;
    let rows = statement.query_map([id_array(parents)?], |row| row.get::<_, String>(0))?;
    rows.map(|row| {
        row?.parse()
            .map_err(|_| SqliteError::corrupt("parent_exec_id"))
    })
    .collect()
}

/// See [`SqliteReader::timed_history`](crate::SqliteReader::timed_history).
pub fn timed_history(conn: &Connection, exec: ExecutionId) -> SqliteResult<Vec<TimedEvent>> {
    let mut statement = conn.prepare_cached(
        "SELECT event_json, recorded_at FROM harvest_events WHERE exec_id = ?1 ORDER BY seq",
    )?;
    let rows = statement.query_map([exec.to_string()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    let events = rows
        .map(|row| {
            let (json, at) = row?;
            Ok(TimedEvent {
                recorded_at: micros(at, "recorded_at")?,
                event: serde_json::from_str(&json)?,
            })
        })
        .collect::<SqliteResult<Vec<_>>>()?;
    if events.is_empty() && !crate::store::execution_exists(conn, exec)? {
        return Err(SqliteError::ExecutionNotFound(exec));
    }
    Ok(events)
}

/// See [`SqliteReader::pending_work`](crate::SqliteReader::pending_work).
pub fn pending_work(conn: &Connection, exec: ExecutionId) -> SqliteResult<PendingWork> {
    if !crate::store::execution_exists(conn, exec)? {
        return Err(SqliteError::ExecutionNotFound(exec));
    }
    let id = exec.to_string();
    Ok(PendingWork {
        tasks: pending_tasks(conn, &id)?,
        timers: pending_timers(conn, &id)?,
        signals: buffered_signals(conn, &id)?,
    })
}

/// The open activity tasks of the run `id`, in schedule order.
fn pending_tasks(conn: &Connection, id: &str) -> SqliteResult<Vec<PendingTask>> {
    let mut tasks = conn.prepare_cached(
        "SELECT activity_id, name, queue, state, attempt, max_attempts, run_at, scheduled_at, \
                start_to_close_ms, schedule_to_close_at, heartbeat_details, last_error \
         FROM harvest_tasks WHERE exec_id = ?1 AND state IN ('PENDING', 'RUNNING') \
         ORDER BY scheduled_at, seq",
    )?;
    tasks
        .query_map([id], |row| {
            Ok((
                (
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ),
                (
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ),
                (
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ),
            ))
        })?
        .map(|row| {
            let (
                (activity_id, name, queue, state),
                (attempt, max_attempts, run_at, scheduled_at),
                (start_to_close_ms, schedule_to_close_at, heartbeat, last_error),
            ) = row?;
            Ok(PendingTask {
                activity_id,
                name,
                queue,
                state,
                attempt,
                max_attempts,
                run_at: millis(run_at, "run_at")?,
                scheduled_at: millis(scheduled_at, "scheduled_at")?,
                start_to_close: start_to_close_ms.map(chrono::Duration::milliseconds),
                schedule_to_close_at: schedule_to_close_at
                    .map(|at| millis(at, "schedule_to_close_at"))
                    .transpose()?,
                heartbeat_details: heartbeat
                    .map(|text| serde_json::from_str(&text))
                    .transpose()?,
                last_error,
            })
        })
        .collect()
}

/// The unfired timers of the run `id`, in arm order.
fn pending_timers(conn: &Connection, id: &str) -> SqliteResult<Vec<PendingTimer>> {
    let mut timers = conn.prepare_cached(
        "SELECT timer_id, fire_at FROM harvest_timers \
         WHERE exec_id = ?1 AND fired = 0 ORDER BY arm_seq",
    )?;
    timers
        .query_map([id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .map(|row| {
            let (timer_id, fire_at) = row?;
            Ok(PendingTimer {
                timer_id,
                fire_at: millis(fire_at, "fire_at")?,
            })
        })
        .collect()
}

/// The undelivered signals of the run `id`, by name.
fn buffered_signals(conn: &Connection, id: &str) -> SqliteResult<Vec<BufferedSignal>> {
    let mut signals = conn.prepare_cached(
        "SELECT name, count(*), min(received_at) FROM harvest_signals \
         WHERE exec_id = ?1 AND delivered = 0 GROUP BY name ORDER BY name",
    )?;
    signals
        .query_map([id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .map(|row| {
            let (name, count, oldest) = row?;
            Ok(BufferedSignal {
                name,
                count,
                oldest_received_at: millis(oldest, "received_at")?,
            })
        })
        .collect()
}
