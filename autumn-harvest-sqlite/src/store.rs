//! `SQLite` event store + execution / signal / attempt persistence.
//!
//! This reimplements the *behavior* of the Postgres `store` module (append event
//! JSON ordered by a per-execution sequence; load ordered by that sequence) on
//! `rusqlite`. The `harvest_events` table is the append-only
//! [`WorkflowEvent`] log and is the *canonical, replayable* history: every row is
//! `serde_json::to_string` of a `WorkflowEvent` in the shared adjacently-tagged
//! (`#[serde(tag = "type", content = "data")]`) form, so a history produced here
//! is byte-identical, per event, to one the Postgres backend would write.
//!
//! Every helper takes `conn: &Connection`. A `rusqlite::Transaction` derefs to
//! `Connection`, so callers that need decision-cycle atomicity pass `&tx` (the
//! [`runtime`](crate::runtime) module does exactly this — see the atomicity
//! discussion there).

use autumn_harvest::{ExecutionId, WorkflowEvent};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};

/// One recorded activity attempt from the per-attempt audit log.
///
/// Retryable failures are recorded here (and on the task-queue row), never in the
/// replayable event log — mirroring the Postgres engine's `requeue_for_retry`.
#[derive(Debug, Clone)]
pub struct ActivityAttempt {
    /// 1-based attempt number.
    pub attempt: u32,
    /// `Ok(output)` on success, `Err(error)` on a (retryable or final) failure.
    pub result: Result<Value, String>,
}

// ── Executions ───────────────────────────────────────────────────────────────

pub fn insert_execution(
    conn: &Connection,
    exec_id: ExecutionId,
    workflow_name: &str,
    input: &Value,
) -> SqliteResult<()> {
    conn.execute(
        "INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
         VALUES (?1, ?2, ?3, 'RUNNING')",
        params![
            exec_id.to_string(),
            workflow_name,
            serde_json::to_string(input)?
        ],
    )?;
    Ok(())
}

pub fn execution_exists(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<bool> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM harvest_executions WHERE exec_id = ?1",
            params![exec_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

pub fn execution_input(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<Value> {
    let raw: String = conn.query_row(
        "SELECT input_json FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn execution_state(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<String> {
    Ok(conn.query_row(
        "SELECT state FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?)
}

pub fn execution_output(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<Option<Value>> {
    let raw: Option<String> = conn.query_row(
        "SELECT output_json FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    match raw {
        Some(s) => Ok(Some(serde_json::from_str(&s)?)),
        None => Ok(None),
    }
}

pub fn execution_error(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<Option<String>> {
    Ok(conn.query_row(
        "SELECT error FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?)
}

pub fn workflow_name_of(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<String> {
    Ok(conn.query_row(
        "SELECT workflow_name FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?)
}

/// The ids of every non-terminal (`RUNNING`) execution, oldest-first.
pub fn running_executions(conn: &Connection) -> SqliteResult<Vec<ExecutionId>> {
    let mut stmt = conn.prepare(
        "SELECT exec_id FROM harvest_executions WHERE state = 'RUNNING' ORDER BY exec_id",
    )?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?.parse().map_err(|_| SqliteError::corrupt("exec_id"))?);
    }
    Ok(out)
}

pub fn set_completed(conn: &Connection, exec_id: ExecutionId, output: &Value) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET state = 'COMPLETED', output_json = ?2 WHERE exec_id = ?1",
        params![exec_id.to_string(), serde_json::to_string(output)?],
    )?;
    Ok(())
}

pub fn set_failed(conn: &Connection, exec_id: ExecutionId, error: &str) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET state = 'FAILED', error = ?2 WHERE exec_id = ?1",
        params![exec_id.to_string(), error],
    )?;
    Ok(())
}

// ── Event log ────────────────────────────────────────────────────────────────

pub fn next_seq(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<i64> {
    let max: Option<i64> = conn.query_row(
        "SELECT MAX(seq) FROM harvest_events WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    Ok(max.map_or(0, |m| m + 1))
}

/// Append one event to the canonical, replayable history.
pub fn append_event(
    conn: &Connection,
    exec_id: ExecutionId,
    event: &WorkflowEvent,
) -> SqliteResult<()> {
    let seq = next_seq(conn, exec_id)?;
    conn.execute(
        "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
        params![exec_id.to_string(), seq, serde_json::to_string(event)?],
    )?;
    Ok(())
}

/// Load the full ordered history — the exact `Vec<WorkflowEvent>` handed to
/// [`run_workflow`](autumn_harvest::run_workflow).
pub fn load_history(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<Vec<WorkflowEvent>> {
    let mut stmt =
        conn.prepare("SELECT event_json FROM harvest_events WHERE exec_id = ?1 ORDER BY seq")?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(serde_json::from_str(&r?)?);
    }
    Ok(out)
}

/// True if history already holds an `ActivityScheduled` for `activity_id` (keeps
/// command application idempotent — a fresh `ScheduleActivity` mints a new id, so
/// this only matches on a defensive re-application of the same cycle).
pub fn history_has_activity_scheduled(events: &[WorkflowEvent], activity_id: &str) -> bool {
    events.iter().any(|e| {
        matches!(e, WorkflowEvent::ActivityScheduled { activity_id: id, .. }
            if id.to_string() == activity_id)
    })
}

/// The number of currently-*pending* arms of `timer_id` — recorded
/// `TimerStarted(timer_id)` events not yet matched by a `TimerFired(timer_id)`.
///
/// A timer id may be RE-ARMED under the same name (the poll-loop idiom
/// `loop { ctx.timer("tick", 1).await?; … }`), which the core supports via
/// cursor-based per-id FIFO pairing. Arm idempotency must therefore key on
/// OCCURRENCE, not the bare id: a plain "history already has a `TimerStarted`
/// for this id?" guard would wrongly match a PRIOR *fired* arm and skip a genuine
/// re-arm, wedging the run. `> 0` means an arm is already recorded and unfired
/// (a replay-cycle re-emit — skip it); `== 0` means every prior arm has fired, so
/// a `StartTimer` for this id is a genuinely new arm that must be persisted.
pub fn pending_timer_arms(events: &[WorkflowEvent], timer_id: &str) -> i64 {
    let started = events
        .iter()
        .filter(|e| {
            matches!(e, WorkflowEvent::TimerStarted { timer_id: id, .. }
                if id.to_string() == timer_id)
        })
        .count();
    let fired = events
        .iter()
        .filter(|e| {
            matches!(e, WorkflowEvent::TimerFired { timer_id: id }
                if id.to_string() == timer_id)
        })
        .count();
    i64::try_from(started).unwrap_or(i64::MAX) - i64::try_from(fired).unwrap_or(i64::MAX)
}

// ── Activity attempt audit log ───────────────────────────────────────────────

pub fn record_attempt(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
    attempt: u32,
    result: &Result<Value, String>,
) -> SqliteResult<()> {
    let (ok, detail) = match result {
        Ok(v) => (1_i64, serde_json::to_string(v)?),
        Err(e) => (0_i64, e.clone()),
    };
    conn.execute(
        "INSERT INTO harvest_activity_attempts (exec_id, name, attempt, ok, detail) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![exec_id.to_string(), name, i64::from(attempt), ok, detail],
    )?;
    Ok(())
}

pub fn load_attempts(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
) -> SqliteResult<Vec<ActivityAttempt>> {
    let mut stmt = conn.prepare(
        "SELECT attempt, ok, detail FROM harvest_activity_attempts \
         WHERE exec_id = ?1 AND name = ?2 ORDER BY attempt_seq",
    )?;
    let rows = stmt.query_map(params![exec_id.to_string(), name], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for r in rows {
        let (attempt, ok, detail) = r?;
        let result = if ok == 1 {
            Ok(serde_json::from_str(&detail)?)
        } else {
            Err(detail)
        };
        out.push(ActivityAttempt {
            attempt: u32::try_from(attempt).unwrap_or(0),
            result,
        });
    }
    Ok(out)
}

// ── Inbound signals ──────────────────────────────────────────────────────────

pub fn stage_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
    payload: &Value,
) -> SqliteResult<()> {
    conn.execute(
        "INSERT INTO harvest_signals (exec_id, name, payload_json, delivered) VALUES (?1, ?2, ?3, 0)",
        params![exec_id.to_string(), name, serde_json::to_string(payload)?],
    )?;
    Ok(())
}

/// Pop the oldest undelivered signal matching `name`, marking it delivered.
pub fn take_pending_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
) -> SqliteResult<Option<Value>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            "SELECT signal_seq, payload_json FROM harvest_signals \
             WHERE exec_id = ?1 AND name = ?2 AND delivered = 0 ORDER BY signal_seq LIMIT 1",
            params![exec_id.to_string(), name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match row {
        Some((seq, payload)) => {
            // Deserialize BEFORE marking delivered: a `?`-return on a corrupt
            // payload must leave the signal `delivered = 0` so it is not lost —
            // a mark-then-deserialize ordering would silently drop it.
            let val = serde_json::from_str(&payload)?;
            conn.execute(
                "UPDATE harvest_signals SET delivered = 1 WHERE signal_seq = ?1",
                params![seq],
            )?;
            Ok(Some(val))
        }
        None => Ok(None),
    }
}
