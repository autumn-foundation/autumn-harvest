//! `SQLite` event store + execution/signal/attempt persistence for the spike.
//!
//! The Postgres analog is the `db`-gated [`store`](crate::store) module. This
//! reimplements only what the single-writer prototype needs, on `rusqlite`.
//!
//! The `spike_events` table is the append-only [`WorkflowEvent`] log and is the
//! *canonical, replayable* history: every row is `serde_json::to_string` of a
//! `WorkflowEvent`. The precise cross-backend claim is **per-event encoding
//! byte-identity, not event-stream identity**: any event both backends write
//! serializes to the same bytes (the shared `#[serde(tag = "type", content =
//! "data")]` encoding), but the streams differ — the Postgres engine appends an
//! `ActivityStarted` event per claim that this spike never writes, so a PG
//! history is `Scheduled → Started → Completed` where the spike's is `Scheduled
//! → Completed`. The two are **replay-equivalent** (cross-backend replay, AC4,
//! holds because `HistoryMatcher::scan_activity_terminal` skips `ActivityStarted`
//! — not because the streams are byte-identical), which is what matters here.

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use super::SpikeError;
use crate::event::WorkflowEvent;
use crate::types::ExecutionId;

/// One recorded activity attempt from the per-attempt audit log.
#[derive(Debug, Clone)]
pub struct ActivityAttempt {
    /// 1-based attempt number.
    pub attempt: u32,
    /// `Ok(output)` on success, `Err(error)` on a (retryable or final) failure.
    pub result: Result<Value, String>,
}

// ── Executions ───────────────────────────────────────────────────────────────

pub(super) fn insert_execution(
    conn: &Connection,
    exec_id: ExecutionId,
    workflow_name: &str,
    input: &Value,
) -> Result<(), SpikeError> {
    conn.execute(
        "INSERT INTO spike_executions (exec_id, workflow_name, input_json, state) \
         VALUES (?1, ?2, ?3, 'RUNNING')",
        params![
            exec_id.to_string(),
            workflow_name,
            serde_json::to_string(input)?
        ],
    )?;
    Ok(())
}

pub(super) fn execution_input(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Value, SpikeError> {
    let raw: String = conn.query_row(
        "SELECT input_json FROM spike_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&raw)?)
}

pub(super) fn execution_state(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<String, SpikeError> {
    Ok(conn.query_row(
        "SELECT state FROM spike_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?)
}

pub(super) fn execution_output(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Option<Value>, SpikeError> {
    let raw: Option<String> = conn.query_row(
        "SELECT output_json FROM spike_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    match raw {
        Some(s) => Ok(Some(serde_json::from_str(&s)?)),
        None => Ok(None),
    }
}

pub(super) fn execution_error(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Option<String>, SpikeError> {
    Ok(conn.query_row(
        "SELECT error FROM spike_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?)
}

pub(super) fn set_completed(
    conn: &Connection,
    exec_id: ExecutionId,
    output: &Value,
) -> Result<(), SpikeError> {
    conn.execute(
        "UPDATE spike_executions SET state = 'COMPLETED', output_json = ?2 WHERE exec_id = ?1",
        params![exec_id.to_string(), serde_json::to_string(output)?],
    )?;
    Ok(())
}

pub(super) fn set_failed(
    conn: &Connection,
    exec_id: ExecutionId,
    error: &str,
) -> Result<(), SpikeError> {
    conn.execute(
        "UPDATE spike_executions SET state = 'FAILED', error = ?2 WHERE exec_id = ?1",
        params![exec_id.to_string(), error],
    )?;
    Ok(())
}

// ── Event log ────────────────────────────────────────────────────────────────

pub(super) fn next_seq(conn: &Connection, exec_id: ExecutionId) -> Result<i64, SpikeError> {
    let max: Option<i64> = conn.query_row(
        "SELECT MAX(seq) FROM spike_events WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    Ok(max.map_or(0, |m| m + 1))
}

/// Append one event to the canonical, replayable history.
pub(super) fn append_event(
    conn: &Connection,
    exec_id: ExecutionId,
    event: &WorkflowEvent,
) -> Result<(), SpikeError> {
    let seq = next_seq(conn, exec_id)?;
    conn.execute(
        "INSERT INTO spike_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
        params![exec_id.to_string(), seq, serde_json::to_string(event)?],
    )?;
    Ok(())
}

/// Load the full ordered history — the exact `Vec<WorkflowEvent>` handed to
/// [`run_workflow`](crate::run_workflow).
pub(super) fn load_history(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Vec<WorkflowEvent>, SpikeError> {
    let mut stmt =
        conn.prepare("SELECT event_json FROM spike_events WHERE exec_id = ?1 ORDER BY seq")?;
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
pub(super) fn history_has_activity_scheduled(events: &[WorkflowEvent], activity_id: &str) -> bool {
    events.iter().any(|e| matches!(e, WorkflowEvent::ActivityScheduled { activity_id: id, .. } if id.to_string() == activity_id))
}

/// True if history already holds `TimerStarted` for `timer_id`.
pub(super) fn history_has_timer_started(events: &[WorkflowEvent], timer_id: &str) -> bool {
    events.iter().any(|e| matches!(e, WorkflowEvent::TimerStarted { timer_id: id, .. } if id.to_string() == timer_id))
}

// ── Activity attempt audit log ───────────────────────────────────────────────

pub(super) fn record_attempt(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
    attempt: u32,
    result: &Result<Value, String>,
) -> Result<(), SpikeError> {
    let (ok, detail) = match result {
        Ok(v) => (1_i64, serde_json::to_string(v)?),
        Err(e) => (0_i64, e.clone()),
    };
    conn.execute(
        "INSERT INTO spike_activity_attempts (exec_id, name, attempt, ok, detail) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![exec_id.to_string(), name, i64::from(attempt), ok, detail],
    )?;
    Ok(())
}

pub(super) fn load_attempts(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
) -> Result<Vec<ActivityAttempt>, SpikeError> {
    let mut stmt = conn.prepare(
        "SELECT attempt, ok, detail FROM spike_activity_attempts \
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

// ── Durable virtual clock ────────────────────────────────────────────────────

/// Load the persisted virtual clock (epoch seconds), defaulting to `0` on a
/// fresh database. Restoring this on [`open`](super::SqliteRuntime::open) is what
/// keeps a timer armed at a non-zero logical time firing after a restart — timers
/// store an *absolute* `fire_at`, so resetting the clock to 0 would leave a
/// later-armed timer permanently not-yet-due.
pub(super) fn load_clock(conn: &Connection) -> Result<i64, SpikeError> {
    let v: Option<i64> = conn
        .query_row(
            "SELECT value FROM spike_meta WHERE key = 'clock'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(v.unwrap_or(0))
}

/// Persist the virtual clock so a subsequent [`open`](super::SqliteRuntime::open)
/// restores it. Written whenever the clock advances.
pub(super) fn persist_clock(conn: &Connection, clock: i64) -> Result<(), SpikeError> {
    conn.execute(
        "INSERT INTO spike_meta (key, value) VALUES ('clock', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![clock],
    )?;
    Ok(())
}

// ── Inbound signals ──────────────────────────────────────────────────────────

pub(super) fn stage_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
    payload: &Value,
) -> Result<(), SpikeError> {
    conn.execute(
        "INSERT INTO spike_signals (exec_id, name, payload_json, delivered) VALUES (?1, ?2, ?3, 0)",
        params![exec_id.to_string(), name, serde_json::to_string(payload)?],
    )?;
    Ok(())
}

/// Pop the oldest undelivered signal matching `name`, marking it delivered.
pub(super) fn take_pending_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
) -> Result<Option<Value>, SpikeError> {
    let row: Option<(i64, String)> = conn
        .query_row(
            "SELECT signal_seq, payload_json FROM spike_signals \
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
                "UPDATE spike_signals SET delivered = 1 WHERE signal_seq = ?1",
                params![seq],
            )?;
            Ok(Some(val))
        }
        None => Ok(None),
    }
}
