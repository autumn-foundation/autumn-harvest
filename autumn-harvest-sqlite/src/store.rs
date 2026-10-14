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
    workflow_id: &str,
    input: &Value,
) -> SqliteResult<()> {
    conn.execute(
        "INSERT INTO harvest_executions (exec_id, workflow_name, workflow_id, input_json, state) \
         VALUES (?1, ?2, ?3, ?4, 'RUNNING')",
        params![
            exec_id.to_string(),
            workflow_name,
            workflow_id,
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

/// The stored state of `exec_id`.
///
/// An unknown id is [`SqliteError::ExecutionNotFound`], not a bare
/// `QueryReturnedNoRows` (issue #1735). One query does the lookup and the
/// existence check.
pub fn execution_state(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<String> {
    conn.query_row(
        "SELECT state FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| row.get(0),
    )
    .optional()?
    .ok_or(SqliteError::ExecutionNotFound(exec_id))
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

/// The run-scoped business `workflow_id` recorded at start (issue #698). Re-read on
/// every drive cycle and threaded into the [`WorkflowContext`] so
/// `ctx.info().workflow_id` is stable across replays and never empty (defaults to
/// the `exec_id` string form when the caller supplied no business id).
pub fn workflow_id_of(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<String> {
    Ok(conn.query_row(
        "SELECT workflow_id FROM harvest_executions WHERE exec_id = ?1",
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

/// The single ACTIVE (non-sealed, non-terminal-sealed) execution for a
/// `(workflow_name, workflow_id)` key, if one exists — the reuse-policy lookup
/// (issue #1068). Returns `(exec_id, state)` where `state` is `RUNNING`,
/// `COMPLETED`, or `FAILED`.
///
/// A prior in a SEALED state (`CONTINUED_AS_NEW`/`TERMINATED`) is EXCLUDED, so it
/// is invisible to the reuse decision — a replaced/superseded run leaves the
/// active set and the key becomes available to a new start, mirroring the Postgres
/// core's uniqueness index (which excludes those sealed states). The single-writer
/// contract guarantees at most one active row per key once this feature has
/// landed; `ORDER BY rowid DESC LIMIT 1` is a defensive tie-break preferring the
/// newest active row if a database created by the pre-#1068 always-fresh behavior
/// already holds duplicate active rows for a reused id.
///
/// **Legacy-duplicate reconciliation (pre-#1068 data only).** Once the reuse
/// matrix has landed, at most one active row per key is ever created, so the
/// newest-only lookup is exact for every start. It matters only for a database
/// created by the pre-#1068 always-fresh behavior, which could already hold
/// MULTIPLE active rows for a reused id — and the four policies reconcile that
/// legacy state differently:
/// - **`TerminateIfRunning`** is the ONLY policy that fully reconciles it: it seals
///   EVERY active row for the key (via [`find_active_executions_by_key`]), so no
///   legacy duplicate survives (Codex #1080 P2). It does NOT use this
///   newest-only accessor.
/// - **`AllowDuplicateFailedOnly`** deliberately does not disturb a running run, so
///   a legacy RUNNING duplicate persists under it. That fits its "replace a
///   FAILED or `CANCELLED` run" contract.
/// - **`AllowDuplicate` / `RejectDuplicate`** never replace, so any legacy
///   duplicates simply persist (they attach to / reject against the newest active
///   row this returns).
pub fn find_active_execution_by_key(
    conn: &Connection,
    workflow_name: &str,
    workflow_id: &str,
) -> SqliteResult<Option<(ExecutionId, String)>> {
    let row = conn
        .query_row(
            "SELECT exec_id, state FROM harvest_executions \
             WHERE workflow_name = ?1 AND workflow_id = ?2 \
               AND state NOT IN ('CONTINUED_AS_NEW','TERMINATED') \
             ORDER BY rowid DESC LIMIT 1",
            params![workflow_name, workflow_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()?;
    match row {
        None => Ok(None),
        Some((exec_str, state)) => {
            let exec = exec_str
                .parse()
                .map_err(|_| SqliteError::corrupt("exec_id"))?;
            Ok(Some((exec, state)))
        }
    }
}

/// EVERY ACTIVE (non-sealed) execution for a `(workflow_name, workflow_id)` key,
/// newest-first — the `TerminateIfRunning` reconciliation lookup (Codex #1080 P2).
///
/// The single-row [`find_active_execution_by_key`] returns only the NEWEST active
/// row, which is exact for every start once the reuse matrix has landed (at most
/// one active row per key is ever created). But a database written by the pre-#1068
/// always-fresh `start_workflow_with_id` can already hold MULTIPLE `RUNNING` rows
/// for one key; `TerminateIfRunning` must seal/cancel EVERY one, or an older
/// duplicate survives `RUNNING` and keeps being driven by `run_until_idle` after the
/// key was "terminated and restarted" — defeating the guarantee for upgraded
/// databases. This returns the full active set (same sealed-state exclusion as the
/// single-row accessor) so the `TerminateIfRunning` arm can iterate it.
///
/// `ORDER BY rowid DESC` (newest-first) is a stable, deterministic order; the
/// caller seals every returned row regardless, so the order is not load-bearing.
pub fn find_active_executions_by_key(
    conn: &Connection,
    workflow_name: &str,
    workflow_id: &str,
) -> SqliteResult<Vec<(ExecutionId, String)>> {
    let mut stmt = conn.prepare(
        "SELECT exec_id, state FROM harvest_executions \
         WHERE workflow_name = ?1 AND workflow_id = ?2 \
           AND state NOT IN ('CONTINUED_AS_NEW','TERMINATED') \
         ORDER BY rowid DESC",
    )?;
    let rows = stmt.query_map(params![workflow_name, workflow_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut out = Vec::new();
    for r in rows {
        let (exec_str, state) = r?;
        let exec = exec_str
            .parse()
            .map_err(|_| SqliteError::corrupt("exec_id"))?;
        out.push((exec, state));
    }
    Ok(out)
}

/// Transition an execution to a SEALED terminal state (issue #1068). The
/// reuse-policy "replace" cases use it: `AllowDuplicateFailedOnly` over a FAILED
/// or `CANCELLED` prior, and `TerminateIfRunning` over any prior. The seal moves
/// the superseded run out of the active set, so the fresh run gets the
/// `(workflow_name, workflow_id)` slot. `sealed_state` is `CONTINUED_AS_NEW` (mirroring core's `replace_execution`,
/// which seals to `CONTINUED_AS_NEW`).
///
/// Sealing a *terminal* prior (e.g. a `COMPLETED` run replaced by
/// `TerminateIfRunning`) moves it into a SEALED state that `erase::is_terminal_state`
/// treats as terminal, so the prior's `exec_id` is no longer resumable via
/// [`run_until_blocked`](crate::SqliteRuntime::run_until_blocked) (it short-circuits
/// to a non-resumable terminal outcome) — consistent with core's `replace_execution`,
/// which likewise seals the superseded run out of the active/resumable set. The pure
/// [`outcome`](crate::SqliteRuntime::outcome) accessor likewise reports a sealed run
/// terminally, as [`ExecutionOutcome::Terminated`](crate::ExecutionOutcome::Terminated)
/// (Codex #1080 P2) — never `Running` — so a client polling a superseded prior sees it
/// end rather than spin forever.
pub fn seal_execution(
    conn: &Connection,
    exec_id: ExecutionId,
    sealed_state: &str,
) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET state = ?2, closed_at = ?3 WHERE exec_id = ?1",
        params![exec_id.to_string(), sealed_state, now_millis()],
    )?;
    Ok(())
}

/// Store a cooperative cancel request on a `RUNNING` row (issue #2021).
///
/// The first request wins. A repeat call changes nothing. The row also keeps
/// the history length at the request, which bounds the cleanup.
pub fn request_cancel(conn: &Connection, exec_id: ExecutionId, reason: &str) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET cancel_reason = ?2, cancel_from_seq = \
           (SELECT COALESCE(MAX(seq) + 1, 0) FROM harvest_events WHERE exec_id = ?1) \
         WHERE exec_id = ?1 AND state = 'RUNNING' AND cancel_reason IS NULL",
        params![exec_id.to_string(), reason],
    )?;
    Ok(())
}

/// The stored cooperative cancel request of `exec_id`, if any (issue #2021):
/// the reason and the history length at the request. It is pending only while
/// the row is `RUNNING`.
pub fn cancel_request(
    conn: &Connection,
    exec_id: ExecutionId,
) -> SqliteResult<Option<(String, usize)>> {
    let (reason, from): (Option<String>, Option<i64>) = conn.query_row(
        "SELECT cancel_reason, cancel_from_seq FROM harvest_executions WHERE exec_id = ?1",
        params![exec_id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(reason.map(|reason| {
        let from = from.and_then(|seq| usize::try_from(seq).ok()).unwrap_or(0);
        (reason, from)
    }))
}

/// Put `exec_id` in a terminal `state` with `reason` as its error (issue
/// #2021). The cancel and terminate paths use it.
pub fn set_terminal_with_reason(
    conn: &Connection,
    exec_id: ExecutionId,
    state: &str,
    reason: &str,
) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET state = ?2, error = ?3, closed_at = ?4 \
         WHERE exec_id = ?1",
        params![exec_id.to_string(), state, reason, now_millis()],
    )?;
    Ok(())
}

pub fn set_completed(conn: &Connection, exec_id: ExecutionId, output: &Value) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET state = 'COMPLETED', output_json = ?2, closed_at = ?3 \
         WHERE exec_id = ?1",
        params![
            exec_id.to_string(),
            serde_json::to_string(output)?,
            now_millis()
        ],
    )?;
    Ok(())
}

pub fn set_failed(conn: &Connection, exec_id: ExecutionId, error: &str) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET state = 'FAILED', error = ?2, closed_at = ?3 \
         WHERE exec_id = ?1",
        params![exec_id.to_string(), error, now_millis()],
    )?;
    Ok(())
}

/// The wall clock in epoch milliseconds. A terminal write stores it as
/// `closed_at` (issue #2027).
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Store the start time, the memo and the search attributes of a new run
/// (issue #2027).
pub fn set_start_fields(
    conn: &Connection,
    exec_id: ExecutionId,
    started_at: i64,
    memo: Option<&Value>,
    search_attrs: Option<&serde_json::Map<String, Value>>,
) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET started_at = ?2, memo = ?3, search_attrs = ?4 \
         WHERE exec_id = ?1",
        params![
            exec_id.to_string(),
            started_at,
            memo.map(serde_json::to_string).transpose()?,
            search_attrs.map(serde_json::to_string).transpose()?
        ],
    )?;
    Ok(())
}

/// Copy the memo and the search attributes of `from` to `to`. A
/// continue-as-new successor keeps both, as on Postgres (issue #2027).
pub fn copy_memo_and_search_attrs(
    conn: &Connection,
    from: ExecutionId,
    to: ExecutionId,
) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_executions SET \
           memo = (SELECT memo FROM harvest_executions WHERE exec_id = ?1), \
           search_attrs = (SELECT search_attrs FROM harvest_executions WHERE exec_id = ?1) \
         WHERE exec_id = ?2",
        params![from.to_string(), to.to_string()],
    )?;
    Ok(())
}

/// Merge one `UpsertSearchAttributes` patch into the stored object (issue
/// #2027). `Some` sets a key and `None` removes it, as on Postgres.
///
/// A stored value that is not a readable JSON object is replaced, as the
/// Postgres merge replaces a base that is not an object. The result is always
/// an object, never NULL.
pub fn upsert_search_attrs(
    conn: &Connection,
    exec_id: ExecutionId,
    patch: &std::collections::HashMap<String, Option<Value>>,
) -> SqliteResult<()> {
    let stored: Option<Vec<u8>> = conn
        .query_row(
            "SELECT CASE WHEN typeof(search_attrs) = 'text' \
                    THEN cast(search_attrs as blob) END \
             FROM harvest_executions WHERE exec_id = ?1",
            params![exec_id.to_string()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(SqliteError::ExecutionNotFound(exec_id))?;
    let mut merged = match stored.and_then(|bytes| serde_json::from_slice(&bytes).ok()) {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    // The patch is a map, so its order is arbitrary. Each key appears once,
    // so the order does not change the result.
    for (key, value) in patch {
        match value {
            Some(value) => {
                merged.insert(key.clone(), value.clone());
            }
            None => {
                merged.remove(key);
            }
        }
    }
    conn.execute(
        "UPDATE harvest_executions SET search_attrs = ?2 WHERE exec_id = ?1",
        params![exec_id.to_string(), serde_json::to_string(&merged)?],
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
    Ok(load_history_sized(conn, exec_id)?.0)
}

/// Load the full ordered history and its stored size in bytes (issue #2023).
///
/// The size is the sum of the stored event JSON lengths. The byte cap reads
/// it, so the load costs no second scan.
pub fn load_history_sized(
    conn: &Connection,
    exec_id: ExecutionId,
) -> SqliteResult<(Vec<WorkflowEvent>, u64)> {
    let mut stmt = conn
        .prepare_cached("SELECT event_json FROM harvest_events WHERE exec_id = ?1 ORDER BY seq")?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    let mut bytes = 0_u64;
    for r in rows {
        let json = r?;
        bytes = bytes.saturating_add(json.len() as u64);
        out.push(serde_json::from_str(&json)?);
    }
    Ok((out, bytes))
}

/// The successor that `exec_id` continued as, if any (issue #2023).
///
/// Only a run that ends with `WorkflowContinuedAsNew` has one. A reuse-policy
/// seal shares the `CONTINUED_AS_NEW` state but writes no such event.
pub fn successor_of(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<Option<ExecutionId>> {
    let last: Option<String> = conn
        .query_row(
            "SELECT event_json FROM harvest_events WHERE exec_id = ?1 ORDER BY seq DESC LIMIT 1",
            params![exec_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    match last.map(|json| serde_json::from_str(&json)).transpose()? {
        Some(WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. }) => Ok(Some(new_exec_id)),
        _ => Ok(None),
    }
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

/// Stage an inbound signal, recording the absolute epoch-millisecond it arrived
/// (`received_at`).
///
/// The cycle-start ingest ([`crate::worker::SignalIngest`]) compares
/// `received_at` with the `fire_at` of a `wait_for_signal_timeout` deadline.
/// So both must use the **same clock**. The public
/// [`send_signal`](crate::SqliteRuntime::send_signal) uses the wall clock, as
/// the wall-clock drivers do. The `*_as_of` seam injects a test epoch, as the
/// `*_as_of` drivers do. Mirrors the Postgres `harvest_signals.received_at`.
pub fn stage_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    name: &str,
    payload: &Value,
    received_at: i64,
) -> SqliteResult<()> {
    conn.execute(
        "INSERT INTO harvest_signals (exec_id, name, payload_json, delivered, received_at) \
         VALUES (?1, ?2, ?3, 0, ?4)",
        params![
            exec_id.to_string(),
            name,
            serde_json::to_string(payload)?,
            received_at
        ],
    )?;
    Ok(())
}

/// Whether `exec_id` has a staged signal named `name`, delivered or not.
///
/// # Errors
///
/// Returns a persistence error if the query fails.
pub fn has_signal(conn: &Connection, exec_id: ExecutionId, name: &str) -> SqliteResult<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM harvest_signals WHERE exec_id = ?1 AND name = ?2)",
        params![exec_id.to_string(), name],
        |r| r.get(0),
    )?)
}

/// A staged signal awaiting delivery.
pub struct PendingSignal {
    /// `harvest_signals.signal_seq`. Pass it to [`mark_signal_delivered`].
    pub seq: i64,
    /// The signal name.
    pub name: String,
    /// Absolute epoch-millisecond the signal arrived (`received_at`).
    pub received_at: i64,
    /// Deserialized payload.
    pub payload: Value,
}

/// The arrival-ordered read of [`for_each_pending_signal`]. An index serves the
/// order, so the first row needs no sort of the whole backlog (issue #2020).
pub const PENDING_SIGNALS_SQL: &str = "SELECT signal_seq, name, received_at, payload_json \
     FROM harvest_signals WHERE exec_id = ?1 AND delivered = 0 \
     ORDER BY received_at, signal_seq";

/// Read the undelivered signals of `exec_id` one at a time, in arrival order.
///
/// The order is `received_at`, then `signal_seq` (issue #1069 P2). Insertion
/// order can differ from arrival order, because `send_signal_as_of` can stage a
/// later arrival first. The signal-or-deadline race (issue #476) must compare
/// the real arrival times. `signal_seq` breaks ties, as on Postgres.
///
/// Each payload is deserialized here. A corrupt row is an error before the
/// caller writes anything, so the row stays staged. The error names the row,
/// so an operator can find it.
///
/// `visit` returns `false` to stop. No row after the stop is read, so a large
/// backlog is never loaded whole.
pub fn for_each_pending_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    mut visit: impl FnMut(PendingSignal) -> SqliteResult<bool>,
) -> SqliteResult<()> {
    let mut stmt = conn.prepare_cached(PENDING_SIGNALS_SQL)?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (seq, name, received_at, payload) = row?;
        let payload = serde_json::from_str(&payload).map_err(|e| {
            SqliteError::Corrupt(format!(
                "harvest_signals row {seq} ('{name}') of execution {exec_id}: {e}"
            ))
        })?;
        let signal = PendingSignal {
            seq,
            name,
            received_at,
            payload,
        };
        if !visit(signal)? {
            break;
        }
    }
    Ok(())
}

/// Mark a staged signal delivered (consumed). Call only after the matching
/// `SignalReceived` has been appended in the same transaction.
pub fn mark_signal_delivered(conn: &Connection, seq: i64) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_signals SET delivered = 1 WHERE signal_seq = ?1",
        params![seq],
    )?;
    Ok(())
}
