//! Read-only inspection of the workflow database.
//!
//! The runtime exposes per-execution reads, not a listing, so the daemon opens
//! a second connection in READ-ONLY mode to enumerate sessions. The backend
//! opens the file in WAL mode with a busy timeout for exactly this case: one
//! writer, plus the occasional reader. Never open a second WRITE handle.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

/// How many recorded events one pending-call lookup reads.
///
/// The awaited call is in the LAST model reply, so the scan runs backwards and
/// stops. Reading the whole history instead would be unbounded twice over: the
/// event count grows with every turn, and each model activity carries the
/// whole transcript. A status call is not worth that, and the runtime is
/// serialised, so one status would block every session drive.
///
/// The cap is generous. One turn records the model reply and one event for
/// each tool call it asked for, so the newest reply is a few events back.
pub const MAX_SCANNED_EVENTS: u32 = 64;

/// How many sessions one listing carries.
///
/// `list` reads the whole row of every session it names. A session's goal and
/// its report are both unbounded, and the count grows for the life of the
/// file. An ordinary `agentd list` on an old database would read all of it
/// into memory, build a view of every row, and serialise the lot. The runtime
/// is serialised, so that also blocks every session drive until it ends.
///
/// The newest sessions are the ones an operator looks for. The cap takes
/// those, and the listing says when it is not the whole history.
pub const MAX_LISTED_SESSIONS: u32 = 200;

/// How long a read waits for the writer's transaction to commit.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// One RUNNING session: its id, and the task it started from.
pub struct RunningSession {
    pub exec_id: String,
    pub input_json: String,
}

/// One row of `harvest_executions`.
pub struct ExecutionRow {
    pub exec_id: String,
    pub state: String,
    pub input_json: String,
    pub output_json: Option<String>,
    pub error: Option<String>,
}

/// Open the inspector connection.
///
/// # Errors
///
/// Returns an error if the database cannot be opened for reading.
pub fn open(db: &Path) -> Result<Connection, String> {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("cannot open {} for reading: {e}", db.display()))?;
    conn.busy_timeout(BUSY_TIMEOUT)
        .map_err(|e| format!("cannot set the busy timeout: {e}"))?;
    Ok(conn)
}

/// Is this execution id a session of the agent workflow?
///
/// A history read needs the answer. The event query of a session that does not
/// exist returns no rows, which looks the same as a session that has recorded
/// nothing yet.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn is_session(conn: &Connection, workflow_name: &str, exec_id: &str) -> Result<bool, String> {
    conn.query_row(
        "SELECT 1 FROM harvest_executions WHERE workflow_name = ?1 AND exec_id = ?2",
        [workflow_name, exec_id],
        |_| Ok(true),
    )
    .or_else(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(false),
        other => Err(format!("cannot look up the session: {other}")),
    })
}

/// Every RUNNING session, oldest first, with the task it started from.
///
/// The startup check reads this, and the drive tick is seeded from it. Both
/// want the sessions that can still run, and neither wants the output of every
/// session that ever finished. A daemon must not pay for its whole history to
/// start.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn running(conn: &Connection, workflow_name: &str) -> Result<Vec<RunningSession>, String> {
    let mut statement = conn
        .prepare(
            "SELECT exec_id, input_json FROM harvest_executions \
             WHERE workflow_name = ?1 AND state = 'RUNNING' ORDER BY rowid",
        )
        .map_err(|e| format!("cannot prepare the running-session query: {e}"))?;
    let rows = statement
        .query_map([workflow_name], |row| {
            Ok(RunningSession {
                exec_id: row.get(0)?,
                input_json: row.get(1)?,
            })
        })
        .map_err(|e| format!("cannot read the running sessions: {e}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot read the running sessions: {e}"))
}

/// Read the newest recorded events of one session, newest first.
///
/// The engine owns this table. The rows are read here, never written: the
/// event log is append-only, and a reader of it must stay a reader.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn recent_events(
    conn: &Connection,
    exec_id: &str,
    limit: u32,
) -> Result<Vec<serde_json::Value>, String> {
    let mut statement = conn
        .prepare(
            "SELECT event_json FROM harvest_events WHERE exec_id = ?1 \
             ORDER BY seq DESC LIMIT ?2",
        )
        .map_err(|e| format!("cannot prepare the event query: {e}"))?;
    let rows = statement
        .query_map(rusqlite::params![exec_id, limit], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| format!("cannot read the events: {e}"))?;

    rows.map(|row| {
        row.map_err(|e| format!("cannot read the events: {e}"))
            .and_then(|json| {
                serde_json::from_str(&json).map_err(|e| format!("cannot decode an event: {e}"))
            })
    })
    .collect()
}

/// One session, by id.
///
/// `status` names one session, so it reads one row. The listing would select
/// and allocate the input and the output of every session that ever ran. The
/// daemon serves its commands one at a time, so that cost blocks every one.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn execution(
    conn: &Connection,
    workflow_name: &str,
    exec_id: &str,
) -> Result<Option<ExecutionRow>, String> {
    conn.query_row(
        "SELECT exec_id, state, input_json, output_json, error FROM harvest_executions \
         WHERE workflow_name = ?1 AND exec_id = ?2",
        [workflow_name, exec_id],
        |row| {
            Ok(ExecutionRow {
                exec_id: row.get(0)?,
                state: row.get(1)?,
                input_json: row.get(2)?,
                output_json: row.get(3)?,
                error: row.get(4)?,
            })
        },
    )
    .map(Some)
    .or_else(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        other => Err(format!("cannot read session {exec_id}: {other}")),
    })
}

/// When the deadline of one awaited signal expires, in epoch milliseconds.
///
/// `wait_for_signal_timeout` arms a race timer named
/// `__signal_timeout:{seq}:{signal}`. The backend fires an EXPIRED timer of
/// this kind before a signal that arrives after it, so a late decision can
/// never win the race. The daemon reads the deadline for the same reason. An
/// acknowledgement after it would tell an operator that a call was approved.
/// The session is going to report that call as denied.
///
/// The name is matched the way the backend matches it, rather than with a
/// `LIKE` pattern. A signal name can hold `%` or `_`.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn signal_deadline(
    conn: &Connection,
    exec_id: &str,
    signal: &str,
) -> Result<Option<i64>, String> {
    let mut statement = conn
        .prepare("SELECT timer_id, fire_at FROM harvest_timers WHERE exec_id = ?1")
        .map_err(|e| format!("cannot prepare the deadline query: {e}"))?;
    let rows = statement
        .query_map([exec_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|e| format!("cannot read the deadlines: {e}"))?;

    for row in rows {
        let (timer_id, fire_at) = row.map_err(|e| format!("cannot read a deadline: {e}"))?;
        if races_signal(&timer_id, signal) {
            return Ok(Some(fire_at));
        }
    }
    Ok(None)
}

/// Is this timer the deadline of that signal's wait?
fn races_signal(timer_id: &str, signal: &str) -> bool {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .is_some_and(|(_seq, name)| name == signal)
}

/// Every execution of the agent workflow, oldest first.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn executions(conn: &Connection, workflow_name: &str) -> Result<Vec<ExecutionRow>, String> {
    let mut statement = conn
        .prepare(
            "SELECT exec_id, state, input_json, output_json, error \
             FROM harvest_executions WHERE workflow_name = ?1 \
             ORDER BY rowid DESC LIMIT ?2",
        )
        .map_err(|e| format!("cannot prepare the session query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![workflow_name, MAX_LISTED_SESSIONS + 1],
            |row| {
                Ok(ExecutionRow {
                    exec_id: row.get(0)?,
                    state: row.get(1)?,
                    input_json: row.get(2)?,
                    output_json: row.get(3)?,
                    error: row.get(4)?,
                })
            },
        )
        .map_err(|e| format!("cannot read the sessions: {e}"))?;

    let mut listed = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot read the sessions: {e}"))?;
    // The newest are read first, so the listing reads oldest first again.
    listed.reverse();
    Ok(listed)
}
