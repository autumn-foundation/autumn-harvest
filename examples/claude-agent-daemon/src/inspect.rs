//! Read-only inspection of the workflow database.
//!
//! The runtime exposes per-execution reads, not a listing, so the daemon opens
//! a second connection in READ-ONLY mode to enumerate sessions. The backend
//! opens the file in WAL mode with a busy timeout for exactly this case: one
//! writer, plus the occasional reader. Never open a second WRITE handle.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

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

/// Every execution of the agent workflow, oldest first.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn executions(conn: &Connection, workflow_name: &str) -> Result<Vec<ExecutionRow>, String> {
    let mut statement = conn
        .prepare(
            "SELECT exec_id, state, input_json, output_json, error \
             FROM harvest_executions WHERE workflow_name = ?1 ORDER BY rowid",
        )
        .map_err(|e| format!("cannot prepare the session query: {e}"))?;
    let rows = statement
        .query_map([workflow_name], |row| {
            Ok(ExecutionRow {
                exec_id: row.get(0)?,
                state: row.get(1)?,
                input_json: row.get(2)?,
                output_json: row.get(3)?,
                error: row.get(4)?,
            })
        })
        .map_err(|e| format!("cannot read the sessions: {e}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot read the sessions: {e}"))
}
