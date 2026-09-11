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
