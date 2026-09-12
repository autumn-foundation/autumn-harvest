//! Read-only inspection of the workflow database.
//!
//! The runtime exposes per-execution reads, not a listing, so the daemon opens
//! a second connection in READ-ONLY mode to enumerate sessions. The backend
//! opens the file in WAL mode with a busy timeout for exactly this case: one
//! writer, plus the occasional reader. Never open a second WRITE handle.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

/// How many recorded events one read of the event log carries at a time.
///
/// Reading a whole history is unbounded twice over: the event count grows
/// with every turn, and each model activity carries the whole transcript. The
/// runtime is serialised, so one such read blocks every session drive.
///
/// This is a PAGE, and not a window. A search reads pages until it finds what
/// it wants or the history ends, so no page size can hide an event from it.
/// Only the memory in hand at one moment is bounded.
pub const EVENT_PAGE: u32 = 64;

/// How many events one `history` command prints.
///
/// The audit trail is the reason the command exists, so the cap is high. The
/// newest events are kept, because they are what an operator reads first, and
/// the command says when it is not the whole log.
pub const MAX_HISTORY_EVENTS: u32 = 500;

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

/// One session as a LISTING shows it, with every field already bounded.
///
/// A listing names many sessions, so it carries no whole payload. The single
/// status of one session still reads its row entire, because that is the one
/// session the operator asked about.
pub struct SessionSummary {
    pub exec_id: String,
    pub state: String,
    /// `None` when the recorded task cannot be read.
    pub goal: Option<String>,
    pub stop: Option<String>,
    pub turns: Option<i64>,
    pub tool_calls: Option<i64>,
    pub answer: Option<String>,
    pub error: Option<String>,
}

/// How many characters of one listed field are read.
///
/// A goal, an answer and an error are all written by somebody else: the
/// operator, the model, or the engine. None of them is bounded at the source.
pub const MAX_LISTED_CHARS: u32 = 500;

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

/// Read one page of a session's events, newest first.
///
/// `before` is the sequence number the previous page ended on, so a caller
/// walks backwards page by page. `None` starts at the newest event. Each item
/// carries its own sequence number, which is the event's real position in the
/// log and the cursor for the next page.
///
/// The engine owns this table. The rows are read here, never written: the
/// event log is append-only, and a reader of it must stay a reader.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn events_before(
    conn: &Connection,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<(i64, serde_json::Value)>, String> {
    read_events(conn, EVERY_EVENT, exec_id, before, limit)
}

/// Read one page of a session's MODEL REPLIES, newest first.
///
/// Same page walk as [`events_before`], over the replies alone. The database
/// applies the filter. A turn that asked for many tools therefore does not
/// put its results in front of the reply that named them.
///
/// That bounds the work as well as the memory. A search for an awaited call
/// visits at most one row per model turn, and the turn count is what
/// `--max-turns` already bounds. Without the filter, one status could walk
/// every tool result of every turn, on the loop that drives every session.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn replies_before(
    conn: &Connection,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<(i64, serde_json::Value)>, String> {
    read_events(conn, ONLY_REPLIES, exec_id, before, limit)
}

/// Every recorded event.
const EVERY_EVENT: &str = "";

/// Only the events that carry a model reply.
///
/// A reply has a stop reason and a tool result does not, so the presence of
/// that field is what tells the two apart. The engine records both as
/// `ActivityCompleted`, and the event carries no activity name.
const ONLY_REPLIES: &str = "AND json_extract(event_json, '$.data.output.stop_reason') IS NOT NULL ";

/// One page of events, optionally narrowed by `filter`.
fn read_events(
    conn: &Connection,
    filter: &str,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<(i64, serde_json::Value)>, String> {
    let sql = format!(
        "SELECT seq, event_json FROM harvest_events \
         WHERE exec_id = ?1 AND (?2 IS NULL OR seq < ?2) {filter}\
         ORDER BY seq DESC LIMIT ?3"
    );
    let mut statement = conn
        .prepare(&sql)
        .map_err(|e| format!("cannot prepare the event query: {e}"))?;
    let rows = statement
        .query_map(rusqlite::params![exec_id, before, limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| format!("cannot read the events: {e}"))?;

    rows.map(|row| {
        row.map_err(|e| format!("cannot read the events: {e}"))
            .and_then(|(seq, json)| {
                serde_json::from_str(&json)
                    .map(|value| (seq, value))
                    .map_err(|e| format!("cannot decode an event: {e}"))
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
pub fn executions(conn: &Connection, workflow_name: &str) -> Result<Vec<SessionSummary>, String> {
    // The FIELDS are selected, and not the rows. A recorded task and a
    // recorded report can each approach the backend's payload cap. A listing
    // that read them whole would hold hundreds of megabytes for a capped
    // number of sessions. It would then copy that to build the views, and
    // once more to serialise the answer. Each field is cut in the database,
    // where the bytes already are.
    let mut statement = conn
        .prepare(
            "SELECT exec_id, state, \
                    substr(json_extract(input_json, '$.goal'), 1, ?3), \
                    json_extract(output_json, '$.stop'), \
                    json_extract(output_json, '$.turns'), \
                    json_extract(output_json, '$.tool_calls'), \
                    substr(json_extract(output_json, '$.answer'), 1, ?3), \
                    substr(error, 1, ?3) \
             FROM harvest_executions WHERE workflow_name = ?1 \
             ORDER BY rowid DESC LIMIT ?2",
        )
        .map_err(|e| format!("cannot prepare the session query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![workflow_name, MAX_LISTED_SESSIONS + 1, MAX_LISTED_CHARS],
            |row| {
                Ok(SessionSummary {
                    exec_id: row.get(0)?,
                    state: row.get(1)?,
                    goal: row.get(2)?,
                    stop: row.get(3)?,
                    turns: row.get(4)?,
                    tool_calls: row.get(5)?,
                    answer: row.get(6)?,
                    error: row.get(7)?,
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
