//! Read-only inspection of the workflow database.
//!
//! The runtime exposes per-execution reads, not a listing, so the daemon opens
//! a second connection in READ-ONLY mode to enumerate sessions. The backend
//! opens the file in WAL mode with a busy timeout for exactly this case: one
//! writer, plus the occasional reader. Never open a second WRITE handle.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

use crate::session::SessionTask;

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
    /// The recorded task. `None` when this daemon cannot read the row.
    pub task: Option<RecordedTask>,
}

/// The recorded task of a RUNNING session, cut to what a startup check reads.
///
/// The fields carry the types the task itself declares, so a value outside
/// one of them is not represented here. A recorded `-1` turn bound, or a
/// bound wider than the field, leaves no `RecordedTask` at all.
pub struct RecordedTask {
    /// The workspace this session was recorded against.
    pub workspace: String,
    /// The model this session was recorded against.
    pub model: String,
    /// The recorded turn bound. The caller refuses a zero, as `submit` does.
    pub max_turns: u32,
    /// The recorded approval deadline. The caller refuses one too large to
    /// arm, as `submit` refuses one.
    pub approval_timeout_secs: u64,
    /// Does the recorded task carry a goal that says something?
    ///
    /// The goal itself is dropped, and never returned. A restart reads every
    /// RUNNING row, a goal reaches the size of a control request, and the
    /// returned set must not grow with them.
    ///
    /// The test is Rust's own `trim`, which is what `submit` refuses a goal
    /// by. The two ends of that invariant therefore run the same code.
    pub has_goal: bool,
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
    /// Where this row sits in the table, which is the cursor that reads the
    /// rows BEFORE it. The listing is capped, so an old session waiting for a
    /// decision would otherwise become unreachable once enough newer ones
    /// arrive.
    ///
    /// This is the `rowid`, because `harvest_executions` carries no time and
    /// no sequence of its own. The rows are ordered by it, and a rowid is
    /// assigned in insert order, so the order is the order sessions started.
    ///
    /// A `VACUUM` may renumber a rowid, unlike the event `seq` the history
    /// cursor uses. A cursor copied before one and used after it would name
    /// another page. The window is the seconds between reading a listing and
    /// typing the next command. No column in this table is stable across a
    /// `VACUUM`, so this is the bound of what the schema allows.
    pub row: i64,
}

/// How many characters of one listed field are read.
///
/// A goal, an answer and an error are all written by somebody else: the
/// operator, the model, or the engine. None of them is bounded at the source.
pub const MAX_LISTED_CHARS: u32 = 500;

/// The BYTES of one listed field the database is asked for.
///
/// The cut is on bytes, because `substr` on TEXT counts to the first NUL and
/// stops. A goal of `"\u0000do it"` is a goal `submit` accepts, and the
/// listing showed nothing for it while the single status showed all of it.
///
/// Four bytes is the longest UTF-8 character, so this budget always carries
/// at least [`MAX_LISTED_CHARS`] characters. The caller cuts the characters.
const MAX_LISTED_BYTES: u32 = MAX_LISTED_CHARS * 4;

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
    // The task is read the way the RUNTIME reads it, and not projected field
    // by field in SQL. `SQLite` and `serde_json` do not agree about what a
    // document says, so no projection can prove a row is readable.
    // `json_valid` accepts a goal of `"\uD800"` and gives it a type and a
    // length, while `serde_json` refuses the unpaired surrogate. A row the
    // projection called readable would be sealed FAILED by the runtime on
    // its first drive, where no later daemon could resume it.
    //
    // `recorded` runs the runtime's own three steps, so a row that answers
    // here deserialises on the first drive by construction.
    //
    // The document is read as BLOB bytes. A field can hold bytes that are
    // not valid UTF-8. Reading one of those as text fails the WHOLE query,
    // which would name no row at all. The bytes name their own row.
    //
    // The STORAGE CLASS is checked first, because the cast hides it. The
    // engine reads this column straight into a `String`, and `rusqlite`
    // refuses a BLOB value there. A column of TEXT affinity still keeps a
    // stored BLOB as a BLOB. A damaged row can therefore hold the right
    // bytes in the wrong class. The cast alone would accept it, and the
    // drive would fail on every tick over a session nothing ever seals.
    //
    // The cost is ONE document at a time. The rows are read as a stream, and
    // the task is cut down before the next row, so the returned set holds no
    // goal. A single control request already costs the daemon that memory.
    let mut statement = conn
        .prepare(
            "SELECT exec_id, \
                    CASE WHEN typeof(input_json) = 'text' \
                         THEN cast(input_json as blob) END \
             FROM harvest_executions \
             WHERE workflow_name = ?1 AND state = 'RUNNING' ORDER BY rowid",
        )
        .map_err(|e| format!("cannot prepare the running-session query: {e}"))?;
    let rows = statement
        .query_map([workflow_name], |row| {
            Ok(RunningSession {
                exec_id: row.get(0)?,
                task: row
                    .get::<_, Option<Vec<u8>>>(1)?
                    .as_deref()
                    .and_then(recorded),
            })
        })
        .map_err(|e| format!("cannot read the running sessions: {e}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot read the running sessions: {e}"))
}

/// Read one recorded task, keeping only what a startup check needs.
///
/// `None` when the document is not a task this daemon can read. The goal is
/// measured here and dropped, so it never leaves this function.
///
/// The three steps are the runtime's own, in its order. The backend reads
/// `input_json` as TEXT, parses the WHOLE document into a `Value`, and the
/// workflow takes its task from that value. The caller has already refused a
/// value of another storage class. That is the first half of the TEXT read,
/// and the UTF-8 test here is the second. Each step refuses something the
/// next one never sees. The middle step is why a fault in a field no check
/// reads still answers `None`. Parsing a document unescapes every string in
/// it, including one this daemon ignores.
fn recorded(document: &[u8]) -> Option<RecordedTask> {
    let text = std::str::from_utf8(document).ok()?;
    let whole = serde_json::from_str::<serde_json::Value>(text).ok()?;
    let task = serde_json::from_value::<SessionTask>(whole).ok()?;
    Some(RecordedTask {
        has_goal: !task.goal.trim().is_empty(),
        workspace: task.workspace,
        model: task.model,
        max_turns: task.max_turns,
        approval_timeout_secs: task.approval_timeout_secs,
    })
}

/// One recorded event, already cut to what an audit line prints.
///
/// The whole event is never read. A recorded activity can approach the
/// backend's payload cap, and a page names hundreds of them. A page that read
/// them whole would hold gigabytes for one command.
pub struct EventLine {
    /// The event's own position in the log, and the cursor of the next page.
    pub seq: i64,
    pub label: String,
    /// The event's data, cut in the database. `None` when it carries none.
    pub detail: Option<String>,
}

/// Read one page of a session's events, newest first, cut for printing.
///
/// `before` is the sequence number the previous page ended on, so a caller
/// walks backwards page by page. `None` starts at the newest event.
///
/// The engine owns this table. The rows are read here, never written: the
/// event log is append-only, and a reader of it must stay a reader.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn event_lines(
    conn: &Connection,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<EventLine>, String> {
    // One character past the printed cap, so the caller can tell a cut line
    // from one that ended by itself.
    let detail_cap = MAX_EVENT_DETAIL_CHARS + 1;
    let mut statement = conn
        .prepare(
            "SELECT seq, json_extract(event_json, '$.type'), \
                    substr(json_extract(event_json, '$.data'), 1, ?4) \
             FROM harvest_events \
             WHERE exec_id = ?1 AND (?2 IS NULL OR seq < ?2) \
             ORDER BY seq DESC LIMIT ?3",
        )
        .map_err(|e| format!("cannot prepare the event query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![exec_id, before, limit, detail_cap],
            |row| {
                Ok(EventLine {
                    seq: row.get(0)?,
                    label: row
                        .get::<_, Option<String>>(1)?
                        .unwrap_or_else(|| "unknown".to_string()),
                    detail: row.get(2)?,
                })
            },
        )
        .map_err(|e| format!("cannot read the events: {e}"))?;

    rows.map(|row| row.map_err(|e| format!("cannot read the events: {e}")))
        .collect()
}

/// How many characters of one event's data an audit line prints.
pub const MAX_EVENT_DETAIL_CHARS: u32 = 240;

/// Read one page of the TOOL CALLS a session's model replies asked for.
///
/// Newest first, and the calls alone. The database drops the tool results,
/// which is what bounds the WORK. The walk visits one row per model turn and
/// not one row per event, and `--max-turns` bounds the turns.
///
/// It also drops the transcript. A reply carries every earlier turn in its
/// content, and none of that names an awaited call. Only the calls are read,
/// which is what bounds the BYTES.
///
/// A reply has a stop reason and a tool result does not, which is how the two
/// are told apart. The engine records both as `ActivityCompleted`, and the
/// event carries no activity name.
///
/// # Errors
///
/// Returns an error if the query cannot run, or if a row is not readable.
pub fn reply_calls(
    conn: &Connection,
    exec_id: &str,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<(i64, serde_json::Value)>, String> {
    let mut statement = conn
        .prepare(
            "SELECT seq, json_extract(event_json, '$.data.output.tool_calls') \
             FROM harvest_events \
             WHERE exec_id = ?1 AND (?2 IS NULL OR seq < ?2) \
             AND json_extract(event_json, '$.data.output.stop_reason') IS NOT NULL \
             ORDER BY seq DESC LIMIT ?3",
        )
        .map_err(|e| format!("cannot prepare the reply query: {e}"))?;
    let rows = statement
        .query_map(rusqlite::params![exec_id, before, limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|e| format!("cannot read the replies: {e}"))?;

    rows.map(|row| {
        row.map_err(|e| format!("cannot read the replies: {e}"))
            .map(|(seq, calls)| {
                let value = calls
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or(serde_json::Value::Null);
                (seq, value)
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

/// The signal one RUNNING session is still waiting for, if it waits.
///
/// A restarted daemon knows which sessions are RUNNING, and not what each one
/// awaits. That is learned from a drive, so the parked state is empty until
/// the first drive runs. An operator who approves a call in that window is
/// told the session is not waiting, on the very path the restart recipe
/// describes.
///
/// The wait is durable, so it can be read instead. A signal wait with a
/// deadline records a `__signal_timeout:` timer, which names the signal.
///
/// A decision already in hand is checked as well. A previous daemon may have
/// taken one and stopped before the drive that consumed it. The timer can
/// outlive that, so a timer ALONE would report a wait that is already over.
/// This is an approval gate, and a decision must not be accepted twice. See
/// [`answered`], which reads the staged decision and the event both.
///
/// # Errors
///
/// Returns an error if either query fails.
pub fn outstanding_signal(
    conn: &Connection,
    exec_id: &str,
    now_ms: i64,
) -> Result<Option<String>, String> {
    // `fired = 0` is the backend's own proof of a wait that is still armed.
    // An approval that timed out leaves its timer behind with `fired = 1`,
    // and a timed-out wait has no answer either. Reading every timer would
    // therefore return the EXPIRED signal of an earlier call. The status
    // would show a token nobody can approve, and hide the one that works.
    // The DEADLINE must still be ahead as well. A daemon stopped past one has
    // had no drive in which to mark the timer fired, so an overdue wait still
    // reads as armed. Restoring it would print a token that `approve` refuses
    // every time, because the session denies that call on its next drive.
    //
    // `fire_at` is an absolute epoch-millisecond, so the caller's clock is
    // read in the same unit. The clock is a parameter, so a test can place a
    // deadline on either side of it.
    let mut timers = conn
        .prepare(
            "SELECT timer_id FROM harvest_timers WHERE exec_id = ?1 \
             AND fired = 0 AND fire_at > ?2",
        )
        .map_err(|e| format!("cannot prepare the wait query: {e}"))?;
    let named = timers
        .query_map(rusqlite::params![exec_id, now_ms], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| format!("cannot read the waits: {e}"))?;

    for timer in named {
        let timer = timer.map_err(|e| format!("cannot read a wait: {e}"))?;
        let Some(name) = signal_of(&timer) else {
            continue;
        };
        if answered(conn, exec_id, &name)? {
            continue;
        }
        return Ok(Some(name));
    }
    Ok(None)
}

/// The signal a deadline timer belongs to, if it is one.
fn signal_of(timer_id: &str) -> Option<String> {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .map(|(_seq, name)| name.to_string())
}

/// Is a decision for this signal already in hand?
///
/// TWO tables answer that, and one of them alone is not enough. A decision is
/// STAGED in `harvest_signals` when it is sent, and the event is appended
/// later, when the workflow takes it up. A daemon that stopped between the
/// two holds a decision that will win on the next drive.
///
/// Reading the event alone would restore the wait over such a decision, and a
/// second answer would be taken for a call already decided.
fn answered(conn: &Connection, exec_id: &str, signal: &str) -> Result<bool, String> {
    let staged = one_row(
        conn,
        "SELECT 1 FROM harvest_signals WHERE exec_id = ?1 AND name = ?2 \
         AND delivered = 0 LIMIT 1",
        exec_id,
        signal,
    )?;
    if staged {
        return Ok(true);
    }
    one_row(
        conn,
        "SELECT 1 FROM harvest_events WHERE exec_id = ?1 \
         AND json_extract(event_json, '$.type') = 'SignalReceived' \
         AND json_extract(event_json, '$.data.signal_name') = ?2 LIMIT 1",
        exec_id,
        signal,
    )
}

/// Does this query find a row?
fn one_row(conn: &Connection, sql: &str, exec_id: &str, signal: &str) -> Result<bool, String> {
    let mut statement = conn
        .prepare(sql)
        .map_err(|e| format!("cannot prepare the decision query: {e}"))?;
    let mut rows = statement
        .query([exec_id, signal])
        .map_err(|e| format!("cannot read the decisions: {e}"))?;
    rows.next()
        .map(|row| row.is_some())
        .map_err(|e| format!("cannot read a decision: {e}"))
}

/// Is this timer the deadline of that signal's wait?
fn races_signal(timer_id: &str, signal: &str) -> bool {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .is_some_and(|(_seq, name)| name == signal)
}

/// Decode one field the database cut to BYTES.
///
/// The cut can land inside a character, so only the valid prefix is kept. A
/// replacement character would name bytes the field does not hold, and the
/// operator would read a character nobody wrote.
///
/// The characters are cut here, because the database was asked for a budget
/// of bytes. See [`MAX_LISTED_BYTES`].
fn cut_text(bytes: Option<Vec<u8>>) -> Option<String> {
    let bytes = bytes?;
    let whole = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(split) => std::str::from_utf8(&bytes[..split.valid_up_to()]).unwrap_or_default(),
    };
    Some(whole.chars().take(MAX_LISTED_CHARS as usize).collect())
}

/// One page of the agent workflow's executions, oldest first.
///
/// `before` reads the page before a row this listing named. The cap is on one
/// page and not on the table, so every session stays reachable.
///
/// # Errors
///
/// Returns an error if the query fails.
pub fn executions(
    conn: &Connection,
    workflow_name: &str,
    before: Option<i64>,
) -> Result<Vec<SessionSummary>, String> {
    // The FIELDS are selected, and not the rows. A recorded task and a
    // recorded report can each approach the backend's payload cap. A listing
    // that read them whole would hold hundreds of megabytes for a capped
    // number of sessions. It would then copy that to build the views, and
    // once more to serialise the answer. Each field is cut in the database,
    // where the bytes already are.
    let mut statement = conn
        .prepare(
            "SELECT exec_id, state, \
                    substr(cast(json_extract(input_json, '$.goal') as blob), 1, ?3), \
                    json_extract(output_json, '$.stop'), \
                    json_extract(output_json, '$.turns'), \
                    json_extract(output_json, '$.tool_calls'), \
                    substr(cast(json_extract(output_json, '$.answer') as blob), 1, ?3), \
                    substr(cast(error as blob), 1, ?3), \
                    rowid \
             FROM harvest_executions WHERE workflow_name = ?1 \
             AND (?4 IS NULL OR rowid < ?4) \
             ORDER BY rowid DESC LIMIT ?2",
        )
        .map_err(|e| format!("cannot prepare the session query: {e}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![
                workflow_name,
                MAX_LISTED_SESSIONS + 1,
                MAX_LISTED_BYTES,
                before
            ],
            |row| {
                Ok(SessionSummary {
                    exec_id: row.get(0)?,
                    state: row.get(1)?,
                    goal: cut_text(row.get(2)?),
                    stop: row.get(3)?,
                    turns: row.get(4)?,
                    tool_calls: row.get(5)?,
                    answer: cut_text(row.get(6)?),
                    error: cut_text(row.get(7)?),
                    row: row.get(8)?,
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
