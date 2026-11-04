//! The durable workflow log sink on `SQLite` (issue #2468).
//!
//! The rules are those of the Postgres sink (issue #790):
//!
//! - `seq` is the call ordinal of a line. A re-driven cycle offers the same
//!   seqs again, so `INSERT OR IGNORE` on `(exec_id, seq)` stores each line
//!   once.
//! - A run keeps at most `max_lines` lines. Later lines are dropped, and one
//!   marker row at `seq = i64::MAX` records the loss.
//! - The marker latches the cap. A later batch adds nothing, even when the
//!   cap was raised since. The stored lines then stay a prefix of the run.
//!
//! The lines share the cycle transaction, so a rolled-back cycle stores none.
//! A line is not an event. Replay never reads it.

use autumn_harvest::context::WorkflowLogPolicy;
use autumn_harvest::{ExecutionId, WorkflowCommand};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{SqliteError, SqliteResult};

/// The `seq` of the truncation marker. It sorts after every real line.
pub const WORKFLOW_LOG_TRUNCATION_SEQ: i64 = i64::MAX;

/// The message of the truncation marker. It is the Postgres text.
pub const WORKFLOW_LOG_TRUNCATION_MESSAGE: &str =
    "[harvest] per-execution log cap reached; subsequent lines were dropped";

/// One stored log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// The emission order of the line, and the page cursor.
    pub seq: i64,
    /// `info`, `warn` or `error`.
    pub level: String,
    /// The message, cut to the policy's byte cap.
    pub message: String,
    /// When the line was stored.
    pub occurred_at: DateTime<Utc>,
}

/// Which lines one page reads. All set filters combine with AND.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogFilter {
    /// Only lines after this `seq`.
    pub after_seq: Option<i64>,
    /// Only lines stored after this time.
    pub since: Option<DateTime<Utc>>,
    /// Only lines of these levels. An empty list matches every level.
    pub levels: Vec<String>,
    /// The page size.
    pub limit: u32,
}

impl LogFilter {
    /// A filter that reads the first `limit` lines.
    #[must_use]
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }
}

/// Store the `RecordLog` lines of one cycle. It is best effort, as on
/// Postgres.
///
/// A log line is observational only. So a failed write never fails the
/// cycle: a savepoint rolls back the write alone, and a warning logs the
/// error. It does nothing when `policy` is `None`. Returns the number of
/// lines that the insert added.
pub fn append_commands<'c>(
    conn: &Connection,
    exec: ExecutionId,
    commands: impl IntoIterator<Item = &'c WorkflowCommand>,
    policy: Option<WorkflowLogPolicy>,
) -> usize {
    let Some(policy) = policy else {
        return 0;
    };
    let lines: Vec<(i64, &'static str, &str)> = commands
        .into_iter()
        .filter_map(|command| match command {
            WorkflowCommand::RecordLog {
                seq,
                level,
                message,
            } => Some((
                // A real line never lands on the marker slot.
                i64::try_from(*seq)
                    .unwrap_or(i64::MAX)
                    .min(WORKFLOW_LOG_TRUNCATION_SEQ - 1),
                level.as_str(),
                message.as_str(),
            )),
            _ => None,
        })
        .collect();
    if lines.is_empty() {
        return 0;
    }
    let written = conn
        .execute_batch("SAVEPOINT harvest_workflow_logs")
        .map_err(SqliteError::from)
        .and_then(|()| append(conn, exec, &lines, policy.max_lines()));
    let undo = match &written {
        Ok(_) => "RELEASE harvest_workflow_logs",
        Err(_) => "ROLLBACK TO harvest_workflow_logs; RELEASE harvest_workflow_logs",
    };
    if let Err(error) = conn.execute_batch(undo) {
        tracing::warn!(%exec, %error, "harvest-sqlite: the log savepoint did not close");
    }
    written.unwrap_or_else(|error| {
        tracing::warn!(%exec, %error, "harvest-sqlite: dropped the log lines of a cycle");
        0
    })
}

/// Store `lines` under the cap `max_lines`. See the module docs.
fn append(
    conn: &Connection,
    exec: ExecutionId,
    lines: &[(i64, &'static str, &str)],
    max_lines: u32,
) -> SqliteResult<usize> {
    if lines.is_empty() {
        return Ok(0);
    }
    let id = exec.to_string();
    if truncated_on(conn, &id)? {
        return Ok(0);
    }
    // A line of this batch that is already stored does not count. A
    // re-driven cycle offers it again, and the insert ignores it.
    let seqs: Vec<i64> = lines.iter().map(|(seq, _, _)| *seq).collect();
    let existing: i64 = conn.query_row(
        "SELECT count(*) FROM harvest_workflow_logs \
         WHERE exec_id = ?1 AND seq <> ?2 \
           AND seq NOT IN (SELECT value FROM json_each(?3))",
        params![
            id,
            WORKFLOW_LOG_TRUNCATION_SEQ,
            serde_json::to_string(&seqs)?
        ],
        |row| row.get(0),
    )?;
    let remaining = i64::from(max_lines).saturating_sub(existing).max(0);
    let admit = usize::try_from(remaining)
        .unwrap_or(usize::MAX)
        .min(lines.len());
    let now = Utc::now().timestamp_micros();
    let mut insert = conn.prepare_cached(
        "INSERT OR IGNORE INTO harvest_workflow_logs \
         (exec_id, seq, level, message, occurred_at) VALUES (?1, ?2, ?3, ?4, ?5)",
    )?;
    let mut inserted = 0;
    for (seq, level, message) in &lines[..admit] {
        inserted += insert.execute(params![id, seq, level, message, now])?;
    }
    // The marker depends on `admit`, not on `inserted`. A re-drive inserts
    // nothing and drops nothing, so it must not record a loss.
    if admit < lines.len() {
        insert.execute(params![
            id,
            WORKFLOW_LOG_TRUNCATION_SEQ,
            "warn",
            WORKFLOW_LOG_TRUNCATION_MESSAGE,
            now
        ])?;
    }
    Ok(inserted)
}

/// Whether the file has the log table.
///
/// A file from before issue #2468 has none. A runtime adds it on open. A
/// read-only reader cannot, so its runs read as having no lines.
fn table_exists(conn: &Connection) -> SqliteResult<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'harvest_workflow_logs'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// Whether the run of `id` holds the truncation marker.
fn truncated_on(conn: &Connection, id: &str) -> SqliteResult<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM harvest_workflow_logs WHERE exec_id = ?1 AND seq = ?2",
            params![id, WORKFLOW_LOG_TRUNCATION_SEQ],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// One page of the lines of `exec`, in `seq` order.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`] if the query fails, and
/// [`SqliteError::Corrupt`] for a row with an unreadable time.
pub fn load(
    conn: &Connection,
    exec: ExecutionId,
    filter: &LogFilter,
) -> SqliteResult<Vec<LogLine>> {
    if !table_exists(conn)? {
        return Ok(Vec::new());
    }
    let levels = serde_json::to_string(&filter.levels)?;
    let mut statement = conn.prepare_cached(
        "SELECT seq, level, message, occurred_at FROM harvest_workflow_logs \
         WHERE exec_id = ?1 AND seq > ?2 AND occurred_at > ?3 \
           AND (json_array_length(?4) = 0 OR level IN (SELECT value FROM json_each(?4))) \
         ORDER BY seq LIMIT ?5",
    )?;
    let rows = statement.query_map(
        params![
            exec.to_string(),
            filter.after_seq.unwrap_or(i64::MIN),
            filter.since.map_or(i64::MIN, |at| at.timestamp_micros()),
            levels,
            i64::from(filter.limit)
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )?;
    rows.map(|row| {
        let (seq, level, message, at) = row?;
        let occurred_at = DateTime::from_timestamp_micros(at).ok_or_else(|| {
            SqliteError::Corrupt(format!("log line {seq} of {exec} has the time {at}"))
        })?;
        Ok(LogLine {
            seq,
            level,
            message,
            occurred_at,
        })
    })
    .collect()
}

/// The number of real lines of `exec`. The marker does not count.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`] if the query fails.
pub fn count(conn: &Connection, exec: ExecutionId) -> SqliteResult<i64> {
    if !table_exists(conn)? {
        return Ok(0);
    }
    Ok(conn.query_row(
        "SELECT count(*) FROM harvest_workflow_logs WHERE exec_id = ?1 AND seq <> ?2",
        params![exec.to_string(), WORKFLOW_LOG_TRUNCATION_SEQ],
        |row| row.get(0),
    )?)
}

/// Whether `exec` reached its line cap.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`] if the query fails.
pub fn truncated(conn: &Connection, exec: ExecutionId) -> SqliteResult<bool> {
    if !table_exists(conn)? {
        return Ok(false);
    }
    truncated_on(conn, &exec.to_string())
}
