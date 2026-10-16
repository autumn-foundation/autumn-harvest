//! Cron and interval schedules (issue #2031).

use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use chrono::{DateTime, Utc};
use rusqlite::Connection;

use crate::error::{SqliteError, SqliteResult};

/// One stored schedule, as [`SqliteRuntime::list_schedules`](crate::SqliteRuntime::list_schedules)
/// returns it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScheduleSummary {
    /// The workflow that each fire starts. It is also the schedule key.
    pub workflow_name: String,
    /// The cron or interval spec.
    pub schedule: Schedule,
    /// `true` while the schedule is paused.
    pub paused: bool,
    /// The next slot. `None` never fires.
    pub next_run_at: Option<DateTime<Utc>>,
    /// The slot of the last started run.
    pub last_run_at: Option<DateTime<Utc>>,
}

/// Insert or update `schedule`.
pub fn upsert(_conn: &Connection, _schedule: &WorkflowSchedule, _now: i64) -> SqliteResult<()> {
    Err(not_yet())
}

/// Remove the schedule of `workflow_name`.
pub fn delete(_conn: &Connection, _workflow_name: &str) -> SqliteResult<()> {
    Err(not_yet())
}

/// Pause or resume the schedule of `workflow_name`.
pub fn set_paused(
    _conn: &Connection,
    _workflow_name: &str,
    _paused: bool,
    _now: i64,
) -> SqliteResult<()> {
    Err(not_yet())
}

/// Every stored schedule, by workflow name.
pub fn list(_conn: &Connection) -> SqliteResult<Vec<ScheduleSummary>> {
    Err(not_yet())
}

fn not_yet() -> SqliteError {
    SqliteError::Unsupported("schedules — not implemented yet (issue #2031)".to_string())
}
