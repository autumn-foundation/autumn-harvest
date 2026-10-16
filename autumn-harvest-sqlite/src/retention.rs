//! Retention: remove old terminal runs (issue #2031).

use rusqlite::Connection;

use crate::error::{SqliteError, SqliteResult};

/// Remove each terminal run closed before `cutoff_micros`. Returns the
/// number of runs removed.
pub fn purge_terminal(_conn: &mut Connection, _cutoff_micros: i64) -> SqliteResult<u64> {
    Err(SqliteError::Unsupported(
        "purge_terminal — not implemented yet (issue #2031)".to_string(),
    ))
}
