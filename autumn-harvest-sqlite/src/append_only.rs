//! A test seam for the append-only guard on `harvest_events` (issue #2031).

use rusqlite::Connection;

use crate::error::SqliteResult;
use crate::schema;

/// Run `f` in one transaction with the append-only guard off, then restore
/// the guard.
///
/// This is for a test fixture that must damage a stored event, as the
/// Postgres `append_only::with_guard_off` is. Production code must not call
/// it. The guard stops mistakes. It is not a security boundary.
///
/// The drop, the write and the restore share one transaction. So an error
/// in `f` rolls back all three, and the guard stays.
///
/// # Errors
///
/// Returns the error of `f`, or [`SqliteError::Sqlite`](crate::SqliteError::Sqlite)
/// if the transaction fails.
#[doc(hidden)]
pub fn with_guard_off<T>(
    conn: &mut Connection,
    f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
) -> SqliteResult<T> {
    let tx = conn.transaction()?;
    tx.execute_batch(schema::DROP_APPEND_ONLY_GUARD)?;
    let out = f(&tx)?;
    tx.execute_batch(schema::APPEND_ONLY_GUARD)?;
    tx.commit()?;
    Ok(out)
}
