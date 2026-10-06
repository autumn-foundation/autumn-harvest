//! The single-writer lock (issue #1834).
//!
//! [`SqliteRuntime::open`](crate::SqliteRuntime::open) reclaims every `RUNNING`
//! task. Two writers on one file would therefore steal each other's work. This
//! module turns the documented one-writer rule into a checked one.
//!
//! The lock is an exclusive OS advisory lock (`flock` / `LockFileEx`) on a
//! sidecar file, `<database>.lock`. The design follows from three rules:
//!
//! - **The kernel releases the lock when its process exits.** A crash therefore
//!   never leaves a stale lock. A PID file would need a liveness check.
//! - **The lock is on a sidecar file, not on the database.** `SQLite` takes
//!   its own byte-range locks on the database file. On Windows a second lock
//!   there blocks `SQLite` I/O. A read-only inspector also keeps working,
//!   because it never touches the sidecar.
//! - **The sidecar path comes from the canonical database path.** A symlink or
//!   a relative path to the same file maps to the same lock.
//!
//! The lock file stays on disk after the runtime drops. Deleting it while a
//! runtime runs lets a second process lock a new file, so do not delete it.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::error::{SqliteError, SqliteResult};

/// An acquired single-writer lock. Dropping it closes the file and so
/// releases the lock.
#[derive(Debug)]
pub struct WriterLock {
    _file: File,
}

/// Take the single-writer lock for the database behind `conn`.
///
/// Returns `None` for an in-memory or temporary database, which no other
/// connection can open.
///
/// # Errors
///
/// Returns [`SqliteError::DatabaseLocked`] when another runtime holds the lock.
/// Returns [`SqliteError::Io`] when the lock file cannot be opened or locked.
pub fn acquire(conn: &Connection) -> SqliteResult<Option<WriterLock>> {
    let Some(db_path) = conn.path().filter(|path| !path.is_empty()) else {
        return Ok(None);
    };
    let path = lock_path(Path::new(db_path))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| SqliteError::Io {
            path: path.clone(),
            source,
        })?;
    // Fully qualified: std has an inherent `File::try_lock` from Rust 1.89
    // with a different error type, and the MSRV is 1.88.
    match fs4::FileExt::try_lock(&file) {
        Ok(()) => Ok(Some(WriterLock { _file: file })),
        Err(fs4::TryLockError::WouldBlock) => Err(SqliteError::DatabaseLocked { path }),
        Err(fs4::TryLockError::Error(source)) => Err(SqliteError::Io { path, source }),
    }
}

/// The sidecar lock path for a database file: `<canonical path>.lock`.
fn lock_path(db_path: &Path) -> SqliteResult<PathBuf> {
    let canonical = std::fs::canonicalize(db_path).map_err(|source| SqliteError::Io {
        path: db_path.to_path_buf(),
        source,
    })?;
    let mut name = canonical.into_os_string();
    name.push(".lock");
    Ok(PathBuf::from(name))
}
