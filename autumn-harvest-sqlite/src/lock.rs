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
//!   a relative path to the same file maps to the same lock. A hard link or a
//!   bind mount gives a second path, and so a second lock.
//!
//! The lock file stays on disk after the runtime drops. Deleting it while a
//! runtime runs lets a second process lock a new file, so do not delete it.
//!
//! On Unix the lock file takes the database file's read and write bits. `flock`
//! needs only an open file, so a user who can open the lock file can hold it.
//! Matching the database keeps that set to the users who can reach the data.

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
/// `requested` is the path the caller passed to `open`. It is the fallback when
/// `SQLite` reports no path, which happens for a non-UTF-8 path.
///
/// Returns `None` when `SQLite` reports an empty path: an in-memory or a
/// temporary database. A shared-cache in-memory URI is in this group too, so
/// two runtimes in one process can share it without a lock. Do not do that.
///
/// # Errors
///
/// Returns [`SqliteError::DatabaseLocked`] when another runtime holds the lock.
/// Returns [`SqliteError::Io`] when the lock file cannot be opened or locked.
pub fn acquire(conn: &Connection, requested: &Path) -> SqliteResult<Option<WriterLock>> {
    let db_path = match conn.path() {
        Some("") => return Ok(None),
        Some(reported) => Path::new(reported),
        None => requested,
    };
    let canonical = canonicalize(db_path)?;
    let path = lock_path(&canonical);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        // The lock file takes the database's read and write bits. The umask
        // still applies.
        let mode =
            std::fs::metadata(&canonical).map_or(0o600, |meta| meta.permissions().mode() & 0o666);
        options.mode(mode);
    }
    let file = options.open(&path).map_err(|source| SqliteError::Io {
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

/// The canonical form of the database path.
fn canonicalize(db_path: &Path) -> SqliteResult<PathBuf> {
    std::fs::canonicalize(db_path).map_err(|source| SqliteError::Io {
        path: db_path.to_path_buf(),
        source,
    })
}

/// The sidecar lock path for a canonical database path: `<path>.lock`.
fn lock_path(canonical: &Path) -> PathBuf {
    let mut name = canonical.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}
