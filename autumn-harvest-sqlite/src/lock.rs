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
//!
//! An in-memory database has no file. Several URI forms still share one by
//! name, such as `cache=shared` or `vfs=memdb`. Its lock is therefore an owner
//! row inside the database itself. `SQLite` decides which connections reach
//! the same database, so no alias can split the lock. A private database starts
//! empty, so it never conflicts. The whole database dies with its process, so
//! a crash leaves no stale row.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::error::{SqliteError, SqliteResult};

/// How many times [`acquire`] tries the lock before it reports
/// [`SqliteError::DatabaseLocked`].
///
/// A dropped runtime can leave its lock held for a moment. A child process
/// that another thread forks holds a copy of the lock until it calls `exec`.
/// On Windows, the release after a process ends can also lag. A short retry
/// rides out those windows. A live holder still fails the open in about
/// 100 ms.
const LOCK_ATTEMPTS: u32 = 5;

/// The pause between two lock attempts.
const LOCK_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

/// The table that holds the owner row of an in-memory database.
const MEMORY_LOCK_TABLE: &str = "harvest_memory_writer_lock";

/// An acquired single-writer lock.
#[derive(Debug)]
pub enum WriterLock {
    /// An OS lock on the sidecar file. Dropping the file releases it.
    // Held only for its drop, which releases the OS lock.
    #[allow(dead_code)]
    File(File),
    /// The owner row of an in-memory database, by its token.
    /// [`release`](Self::release) deletes it.
    Memory(String),
}

impl WriterLock {
    /// Release a lock that needs the connection. The runtime calls this when it
    /// drops. A file lock releases itself.
    pub fn release(&self, conn: &Connection) {
        if let Self::Memory(token) = self {
            let sql = format!("DELETE FROM {MEMORY_LOCK_TABLE} WHERE token = ?1");
            if let Err(err) = conn.execute(&sql, [token]) {
                tracing::warn!(error = %err, "could not release the in-memory writer lock");
            }
        }
    }
}

/// Take the single-writer lock for the database behind `conn`.
///
/// `requested` is the path the caller passed to `open`. It is the fallback when
/// `SQLite` reports no path, which happens for a non-UTF-8 path.
///
/// An in-memory or temporary database, which `SQLite` reports with an empty
/// path, takes the owner-row lock instead of a file lock.
///
/// # Errors
///
/// Returns [`SqliteError::DatabaseLocked`] when another runtime holds the lock.
/// Returns [`SqliteError::Io`] when the lock file cannot be opened or locked.
/// Returns [`SqliteError::Sqlite`] when the owner row cannot be written.
pub fn acquire(conn: &Connection, requested: &Path) -> SqliteResult<Option<WriterLock>> {
    let db_path = match conn.path() {
        Some("") => return acquire_memory(conn, requested),
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
    let mut attempt = 1;
    loop {
        // Fully qualified: std has an inherent `File::try_lock` from Rust 1.89
        // with a different error type, and the MSRV is 1.88.
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => return Ok(Some(WriterLock::File(file))),
            Err(fs4::TryLockError::WouldBlock) if attempt < LOCK_ATTEMPTS => {
                attempt += 1;
                std::thread::sleep(LOCK_RETRY_DELAY);
            }
            Err(fs4::TryLockError::WouldBlock) => {
                return Err(SqliteError::DatabaseLocked { path });
            }
            Err(fs4::TryLockError::Error(source)) => {
                return Err(SqliteError::Io { path, source });
            }
        }
    }
}

/// Take the owner row of an in-memory database. A second runtime that reaches
/// the same database finds the row and fails.
fn acquire_memory(conn: &Connection, requested: &Path) -> SqliteResult<Option<WriterLock>> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {MEMORY_LOCK_TABLE} \
         (id INTEGER PRIMARY KEY CHECK (id = 1), token TEXT NOT NULL);"
    ))?;
    let token = uuid::Uuid::new_v4().to_string();
    let inserted = conn.execute(
        &format!("INSERT OR IGNORE INTO {MEMORY_LOCK_TABLE} (id, token) VALUES (1, ?1)"),
        [&token],
    )?;
    if inserted == 0 {
        return Err(SqliteError::DatabaseLocked {
            path: requested.to_path_buf(),
        });
    }
    Ok(Some(WriterLock::Memory(token)))
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
