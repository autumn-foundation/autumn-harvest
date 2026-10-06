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

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

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

/// The named in-memory databases that a runtime in this process holds, keyed
/// by decoded name.
///
/// `SQLite` reports no file for such a database, so no OS lock applies. Every
/// connection in the process can still open it, so this set stands in.
static SHARED_MEMORY: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// An acquired single-writer lock. Dropping it releases the lock.
#[derive(Debug)]
pub enum WriterLock {
    /// An OS lock on the sidecar file. Dropping the file releases it.
    // Held only for its drop, which releases the OS lock.
    #[allow(dead_code)]
    File(File),
    /// An entry in [`SHARED_MEMORY`]. Drop removes it.
    SharedMemory(String),
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        if let Self::SharedMemory(name) = self {
            SHARED_MEMORY
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(name);
        }
    }
}

/// Take the single-writer lock for the database behind `conn`.
///
/// `requested` is the path the caller passed to `open`. It is the fallback when
/// `SQLite` reports no path, which happens for a non-UTF-8 path.
///
/// Returns `None` when `SQLite` reports an empty path for a private database:
/// `:memory:` or a temporary file. A named in-memory URI, such as
/// `file:name?mode=memory&cache=shared` or `file:/name?vfs=memdb`, takes an
/// in-process lock instead.
///
/// # Errors
///
/// Returns [`SqliteError::DatabaseLocked`] when another runtime holds the lock.
/// Returns [`SqliteError::Io`] when the lock file cannot be opened or locked.
pub fn acquire(conn: &Connection, requested: &Path) -> SqliteResult<Option<WriterLock>> {
    let db_path = match conn.path() {
        Some("") => return acquire_shared_memory(requested),
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

/// Lock a named in-memory database in this process. A private one needs no
/// lock.
///
/// The URI is read as `SQLite` reads it. The name is the part before `?`.
/// Every query key and value has its `%HH` escapes decoded.
///
/// The rule fails closed. Several URI forms share one database by name, for
/// example `cache=shared` or `vfs=memdb`. So every `file:` URI takes the lock,
/// keyed by its decoded name. Only an empty or `:memory:` name without
/// `cache=shared` is provably private and takes none. Two runtimes on one
/// private named URI in one process therefore conflict. That is safe.
fn acquire_shared_memory(requested: &Path) -> SqliteResult<Option<WriterLock>> {
    let uri = requested.to_string_lossy();
    let Some(rest) = uri
        .get(..5)
        .filter(|scheme| scheme.eq_ignore_ascii_case("file:"))
        .map(|_| &uri[5..])
    else {
        // Not a URI, such as `:memory:` or an empty name: always private.
        return Ok(None);
    };
    let rest = rest.split_once('#').map_or(rest, |(before, _)| before);
    let (raw_name, query) = rest.split_once('?').unwrap_or((rest, ""));
    let params: Vec<(String, String)> = query
        .split('&')
        .filter(|param| !param.is_empty())
        .map(|param| {
            let (key, value) = param.split_once('=').unwrap_or((param, ""));
            (percent_decode(key), percent_decode(value))
        })
        .collect();
    let cache_shared = params
        .iter()
        .any(|(key, value)| key == "cache" && value.eq_ignore_ascii_case("shared"));
    let name = percent_decode(raw_name);
    if !cache_shared && (name.is_empty() || name == ":memory:") {
        return Ok(None);
    }
    // The key is the name alone. A VFS has several spellings, such as an
    // omitted default and `vfs=unix`. One name under two VFSes then conflicts,
    // which is safe.
    let inserted = SHARED_MEMORY
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(name.clone());
    if !inserted {
        return Err(SqliteError::DatabaseLocked {
            path: requested.to_path_buf(),
        });
    }
    Ok(Some(WriterLock::SharedMemory(name)))
}

/// Decode `%HH` escapes as `SQLite` does for a URI component. A malformed
/// escape stays as it is. A decoded NUL ends the component, as in `SQLite`.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|pair| std::str::from_utf8(pair).ok())
            .and_then(|pair| u8::from_str_radix(pair, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(0)) => break,
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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
