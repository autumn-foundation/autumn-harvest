//! The exclusive, per-database daemon lock.
//!
//! [`SqliteRuntime::open`](autumn_harvest_sqlite::SqliteRuntime::open) reclaims
//! every task left `RUNNING` by a previous process: with one writer, such a row
//! can only be an orphan. A second daemon that opens the same file while the
//! first is executing an activity breaks that assumption. It reclaims a task
//! that is genuinely running, so the activity runs a second time. That is a
//! duplicate model request, a duplicate charge, and a duplicate tool effect.
//!
//! The socket is not the guard for this. A second daemon reaches
//! `SqliteRuntime::open` before it discovers the socket, and a different
//! `--socket` lets two daemons write one file forever. So the lock is held on
//! the DATABASE, and it is taken before the file is opened.
//!
//! `flock` is the mechanism because the kernel releases it when the holder
//! dies, however it dies. A crashed daemon therefore strands nothing, which is
//! what keeps the restart path clean.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use rustix::fs::{FlockOperation, flock};

/// The lock one daemon holds for the life of its process.
///
/// The open descriptor **is** the lock. Dropping this value closes it and
/// releases the lock; so does process exit.
pub struct DaemonLock {
    _file: File,
    path: PathBuf,
}

impl DaemonLock {
    /// The lock file this daemon holds.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Take the exclusive lock for `db`, or report who holds it.
///
/// # Errors
///
/// Returns an error if the lock file cannot be created, or if another process
/// already holds the lock.
pub fn acquire(db: &Path) -> Result<DaemonLock, String> {
    let path = lock_path(db)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| format!("cannot open the lock file {}: {e}", path.display()))?;

    // Non-blocking, so a second daemon fails at once with a clear message
    // rather than hanging on a lock it will never get.
    flock(&file, FlockOperation::NonBlockingLockExclusive).map_err(|_| {
        format!(
            "another daemon holds {}. One writer owns one database file.",
            db.display()
        )
    })?;

    Ok(DaemonLock { _file: file, path })
}

/// The lock file that belongs to `db`.
///
/// The name is derived rather than fixed, so two databases in one directory
/// take two different locks. It is derived from the RESOLVED path, not the
/// spelling. Two daemons can name one file differently: `current.db -> real.db`
/// and `real.db` open the same database, so both must take the same lock. A
/// database that does not exist yet is resolved through its parent directory,
/// which does.
fn lock_path(db: &Path) -> Result<PathBuf, String> {
    let resolved = if db.exists() {
        db.canonicalize()
            .map_err(|e| format!("cannot resolve {}: {e}", db.display()))?
    } else {
        let parent = match db.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        let name = db
            .file_name()
            .ok_or_else(|| format!("{} is not a database file name", db.display()))?;
        parent
            .canonicalize()
            .map_err(|e| format!("cannot resolve {}: {e}", parent.display()))?
            .join(name)
    };

    let mut name = resolved.into_os_string();
    name.push(".lock");
    Ok(PathBuf::from(name))
}
