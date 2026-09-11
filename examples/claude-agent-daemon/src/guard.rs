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
//! The lock is taken on the database FILE ITSELF, never on a sidecar named
//! after its path. Many names reach one database: a symbolic link, a hard link,
//! a relative spelling. Only the file's own identity collapses every one of
//! them, and an open descriptor is exactly that identity.
//!
//! `flock` is the mechanism for two reasons. The kernel releases it when the
//! holder dies, however it dies, so a crashed daemon strands nothing and the
//! restart path stays clean. And `SQLite` locks with `fcntl` record locks,
//! which is a separate domain, so this lock never contends with the engine.

use std::fs::{File, OpenOptions};
use std::path::Path;

use rustix::fs::{FlockOperation, flock};

/// The lock one daemon holds for the life of its process.
///
/// The open descriptor **is** the lock. Dropping this value closes it and
/// releases the lock; so does process exit.
pub struct DaemonLock {
    _file: File,
}

/// Take the exclusive lock for `db`, or report that another daemon holds it.
///
/// # Errors
///
/// Returns an error if the database file cannot be opened, or if another
/// process already holds the lock.
pub fn acquire(db: &Path) -> Result<DaemonLock, String> {
    // Create the file when it is absent. An empty file is a valid, empty
    // `SQLite` database, and the runtime initializes it on open. Creating it
    // here is what gives a brand-new database an identity to lock.
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(db)
        .map_err(|e| format!("cannot open {}: {e}", db.display()))?;

    // Non-blocking, so a second daemon fails at once with a clear message
    // rather than hanging on a lock it will never get.
    flock(&file, FlockOperation::NonBlockingLockExclusive).map_err(|_| {
        format!(
            "another daemon holds {}. One writer owns one database file.",
            db.display()
        )
    })?;

    Ok(DaemonLock { _file: file })
}
