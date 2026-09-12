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
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

use rustix::fs::{FlockOperation, Mode, flock};
use rustix::process::umask;

/// The mode a new database and its sidecars are created with.
const PRIVATE_MODE: u32 = 0o600;

/// The mask that yields [`PRIVATE_MODE`] for anything created under it.
const PRIVATE_UMASK: u32 = 0o177;

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
    // The database holds every prompt, tool input, and tool result, which
    // includes the content of each file the agent read. It is at least as
    // sensitive as the control socket, so it is private from the moment it
    // exists. The mode applies at CREATION only, so an existing database keeps
    // whatever the operator chose for it.
    //
    // The umask is narrowed around the open, and the mode alone is not enough.
    // A creation mode is filtered by the umask in force. Under `0777` the file
    // would be created mode `000`, and `SQLite` could then not reopen the path
    // it was just given. The daemon would leave a database no later start
    // could use.
    let file = with_private_umask(|| {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(PRIVATE_MODE)
            .open(db)
    })
    .map_err(|e| format!("cannot open {}: {e}", db.display()))?;

    // A hard-linked database has no single identity, and `SQLite` cannot work
    // with that. It derives the `-wal` name from the PATH, so opening
    // `alias.db` reads `alias.db-wal` and never sees what `real.db-wal` holds.
    // After an unclean exit the committed sessions in the original write-ahead
    // log become invisible, and the daemon reports an empty database. A
    // symbolic link is fine, because `SQLite` resolves it and reaches the same
    // sidecars. The lock below cannot help here: it makes the two names share
    // one lock, not one write-ahead log.
    let links = file
        .metadata()
        .map_err(|e| format!("cannot inspect {}: {e}", db.display()))?
        .nlink();
    if links > 1 {
        return Err(format!(
            "{} has {links} hard links. `SQLite` derives its write-ahead log \
             from the path, so each name would read a different log and lose \
             the other's committed sessions. Open it by one name only.",
            db.display()
        ));
    }

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

/// Run `f` with a mask that makes everything it creates owner-only.
///
/// `SQLite` creates the `-wal` and `-shm` sidecars itself, and the control
/// socket is created by the listener. Neither takes a mode from this code, so
/// the mask is what makes them private. It does so AT CREATION, leaving no
/// window in which another local user can open them.
pub fn with_private_umask<T>(f: impl FnOnce() -> T) -> T {
    let previous = umask(Mode::from_bits_truncate(PRIVATE_UMASK));
    let result = f();
    umask(previous);
    result
}
