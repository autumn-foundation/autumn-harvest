//! Ephemeral-session bookkeeping and the stale-session reaper (issue #525, AC5).
//!
//! "Teardown reclaims all ephemeral state" has to hold for three exits, not one:
//!
//! 1. a clean `Ctrl-C` — handled by `EphemeralPostgres::shutdown`;
//! 2. a panic or an early return — handled by its `Drop` guard;
//! 3. `SIGKILL`, a closed laptop lid, a pulled plug — handled *here*, on the
//!    next start, because nothing in the dying process gets to run.
//!
//! Each session therefore writes a small record next to its data directory
//! naming the process that owns it and the postmaster it started. A later run
//! reads those records and reclaims any session whose owner is gone.
//!
//! The decision itself is a pure function so every branch is testable without
//! spawning anything.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// File name of the per-session record, written inside the session directory.
pub const SESSION_RECORD_FILE: &str = "session.json";

/// Prefix of every dev-runtime session directory. The reaper only ever
/// considers directories with this prefix that also hold a parseable record.
pub const SESSION_DIR_PREFIX: &str = "session-";

/// Name of the per-user root that holds every session directory.
///
/// Sessions deliberately do **not** live directly in the system temp
/// directory. `/tmp` is world-writable, and the reaper stops processes by pid
/// and deletes directory trees — so a session record there is an instruction
/// any other local user could plant. The root is created `0700` and owned by
/// us, and the reaper refuses to work in one that is not.
pub const SESSION_ROOT_PREFIX: &str = "harvest-dev-";

/// What one dev-runtime session left on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    /// Process that created the session and is responsible for tearing it down.
    pub owner_pid: u32,
    /// The owner's start time, as the kernel reports it.
    ///
    /// The same reasoning as [`postmaster_start_token`](Self::postmaster_start_token),
    /// applied to the other pid in this record: a force-killed run frees its
    /// pid, and an unrelated process that inherits the number would make the
    /// session look permanently alive, so it would never be reclaimed.
    #[serde(default)]
    pub owner_start_token: Option<String>,
    /// The postmaster this session started, once it is running.
    pub postmaster_pid: Option<u32>,
    /// The cluster's data directory.
    pub data_dir: PathBuf,
    /// The `bin` directory of the `PostgreSQL` install that started this
    /// cluster.
    ///
    /// Recorded because *discovery cannot always find it again*. A default
    /// `cargo dev` on a machine with no `PostgreSQL` downloads one into a
    /// per-user cache that `PostgresBinaries::discover` does not search — so
    /// after a force-kill, the reaper had no `pg_ctl` for that cluster. On
    /// Windows it also had no fallback: `process_start_token` returns `None`
    /// there, so the identity check that gates a direct `taskkill` can never
    /// pass, and the orphaned postmaster and its data directory would have
    /// survived every later start, forever.
    ///
    /// `#[serde(default)]` so a record written before this field existed still
    /// parses rather than being skipped as unreadable.
    #[serde(default)]
    pub bin_dir: Option<PathBuf>,
    /// The postmaster's start time, as the kernel reports it.
    ///
    /// A pid alone does not identify a process: pids are reused, and the gap
    /// between a `SIGKILL`ed run and the next `cargo dev` is exactly long
    /// enough for that to happen. Recording the start time turns "pid 4243"
    /// into "the process that started at tick 87231", so the reaper can prove
    /// the thing it is about to stop is the thing it recorded. `None` where the
    /// platform cannot supply it, in which case the reaper never signals — it
    /// stops the cluster through `pg_ctl` or leaves it alone.
    #[serde(default)]
    pub postmaster_start_token: Option<String>,
    /// When the session started, for diagnostics.
    pub created_at: DateTime<Utc>,
}

impl SessionRecord {
    /// Serialise to the on-disk form.
    ///
    /// # Errors
    ///
    /// Propagates any `serde_json` failure.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Parse the on-disk form.
    ///
    /// # Errors
    ///
    /// Returns the `serde_json` error for malformed or incomplete input. A
    /// record we cannot read is reported, never guessed at: the reaper stops
    /// processes and deletes directories, so it acts only on records it fully
    /// understands.
    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }
}

/// What the reaper should do with one session directory.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReapDecision {
    /// Leave it alone; it belongs to a live session.
    Skip(SkipReason),
    /// Stop the orphaned postmaster, then remove the directory.
    StopThenRemove {
        /// The postmaster to stop.
        postmaster_pid: u32,
    },
    /// Nothing is running; just remove the directory.
    Remove,
}

/// Why a session directory is left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// This very process owns it.
    OwnedByThisProcess,
    /// Another live process owns it — a concurrent `cargo dev`.
    OwnerAlive,
}

/// Whether a session record describes a directory we are willing to act on.
///
/// The reaper stops processes and deletes trees, so it acts only on records
/// whose claims are self-consistent: the data directory must be the one this
/// layout puts inside the session directory. A record pointing anywhere else is
/// either corrupt or planted, and either way is not ours to act on.
#[must_use]
pub fn record_is_self_consistent(record: &SessionRecord, session_dir: &Path) -> bool {
    record.data_dir == session_dir.join("data")
}

/// Decide what to do with one session record.
///
/// Pure: liveness is supplied by the caller so the whole table can be tested
/// without processes.
#[must_use]
pub const fn decide_reap(
    record: &SessionRecord,
    owner_alive: bool,
    postmaster_alive: bool,
    self_pid: u32,
) -> ReapDecision {
    // Identity beats liveness. A session directory carrying our own pid is one
    // we are about to use (or a same-pid predecessor's, which we would then be
    // deleting out from under ourselves).
    if record.owner_pid == self_pid {
        return ReapDecision::Skip(SkipReason::OwnedByThisProcess);
    }
    if owner_alive {
        return ReapDecision::Skip(SkipReason::OwnerAlive);
    }
    match record.postmaster_pid {
        Some(postmaster_pid) if postmaster_alive => ReapDecision::StopThenRemove { postmaster_pid },
        _ => ReapDecision::Remove,
    }
}

/// The postmaster pid to act on for one session.
///
/// The record is written **before** the server starts (so a crash during
/// startup still leaves something the reaper can find), which means its
/// `postmaster_pid` is `None` for the whole start window. Postgres's own
/// `postmaster.pid` covers exactly that window, so it is the fallback: without
/// it, a crash between `pg_ctl start` and the record update would leave a
/// record saying "no server" next to a running server, and the reaper would
/// delete a live cluster's data directory out from under it.
#[must_use]
pub fn effective_postmaster_pid(
    record: &SessionRecord,
    pid_file_contents: Option<&str>,
) -> Option<u32> {
    record
        .postmaster_pid
        .or_else(|| pid_file_contents.and_then(parse_postmaster_pid))
}

/// Read the postmaster's pid from a `postmaster.pid` file's contents.
///
/// Postgres writes the pid on the first line. A truncated or half-written file
/// — which is exactly what a crash leaves — yields `None` rather than a wrong
/// pid, because the reaper would otherwise stop an unrelated process.
#[must_use]
pub fn parse_postmaster_pid(contents: &str) -> Option<u32> {
    contents.lines().next()?.trim().parse().ok()
}

/// Whether `dir` looks like a dev-runtime session directory.
#[must_use]
pub fn is_session_dir(dir: &Path) -> bool {
    dir.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(SESSION_DIR_PREFIX))
        && dir.join(SESSION_RECORD_FILE).is_file()
}
