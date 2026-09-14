//! Reclaiming what a killed dev run left behind (issue #525, AC5).
//!
//! `shutdown` and the `Drop` guard cover the exits a process gets to observe.
//! `SIGKILL` is not one of them, and a developer who kills a wedged `cargo dev`
//! is doing the most ordinary thing in the world. Without this, every such kill
//! would leak a postmaster holding a port and a data directory that nothing will
//! ever remove.
//!
//! So the *next* start reaps: it reads the session records under the per-user
//! session root and reclaims each one whose owning process is gone.
//!
//! # This code stops processes and deletes trees, so it is paranoid
//!
//! A session record is an instruction to `SIGKILL` a pid and `rm -rf` a
//! directory. Four things stand between that and a planted record:
//!
//! 1. **The root is per-user, owned by us, and `0700` — checked on every use.**
//!    Sessions never live directly in the world-writable system temp directory,
//!    where any other local account could create `harvest-dev-*/session.json`
//!    naming a pid of ours, or a `bin_dir` whose `pg_ctl` we would then run.
//!    Ownership is a real, uid-based check on Unix. Windows has no such check
//!    yet (issue #1287); `directory_is_ours` falls back to a weaker location
//!    heuristic there, and says so in its own doc comment.
//! 2. **The record must be self-consistent.** Its `data_dir` has to be the one
//!    this layout puts inside the session directory; a record pointing elsewhere
//!    is corrupt or planted and is left alone.
//! 3. **A pid is not an identity.** The recorded postmaster start time must
//!    still match, so a reused pid is never mistaken for the process we
//!    started. A record with no start time is *unknown*, not a match — a
//!    live pid there is left alone rather than reaped (issue #1295).
//! 4. **No blind kill.** A cluster we could not stop through `pg_ctl` is left
//!    running *and* its directory is left in place, because deleting the data
//!    directory out from under a live postmaster is worse than leaking it.

use std::path::{Path, PathBuf};

use super::discovery::PostgresBinaries;
use super::session::{
    PostmasterIdentity, ReapDecision, SESSION_RECORD_FILE, SESSION_ROOT_PREFIX, SessionRecord,
    SkipReason, decide_reap, effective_postmaster_pid, is_session_dir, parse_postmaster_pid,
    record_is_self_consistent,
};
use super::{DevError, postgres};

/// The per-user root that holds this machine's dev sessions.
///
/// # Errors
///
/// [`DevError::SessionDir`] if the root cannot be created or cannot be made
/// owner-only. Failing closed here is deliberate: a root we cannot keep private
/// is a root whose records we must not trust.
pub fn session_root(base: &Path) -> Result<PathBuf, DevError> {
    let root = base.join(format!("{SESSION_ROOT_PREFIX}{}", current_user_token()));
    std::fs::create_dir_all(&root).map_err(|source| DevError::SessionDir {
        path: root.clone(),
        source,
    })?;
    harden_root(&root)?;
    Ok(root)
}

/// Make the session root owner-only, and refuse a root that is not ours.
///
/// A symlink is rejected outright — another user could repoint it. Ownership is
/// then checked directly: an unprivileged process cannot `chmod` a foreign
/// directory, but `root` can, so a successful `chmod` is evidence only when we
/// are not `root` and the uid comparison is what actually decides.
fn harden_root(root: &Path) -> Result<(), DevError> {
    let metadata = std::fs::symlink_metadata(root).map_err(|source| DevError::SessionDir {
        path: root.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DevError::UntrustedSessionRoot {
            path: root.to_path_buf(),
            reason: "it is a symlink, so another local user could repoint it",
        });
    }
    if let Err(reason) = directory_is_ours(root, &metadata) {
        return Err(DevError::UntrustedSessionRoot {
            path: root.to_path_buf(),
            reason,
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // Ownership, not permission to chmod. For an unprivileged user those
        // are the same question, but `root` can `chmod` a directory any local
        // user pre-created — so a successful chmod proves nothing at uid 0,
        // and the records inside name a `bin_dir` whose `pg_ctl` the reaper
        // then runs. `directory_is_ours` above is what actually decides.
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).map_err(|_| {
            DevError::UntrustedSessionRoot {
                path: root.to_path_buf(),
                reason: "it cannot be made owner-only (0700), so it is not ours",
            }
        })?;
    }
    Ok(())
}

/// Whether a non-symlink directory is ours alone to trust.
///
/// The ownership question `harden_root` above and `directory_is_private`
/// (`acquire.rs`) both ask, before this crate executes or deletes anything a
/// directory holds. One helper, so the two sites cannot drift the way a
/// `cfg(unix)`-only gap once let them (issue #1287).
///
/// # Unix vs Windows
///
/// Unix answers directly: the uid on `metadata` must be ours. Windows has no
/// such check yet. Its own follow-up is issue #1287. The fallback there is a
/// location heuristic: `dir` must resolve under a per-user root
/// (`%LOCALAPPDATA%` or `%USERPROFILE%`). That proves WHERE the directory is,
/// not WHO else can write to it. It is weaker than the Unix answer, and the
/// error text below says so rather than claiming ownership was verified.
pub(super) fn directory_is_ours(
    dir: &Path,
    metadata: &std::fs::Metadata,
) -> Result<(), &'static str> {
    // Each parameter is read on only one platform below. Discarding both up
    // front keeps every other platform from warning on the unused one.
    // This needs no per-cfg `#[allow(unused)]`. References are `Copy`, so
    // the real reads further down still see the same values.
    let _ = (dir, metadata);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if let Some(uid) = unix_uid()
            && metadata.uid() != uid
        {
            return Err("it belongs to another user, so its contents are not ours to trust");
        }
    }
    #[cfg(windows)]
    {
        if !windows_path_is_per_user(dir) {
            return Err(
                "on Windows its ownership cannot yet be verified, and it is not under a \
                 per-user directory (%LOCALAPPDATA% or %USERPROFILE%) either",
            );
        }
    }
    Ok(())
}

/// Whether `dir` resolves under a per-user Windows root.
///
/// A location heuristic, not proof of ownership: nothing stops an
/// administrator from redirecting `%LOCALAPPDATA%` or `%USERPROFILE%`
/// machine-wide. It still closes the exposure issue #1287 describes. Every
/// default this crate ever picks on its own — `std::env::temp_dir()`, the
/// managed-cache root — already resolves under one of these two roots. So
/// this only ever refuses an explicitly configured shared location, exactly
/// the case that had no guard at all.
///
/// Both sides are canonicalised before comparison, so a case difference or a
/// `\\?\` prefix does not produce a false refusal. Canonicalisation failure
/// fails closed. A directory that cannot be resolved (a broken junction, say)
/// is refused rather than compared by its unresolved, lexical path. The
/// resolved target could turn out to be shared once the failure clears.
#[cfg(windows)]
fn windows_path_is_per_user(dir: &Path) -> bool {
    let Ok(dir) = std::fs::canonicalize(dir) else {
        return false;
    };
    ["LOCALAPPDATA", "USERPROFILE"]
        .into_iter()
        .filter_map(std::env::var_os)
        .filter(|root| !root.is_empty())
        .any(|root| {
            std::fs::canonicalize(&root)
                .is_ok_and(|canonical_root| dir.starts_with(&canonical_root))
        })
}

/// A stable, filesystem-safe identifier for the current user.
fn current_user_token() -> String {
    #[cfg(unix)]
    {
        if let Some(uid) = unix_uid() {
            return uid.to_string();
        }
    }
    std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .map_or_else(
            |_| "unknown".to_owned(),
            |name| {
                name.chars()
                    .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
                    .collect()
            },
        )
}

/// The **effective** uid, via `/proc/self/status` or `id -u`.
///
/// Effective, not real, and the distinction is the whole point: this answer
/// gates the root refusal *and* the session-root ownership check, and a process
/// with a non-zero real uid but euid 0 — a setuid-root launcher — has every
/// privilege the refusal exists to keep away from a planted session record.
/// `id -u` reports the effective id (`id -ru` is the real one), so both sources
/// must agree on that or the answer depends on which one happened to work.
///
/// `/usr/bin/id` by absolute path: resolving it through `PATH` would let a
/// planted `id` on a developer's `PATH` silently answer whatever it liked.
#[cfg(unix)]
pub(super) fn unix_uid() -> Option<u32> {
    // `/proc/self/status` first where it exists: no process spawn, and — more
    // importantly — no dependence on an `id` binary being present at all. When
    // it is missing, `unix_uid` returning `None` makes the root refusal fail
    // OPEN, replacing a legible error with `initdb`'s raw one.
    #[cfg(target_os = "linux")]
    if let Ok(status) = std::fs::read_to_string("/proc/self/status")
        && let Some(uid) = parse_proc_status_uid(&status)
    {
        return Some(uid);
    }
    for program in ["/usr/bin/id", "/bin/id"] {
        if let Ok(output) = std::process::Command::new(program).arg("-u").output()
            && output.status.success()
            && let Ok(uid) = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<u32>()
        {
            return Some(uid);
        }
    }
    None
}

/// The **effective** uid from a `/proc/self/status` body.
///
/// The `Uid:` line is `real  effective  saved  filesystem`, and the second
/// column is what `id -u` reports — reading the first made the two sources
/// disagree for any process whose real and effective ids differ, which is
/// exactly the setuid-root case the root refusal must not fail open on.
///
/// The filesystem uid (fourth column) is what the kernel actually attributes
/// new files to, and it equals the effective uid unless something calls
/// `setfsuid`, which nothing here does; the ownership check is written against
/// the effective uid on that basis.
#[cfg(target_os = "linux")]
#[must_use]
pub fn parse_proc_status_uid(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|rest| rest.split_whitespace().nth(1))
        .and_then(|uid| uid.parse().ok())
}

/// What Postgres's own `postmaster.pid` file says about a session.
///
/// Three states, not two: "there is no pid file" and "I could not read the pid
/// file" mean opposite things to a reaper that deletes data directories.
enum PidFile {
    /// Confirmed absent. `pg_ctl` removes it on a clean stop, so this is real
    /// evidence that no server is running.
    Absent,
    /// Present and readable.
    Present(String),
    /// Present but unreadable, or read but unparseable — a truncated,
    /// half-written file is exactly what a crash leaves. Evidence of nothing.
    Unreadable,
}

impl PidFile {
    /// The contents to hand [`effective_postmaster_pid`], if any.
    fn contents(&self) -> Option<&str> {
        match self {
            Self::Present(contents) => Some(contents),
            Self::Absent | Self::Unreadable => None,
        }
    }
}

/// Read a session's `postmaster.pid`, distinguishing absent from unreadable.
fn read_postmaster_pid_file(data_dir: &Path) -> PidFile {
    match std::fs::read_to_string(data_dir.join("postmaster.pid")) {
        Ok(contents) if parse_postmaster_pid(&contents).is_some() => PidFile::Present(contents),
        // `NotFound` is the one answer that is evidence. Everything else —
        // readable but with no pid on the first line (a half-written file), a
        // permission error, an I/O error — is uncertainty.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PidFile::Absent,
        Ok(_) | Err(_) => PidFile::Unreadable,
    }
}

/// Reclaim every abandoned session directory under `root`, returning how many
/// were reclaimed.
///
/// Conservative at every step — see the module docs for what each check buys.
/// A session whose owner is still alive is a concurrent `cargo dev`, not a
/// corpse.
///
/// # Errors
///
/// Only if `root` itself cannot be read. Per-session failures are logged and
/// skipped, because one unreadable leftover must not stop a developer's run.
pub fn reap_stale_sessions(root: &Path) -> Result<usize, std::io::Error> {
    if !root.is_dir() {
        return Ok(0);
    }
    let self_pid = std::process::id();
    // Resolved once, and only if there is something to stop: a machine whose
    // Postgres install has since been removed can still have its directories
    // reclaimed.
    let mut binaries: Option<Option<PostgresBinaries>> = None;
    let mut reclaimed = 0;

    for entry in std::fs::read_dir(root)? {
        let Ok(entry) = entry else { continue };
        let dir = entry.path();
        if !dir.is_dir() || !is_session_dir(&dir) {
            continue;
        }
        let Ok(raw) = std::fs::read_to_string(dir.join(SESSION_RECORD_FILE)) else {
            continue;
        };
        let mut record = match SessionRecord::from_json(&raw) {
            Ok(record) => record,
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    path = %dir.display(),
                    "dev runtime: leaving an unreadable session record alone"
                );
                continue;
            }
        };
        if !record_is_self_consistent(&record, &dir) {
            tracing::warn!(
                path = %dir.display(),
                "dev runtime: leaving a session record whose data directory is not its own"
            );
            continue;
        }

        // Close the start window: a record written before `pg_ctl start`
        // carries no pid, but the cluster it belongs to may well be running.
        let pid_file = read_postmaster_pid_file(&record.data_dir);
        if record.postmaster_pid.is_none() && matches!(pid_file, PidFile::Unreadable) {
            // Absence of a pid is only evidence when it is *confirmed* absence.
            // A transient read error, or the truncated file a crash mid-write
            // leaves, would otherwise read as "no server" — and `decide_reap`
            // would answer `Remove`, deleting the data directory out from under
            // a postmaster that may well still be running, along with the only
            // record that could ever stop it. Leave it for a run that can tell.
            tracing::warn!(
                path = %dir.display(),
                "dev runtime: leaving a session whose postmaster.pid could not be read"
            );
            continue;
        }
        record.postmaster_pid = effective_postmaster_pid(&record, pid_file.contents());

        let postmaster = record
            .postmaster_pid
            .map_or(PostmasterIdentity::NotRunning, |pid| {
                postmaster_identity(&record, pid)
            });
        let decision = decide_reap(
            &record,
            owner_is_the_recorded_one(&record),
            postmaster,
            self_pid,
        );
        match decision {
            ReapDecision::Skip(SkipReason::PostmasterIdentityUnknown) => {
                tracing::warn!(
                    path = %dir.display(),
                    "dev runtime: leaving a session whose postmaster identity cannot be confirmed"
                );
                continue;
            }
            ReapDecision::Skip(_) => continue,
            ReapDecision::StopThenRemove { postmaster_pid } => {
                // The record's own `bin_dir` first: a cluster started from the
                // downloaded cache lives where discovery does not look.
                let recorded = record
                    .bin_dir
                    .clone()
                    .filter(|dir| dir.is_dir())
                    .map(PostgresBinaries::at);
                let resolved = recorded.as_ref().or_else(|| {
                    binaries
                        .get_or_insert_with(|| PostgresBinaries::discover().ok())
                        .as_ref()
                });
                if !stop_orphan(resolved, &record, postmaster_pid) {
                    // Still running and we could not stop it. Removing the data
                    // directory now would corrupt a live cluster, so leave both.
                    tracing::warn!(
                        path = %dir.display(),
                        postmaster_pid,
                        "dev runtime: could not stop an abandoned cluster; leaving it and its \
                         data directory in place"
                    );
                    continue;
                }
            }
            ReapDecision::Remove => {}
        }

        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {
                reclaimed += 1;
                tracing::info!(
                    path = %dir.display(),
                    "dev runtime: reclaimed an abandoned session"
                );
            }
            Err(error) => tracing::warn!(
                error = %error,
                path = %dir.display(),
                "dev runtime: could not reclaim an abandoned session directory"
            ),
        }
    }

    Ok(reclaimed)
}

/// Whether the process at the recorded owner pid is still the run that created
/// this session.
///
/// Liveness alone is not enough, for the same reason it is not enough for the
/// postmaster: a force-killed run frees its pid, and an unrelated long-lived
/// process that inherits the number makes the session look permanently active —
/// so its cluster and data directory would survive every later start, forever.
/// The recorded start time is what tells the two apart.
///
/// Falls back to plain liveness when either side has no token (a record written
/// before the field existed; a platform that cannot supply one). That is the
/// pre-existing behaviour, and the conservative direction: it can only make us
/// skip a session, never reap a live one.
fn owner_is_the_recorded_one(record: &SessionRecord) -> bool {
    if !process_is_alive(record.owner_pid) {
        return false;
    }
    match (
        &record.owner_start_token,
        process_start_token(record.owner_pid),
    ) {
        (Some(recorded), Some(current)) => recorded == &current,
        _ => true,
    }
}

/// Identity of the process at `pid`, against the postmaster this record
/// started.
///
/// Pids are reused. A live pid alone does not prove identity. The window
/// between a `SIGKILL`ed run and the next `cargo dev` is exactly where reuse
/// happens.
///
/// `Unknown` is the answer when the record predates start-token recording,
/// or the platform cannot supply one — issue #1295. An earlier version of
/// this function treated that case as a match; that was wrong, for the
/// reason `stop_orphan` explains. Callers must treat `Unknown` as "leave it
/// alone", never as a match.
fn postmaster_identity(record: &SessionRecord, pid: u32) -> PostmasterIdentity {
    if !process_is_alive(pid) {
        return PostmasterIdentity::NotRunning;
    }
    match (&record.postmaster_start_token, process_start_token(pid)) {
        (Some(recorded), Some(current)) if recorded == &current => PostmasterIdentity::Confirmed,
        (Some(_), Some(_)) => PostmasterIdentity::NotRunning,
        _ => PostmasterIdentity::Unknown,
    }
}

/// Stop an orphaned cluster. Returns whether it is now confirmed stopped.
///
/// # Callers must already know the pid's identity
///
/// `decide_reap` reaches `StopThenRemove` only on `PostmasterIdentity::Confirmed`.
/// This function runs only on that path. The identity proof happens in the
/// caller, not here.
///
/// It cannot happen here: `pg_ctl stop` reads `postmaster.pid` itself. A
/// `SIGKILL` leaves that file stale. `PostgreSQL` removes the file only on a
/// clean shutdown. The `pg_ctl` liveness check on a stale pid also passes
/// for a reused pid. So `pg_ctl` is not a substitute identity check. Issue
/// #1295 fixed this by moving the proof into `decide_reap`, ahead of every
/// call here.
///
/// A direct `kill` below runs only when the recorded start token still
/// matches. That re-proves the same fact right before the signal. Windows
/// has no such token, so `pg_ctl` is the only path there. That is why the
/// record carries the `bin_dir` that started the cluster. Without it, a
/// force-killed run that had downloaded its own `PostgreSQL` would be
/// unstoppable on Windows.
fn stop_orphan(
    binaries: Option<&PostgresBinaries>,
    record: &SessionRecord,
    postmaster_pid: u32,
) -> bool {
    if let Some(binaries) = binaries {
        // Ignore the result and check the process instead: `pg_ctl` can exit 0
        // for a cluster it did not actually stop (a missing `postmaster.pid`,
        // say), and an early return on "it said OK" used to skip the
        // identity-proven signal below entirely — leaving the session
        // unreclaimable and re-warning on every future run.
        let _ = postgres::stop_cluster_blocking(binaries, &record.data_dir, Some(postmaster_pid));
        if !process_is_alive(postmaster_pid) {
            return true;
        }
    }
    // Either there are no binaries (an upgraded or uninstalled Postgres whose
    // process outlived it) or `pg_ctl` did not do the job. Signal only where we
    // can prove the pid is still the process we recorded.
    match (
        &record.postmaster_start_token,
        process_start_token(postmaster_pid),
    ) {
        (Some(recorded), Some(current)) if recorded == &current => {
            terminate_process(postmaster_pid);
            !process_is_alive(postmaster_pid)
        }
        _ => false,
    }
}

/// A token that, together with a pid, identifies one specific process.
///
/// The kernel's own start time. `None` where the platform cannot supply it, in
/// which case the caller must not signal.
#[must_use]
pub fn process_start_token(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| proc_stat_start_time(&stat))
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        std::process::Command::new("/bin/ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .filter(|token| !token.is_empty())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        None
    }
}

/// Field 22 (`starttime`) of a `/proc/<pid>/stat` line.
///
/// Fields are counted from the first token after the **last** `)`, because the
/// executable name in field 2 may itself contain spaces and parentheses. That
/// token is field 3 (`state`), so `starttime` is 19 further along.
#[must_use]
pub fn proc_stat_start_time(stat: &str) -> Option<String> {
    let (_, after_comm) = stat.rsplit_once(')')?;
    after_comm.split_whitespace().nth(19).map(str::to_owned)
}

/// Whether a process with this id is still *running*.
///
/// **A zombie is not running.** This distinction is the whole point: `pg_ctl`
/// daemonises the postmaster, so once it exits it is an orphan whose reaping
/// belongs to init — and in a container, or under any supervisor that is not a
/// subreaper, that can take arbitrarily long. Both `/proc/<pid>` existing and
/// `kill -0` succeeding stay true for the entire zombie window, so a naive
/// check reports a cleanly stopped cluster as still running and makes correct
/// teardown look broken.
///
/// Deliberately dependency-free: `/proc` where it exists, and the platform's own
/// tool (by absolute path) elsewhere.
#[must_use]
pub fn process_is_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .is_ok_and(|stat| proc_stat_is_live(&stat))
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        // `ps -o state=` rather than `kill -0`, for the zombie reason above.
        std::process::Command::new("/bin/ps")
            .args(["-o", "state=", "-p", &pid.to_string()])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .is_some_and(|output| {
                let state = String::from_utf8_lossy(&output.stdout);
                let state = state.trim();
                !state.is_empty() && !state.starts_with('Z')
            })
    }
    #[cfg(windows)]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).contains(&pid.to_string()))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// Whether a `/proc/<pid>/stat` line describes a live (non-zombie) process.
///
/// The second field is the executable name in parentheses and can itself contain
/// spaces and parentheses, so the state is the first token after the **last**
/// `)` — the documented way to parse this file, and the reason this is not a
/// plain `split_whitespace().nth(2)`.
#[must_use]
pub fn proc_stat_is_live(stat: &str) -> bool {
    let Some((_, after_comm)) = stat.rsplit_once(')') else {
        return false;
    };
    after_comm
        .split_whitespace()
        .next()
        // `Z` = zombie (exited, not yet reaped), `X` = dead.
        .is_some_and(|state| state != "Z" && state != "X")
}

/// Terminate a process, escalating only if it does not go.
///
/// Only ever reached once the caller has proved the pid is the process it
/// recorded. Absolute paths for the same reason as [`unix_uid`].
fn terminate_process(pid: u32) {
    #[cfg(unix)]
    {
        for (signal, grace_ms) in [("-TERM", 2000), ("-KILL", 500)] {
            let sent = ["/bin/kill", "/usr/bin/kill"].iter().any(|program| {
                std::process::Command::new(program)
                    .args([signal, &pid.to_string()])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .is_ok_and(|status| status.success())
            });
            if !sent {
                tracing::warn!(
                    pid,
                    signal,
                    "dev runtime: could not signal an orphaned process"
                );
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(grace_ms);
            while std::time::Instant::now() < deadline {
                if !process_is_alive(pid) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
    }
}

/// Rewrite a session record's owner pid.
///
/// Only for exercising the reaper: it is how a test manufactures the "the
/// process that owned this is gone" state without actually killing anything.
///
/// # Panics
///
/// If the record cannot be read, rewritten or written back — a test helper that
/// silently did nothing would make the reaper test vacuous.
#[doc(hidden)] // exposed for the #525 reaper test; not a stable API
pub fn rewrite_owner_pid_for_test(session_dir: &Path, owner_pid: u32) {
    let path = session_dir.join(SESSION_RECORD_FILE);
    let raw = std::fs::read_to_string(&path).expect("session record should be readable");
    let mut record = SessionRecord::from_json(&raw).expect("session record should parse");
    record.owner_pid = owner_pid;
    std::fs::write(&path, record.to_json().expect("serialize")).expect("rewrite session record");
}

#[cfg(test)]
mod tests {
    use super::{PostmasterIdentity, SessionRecord, directory_is_ours, postmaster_identity};

    /// A pid guaranteed dead: past the 32-bit ceiling, above every `pid_max`
    /// this crate supports.
    const fn dead_pid() -> u32 {
        u32::MAX - 1
    }

    fn minimal_record(postmaster_start_token: Option<String>) -> SessionRecord {
        SessionRecord {
            owner_pid: 1,
            owner_start_token: None,
            postmaster_pid: None,
            postmaster_start_token,
            bin_dir: None,
            data_dir: std::path::PathBuf::from("/tmp/harvest-dev-0/session-1-aa/data"),
            created_at: chrono::Utc::now(),
        }
    }

    /// Issue #1295. A tokenless record must read as `Unknown`, not as a
    /// match, even when the pid is genuinely alive. This is the exact gap
    /// the issue reports: a fallback to plain liveness let a reused pid pass
    /// as the recorded postmaster.
    #[test]
    fn a_tokenless_record_reads_as_unknown_identity_for_a_live_pid() {
        let record = minimal_record(None);
        let pid = std::process::id();
        assert_eq!(
            postmaster_identity(&record, pid),
            PostmasterIdentity::Unknown
        );
    }

    /// Issue #1295. A dead pid is `NotRunning` regardless of the recorded
    /// token. Nothing is there to have an identity.
    #[test]
    fn a_dead_pid_reads_as_not_running_even_with_a_recorded_token() {
        let record = minimal_record(Some("anything".to_owned()));
        assert_eq!(
            postmaster_identity(&record, dead_pid()),
            PostmasterIdentity::NotRunning
        );
    }

    /// Issue #1295. A live pid whose start token does not match the record
    /// is `NotRunning`, not `Unknown`: the recorded postmaster already
    /// exited, and the OS reused its pid for an unrelated live process. The
    /// reaper must not signal that process, but it is safe to remove the
    /// directory the departed postmaster left behind.
    #[cfg(unix)]
    #[test]
    fn a_mismatched_token_reads_as_not_running_not_unknown() {
        let pid = std::process::id();
        let record = minimal_record(Some("not-the-real-token".to_owned()));
        assert_eq!(
            postmaster_identity(&record, pid),
            PostmasterIdentity::NotRunning
        );
    }

    /// The steady state this fix must not regress: a matching token confirms
    /// identity for a live pid.
    #[cfg(unix)]
    #[test]
    fn a_matching_token_confirms_identity_for_a_live_pid() {
        let pid = std::process::id();
        let token =
            super::process_start_token(pid).expect("a live process has a start token on unix");
        let record = minimal_record(Some(token));
        assert_eq!(
            postmaster_identity(&record, pid),
            PostmasterIdentity::Confirmed
        );
    }

    /// Issue #1287 regression: the Unix answer must not change. A directory
    /// we own, with default `tempfile` permissions, is still trusted.
    #[cfg(unix)]
    #[test]
    fn an_owner_owned_directory_is_still_trusted_on_unix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let metadata = std::fs::symlink_metadata(dir.path()).expect("metadata");
        assert!(directory_is_ours(dir.path(), &metadata).is_ok());
    }

    /// Issue #1287. Before this fix, `directory_is_ours` (formerly inlined in
    /// `harden_root`) always returned `Ok` on Windows, for any non-symlink
    /// directory. `%LOCALAPPDATA%` is per-user by Windows convention, so a
    /// directory under it must still be trusted.
    #[cfg(windows)]
    #[test]
    fn a_directory_under_localappdata_is_trusted_on_windows() {
        let base = std::env::var_os("LOCALAPPDATA").expect("LOCALAPPDATA must be set on Windows");
        // A unique temp dir, not a fixed name: a fixed name could already
        // exist with real contents, which dropping a `TempDir` would delete.
        let dir = tempfile::tempdir_in(base).expect("temp dir under LOCALAPPDATA");
        let metadata = std::fs::symlink_metadata(dir.path()).expect("metadata");
        assert!(directory_is_ours(dir.path(), &metadata).is_ok());
    }

    /// Issue #1287's actual regression: a directory outside any per-user root
    /// must be refused, not silently trusted. `C:\Windows\Temp` is the
    /// machine-wide location the issue names as the realistic exposure (a
    /// service, or a CI agent, whose `TEMP` points there).
    #[cfg(windows)]
    #[test]
    fn a_directory_outside_any_per_user_root_is_refused_on_windows() {
        let dir = std::path::PathBuf::from(r"C:\Windows\Temp");
        let metadata = std::fs::symlink_metadata(&dir).expect("metadata");
        assert!(
            directory_is_ours(&dir, &metadata).is_err(),
            "a directory outside %LOCALAPPDATA%/%USERPROFILE% must be refused"
        );
    }
}
