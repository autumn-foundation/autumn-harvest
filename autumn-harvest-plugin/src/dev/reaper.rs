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
//! 1. **The root is per-user and `0700`, and is checked on every use.** Sessions
//!    never live directly in the world-writable system temp directory, where any
//!    other local account could create `harvest-dev-*/session.json` naming a pid
//!    of ours.
//! 2. **The record must be self-consistent.** Its `data_dir` has to be the one
//!    this layout puts inside the session directory; a record pointing elsewhere
//!    is corrupt or planted and is left alone.
//! 3. **A pid is not an identity.** The recorded postmaster start time must
//!    still match, so a reused pid is never mistaken for the process we started.
//! 4. **No blind kill.** A cluster we could not stop through `pg_ctl` is left
//!    running *and* its directory is left in place, because deleting the data
//!    directory out from under a live postmaster is worse than leaking it.

use std::path::{Path, PathBuf};

use super::discovery::PostgresBinaries;
use super::session::{
    ReapDecision, SESSION_RECORD_FILE, SESSION_ROOT_PREFIX, SessionRecord, decide_reap,
    effective_postmaster_pid, is_session_dir, record_is_self_consistent,
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
/// A symlink is rejected outright — another user could repoint it — and a
/// `chmod` we are not permitted to make means the directory belongs to someone
/// else.
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).map_err(|_| {
            DevError::UntrustedSessionRoot {
                path: root.to_path_buf(),
                reason: "it cannot be made owner-only (0700), so it is not ours",
            }
        })?;
    }
    Ok(())
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

/// The effective uid, via `id -u`.
///
/// `/usr/bin/id` by absolute path: resolving it through `PATH` would let a
/// planted `id` on a developer's `PATH` silently answer whatever it liked, and
/// this same answer gates the root-refusal check.
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

/// The real uid from a `/proc/self/status` body.
///
/// The `Uid:` line is `real  effective  saved  filesystem`; the first is what
/// `id -u` reports.
#[cfg(target_os = "linux")]
#[must_use]
pub fn parse_proc_status_uid(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|uid| uid.parse().ok())
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
        record.postmaster_pid = effective_postmaster_pid(
            &record,
            std::fs::read_to_string(record.data_dir.join("postmaster.pid"))
                .ok()
                .as_deref(),
        );

        let decision = decide_reap(
            &record,
            process_is_alive(record.owner_pid),
            record
                .postmaster_pid
                .is_some_and(|pid| postmaster_is_the_recorded_one(&record, pid)),
            self_pid,
        );
        match decision {
            ReapDecision::Skip(_) => continue,
            ReapDecision::StopThenRemove { postmaster_pid } => {
                let resolved = binaries.get_or_insert_with(|| PostgresBinaries::discover().ok());
                if !stop_orphan(resolved.as_ref(), &record, postmaster_pid) {
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

/// Whether the process at `pid` is still the postmaster this record recorded.
///
/// A live pid alone is not enough: pids are reused, and the window between a
/// `SIGKILL`ed run and the next `cargo dev` is exactly where that happens. When
/// the record predates start-token recording (or the platform cannot supply
/// one), fall back to plain liveness — the `pg_ctl`-only stop path below is
/// what keeps that case safe.
fn postmaster_is_the_recorded_one(record: &SessionRecord, pid: u32) -> bool {
    if !process_is_alive(pid) {
        return false;
    }
    match (&record.postmaster_start_token, process_start_token(pid)) {
        (Some(recorded), Some(current)) => recorded == &current,
        _ => true,
    }
}

/// Stop an orphaned cluster. Returns whether it is now confirmed stopped.
///
/// `pg_ctl` is the only path that may signal, because it derives the pid from
/// the data directory itself rather than trusting the record. A direct `kill`
/// is used **only** when the recorded start token still matches, which proves
/// the pid has not been reused.
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
            .ok()
            .is_some_and(|stat| proc_stat_is_live(&stat))
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
