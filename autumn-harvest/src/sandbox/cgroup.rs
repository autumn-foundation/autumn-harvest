//! Kill and remove a cgroup v2 directory.
//!
//! The launcher uses it for the cgroup of each microVM. The executor uses it
//! for the cgroup of each run.

use std::path::Path;
use std::time::{Duration, Instant};

/// The time between two checks of a cgroup.
const POLL: Duration = Duration::from_millis(10);

/// Kill every process in the cgroup at `dir`.
///
/// `cgroup.kill` needs Linux 5.14. When the write fails, the cgroup is
/// frozen first. The freeze is not instant, and a process that exits before
/// it holds frees its pid for another process. So the kill waits up to
/// `wait` until `cgroup.events` reports `frozen 1`. A frozen process cannot
/// exit or fork. Then each pid in `cgroup.procs` names a process of the
/// cgroup, and one pass of `SIGKILL` reaches each one. A cgroup that is not
/// frozen in time gets no signal, and a later call kills it. A cgroup that
/// cannot freeze gets no signal at all.
pub fn kill(dir: &Path, wait: Duration) {
    if std::fs::write(dir.join("cgroup.kill"), b"1").is_ok() {
        return;
    }
    // A cgroup that cannot freeze is gone or has no freezer. Its pids are
    // not safe to signal.
    if std::fs::write(dir.join("cgroup.freeze"), b"1").is_err() {
        return;
    }
    let deadline = Instant::now() + wait;
    while !is_frozen(dir) {
        if Instant::now() >= deadline {
            return;
        }
        std::thread::sleep(POLL);
    }
    let Ok(procs) = std::fs::read_to_string(dir.join("cgroup.procs")) else {
        return;
    };
    for pid in procs
        .lines()
        .filter_map(|line| line.trim().parse::<libc::pid_t>().ok())
        .filter(|pid| *pid > 0)
    {
        // SAFETY: kill reads no memory. A positive pid names one process.
        unsafe { libc::kill(pid, libc::SIGKILL) };
    }
}

/// Whether `cgroup.events` reports the cgroup frozen.
fn is_frozen(dir: &Path) -> bool {
    std::fs::read_to_string(dir.join("cgroup.events"))
        .is_ok_and(|events| events.lines().any(|line| line == "frozen 1"))
}

/// Whether `cgroup.events` reports no process left. A cgroup that cannot be
/// read counts as empty.
#[must_use]
pub fn is_empty(dir: &Path) -> bool {
    std::fs::read_to_string(dir.join("cgroup.events")).map_or(true, |events| {
        events.lines().any(|line| line == "populated 0")
    })
}

/// Kill every process in the cgroup at `dir`, wait up to `wait` for it to
/// empty, and remove it. The removal fails while a process is left.
pub fn kill_and_remove(dir: &Path, wait: Duration) {
    let deadline = Instant::now() + wait;
    kill(dir, wait);
    while !is_empty(dir) && Instant::now() < deadline {
        std::thread::sleep(POLL);
    }
    let _ = std::fs::remove_dir(dir);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;

    use super::*;

    #[test]
    fn a_missing_cgroup_ends_the_kill_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("gone");
        let started = std::time::Instant::now();
        kill(&gone, Duration::from_secs(2));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn the_fallback_signals_no_pid_before_the_freeze_holds() {
        let dir = tempfile::tempdir().unwrap();
        // A directory makes the kill write fail, as on Linux before 5.14.
        std::fs::create_dir(dir.path().join("cgroup.kill")).unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("10")
            .spawn()
            .unwrap();
        std::fs::write(dir.path().join("cgroup.procs"), format!("{}\n", child.id())).unwrap();
        let events = dir.path().join("cgroup.events");
        std::fs::write(&events, "populated 1\nfrozen 0\n").unwrap();
        kill(dir.path(), Duration::from_millis(100));
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            child.try_wait().unwrap().is_none(),
            "a pid got a signal before the freeze held"
        );
        // Once the cgroup is frozen, each process gets SIGKILL.
        std::fs::write(&events, "populated 1\nfrozen 1\n").unwrap();
        kill(dir.path(), Duration::ZERO);
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{status:?}");
    }
}
