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
/// frozen so that no process can fork. Then each process in `cgroup.procs`
/// gets `SIGKILL`, until none is left or `wait` ends.
pub fn kill(dir: &Path, wait: Duration) {
    if std::fs::write(dir.join("cgroup.kill"), b"1").is_ok() {
        return;
    }
    let _ = std::fs::write(dir.join("cgroup.freeze"), b"1");
    let deadline = Instant::now() + wait;
    loop {
        let Ok(procs) = std::fs::read_to_string(dir.join("cgroup.procs")) else {
            return;
        };
        let mut signalled = false;
        for pid in procs
            .lines()
            .filter_map(|line| line.trim().parse::<libc::pid_t>().ok())
            .filter(|pid| *pid > 0)
        {
            // SAFETY: kill reads no memory. A positive pid names one process.
            signalled |= unsafe { libc::kill(pid, libc::SIGKILL) } == 0;
        }
        if !signalled || Instant::now() >= deadline {
            return;
        }
        std::thread::sleep(POLL);
    }
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
