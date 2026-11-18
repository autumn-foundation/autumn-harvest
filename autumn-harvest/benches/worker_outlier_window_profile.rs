//! Deterministic instruction/allocation-count harness for
//! `worker_outlier::TaskOutcomeWindow`: `record_at` runs once per finished
//! task, `snapshot_at` once per heartbeat. Run it under
//! `valgrind --tool=callgrind` or `--tool=dhat`; wall-clock is not used.
//!
//! Workload: `WINDOW_PROFILE_TASKS` (default 1000) records per snapshot, with
//! a small share of out-of-order timestamps, repeated `WINDOW_PROFILE_REPS`
//! (default 200) times against a default-capacity window.

use std::hint::black_box;
use std::time::{Duration, Instant};

use autumn_harvest::worker_outlier::TaskOutcomeWindow;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let tasks = env_usize("WINDOW_PROFILE_TASKS", 1000);
    let reps = env_usize("WINDOW_PROFILE_REPS", 200);
    let base = Instant::now();
    let window = TaskOutcomeWindow::default();
    let mut tick: u64 = 0;
    let mut sink = 0u64;
    for _ in 0..reps {
        for i in 0..tasks {
            tick += 1;
            let jitter = if i % 16 == 0 { 3 } else { 0 };
            let at = base + Duration::from_millis(tick.saturating_sub(jitter));
            let latency = Duration::from_micros(500 + (tick * 7919) % 90_000);
            window.record_at(at, tick % 23 == 0, latency);
        }
        let stats = window.snapshot_at(base + Duration::from_millis(tick));
        sink += u64::from(stats.tasks) + stats.p99_latency_ms.unwrap_or(0);
    }
    black_box(sink);
}
