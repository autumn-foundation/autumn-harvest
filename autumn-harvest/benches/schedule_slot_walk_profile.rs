//! Deterministic (non-criterion) instruction/allocation profiling harness for
//! `schedule_plan::next_fire_after`, the slot arithmetic behind the scheduler's
//! catch-up walk (issue #484).
//!
//! Wall-clock time is not admissible on this machine. Use
//! `valgrind --tool=callgrind` for instructions and `valgrind --tool=dhat`
//! for allocations.
//!
//! # Workload
//!
//! A scheduler was down. On restart, each cron schedule walks every missed
//! slot from `next_run_at` up to now, one `next_fire_after` call per slot.
//!
//! Knobs (env): `SLOT_WALK_SCHEDULES` (default 20), `SLOT_WALK_SLOTS`
//! (default 1440, one day of a per-minute cron).

use autumn_harvest::policy::Schedule;
use autumn_harvest::schedule_plan::SlotWalker;
use chrono::{DateTime, Utc};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let schedules = env_usize("SLOT_WALK_SCHEDULES", 20);
    let slots = env_usize("SLOT_WALK_SLOTS", 1440);
    let exprs = ["* * * * *", "*/5 * * * *", "0/10 * * * *", "*/15 * * * * *"];
    let start: DateTime<Utc> = "2026-03-01T00:00:00Z".parse().expect("timestamp");
    let mut checksum = 0_i64;
    for i in 0..schedules {
        let schedule = Schedule::Cron(exprs[i % exprs.len()].to_string());
        let walker = SlotWalker::new(&schedule);
        let mut cursor = start;
        for _ in 0..slots {
            cursor = walker.next_after(cursor).expect("next slot");
        }
        checksum ^= cursor.timestamp();
    }
    println!("checksum {checksum}");
}
