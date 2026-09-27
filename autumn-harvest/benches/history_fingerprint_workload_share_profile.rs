//! Isolates `build_history`'s inclusive cost from
//! `history_fingerprint_profile.rs`'s combined total.
//!
//! A flat callgrind self-cost line for `build_history` undercounts its
//! true cost. Any callee not inlined into `build_history` shows up under
//! its own name instead. `activity_payload`'s `json!` macro expansion,
//! `format!` calls, and their allocations are examples. Subtracting only
//! `build_history`'s flat self-cost line therefore overstates
//! `history_fingerprint`'s share of the combined total.
//!
//! This binary's only job is `build_history`. Its whole-process
//! instruction count is `build_history`'s true, inclusive cost instead.
//! Process startup adds a few thousand instructions, negligible against
//! the totals here. See `docs/performance-history-fingerprint.md`'s
//! "Workload share" section for the resulting arithmetic.
//!
//! `HFP_PROFILE_N`/`HFP_PROFILE_REPS` match `history_fingerprint_profile.rs`,
//! so the two binaries compare at the same input size.

#[path = "replay_profile_support.rs"]
mod support;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let n = env_usize("HFP_PROFILE_N", 5_000);
    let reps = env_usize("HFP_PROFILE_REPS", 1);

    let mut total_events = 0usize;
    for _ in 0..reps {
        let (_exec_id, events) = support::build_history(n);
        total_events += events.len();
        std::hint::black_box(&events);
    }

    println!(
        "history_fingerprint_workload_share_profile: n={n} reps={reps} total_events={total_events}"
    );
}
