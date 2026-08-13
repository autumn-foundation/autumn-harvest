//! Deterministic (non-criterion) harness for profiling `WorkflowReplayer`'s
//! in-memory replay cost -- issue #135's realistic replay budget ("a
//! 10,000-event history replays in under 200ms").
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is driven
//! directly under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes), which are deterministic
//! across runs. See `docs/performance-replay.md` for the numbers this
//! produces and how to reproduce them.
//!
//! `harness = false`, own `main()` -- same shape as `claim_bench.rs` -- so the
//! compiled artifact is a plain executable a profiler can be pointed at
//! directly, with no criterion wall-clock loop diluting the measured work.
//!
//! # Running
//!
//! ```text
//! # Locate the compiled binary (no criterion timing loop runs; this just
//! # resolves the path cargo built):
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench replay_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//!
//! # Instruction counts:
//! valgrind --tool=callgrind --callgrind-out-file=callgrind.out <path>
//! callgrind_annotate callgrind.out
//!
//! # Allocation counts/bytes:
//! valgrind --tool=dhat --dhat-out-file=dhat.json <path>
//! ```
//!
//! `REPLAY_PROFILE_N` (default `5_000`, matching `replay_bench.rs`'s
//! `bench_replay_10k` -- `5_000` activities = `10_001` events, the exact size
//! issue #135 budgets) sets the activity count. `REPLAY_PROFILE_REPS`
//! (default 1) repeats the whole build+replay cycle -- each rep rebuilds its
//! own history (never reuses/clones a prior one), so no extra `Clone` cost
//! leaks into the measured `replay_from_events` call; use it to trade
//! process-startup noise for (linearly) more valgrind wall time when a
//! single rep's signal is too close to one-time process overhead.

#[path = "replay_profile_support.rs"]
mod support;

use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let n = env_usize("REPLAY_PROFILE_N", 5_000);
    let reps = env_usize("REPLAY_PROFILE_REPS", 1);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let replayer = WorkflowReplayer::new().register_fn("sequential", support::sequential_workflow);

    let mut total_events = 0usize;
    for _ in 0..reps {
        let (_exec_id, events) = support::build_history(n);
        total_events += events.len();
        let report = rt.block_on(replayer.replay_from_events(events));
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay must succeed for a profile run to be meaningful: {report}"
        );
        // Keep the report alive across the black_box so a sufficiently smart
        // optimizer cannot prove it is dead and elide the call it came from.
        std::hint::black_box(&report);
    }

    println!("replay_profile: n={n} reps={reps} total_events_replayed={total_events}");
}
