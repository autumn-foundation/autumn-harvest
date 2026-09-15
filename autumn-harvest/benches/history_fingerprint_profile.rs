//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `shard_rebalance::history_fingerprint` -- the replay-
//! determinism check `shard_rebalance::db::verify_target_copy` runs on both
//! sides of a shard migration (see `docs/performance-history-fingerprint.md`
//! and `docs/sharding.md`). Wall-clock timing is not admissible evidence on
//! this shared-vCPU machine; every number this harness produces is either a
//! deterministic instruction count (`valgrind --tool=callgrind`) or an
//! allocation count/bytes figure (`valgrind --tool=dhat`).
//!
//! # Workload
//!
//! Reuses `replay_profile_support.rs`'s `build_history` -- the exact issue
//! #135 history shape (`ActivityScheduled`/`ActivityCompleted` pairs, each
//! carrying a ~230-byte realistic JSON payload) `replay_profile.rs` already
//! profiles `WorkflowReplayer` against. `verify_target_copy` fingerprints
//! whatever history a migrated execution actually has, and a long-lived
//! continue-as-new chain (see `run_chain_profile.rs`'s doc comment) or a
//! wide fan-out workflow can carry thousands of events, so this is not a
//! toy input size.
//!
//! `HFP_PROFILE_N` (default `5_000`, matching `replay_profile.rs`) sets the
//! activity count -- `5_000` activities build the same `10_001`-event
//! history issue #135 budgets. `HFP_PROFILE_REPS` (default `1`) repeats the
//! whole build+fingerprint cycle; each rep rebuilds its own history, so no
//! extra `Clone` cost leaks into the measured call.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench history_fingerprint_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.executable != null) | .executable')
//!
//! # Instructions:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//!
//! # Allocations:
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

#[path = "replay_profile_support.rs"]
mod support;

use autumn_harvest::shard_rebalance::history_fingerprint;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let n = env_usize("HFP_PROFILE_N", 5_000);
    let reps = env_usize("HFP_PROFILE_REPS", 1);

    let mut total_fp_len = 0usize;
    for _ in 0..reps {
        let (_exec_id, events) = support::build_history(n);
        let fingerprint = history_fingerprint(&events);
        total_fp_len += fingerprint.len();
        // Keep the fingerprint alive across the black_box so a sufficiently
        // smart optimizer cannot prove it dead and elide the call it came from.
        std::hint::black_box(&fingerprint);
    }

    println!("history_fingerprint_profile: n={n} reps={reps} total_fp_len={total_fp_len}");
}
