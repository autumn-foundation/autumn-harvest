//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::queue_fairness::{effective_queue_weights,
//! weighted_queue_order}`. This is the weighted-random queue-selection step
//! `Worker::poll_once` (`worker.rs`) runs on every poll iteration once an
//! operator configures `WorkerConfig::queue_weights` (issue #515). Wall-clock
//! timing is not admissible evidence on this (shared-vCPU) machine. Every
//! number this harness produces evidence for is a deterministic instruction
//! count (`valgrind --tool=callgrind`) or allocation count/bytes
//! (`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.
//!
//! # Workload
//!
//! `poll_once` calls `effective_queue_weights` then `weighted_queue_order`
//! once per poll, unconditionally, for the worker's whole bound-queue list.
//! It does this for every bound queue, not just the ones an operator
//! bothered to assign an explicit weight to. This harness reproduces
//! exactly that pair of calls, over a realistic multi-queue binding. A
//! handful of queues carry an explicit weight -- the operator's actual
//! incident-response/prioritization intent. The rest default to weight 1.
//! One weight-0 "backfill" queue drains only when every positive-weight
//! queue is empty. This is the shape `queue_fairness.rs`'s own doc comment
//! describes.
//!
//! `QUEUE_FAIRNESS_PROFILE_QUEUES` (default `16`) sets how many queues the
//! worker is bound to. `QUEUE_FAIRNESS_PROFILE_REPS` (default `20_000`) sets
//! how many poll iterations are simulated. That is roughly 500 seconds of
//! continuous back-to-back polling at the documented 25ms poll interval
//! (`docs/benchmarks.md`). It is the regime a saturated queue actually runs
//! under: a worker claiming tasks back to back never waits at all, per
//! that page. The queue list and weight map are built ONCE, outside the
//! measured loop, isolating the two functions' own cost from setup.
//!
//! A fixed-seed `StdRng` is used instead of `rand::thread_rng()` so the
//! instruction count is bit-for-bit reproducible across runs. The RNG
//! itself is deterministic once seeded; only which draws are made must not
//! depend on wall-clock or OS entropy timing.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench queue_fairness_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_fairness_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use std::collections::HashMap;

use autumn_harvest::queue_fairness::{effective_queue_weights, weighted_queue_order};
use rand::SeedableRng;

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A *present but malformed* value is a configuration
/// error, not silently substituted for the default.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

/// Build `n` queue names and a weight map. Every third queue gets an
/// explicit weight (the operator's deliberately prioritized queues). One
/// queue is pinned to weight 0 (a backfill queue). The rest fall back to
/// the default weight of 1 via `effective_queue_weights`.
fn build_queues(n: usize) -> (Vec<String>, HashMap<String, u32>) {
    let queues: Vec<String> = (0..n).map(|i| format!("queue-{i}")).collect();
    let mut weights = HashMap::new();
    for (i, name) in queues.iter().enumerate() {
        if i == 0 {
            weights.insert(name.clone(), 0);
        } else if i % 3 == 0 {
            weights.insert(name.clone(), 10);
        }
    }
    (queues, weights)
}

fn main() {
    let n = env_usize("QUEUE_FAIRNESS_PROFILE_QUEUES", 16);
    let reps = env_usize("QUEUE_FAIRNESS_PROFILE_REPS", 20_000);

    assert!(n > 0, "QUEUE_FAIRNESS_PROFILE_QUEUES must be at least 1");
    // reps=0 would exit having measured nothing but setup, and could be
    // mistaken for a valid (implausibly fast) measurement.
    assert!(reps > 0, "QUEUE_FAIRNESS_PROFILE_REPS must be at least 1");

    let (queues, weights) = build_queues(n);
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5157_4649_524e_4553);

    let mut total_len = 0usize;
    for _ in 0..reps {
        let pairs = effective_queue_weights(&queues, &weights);
        let ordered = weighted_queue_order(&pairs, &mut rng);
        // Self-checked. A change that silently dropped or duplicated a
        // queue, not just its cost, then fails this harness instead of
        // producing a quietly-wrong "faster" number.
        assert_eq!(
            ordered.len(),
            n,
            "weighted_queue_order must return a permutation of all {n} queues"
        );
        total_len += ordered.len();
        std::hint::black_box(&ordered);
    }

    println!("queue_fairness_profile: queues={n} reps={reps} total_ordered_len={total_len}");
}
