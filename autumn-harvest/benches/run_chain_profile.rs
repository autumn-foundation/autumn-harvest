//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::run_chain::assemble_run_chain` (issue #701)
//! -- the pure ordering pass behind `GET /workflows/{id}/run-chain`
//! (`get_run_chain` in `autumn-harvest-plugin/src/api.rs`). Wall-clock timing
//! is not admissible evidence on this (shared-vCPU) machine -- every number
//! this harness produces evidence for is a deterministic instruction count
//! (`valgrind --tool=callgrind`) or allocation count/bytes
//! (`valgrind --tool=dhat`).
//!
//! Not bit-for-bit reproducible run-to-run: the fix this harness measures
//! adds a `HashMap`/`HashSet` index over the chain's `exec_id`s, and
//! `std::collections::HashMap`'s randomly-seeded `RandomState` makes hash
//! values (and so instruction counts) vary by a small fraction between runs
//! -- the same source of variance `awaitables_profile.rs`,
//! `dlq_aggregate_profile.rs` and `timeline_profile.rs` already document for
//! the same reason. dhat's allocation count/bytes are unaffected.
//!
//! # Workload
//!
//! The plugin's `gather_run_chain_rows` loads every row sharing the chain's
//! `first_exec_id` in ONE query (denormalized onto every post-#701
//! successor, per the module's own doc comment), so a chain that has
//! continued-as-new `n` times hands `assemble_run_chain` all `n` rows in one
//! call -- uncapped by `MAX_RUN_CHAIN_HOPS` (that constant only bounds the
//! separate *legacy* forward-walk for pre-#701 rows lacking back-links).
//! Continue-as-new exists precisely to let a long-lived "entity" workflow
//! (an account, a device, a subscription) run forever as a chain of bounded
//! executions; unlike most collections in this codebase, chain length is NOT
//! bounded by configuration, only by how long the entity has been alive. A
//! chain thousands of runs deep is the documented use case, not an edge
//! case.
//!
//! This harness builds exactly that shape: a fully-linked, one-continuation-
//! per-day chain of `RUN_CHAIN_PROFILE_N` executions, each carrying a real
//! `continued_from_exec_id` back-link and the denormalized `first_exec_id`
//! -- the common, non-legacy fast path (`assemble_run_chain`'s "explicit
//! forward link" branch), which every post-#701 chain takes. `head_exec_id`
//! is always the origin's `exec_id`: the plugin resolves it as
//! `queried_row.first_exec_id.unwrap_or(queried_row.id)` regardless of which
//! chain member the operator queried, so every real call passes the origin.
//!
//! `RUN_CHAIN_PROFILE_N` (default `4000`) sets the chain length.
//! `RUN_CHAIN_PROFILE_REPS` (default `1`) sets how many independent chains
//! are built and assembled -- mirroring `dlq_aggregate_profile.rs`'s
//! default-1 pattern for a single sizeable input, since `assemble_run_chain`
//! consumes its rows by value and a fresh chain is cheap to build relative
//! to the O(n) (post-fix) / O(n^2) (pre-fix) assembly cost it measures.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench run_chain_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="run_chain_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! To see the asymptotic curve (the admissible evidence for an O(n^2) ->
//! O(n) claim), re-run at several sizes:
//!
//! ```text
//! for n in 500 1500 3000 6000; do
//!   RUN_CHAIN_PROFILE_N=$n valgrind --tool=callgrind --branch-sim=no \
//!     --cache-sim=no --callgrind-out-file=cg-$n.out "$BIN"
//!   callgrind_annotate --threshold=100 cg-$n.out | grep 'PROGRAM TOTALS' -A2
//! done
//! ```

use autumn_harvest::run_chain::{RunChainRow, assemble_run_chain};
use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

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

fn day(offset_days: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000 + offset_days * 86_400, 0)
        .unwrap()
}

/// Build a fully-linked, `n`-long continue-as-new chain and the origin's
/// `exec_id` (the `head_exec_id` every real `/run-chain` request resolves
/// to, per the module doc above).
fn build_chain(n: usize) -> (Vec<RunChainRow>, Uuid) {
    let workflow_id = "entity-workflow-acct-000042".to_string();
    let origin_exec_id = Uuid::new_v4();

    let mut rows = Vec::with_capacity(n);
    let mut previous_exec_id: Option<Uuid> = None;
    for i in 0..n {
        let exec_id = if i == 0 { origin_exec_id } else { Uuid::new_v4() };
        let is_tail = i + 1 == n;
        rows.push(RunChainRow {
            exec_id,
            run_id: Uuid::new_v4(),
            workflow_id: workflow_id.clone(),
            state: if is_tail {
                "RUNNING".to_string()
            } else {
                "CONTINUED_AS_NEW".to_string()
            },
            started_at: day(i64::try_from(i).unwrap_or(i64::MAX)),
            completed_at: if is_tail {
                None
            } else {
                Some(day(i64::try_from(i).unwrap_or(i64::MAX)) + chrono::Duration::hours(1))
            },
            continued_from_exec_id: previous_exec_id,
            first_exec_id: if i == 0 { None } else { Some(origin_exec_id) },
        });
        previous_exec_id = Some(exec_id);
    }
    (rows, origin_exec_id)
}

fn main() {
    let n = env_usize("RUN_CHAIN_PROFILE_N", 4_000);
    let reps = env_usize("RUN_CHAIN_PROFILE_REPS", 1);

    assert!(n > 0, "RUN_CHAIN_PROFILE_N must be at least 1, got 0");
    // reps=0 would exit having measured nothing but arg-parsing cost, and
    // could be mistaken for a valid (implausibly fast) measurement.
    assert!(reps > 0, "RUN_CHAIN_PROFILE_REPS must be at least 1, got 0");

    let mut total_runs = 0usize;
    for _ in 0..reps {
        let (rows, head_exec_id) = build_chain(n);
        let response = assemble_run_chain(rows, head_exec_id);
        // Self-check, outside what the harness claims to measure only in the
        // sense that it's O(1) relative to the O(n)/O(n^2) assembly itself:
        // a fully-linked post-#701 chain must resolve every row and must
        // never report `head_unknown` -- if a future change broke that, this
        // harness would otherwise keep "measuring" a silently wrong result.
        assert_eq!(
            response.runs.len(),
            n,
            "assemble_run_chain should place every row of a fully-linked {n}-long chain"
        );
        assert!(
            !response.head_unknown,
            "a fully-linked post-#701 chain queried from its own origin should resolve \
             head_unknown=false"
        );
        total_runs += response.runs.len();
        std::hint::black_box(&response);
    }

    println!("run_chain_profile: n={n} reps={reps} total_runs={total_runs}");
}
