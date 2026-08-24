//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::debugger::ReplayDebugger::trace_snapshot` --
//! the prefix-replay engine behind the **library** arm of the time-travel
//! replay debugger (issue #949).
//!
//! This profiles the library API only. The shipped `harvest debug replay`
//! CLI subcommand never calls `trace_snapshot` -- it is statically linked
//! and cannot register an embedder's `#[workflow]` handler, which
//! `trace_snapshot` requires, so it always builds its trace via the
//! separate, cheaper, handler-free `ReplayTrace::from_history_capped`
//! projection instead (confirmed by reading
//! `autumn-harvest-cli/src/debug.rs::run_replay`). See
//! `docs/performance-debugger-trace.md`'s "Scope" section for the full
//! source-confirmed argument; this workload models a **library caller** of
//! `ReplayDebugger`, not a default or flag-reachable CLI invocation.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is driven
//! directly under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes), which are deterministic
//! across runs.
//!
//! # Workload
//!
//! `trace_snapshot` builds one [`DebugStep`](autumn_harvest::debugger::DebugStep)
//! per history event by construction (`ReplayTrace::from_history_capped`,
//! "one step per consumed event"), and for step `k` it clones the prefix
//! `events[..=k]` and drives a **fresh canary replay** of it
//! (`replay_prefix` -> `drive_query_replay_async`) -- "step k is a fresh
//! replay of `events[0..=k]`", per the module doc comment. That is `n` steps
//! each doing `O(k)` work, so the documented design already implies
//! `Sum(k=1..n) O(k) = O(n^2)` total cost for an `n`-event history -- this
//! harness exists to confirm that scaling empirically (not just by reading
//! the doc comment) and to find out how much of the constant factor, if
//! any, is addressable without touching the fresh-replay-per-step design
//! itself (which is what makes "what does this code do at step k" an
//! honest answer -- collapsing it to an amortized/cached model would
//! change what the tool proves).
//!
//! Reuses `replay_profile_support.rs`'s `sequential_workflow` /
//! `build_history` -- the exact same issue #135 realistic-payload workload
//! `replay_profile.rs` and `verify_profile.rs` already use -- so this
//! harness measures debugger tracing over the *same* documented shape, not
//! a bespoke one invented to flatter a particular change. `n` activities
//! produce `2n + 1` events (`WorkflowStarted` + `ActivityScheduled` +
//! `ActivityCompleted` per activity), each carrying a ~230-byte realistic
//! payload.
//!
//! `ReplayDebugger::new()` is driven with **library defaults** (no
//! `.max_steps()` override) -- `DEFAULT_MAX_STEPS = 500` -- which is what an
//! embedder gets from `ReplayDebugger::new()` without an explicit
//! `.max_steps(...)` override (the packaged `harvest debug` CLI never
//! reaches this code path at all -- see above). `N` is kept small enough at
//! every measured point that `2N + 1 < 500`, so no run in the sweep below is
//! truncated by the cap; the reported scaling is the real
//! default-configuration cost, not an artifact of hitting the ceiling.
//!
//! `harness = false` + its own `main()` -- same shape as `replay_profile.rs`
//! / `runtime_drive_profile.rs` -- so the compiled artifact is a plain
//! executable a profiler can be pointed at directly.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features debugger \
//!   --bench debugger_trace_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable')
//!
//! # Instruction counts (repeat per DEBUGGER_TRACE_PROFILE_N to build a
//! # scaling table):
//! DEBUGGER_TRACE_PROFILE_N=80 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=callgrind.out "$BIN"
//! callgrind_annotate --threshold=99 callgrind.out | head -40
//!
//! # Allocation counts/bytes:
//! DEBUGGER_TRACE_PROFILE_N=80 valgrind --tool=dhat --num-callers=30 \
//!   --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `DEBUGGER_TRACE_PROFILE_N` (default `80`) sets the activity count.
//! `DEBUGGER_TRACE_PROFILE_REPS` (default `1`) repeats the whole
//! build+trace cycle -- each rep rebuilds its own history and debugger (never
//! reuses/clones a prior one), so no extra `Clone` cost leaks into the
//! measured `trace_snapshot` call; use it to trade process-startup noise for
//! (linearly) more valgrind wall time when a single rep's signal is too
//! close to one-time process overhead.

#[path = "replay_profile_support.rs"]
mod support;

use std::time::Duration;

use autumn_harvest::debugger::ReplayDebugger;
use autumn_harvest::testing::HistorySnapshot;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let n = env_usize("DEBUGGER_TRACE_PROFILE_N", 80);
    let reps = env_usize("DEBUGGER_TRACE_PROFILE_REPS", 1);
    // Under `valgrind --tool=callgrind`/`--tool=dhat` a single step's O(k)
    // prefix replay can run tens to hundreds of times slower in real
    // wall-clock time than an uninstrumented build. `ReplayDebugger`'s
    // per-step drive budget (`DEFAULT_STEP_TIMEOUT` = 5s) is measured
    // against `std::time::Instant` -- a real OS clock, unaffected by
    // CPU-bound instrumentation slowdown -- so in production it exists to
    // catch a genuinely spinning workflow, not to bound how long a
    // *profiler* is allowed to spend on one step. Raising it here is an
    // instrumentation-headroom knob, orthogonal to the `DEFAULT_MAX_STEPS`
    // "library defaults" framing in the module doc comment above (which is
    // about step *count*, never step *wall-clock budget*): a step that
    // still times out under this generous allowance means the workflow
    // genuinely stopped making progress, which the assertion below catches
    // and fails loudly on rather than silently profiling a truncated
    // replay.
    let step_timeout_secs = env_usize("DEBUGGER_TRACE_PROFILE_STEP_TIMEOUT_SECS", 60);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");

    let mut total_steps = 0usize;
    for _ in 0..reps {
        let (exec_id, events) = support::build_history(n);
        let total_events = events.len();
        let snapshot = HistorySnapshot {
            workflow_name: "sequential".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
            queue_name: None,
        };
        let debugger = ReplayDebugger::new()
            .register_fn("sequential", support::sequential_workflow)
            .step_timeout(Duration::from_secs(step_timeout_secs as u64));

        let trace = rt.block_on(debugger.trace_snapshot(snapshot));
        let trace = trace.expect("trace_snapshot must succeed for a profile run to be meaningful");
        assert_eq!(
            trace.steps.len(),
            total_events,
            "N={n} must stay under DEFAULT_MAX_STEPS so this run measures untruncated cost"
        );
        assert!(!trace.truncated, "trace must not be capped at N={n}");
        for step in &trace.steps {
            // `replay_succeeded()` is the load-bearing check: `TimedOut` and
            // `Panicked` steps leave `divergence: None` (the drive never
            // reached a conclusion to compare against the recording), so
            // checking `divergence.is_none()` alone would silently accept a
            // profile run built from an incomplete replay -- exactly the
            // false-clean failure mode `StepOutcome::replay_succeeded`'s own
            // doc comment warns against.
            assert!(
                step.outcome.replay_succeeded(),
                "step {} did not replay to a conclusion (outcome={:?}) -- a \
                 profile run must not silently include a step that timed out \
                 or panicked; raise DEBUGGER_TRACE_PROFILE_STEP_TIMEOUT_SECS \
                 (currently {step_timeout_secs}s) if valgrind instrumentation \
                 slowdown is the cause",
                step.index,
                step.outcome,
            );
            assert!(
                step.divergence.is_none(),
                "step {} reached a conclusion but diverged from the recorded \
                 history -- not meaningful for a profile run: {:?}",
                step.index,
                step.divergence
            );
        }
        total_steps += trace.steps.len();
        // Keep the trace alive across the black_box so a sufficiently smart
        // optimizer cannot prove it is dead and elide the call it came from.
        std::hint::black_box(&trace);
    }

    println!("debugger_trace_profile: n={n} reps={reps} total_steps_traced={total_steps}");
}
