//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::circuit_breaker::CircuitBreakerRegistry::{
//! on_dispatch, on_result}` -- the per-attempt breaker check `worker.rs`
//! runs around every dispatch of a circuit-breaker-guarded activity
//! (issue #369). `on_dispatch` gates the attempt before it runs
//! (`worker.rs`'s `dispatch_activity_task`, around line 14761); `on_result`
//! records the outcome once the attempt returns (around line 15401). Both
//! run once per attempt, for every activity that has a
//! `CircuitBreakerPolicy` configured. This is not an edge case: it is the
//! steady-state cost of running any circuit-breaker-protected activity at
//! all. Wall-clock timing is not admissible evidence on this (shared-vCPU)
//! machine. Every number this harness produces evidence for is a
//! deterministic instruction count (`valgrind --tool=callgrind`) or
//! allocation count/bytes (`valgrind --tool=dhat`), both reproducible
//! bit-for-bit on any machine.
//!
//! # Workload
//!
//! `CIRCUIT_BREAKER_PROFILE_ACTIVITIES` (default `25`) activities are
//! registered with a circuit-breaker policy (`failure_threshold=10`,
//! `window=60s`, `cooldown=30s` -- plausible operator-chosen values, not
//! extremes). `CIRCUIT_BREAKER_PROFILE_REPS` (default `200_000`) dispatch
//! attempts are simulated, round-robin across the activity set. Each
//! activity's own breaker state is therefore looked up roughly
//! `REPS / ACTIVITIES` times. That is the "same activity dispatched over
//! and over across the fleet's lifetime" shape a real worker produces.
//!
//! Every attempt calls `on_dispatch` then, when admitted, `on_result`
//! immediately after -- the same pairing `worker.rs` performs. One in 211
//! attempts reports `RetryableFailure`; the rest report `Success`. 211 is
//! a prime, so it does not land in lockstep with the activity count or
//! any power-of-two stride.
//!
//! That failure rate is deliberately the **common, healthy** case. A
//! `Success` result clears the rolling failure window outright (the
//! `CircuitPhase::Closed`'s `AttemptOutcome::Success` arm), so an isolated
//! failure this infrequent never accumulates toward the trip threshold.
//! This matches how a circuit breaker spends nearly all of its life in
//! production: guarding a healthy dependency, not mid-outage. The
//! `assert_eq!` below pins that down: every single dispatch in this
//! workload is admitted (`DispatchDecision::Allow`), and the breaker never
//! trips. A change that silently altered tripping behaviour, not just its
//! cost, therefore fails this harness instead of producing a
//! quietly-wrong "faster" number.
//!
//! Time is advanced with a fixed, synthetic `Instant` step
//! (`STEP` below) rather than by calling `Instant::now()` on every
//! iteration. The real caller does call `Instant::now()` fresh per
//! dispatch. That is a `clock_gettime` syscall whose cost has nothing to
//! do with `CircuitBreakerRegistry`'s own internals. Folding it into the
//! measured loop would dilute this harness's signal with a fixed,
//! unrelated cost. That fixed cost would land identically in both the
//! before and after trace anyway.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench circuit_breaker_dispatch_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="circuit_breaker_dispatch_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use autumn_harvest::circuit_breaker::{AttemptOutcome, CircuitBreakerRegistry, DispatchDecision};
use autumn_harvest::policy::CircuitBreakerPolicy;

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

/// Synthetic per-iteration time advance. `ACTIVITIES` iterations elapse
/// between two dispatches of the *same* activity (round-robin). So each
/// activity sees the same wall-clock cadence a real fleet spreads across
/// its whole poll history for one activity type.
const STEP: Duration = Duration::from_millis(5);

fn build_registry(n: usize) -> (CircuitBreakerRegistry, Vec<String>) {
    let policy = CircuitBreakerPolicy::new(10, Duration::from_secs(60), Duration::from_secs(30));
    let names: Vec<String> = (0..n).map(|i| format!("activity-{i}")).collect();
    let policies: HashMap<String, CircuitBreakerPolicy> =
        names.iter().cloned().map(|name| (name, policy)).collect();
    (CircuitBreakerRegistry::new(policies), names)
}

fn main() {
    let n = env_usize("CIRCUIT_BREAKER_PROFILE_ACTIVITIES", 25);
    let reps = env_usize("CIRCUIT_BREAKER_PROFILE_REPS", 200_000);

    assert!(
        n > 0,
        "CIRCUIT_BREAKER_PROFILE_ACTIVITIES must be at least 1"
    );
    // reps=0 would exit having measured nothing but setup, and could be
    // mistaken for a valid (implausibly fast) measurement.
    assert!(reps > 0, "CIRCUIT_BREAKER_PROFILE_REPS must be at least 1");

    let (registry, names) = build_registry(n);
    let base = Instant::now();

    let mut allowed = 0usize;
    let mut successes = 0usize;
    let mut failures = 0usize;
    for i in 0..reps {
        let name = &names[i % n];
        let now = base + STEP * u32::try_from(i).expect("rep index fits u32 for this workload");

        let decision = registry.on_dispatch(name, now);
        let DispatchDecision::Allow { token } = decision else {
            panic!(
                "activity {name} short-circuited at rep {i} -- the workload is meant to keep \
                 every breaker closed; a real trip here means the failure-rate/window/threshold \
                 constants above need re-tuning, not that this harness's assumption is safe to \
                 ignore"
            );
        };
        allowed += 1;

        let outcome = if i % 211 == 0 {
            failures += 1;
            AttemptOutcome::RetryableFailure
        } else {
            successes += 1;
            AttemptOutcome::Success
        };
        let transition = registry.on_result(name, outcome, token, now);
        assert!(
            transition.is_none(),
            "rep {i} unexpectedly transitioned the breaker ({transition:?}) -- this workload is \
             meant to stay fully closed throughout"
        );
        std::hint::black_box(&token);
    }

    assert_eq!(
        allowed, reps,
        "every dispatch in this workload must be admitted"
    );

    println!(
        "circuit_breaker_dispatch_profile: activities={n} reps={reps} successes={successes} \
         failures={failures}"
    );
}
