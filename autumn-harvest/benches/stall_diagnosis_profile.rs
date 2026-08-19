//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::stall_diagnosis::classify_execution` — the
//! pure root-cause classifier behind `GET /api/harvest/workflows/{id}/diagnose`
//! (issue #809). Wall-clock timing is not admissible evidence on this
//! (shared-vCPU) machine — every number this harness produces evidence for is
//! a deterministic instruction count (`valgrind --tool=callgrind`) or
//! allocation count/bytes (`valgrind --tool=dhat`), both reproducible
//! bit-for-bit on any machine.
//!
//! # Workload
//!
//! The plugin handler for `GET /workflows/{id}/diagnose`
//! (`autumn-harvest-plugin/src/api.rs`) calls `classify_execution` **twice**
//! per HTTP request: once to compute `db_verdict` (deciding whether an
//! additional replay pass could change the answer) and once more, after
//! populating `inputs.awaited_signals`/`inputs.replay_waits`, to compute the
//! actual `blocked_on` response field. For the workload this harness
//! exercises (a wide *pending-activity* fan-out with a genuine worst-cause
//! row somewhere in it), `db_verdict` already resolves to an activity
//! verdict, so `replay_could_win` is `false` and the second call runs
//! unconditionally regardless. This harness reproduces that exact
//! double-call shape.
//!
//! The module's own docs name the wide fan-out as its **headline** use case:
//! "one wedged slot among nineteen healthy ones" — the whole reason
//! `classify_execution` folds over every pending activity row and reports the
//! *worst* verdict, rather than the first. This harness builds exactly that
//! shape: a realistic mix of task-queue rows dominated by ones an operator
//! would actually be paging over (retry backoff with a recorded downstream
//! error, an open circuit breaker), plus a handful of healthy rows, plus
//! exactly one genuine `activity_no_worker` row (the true, permanent root
//! cause) placed away from either end of the collection so a correctness
//! fix cannot get away with only checking the first or last row.
//!
//! This is deliberately **not** the all-healthy case: the endpoint exists to
//! answer "why is this stalled?", so the realistic worst-case invocation this
//! harness measures is exactly the one operators trigger it for.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench stall_diagnosis_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="stall_diagnosis_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `STALL_DIAGNOSIS_PROFILE_N` (default `500`) sets the number of pending
//! activity rows per simulated execution. `STALL_DIAGNOSIS_PROFILE_REPS`
//! (default `2_000`) sets how many times the (fixed) fan-out is diagnosed —
//! i.e. how many simulated `/diagnose` HTTP requests are served.

use autumn_harvest::stall_diagnosis::{
    BlockingCircuitPhase, DiagnosisInputs, PendingActivityFacts, classify_execution,
};
use chrono::{DateTime, TimeZone, Utc};

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000, 0).unwrap()
}

const QUEUES: [&str; 4] = ["payments", "email", "webhooks", "inventory"];
const ACTIVITIES: [&str; 4] = [
    "charge_card",
    "send_receipt",
    "notify_partner",
    "sync_inventory",
];
const DOWNSTREAM_ERRORS: [&str; 3] = [
    "connection reset by peer while calling downstream payment gateway",
    "timed out waiting for response from partner webhook endpoint",
    "503 Service Unavailable from inventory sync service",
];

/// A row in retry backoff with a recorded failure — the single most common
/// shape in a real stalled fan-out (a flaking downstream), and the one that
/// forces `classify_pending_activity` down its most allocation-heavy branch
/// (`ActivityRetrying`, which clones both `activity_name` and `last_error`).
fn retrying_activity(idx: usize, now: DateTime<Utc>) -> PendingActivityFacts {
    PendingActivityFacts {
        activity_name: Some(ACTIVITIES[idx % ACTIVITIES.len()].to_string()),
        queue: QUEUES[idx % QUEUES.len()].to_string(),
        task_state: "PENDING".to_string(),
        attempt: 2 + i32::try_from(idx % 4).unwrap_or(i32::MAX),
        last_error: Some(DOWNSTREAM_ERRORS[idx % DOWNSTREAM_ERRORS.len()].to_string()),
        scheduled_at: now
            + chrono::Duration::seconds(30 + i64::try_from(idx % 60).unwrap_or(i64::MAX)),
        rate_limit_key: None,
        concurrency_key: None,
        queue_paused: false,
        activity_paused: false,
        has_live_worker: true,
        claimant_is_live: None,
        circuit_phase: None,
        circuit_cooldown_until: None,
        rate_limit_saturated: false,
        rate_limit_bucket_missing: false,
        concurrency_saturated: false,
    }
}

/// A due, unimpeded row — the healthy minority in a mostly-degraded fan-out.
fn healthy_activity(idx: usize, now: DateTime<Utc>) -> PendingActivityFacts {
    PendingActivityFacts {
        activity_name: Some(ACTIVITIES[idx % ACTIVITIES.len()].to_string()),
        queue: QUEUES[idx % QUEUES.len()].to_string(),
        task_state: "PENDING".to_string(),
        attempt: 1,
        last_error: None,
        scheduled_at: now - chrono::Duration::seconds(5),
        rate_limit_key: None,
        concurrency_key: None,
        queue_paused: false,
        activity_paused: false,
        has_live_worker: true,
        claimant_is_live: None,
        circuit_phase: None,
        circuit_cooldown_until: None,
        rate_limit_saturated: false,
        rate_limit_bucket_missing: false,
        concurrency_saturated: false,
    }
}

/// An open-breaker row — the second most common "actually a problem" shape.
fn circuit_open_activity(idx: usize, now: DateTime<Utc>) -> PendingActivityFacts {
    PendingActivityFacts {
        circuit_phase: Some(BlockingCircuitPhase::Open),
        circuit_cooldown_until: Some(now + chrono::Duration::seconds(30)),
        ..retrying_activity(idx, now)
    }
}

/// The genuine, permanent root cause: nothing polls this queue at all. Placed
/// at a non-trivial index by the caller (never index 0 or N-1) so a fold
/// implementation that only checks the ends of the collection cannot pass by
/// accident.
fn no_worker_activity(idx: usize, now: DateTime<Utc>) -> PendingActivityFacts {
    PendingActivityFacts {
        has_live_worker: false,
        ..healthy_activity(idx, now)
    }
}

/// Where [`build_inputs`] plants the genuine `activity_no_worker` root
/// cause: two-thirds of the way into the fan-out, clamped to the last valid
/// index. Shared with [`validate_workload_params`] so the planted position
/// and the "is it actually interior" check can never drift apart.
fn no_worker_index(n: usize) -> usize {
    (n * 2 / 3).min(n.saturating_sub(1))
}

/// Build the fixed, realistic fan-out this harness diagnoses repeatedly:
/// ~70% retry backoff, ~20% healthy, ~9% open-breaker, and exactly one
/// genuine `activity_no_worker` row placed at two-thirds of the way through.
fn build_inputs(n: usize, now: DateTime<Utc>) -> DiagnosisInputs {
    let no_worker_index = no_worker_index(n);
    let activities: Vec<PendingActivityFacts> = (0..n)
        .map(|idx| {
            if idx == no_worker_index {
                return no_worker_activity(idx, now);
            }
            match idx % 10 {
                0..=6 => retrying_activity(idx, now),
                7..=8 => healthy_activity(idx, now),
                _ => circuit_open_activity(idx, now),
            }
        })
        .collect();

    DiagnosisInputs {
        is_terminal: false,
        is_paused: false,
        pause_actor: None,
        paused_since: None,
        activities,
        external_handoffs: Vec::new(),
        children: Vec::new(),
        awaited_signals: Vec::new(),
        timers: Vec::new(),
        replay_waits: Vec::new(),
        workflow_task: None,
        nd_block: None,
    }
}

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A *present but malformed* value (a typo, e.g.
/// `STALL_DIAGNOSIS_PROFILE_N=50O`, or non-Unicode content) is a
/// configuration error, not silently substituted for the default -- a
/// harness whose whole point is producing a specific, documented,
/// reproducible number must not silently profile a *different*
/// configuration than the one actually requested and report success as if
/// nothing were wrong (the fixture's own sanity check would still pass,
/// since it's computed from the same substituted value).
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

/// Rejects a workload size that would silently stop exercising the
/// documented "wide fan-out with a genuine root cause away from either end"
/// shape this harness exists to measure -- so shortening a Valgrind run
/// can't quietly produce measurements for a degenerate workload instead.
fn validate_workload_params(n: usize, reps: usize) {
    // reps=0 skips the measured loop entirely -- the harness would exit 0
    // and print a plausible-looking summary line without ever profiling the
    // fold beyond the one unmeasured sanity call, which a profiler pointed
    // at the resulting process could easily mistake for a valid
    // (implausibly fast) measurement.
    assert!(
        reps >= 1,
        "STALL_DIAGNOSIS_PROFILE_REPS must be at least 1, got 0"
    );
    // n=0 builds no activities at all, so the fold never finds the planted
    // `activity_no_worker` row and the sanity check below fails with an
    // opaque mismatch instead of a message naming the actual problem.
    assert!(
        n >= 1,
        "STALL_DIAGNOSIS_PROFILE_N must be at least 1, got 0"
    );
    let idx = no_worker_index(n);
    // For n in 1..=3 the planted row lands at index 0 or n-1 -- the first
    // or last position -- no longer exercising the documented "away from
    // either end" wide-fan-out workload the fold's worst-of tie-break logic
    // is meant to be tested against.
    assert!(
        idx > 0 && idx < n - 1,
        "STALL_DIAGNOSIS_PROFILE_N={n} is too small: the planted activity_no_worker row would \
         land at index {idx} (the first or last position), no longer exercising the documented \
         'away from either end' wide-fan-out workload -- use a larger N (>= 4)"
    );
}

fn main() {
    let n = env_usize("STALL_DIAGNOSIS_PROFILE_N", 500);
    let reps = env_usize("STALL_DIAGNOSIS_PROFILE_REPS", 2_000);
    validate_workload_params(n, reps);
    let now = now();

    let inputs = build_inputs(n, now);

    // Sanity check the fixture once, unmeasured, exactly like the real
    // handler would read the answer: the fold must find the one genuine
    // root cause among the crowd of retrying/healthy/circuit-open rows.
    let sanity = classify_execution(&inputs, now).expect("non-terminal execution");
    assert_eq!(
        sanity.kind(),
        "activity_no_worker",
        "fixture bug: fold did not find the planted worst-case row"
    );

    let mut kind_tally: std::collections::HashMap<&'static str, u64> =
        std::collections::HashMap::new();
    for _ in 0..reps {
        // Mirrors the real HTTP handler: `db_verdict` first, then `blocked_on`
        // again after (in production, conditionally) populating replay-only
        // fields. For this fan-out shape `replay_could_win` is false, so the
        // handler always pays for both calls.
        let db_verdict = classify_execution(std::hint::black_box(&inputs), now);
        let blocked_on = classify_execution(std::hint::black_box(&inputs), now)
            .unwrap_or(autumn_harvest::stall_diagnosis::BlockedOn::NoPendingWork);
        *kind_tally
            .entry(db_verdict.map_or("none", |v| v.kind()))
            .or_insert(0) += 1;
        *kind_tally.entry(blocked_on.kind()).or_insert(0) += 1;
    }

    // Printed so the binary can't be trivially dead-code-eliminated and so a
    // regression that changes which verdict wins is loudly visible rather
    // than silently under- or over-measuring the fold.
    let mut kinds: Vec<(&str, u64)> = kind_tally.into_iter().collect();
    kinds.sort_unstable();
    println!(
        "stall_diagnosis_profile: n={n} reps={reps} calls={} kinds={kinds:?}",
        reps * 2
    );
}
