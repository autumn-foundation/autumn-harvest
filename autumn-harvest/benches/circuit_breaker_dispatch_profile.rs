//! Non-criterion instruction/allocation-count profiling harness for
//! [`CircuitBreakerRegistry`] on the per-attempt dispatch path.
//!
//! The worker calls the registry four times for every attempt of an activity
//! with a circuit-breaker policy: `on_dispatch`, `begin_claim`,
//! `on_claim_result` and `end_claim`. This harness drives exactly that
//! sequence, so its cost per rep is the registry's cost per attempt.
//!
//! Wall-clock timing is unreliable on a shared-vCPU machine, so this binary
//! is not measured with criterion. Run it under
//! `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts), like the other
//! `*_profile.rs` harnesses. See `docs/performance-metrics-rs-labels.md`.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --bench circuit_breaker_dispatch_profile \
//!   --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//!
//! valgrind --tool=dhat --dhat-out-file=dhat.out <path>
//! valgrind --tool=callgrind --callgrind-out-file=callgrind.out <path>
//! ```
//!
//! `CIRCUIT_PROFILE_REPS` (default 10000) sets the number of simulated
//! attempts.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use autumn_harvest::circuit_breaker::{
    AttemptOutcome, CircuitBreakerRegistry, ClaimKey, DispatchDecision,
};
use autumn_harvest::policy::{CircuitBreakerPolicy, CircuitOpenMode};
use uuid::Uuid;

/// Activities with a policy. A real worker cycles through a small fixed set.
const ACTIVITIES: [&str; 4] = ["send_email", "charge_card", "reserve_stock", "ship_order"];

fn main() {
    let reps: usize = std::env::var("CIRCUIT_PROFILE_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let policy = CircuitBreakerPolicy {
        failure_threshold: 5,
        window: Duration::from_secs(60),
        cooldown: Duration::from_secs(30),
        open_mode: CircuitOpenMode::default(),
    };
    let policies: HashMap<String, CircuitBreakerPolicy> = ACTIVITIES
        .iter()
        .map(|name| ((*name).to_owned(), policy))
        .collect();
    let registry = CircuitBreakerRegistry::new(policies);
    let task_id = Uuid::nil();
    let now = Instant::now();

    for i in 0..reps {
        let activity = black_box(ACTIVITIES[i % ACTIVITIES.len()]);
        let claim = ClaimKey {
            task_id,
            attempt: i32::try_from(i).unwrap_or(i32::MAX),
            started_at: None,
        };
        let DispatchDecision::Allow { token } = registry.on_dispatch(activity, now) else {
            unreachable!("a breaker that only sees successes stays closed");
        };
        registry.begin_claim(activity, claim, token);
        black_box(registry.on_claim_result(activity, AttemptOutcome::Success, token, claim, now));
        registry.end_claim(activity, claim);
    }
    println!("circuit_breaker_dispatch_profile: {reps} simulated attempts");
}
