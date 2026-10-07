//! Example: adaptive concurrency limit per activity type (issue #1836).
//!
//! The limit caps the in-flight attempts of one activity type on one worker.
//! The cap follows the handler latency and the retryable failures. This
//! example shows two things:
//!
//! 1. How to turn the limit on in a `WorkerConfig`.
//! 2. How the cap moves against a dependency that slows down above a knee.
//!    The example drives the real limiter with a simulated dependency on a
//!    virtual clock. It needs no database and gives the same output on each
//!    run.
//!
//! The run has three phases:
//!
//! - **Healthy.** The dependency serves 20 calls at once at full speed. The
//!   cap grows from 4 and settles near `tolerance × knee + 4`, here 29.
//! - **Degraded.** The knee drops to 8. Latency inflates, so the cap falls
//!   and settles near 14.
//! - **Failing.** Every call above 8 at once fails with a retryable error.
//!   The error backoff holds the cap near the knee.
//!
//! See `docs/runbooks/activity-concurrency-limit.md` for tuning and metrics.
//!
//! Run with:
//!   `cargo run --example adaptive_concurrency_limit`

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::adaptive_limit::{
    Acquire, AdaptiveLimitRegistry, LimitPermit, QUEUE_SIZE, SampleOutcome,
};
use autumn_harvest::prelude::*;

/// The activity type that calls the slow dependency. The limit keys on the
/// activity name, as registered with `#[activity]`.
const CHARGE_CARD: &str = "charge_card";

/// A fast internal activity. It opts out of the default policy.
const LOAD_CART: &str = "load_cart";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The policy for `charge_card`. Each field is shown with its default value.
fn charge_card_policy() -> AdaptiveLimitPolicy {
    let mut policy = AdaptiveLimitPolicy::new(1, 200);
    // Latency can rise 25 % over the no-load baseline before the cap slows.
    policy.tolerance = 1.25;
    // An overloaded window cuts the cap to 90 %.
    policy.backoff_ratio = 0.9;
    // More than 5 % retryable failures in a window is overload.
    policy.error_threshold = 0.05;
    // Measure the no-load baseline again after 1000 samples.
    policy.probe_interval = 1000;
    policy
}

/// The limit config for a worker. Every activity type gets the default
/// policy, `charge_card` gets its own, and `load_cart` has no limit.
fn adaptive_limit_config() -> AdaptiveLimitConfig {
    AdaptiveLimitConfig::disabled()
        .with_default(Some(AdaptiveLimitPolicy::default()))
        .with_activity(CHARGE_CARD, Some(charge_card_policy()))
        .with_activity(LOAD_CART, None)
}

/// Pass the config to the worker. The limit is off without this call.
fn worker_config() -> WorkerConfig {
    WorkerConfig {
        max_concurrent_activities: 64,
        ..WorkerConfig::default()
    }
    .with_adaptive_limit(adaptive_limit_config())
}

// ---------------------------------------------------------------------------
// Simulated dependency
// ---------------------------------------------------------------------------

/// A dependency that serves `knee` calls at once at full speed. Above the
/// knee, latency grows in proportion to the concurrency, as for a pool of
/// `knee` servers.
#[derive(Clone, Copy)]
struct Dependency {
    knee: u32,
    base: Duration,
    /// Fail every call that starts above the knee.
    fail_above_knee: bool,
}

impl Dependency {
    fn call(self, concurrency: u32) -> (Duration, SampleOutcome) {
        let load = (f64::from(concurrency) / f64::from(self.knee)).max(1.0);
        let outcome = if self.fail_above_knee && concurrency > self.knee {
            SampleOutcome::Overloaded
        } else {
            SampleOutcome::Answered
        };
        (self.base.mul_f64(load), outcome)
    }
}

/// One phase of the run.
struct Phase {
    name: &'static str,
    dependency: Dependency,
    samples: usize,
}

/// Calls in flight on the virtual clock.
#[derive(Default)]
struct InFlight {
    /// Completion time in nanoseconds, and an index into `calls`.
    due: BinaryHeap<Reverse<(u128, usize)>>,
    calls: Vec<Option<(LimitPermit, Duration, SampleOutcome)>>,
    now: u128,
}

impl InFlight {
    /// Start calls until the limit refuses one. Demand never runs out, so
    /// only the cap bounds the concurrency.
    fn fill(&mut self, limits: &Arc<AdaptiveLimitRegistry>, dependency: Dependency) {
        while let Acquire::Acquired(permit) = limits.try_acquire(CHARGE_CARD) {
            let concurrency = limits.snapshot(CHARGE_CARD).map_or(0, |s| s.in_flight);
            let (latency, outcome) = dependency.call(concurrency);
            self.due
                .push(Reverse((self.now + latency.as_nanos(), self.calls.len())));
            self.calls.push(Some((permit, latency, outcome)));
        }
    }

    /// Complete the call that ends first. The permit reports its latency
    /// and outcome to the limiter.
    fn complete_next(&mut self) {
        let Some(Reverse((at, index))) = self.due.pop() else {
            return;
        };
        self.now = at;
        if let Some((permit, latency, outcome)) = self.calls[index].take() {
            permit.complete(latency, outcome);
        }
    }
}

fn main() {
    let config = adaptive_limit_config();
    println!("Worker config:");
    println!("  {CHARGE_CARD}: {:?}", config.policy_for(CHARGE_CARD));
    println!(
        "  {LOAD_CART}: {:?} (opted out)",
        config.policy_for(LOAD_CART)
    );
    println!(
        "  other types: {:?}",
        config.policy_for("any_other_activity")
    );
    let _worker = worker_config();
    println!();

    let policy = charge_card_policy();
    let limits = Arc::new(AdaptiveLimitRegistry::new(config));
    let mut in_flight = InFlight::default();
    let base = Duration::from_millis(50);
    let phases = [
        Phase {
            name: "healthy, knee 20",
            dependency: Dependency {
                knee: 20,
                base,
                fail_above_knee: false,
            },
            samples: 4_000,
        },
        Phase {
            name: "degraded, knee 8",
            dependency: Dependency {
                knee: 8,
                base,
                fail_above_knee: false,
            },
            samples: 4_000,
        },
        Phase {
            name: "failing above knee 8",
            dependency: Dependency {
                knee: 8,
                base,
                fail_above_knee: true,
            },
            samples: 4_000,
        },
    ];

    println!(
        "{:<24} {:>8} {:>8} {:>8} {:>12}",
        "phase", "samples", "cap", "expected", "baseline"
    );
    for phase in &phases {
        // The latency rule settles at `tolerance × knee + 4`. Failures
        // above the knee hold the cap near the knee instead.
        let knee = f64::from(phase.dependency.knee);
        let expected = if phase.dependency.fail_above_knee {
            knee
        } else {
            policy.tolerance.mul_add(knee, QUEUE_SIZE)
        };
        for done in 1..=phase.samples {
            in_flight.fill(&limits, phase.dependency);
            in_flight.complete_next();
            if done % 1_000 == 0 {
                let state = limits.snapshot(CHARGE_CARD).expect("the type is limited");
                let baseline = state
                    .baseline
                    .map_or_else(|| "-".to_owned(), |b| format!("{b:?}"));
                println!(
                    "{:<24} {:>8} {:>8} {:>8.0} {:>12}",
                    phase.name, done, state.limit, expected, baseline
                );
            }
        }
    }

    println!();
    println!("The cap follows the knee of the dependency in each phase.");
    println!("In a worker, the same state is exported as metrics:");
    println!("  harvest.activity.concurrency_limit{{activity=\"{CHARGE_CARD}\"}}");
    println!("  harvest.activity.concurrency_in_flight{{activity=\"{CHARGE_CARD}\"}}");
    println!("  harvest.activity.latency_baseline_seconds{{activity=\"{CHARGE_CARD}\"}}");
    println!("  harvest.activity.concurrency_deferred{{activity=\"{CHARGE_CARD}\"}}");
}
