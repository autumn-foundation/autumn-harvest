//! Property tests for [`autumn_harvest::policy::compute_retry_delay`] and the
//! seeded/jittered variant (Tier A target 1).
//!
//! Contract under test (from the fn docs):
//! - the result never exceeds `max_interval` (the cap) — for *any* params;
//! - it is monotonic non-decreasing in `attempt` when `backoff_coefficient >= 1`;
//! - it never panics for any `attempt` (incl. `u32::MAX`) or degenerate params
//!   (`backoff == 0`, `backoff < 1`, zero/huge intervals);
//! - jitter stays within its documented band (Full: `[0, base]`; Equal:
//!   `[base/2, base]`; None: exactly `base`) and never breaches the cap.

use std::time::Duration;

use autumn_harvest::policy::{
    JitterPolicy, RetryPolicy, compute_retry_delay, compute_retry_delay_with_seed,
};
use proptest::prelude::*;

use super::prop_config::config;

proptest! {
    #![proptest_config(config())]

    /// The cap holds for *every* combination of params — this is the load-bearing
    /// safety invariant (a retry delay can never exceed the configured ceiling).
    #[test]
    fn compute_retry_delay_never_exceeds_max_interval(
        initial_secs in 0u64..=100_000,
        backoff in 0.0f64..=100.0,
        max_secs in 0u64..=100_000,
        attempt in any::<u32>(),
    ) {
        let max = Duration::from_secs(max_secs);
        let delay = compute_retry_delay(Duration::from_secs(initial_secs), backoff, max, attempt);
        prop_assert!(delay <= max, "delay {delay:?} exceeded cap {max:?}");
    }

    /// Never panics — including `attempt == u32::MAX`, `backoff == 0.0`,
    /// `backoff < 1.0`, and a zero cap. (The proptest harness turns any panic
    /// into a failing case, so simply invoking is the assertion.)
    #[test]
    fn compute_retry_delay_is_total(
        initial_secs in 0u64..=1_000_000,
        backoff in prop_oneof![Just(0.0f64), 0.0f64..1000.0],
        max_secs in 0u64..=1_000_000,
        attempt in any::<u32>(),
    ) {
        let _ = compute_retry_delay(
            Duration::from_secs(initial_secs),
            backoff,
            Duration::from_secs(max_secs),
            attempt,
        );
    }

    /// Monotonic non-decreasing in `attempt` when `backoff_coefficient >= 1.0`
    /// (the documented growth regime). Consecutive attempts either grow or, once
    /// capped, stay equal — both satisfy `<=`.
    #[test]
    fn compute_retry_delay_monotonic_when_backoff_ge_one(
        initial_secs in 1u64..=100,
        backoff in 1.0f64..=8.0,
        max_secs in 1u64..=100_000,
        attempt in 0u32..=200,
    ) {
        let initial = Duration::from_secs(initial_secs);
        let max = Duration::from_secs(max_secs);
        let a = compute_retry_delay(initial, backoff, max, attempt);
        let b = compute_retry_delay(initial, backoff, max, attempt + 1);
        prop_assert!(a <= b, "not monotonic: attempt {attempt} -> {a:?}, {} -> {b:?}", attempt + 1);
    }

    /// Full jitter stays within `[0, base]` and never breaches the cap; None
    /// jitter is exactly `base`; Equal jitter stays within `[base/2, base]`.
    #[test]
    fn jitter_stays_within_documented_band(
        initial_secs in 1u64..=1_000,
        backoff in 1.0f64..=4.0,
        max_secs in 1u64..=100_000,
        attempt in 1u32..=64,
        seed in any::<u64>(),
    ) {
        let max = Duration::from_secs(max_secs);
        let base = compute_retry_delay(Duration::from_secs(initial_secs), backoff, max, attempt);

        let full = RetryPolicy {
            max_attempts: u32::MAX,
            initial_interval: Duration::from_secs(initial_secs),
            backoff_coefficient: backoff,
            max_interval: max,
            non_retryable_errors: vec![],
            jitter: JitterPolicy::Full,
        };
        let none = RetryPolicy { jitter: JitterPolicy::None, ..full.clone() };
        let equal = RetryPolicy { jitter: JitterPolicy::Equal, ..full.clone() };

        let full_d = compute_retry_delay_with_seed(&full, attempt, seed);
        let none_d = compute_retry_delay_with_seed(&none, attempt, seed);
        let equal_d = compute_retry_delay_with_seed(&equal, attempt, seed);

        // Cap holds through every jitter policy.
        prop_assert!(full_d <= max);
        prop_assert!(none_d <= max);
        prop_assert!(equal_d <= max);

        // None jitter is exactly the base delay.
        prop_assert_eq!(none_d, base, "None jitter changed the delay");

        // Full jitter: [0, base].
        prop_assert!(full_d <= base, "Full jitter {full_d:?} exceeded base {base:?}");

        // Equal jitter: [base/2, base]. Integer-nanos halving can lose one nano,
        // so allow a 1ns slack on the lower bound.
        prop_assert!(equal_d <= base, "Equal jitter {equal_d:?} exceeded base {base:?}");
        let base_half = base / 2;
        prop_assert!(
            equal_d + Duration::from_nanos(1) >= base_half,
            "Equal jitter {equal_d:?} below base/2 {base_half:?}"
        );
    }
}
