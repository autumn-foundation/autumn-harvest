use proptest::prelude::*;
use std::time::Duration;

#[path = "autumn-harvest/src/policy.rs"]
mod policy;

proptest! {
    #[test]
    fn test_compute_retry_delay(
        initial_secs in 0.0f64..10000.0,
        backoff in -100.0f64..100.0,
        max_secs in 0.0f64..10000.0,
        attempt in 0u32..u32::MAX,
    ) {
        policy::compute_retry_delay(
            Duration::from_secs_f64(initial_secs),
            backoff,
            Duration::from_secs_f64(max_secs),
            attempt
        );
    }
}
