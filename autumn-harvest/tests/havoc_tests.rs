use autumn_harvest::policy::compute_retry_delay;
use proptest::prelude::*;
use std::time::Duration;

proptest! {
    #[test]
    fn havoc_compute_retry_delay_does_not_panic(
        initial_secs in 1u64..u64::MAX,
        backoff in -100.0f64..100.0f64,
        max_interval_secs in 1u64..u64::MAX,
        attempt in 1u32..u32::MAX
    ) {
        let initial = Duration::from_secs(initial_secs);
        let max_interval = Duration::from_secs(max_interval_secs);
        let _delay = compute_retry_delay(initial, backoff, max_interval, attempt);
    }
}
