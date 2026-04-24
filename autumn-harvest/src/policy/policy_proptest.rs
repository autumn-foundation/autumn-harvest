use proptest::prelude::*;
use std::time::Duration;

proptest! {
    #[test]
    fn test_compute_retry_delay_fuzz(
        initial_secs in prop::num::f64::ANY,
        backoff in prop::num::f64::ANY,
        max_secs in prop::num::f64::ANY,
        attempt in prop::num::u32::ANY,
    ) {
        if initial_secs >= 0.0 && max_secs >= 0.0 && max_secs <= (u64::MAX as f64) && initial_secs <= (u64::MAX as f64) {
            if let Ok(initial) = Duration::try_from_secs_f64(initial_secs) {
                if let Ok(max_val) = Duration::try_from_secs_f64(max_secs) {
                    let _ = super::compute_retry_delay(
                        initial,
                        backoff,
                        max_val,
                        attempt
                    );
                }
            }
        }
    }
}
