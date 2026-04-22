use autumn_harvest::policy::compute_retry_delay;
use autumn_harvest::types::*;
use proptest::prelude::*;
use std::str::FromStr;
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

proptest! {
    #[test]
    fn havoc_task_duration_does_not_panic(s in "\\PC*") {
        let _ = autumn_harvest::task_duration(&s);
    }
}

proptest! {
    #[test]
    fn havoc_types_do_not_panic(s in "\\PC*") {
        let _ = ExecutionId::from_str(&s);
        let _ = ActivityExecId::from_str(&s);
        let _ = TimerId::new(&s);
    }
}

#[cfg(feature = "db")]
proptest! {
    #[test]
    fn havoc_cron_schedule_does_not_panic(s in "\\PC*") {
        let _ = croner::Cron::new(&s).parse();
    }
}
