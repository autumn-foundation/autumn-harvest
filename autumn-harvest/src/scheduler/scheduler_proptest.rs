use proptest::prelude::*;
use crate::scheduler::{SchedulerMonitor, DEFAULT_SCHEDULER_TICK_INTERVAL};

proptest! {
    #[test]
    fn test_scheduler_monitor_new_fuzz(dag_count in prop::num::usize::ANY) {
        let monitor = SchedulerMonitor::new(dag_count);
        let snapshot = monitor.snapshot();
        assert_eq!(snapshot.dag_count, dag_count);
        assert_eq!(snapshot.tick_interval_ms, DEFAULT_SCHEDULER_TICK_INTERVAL.as_millis().try_into().unwrap_or(u64::MAX));
        assert!(snapshot.running);
        assert!(snapshot.last_tick_at.is_none());
    }
}
