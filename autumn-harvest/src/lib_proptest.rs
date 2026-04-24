use proptest::prelude::*;

proptest! {
    #[test]
    fn test_task_duration_fuzz(s in ".*") {
        let _ = super::task_duration(&s);
    }
}
