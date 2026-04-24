use proptest::prelude::*;

#[path = "autumn-harvest/src/lib.rs"]
mod lib;

proptest! {
    #[test]
    fn test_task_duration_fuzz(s in ".*") {
        let _ = lib::task_duration(&s);
    }
}
