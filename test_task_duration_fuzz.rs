use proptest::prelude::*;

#[path = "autumn-harvest/src/lib.rs"]
mod lib_mod;

proptest! {
    #[test]
    fn test_task_duration_fuzz(s in ".*") {
        lib_mod::task_duration(&s);
    }
}
