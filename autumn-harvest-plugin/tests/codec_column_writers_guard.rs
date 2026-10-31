//! A route must store a payload through the codec-aware writer (issue #2043).
//!
//! The plain variants store the payload as the identity codec writes it. A
//! route that calls one leaves the column in clear, and the rotation sweep
//! never encrypts a value written in clear. The core crate pins its own
//! sources in the same way. This test pins `api.rs` and `ui.rs`.

const API: &str = include_str!("../src/api.rs");
const UI: &str = include_str!("../src/ui.rs");

#[test]
fn routes_call_the_codec_aware_column_writers() {
    for (file, source) in [("api.rs", API), ("ui.rs", UI)] {
        for plain in [
            "throttle::reserve_or_defer(",
            "throttle::reserve_or_defer_or_shed(",
            "debounce::admit_debounced_start(",
            "batch::submit_batch_job(",
            "event_batch::admit_batched_start(",
            "execution::rerun_workflow_execution(",
            "::register_workflow_schedules(",
            "::update_workflow_schedule(",
            "::record_cross_shard_child(",
        ] {
            assert!(
                !source.contains(plain),
                "{file} calls `{plain}`. Use the `_with_codecs` variant."
            );
        }
    }
}
