//! A route must store a payload through the codec-aware writer (issue #2043).
//!
//! The plain variants store the payload as the identity codec writes it. A
//! route that calls one leaves the column in clear, and the rotation sweep
//! never encrypts a value written in clear. The core crate pins its own
//! sources in the same way. This test pins `api.rs`.

const API: &str = include_str!("../src/api.rs");

#[test]
fn routes_call_the_codec_aware_column_writers() {
    for plain in [
        "throttle::reserve_or_defer(",
        "throttle::reserve_or_defer_or_shed(",
        "debounce::admit_debounced_start(",
        "batch::submit_batch_job(",
    ] {
        assert!(
            !API.contains(plain),
            "api.rs calls `{plain}`. Use the `_with_codecs` variant."
        );
    }
}
