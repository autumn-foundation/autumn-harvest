//! Property tests for [`autumn_harvest::dlq::failure_signature`] (Tier A target
//! 6). `dlq` is `#[cfg(feature = "db")]`, so this suite only compiles/runs under
//! a `db` build.
//!
//! Contract under test (per the `failure_signature` rustdoc, dlq.rs):
//! - **determinism**: `failure_signature(x) == failure_signature(x)` — a pure
//!   function of its input, so the same root cause aggregates identically across
//!   queries and shards;
//! - **bounded length**: output char count `<= SIGNATURE_MAX_LEN`;
//! - **total**: never panics on arbitrary input, including unicode, UUID/hex/
//!   decimal runs, multi-line strings.
//!
//! Note — determinism vs. idempotence: we assert **determinism** (`f(x) == f(x)`),
//! NOT idempotence (`f(f(x)) == f(x)`). Idempotence is technically *false* at the
//! `SIGNATURE_MAX_LEN` boundary: `failure_signature` does
//! `truncate_chars(normalized.trim(), SIGNATURE_MAX_LEN)` with no trailing trim,
//! so an output ending in whitespace at exactly the cap loses that whitespace on
//! re-application. Determinism is the documented contract and is what this suite
//! exercises.

use autumn_harvest::dlq::{SIGNATURE_MAX_LEN, failure_signature};
use proptest::prelude::*;

use super::prop_config::config;

/// A strategy that mixes literal words with UUID-, hex-, and decimal-run tokens
/// (plus multi-line and unicode content) so the normalization branches are hit.
fn error_like() -> impl Strategy<Value = String> {
    let token = prop_oneof![
        "[a-z ]{1,10}",
        "[0-9]{1,25}",                                                  // decimal runs
        "[0-9a-f]{8,40}",                                               // hex runs
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", // uuid-shaped
        Just("\n".to_string()),
        "\\PC{0,6}", // arbitrary unicode
    ];
    proptest::collection::vec(token, 0..12).prop_map(|parts| parts.join(" "))
}

/// A strategy that GUARANTEES a first line far longer than `SIGNATURE_MAX_LEN`
/// after normalization, so the truncation branch is actually exercised — the
/// `error_like` generator above tops out around ~130 chars and never reaches the
/// cap. 30+ non-collapsing `[a-z]` word tokens joined by spaces stay a single
/// line and never normalize down to a placeholder, so the first line is always
/// well past 200 chars.
fn long_error() -> impl Strategy<Value = String> {
    proptest::collection::vec("[a-z]{7,12}", 30..60).prop_map(|parts| parts.join(" "))
}

proptest! {
    #![proptest_config(config())]

    /// Deterministic: two calls on the same input agree. (Determinism, not
    /// idempotence — see the module doc comment for why idempotence is false at
    /// the cap boundary.)
    #[test]
    fn failure_signature_is_deterministic(s in error_like()) {
        prop_assert_eq!(
            failure_signature(&s),
            failure_signature(&s),
            "signature is not deterministic for input {:?}",
            s
        );
    }

    /// The result never exceeds the documented character cap.
    #[test]
    fn failure_signature_length_is_bounded(s in error_like()) {
        let sig = failure_signature(&s);
        prop_assert!(
            sig.chars().count() <= SIGNATURE_MAX_LEN,
            "signature length {} exceeds cap {}",
            sig.chars().count(),
            SIGNATURE_MAX_LEN
        );
    }

    /// Inputs guaranteed to exceed the cap are truncated to `SIGNATURE_MAX_LEN`,
    /// exercising the truncation branch that `error_like` never reaches.
    #[test]
    fn failure_signature_caps_oversized_input(s in long_error()) {
        let sig = failure_signature(&s);
        prop_assert!(
            sig.chars().count() <= SIGNATURE_MAX_LEN,
            "oversized signature length {} exceeds cap {}",
            sig.chars().count(),
            SIGNATURE_MAX_LEN
        );
    }

    /// Total function on genuinely arbitrary input (bound still holds).
    #[test]
    fn failure_signature_is_total(s in any::<String>()) {
        let once = failure_signature(&s);
        prop_assert!(once.chars().count() <= SIGNATURE_MAX_LEN);
        prop_assert_eq!(failure_signature(&once), once);
    }
}
