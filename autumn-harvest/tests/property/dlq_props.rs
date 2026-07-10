//! Property tests for [`autumn_harvest::dlq::failure_signature`] (Tier A target
//! 6). `dlq` is `#[cfg(feature = "db")]`, so this suite only compiles/runs under
//! a `db` build.
//!
//! Contract under test:
//! - **idempotence**: `failure_signature(&failure_signature(x)) ==
//!   failure_signature(x)` (stable under re-application — the normalized form is
//!   a fixpoint);
//! - **bounded length**: output char count `<= SIGNATURE_MAX_LEN`;
//! - **total**: never panics on arbitrary input, including unicode, UUID/hex/
//!   decimal runs, multi-line strings.

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

proptest! {
    #![proptest_config(config())]

    /// Re-applying the signature to its own output is a no-op (fixpoint).
    #[test]
    fn failure_signature_is_idempotent(s in error_like()) {
        let once = failure_signature(&s);
        let twice = failure_signature(&once);
        prop_assert_eq!(&twice, &once, "signature is not idempotent for input {:?}", s);
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

    /// Total function on genuinely arbitrary input (idempotence + bound still hold).
    #[test]
    fn failure_signature_is_total(s in any::<String>()) {
        let once = failure_signature(&s);
        prop_assert!(once.chars().count() <= SIGNATURE_MAX_LEN);
        prop_assert_eq!(failure_signature(&once), once);
    }
}
