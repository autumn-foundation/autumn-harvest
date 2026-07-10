//! Fuzz target: DLQ failure-signature normalization is total and bounded.
//!
//! `dlq::failure_signature` (issue #385) normalizes an arbitrary error string
//! (first line, UUID/hex/decimal runs collapsed to placeholders, truncated to
//! `SIGNATURE_MAX_LEN` chars) into a shard-stable grouping key. It is fed raw,
//! unbounded error text, so it must never panic and must respect its length
//! cap even under adversarial unicode / multi-byte-boundary input. The
//! `dlq` module is `#[cfg(feature = "db")]`, so this target requires the
//! `db` feature on the `autumn-harvest` dependency (see `fuzz/Cargo.toml`).
//!
//! Invariants: never panics; output char count is always `<= SIGNATURE_MAX_LEN`.
#![no_main]

use autumn_harvest::dlq::{SIGNATURE_MAX_LEN, failure_signature};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let error = String::from_utf8_lossy(data);
    let sig = failure_signature(&error);
    assert!(
        sig.chars().count() <= SIGNATURE_MAX_LEN,
        "failure_signature exceeded SIGNATURE_MAX_LEN: {} chars",
        sig.chars().count()
    );
});
