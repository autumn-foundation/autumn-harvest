//! Fuzz target: DLQ failure-signature normalization is total and bounded.
//!
//! `dlq::failure_signature` (issue #385) turns an error string into a
//! shard-stable grouping key. It keeps the first line, collapses UUID, hex
//! and decimal runs to placeholders, and truncates to `SIGNATURE_MAX_LEN`
//! chars. Its input is raw, unbounded error text. So it must never panic,
//! and it must keep its length cap, also for adversarial unicode that
//! splits a multi-byte boundary.
//! The `dlq` module is `#[cfg(feature = "db")]`, so this target needs the
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
