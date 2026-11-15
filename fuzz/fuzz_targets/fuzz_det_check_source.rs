//! Fuzz target: the determinism guardrail source checker is total.
//!
//! `det_check::check_source` is a hand-rolled, line-oriented scanner over
//! arbitrary Rust source text (`#[workflow]` body extraction, brace tracking,
//! binding tracking). It has a long history of parity and edge-case churn
//! against the sibling proc-macro lint. It must never panic on malformed input,
//! because CI and editor tooling run it over user code. Raw-bytes fuzzing (via
//! lossy UTF-8) is the highest-value target here: it exercises the parser's
//! own byte/line handling far past what curated proptest strategies reach.
//!
//! Invariant: `check_source` always returns a `DetCheckReport` and never
//! panics, for any input.
#![no_main]

use autumn_harvest::det_check;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let source = String::from_utf8_lossy(data);
    // `file` is used only for location reporting; no file is read.
    let _ = det_check::check_source(&source, "fuzz.rs");
});
