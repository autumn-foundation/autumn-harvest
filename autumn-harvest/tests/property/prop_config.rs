//! Shared proptest configuration for the property-test suite.
//!
//! # Bounded runtime
//!
//! The default case count is deliberately **low** (128) so the pure-function
//! suites run in well under a minute in CI. This is not a fuzz campaign — it is
//! a fast regression net that runs on every push.
//!
//! # Deep-run override
//!
//! Set the `PROPTEST_CASES` environment variable to run more cases locally or
//! in a nightly job, e.g.:
//!
//! ```text
//! PROPTEST_CASES=100000 cargo test -p autumn-harvest --no-default-features --test property
//! ```
//!
//! `PROPTEST_CASES` is proptest's own native knob; we read it here explicitly so
//! that our low hardcoded default (128) is still *overridable* upward (an
//! explicit `cases:` struct field would otherwise win over the env var).
//!
//! # Failure persistence
//!
//! `failure_persistence` is set to `None`, so a shrunk counterexample is
//! reported in the panic message but **not** written to a `proptest-regressions/`
//! file. That keeps CI runners read-only-clean (no untracked artifacts, no
//! attempt to write to disk). A discovered counterexample must be reproduced by
//! re-running (or pinned as an explicit `#[test]` case), which is the right
//! workflow for a bootstrap slice.

use proptest::test_runner::Config;

/// The default number of cases when `PROPTEST_CASES` is unset.
const DEFAULT_CASES: u32 = 128;

/// Build the shared proptest [`Config`]: a low default case count that
/// `PROPTEST_CASES` can override upward, with on-disk failure persistence
/// disabled.
#[must_use]
pub fn config() -> Config {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&c| c > 0)
        .unwrap_or(DEFAULT_CASES);

    Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    }
}
