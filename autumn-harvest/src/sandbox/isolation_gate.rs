//! The gate for tests that need a namespace (issue #2741).
//!
//! A test that needs a namespace skips on a host without one. The skip
//! passes, and libtest hides its output. So CI could pass with no isolation
//! test run. CI sets [`REQUIRE_VAR`] to `1`, and each such skip then fails.

use std::env::VarError;

/// With this variable at `1`, a test fails where it would skip.
pub const REQUIRE_VAR: &str = "HARVEST_TEST_REQUIRE_ISOLATION";

/// Returns `available`. Panics when it is false and [`REQUIRE_VAR`] is `1`.
///
/// `what` names the missing namespace in the panic message.
pub fn host_has(available: bool, what: &str) -> bool {
    check(available, required(&std::env::var(REQUIRE_VAR)), what)
}

/// Reads the value of [`REQUIRE_VAR`]. An unknown value panics, so a typo
/// cannot turn the gate off.
fn required(value: &Result<String, VarError>) -> bool {
    match value.as_deref() {
        Ok("1") => true,
        Ok("" | "0") | Err(VarError::NotPresent) => false,
        other => panic!("{REQUIRE_VAR} must be 0 or 1, not {other:?}"),
    }
}

fn check(available: bool, required: bool, what: &str) -> bool {
    assert!(
        available || !required,
        "{REQUIRE_VAR}=1, but this host has no {what}. \
         Allow user namespaces, for example with \
         `sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`."
    );
    available
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_available_namespace_runs_the_test() {
        assert!(check(true, true, "x"));
        assert!(check(true, false, "x"));
    }

    #[test]
    fn a_missing_namespace_skips_by_default() {
        assert!(!check(false, false, "x"));
    }

    #[test]
    #[should_panic(expected = "HARVEST_TEST_REQUIRE_ISOLATION=1, but this host has no mount")]
    fn a_missing_namespace_fails_when_required() {
        check(false, true, "mount namespace");
    }

    #[test]
    fn only_one_requires_isolation() {
        assert!(required(&Ok("1".to_owned())));
        assert!(!required(&Ok("0".to_owned())));
        assert!(!required(&Ok(String::new())));
        assert!(!required(&Err(VarError::NotPresent)));
    }

    #[test]
    #[should_panic(expected = "must be 0 or 1")]
    fn an_unknown_value_is_refused() {
        required(&Ok("true".to_owned()));
    }

    #[test]
    #[should_panic(expected = "must be 0 or 1")]
    fn a_value_that_is_not_unicode_is_refused() {
        required(&Err(VarError::NotUnicode(std::ffi::OsString::from("x"))));
    }

    /// CI must set the variable on the sandbox test step. Otherwise the
    /// gate stays off and the skips stay silent.
    #[test]
    fn ci_requires_isolation_for_the_sandbox_tests() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/workflows/ci.yml");
        let workflow = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        let step = workflow
            .split("\n      - name: ")
            .find(|step| step.contains("run: cargo sandbox-test"))
            .expect("ci.yml must run `cargo sandbox-test`");
        assert!(
            step.contains(&format!("{REQUIRE_VAR}: \"1\"")),
            "the `cargo sandbox-test` step must set {REQUIRE_VAR}: \"1\":\n{step}"
        );
    }
}
