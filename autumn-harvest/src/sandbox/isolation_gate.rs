//! The gate for tests that need a namespace (issue #2741).
//!
//! A test that needs a namespace skips on a host without one. The skip
//! passes, and libtest hides its output. Without the gate, CI can pass with
//! no isolation test run. CI sets [`REQUIRE_VAR`] to `1`, and each such skip
//! then fails.

use std::env::VarError;

/// With this variable at `1`, a test fails where it would skip.
pub const REQUIRE_VAR: &str = "HARVEST_TEST_REQUIRE_ISOLATION";

/// Returns `available`. Panics when it is false and [`REQUIRE_VAR`] is `1`.
///
/// Panics also when [`REQUIRE_VAR`] holds a value other than `0`, `1` or
/// empty. `what` names the missing namespace in the panic message.
pub fn host_has(available: bool, what: &str) -> bool {
    check(available, required(&std::env::var(REQUIRE_VAR)), what)
}

/// Parses a value of [`REQUIRE_VAR`]. An unknown value panics, so a typo
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
         For a non-root user, allow user namespaces, for example with \
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

    /// The value of [`REQUIRE_VAR`] in the `env` of a step or a job.
    fn env_value(node: &serde_yaml::Value) -> Option<&str> {
        node.get("env")?.get(REQUIRE_VAR)?.as_str()
    }

    /// CI must set the variable on each step that runs `cargo sandbox-test`.
    /// Otherwise the gate stays off and the skips stay silent.
    #[test]
    fn ci_requires_isolation_for_the_sandbox_tests() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/workflows/ci.yml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        let workflow: serde_yaml::Value = serde_yaml::from_str(&text).expect("ci.yml must parse");
        let jobs = workflow["jobs"].as_mapping().expect("ci.yml has jobs");
        let mut found = 0;
        for (name, job) in jobs {
            let steps = job.get("steps").and_then(serde_yaml::Value::as_sequence);
            for step in steps.into_iter().flatten() {
                let run = step.get("run").and_then(serde_yaml::Value::as_str);
                if !run.is_some_and(|run| run.trim().starts_with("cargo sandbox-test")) {
                    continue;
                }
                found += 1;
                let value = env_value(step).or_else(|| env_value(job));
                assert_eq!(
                    value,
                    Some("1"),
                    "a `cargo sandbox-test` step of job {name:?} must set {REQUIRE_VAR}: \"1\""
                );
            }
        }
        assert!(found > 0, "ci.yml must run `cargo sandbox-test`");
    }
}
