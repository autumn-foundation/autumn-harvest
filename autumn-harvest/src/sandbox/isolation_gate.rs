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

    /// The parsed workflow `.github/workflows/{file}`.
    fn workflow(file: &str) -> serde_yaml::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../.github/workflows")
            .join(file);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{file} must parse: {e}"))
    }

    /// CI must set the variable on each step that runs `cargo sandbox-test`.
    /// Otherwise the gate stays off and the skips stay silent.
    #[test]
    fn ci_requires_isolation_for_the_sandbox_tests() {
        let workflow = workflow("ci.yml");
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

    /// The sudo step of `sandbox-kvm.yml` runs the `root_` tests. `sudo`
    /// clears the environment, so the step must set the variable inside
    /// `sudo env`. Otherwise a missing namespace skips a root test.
    #[test]
    fn the_sudo_step_requires_isolation() {
        let workflow = workflow("sandbox-kvm.yml");
        let jobs = workflow["jobs"]
            .as_mapping()
            .expect("sandbox-kvm.yml has jobs");
        let mut found = 0;
        for job in jobs.values() {
            let steps = job.get("steps").and_then(serde_yaml::Value::as_sequence);
            for step in steps.into_iter().flatten() {
                let run = step.get("run").and_then(serde_yaml::Value::as_str);
                let Some(run) = run.filter(|run| run.contains("sudo env")) else {
                    continue;
                };
                found += 1;
                assert!(
                    run.contains(" root_ --ignored"),
                    "the sudo step must run the ignored `root_` tests"
                );
                assert!(
                    run.contains(&format!("\"{REQUIRE_VAR}=1\"")),
                    "the sudo step must pass {REQUIRE_VAR}=1 to `sudo env`"
                );
            }
        }
        assert_eq!(found, 1, "sandbox-kvm.yml must have one sudo test step");
    }

    /// A function in a source file: its name, its attributes and its body.
    struct Func {
        name: String,
        attrs: Vec<String>,
        body: String,
    }

    impl Func {
        fn is_test(&self) -> bool {
            self.attrs.iter().any(|a| is_test_attr(a))
        }
    }

    /// Whether `attr` marks a test function, such as `#[test]` or
    /// `#[tokio::test(flavor = "multi_thread")]`.
    fn is_test_attr(attr: &str) -> bool {
        attr.starts_with("#[test")
            || (attr.starts_with("#[") && (attr.contains("::test]") || attr.contains("::test(")))
    }

    /// The name of the function that `line` starts, if any.
    fn fn_name(line: &str) -> Option<&str> {
        const QUALIFIERS: [&str; 6] = [
            "pub(crate) ",
            "pub(super) ",
            "pub ",
            "const ",
            "async ",
            "unsafe ",
        ];
        let mut rest = line;
        while let Some(q) = QUALIFIERS.iter().find(|q| rest.starts_with(**q)) {
            rest = &rest[q.len()..];
        }
        let header = rest.strip_prefix("fn ")?;
        header.split(['(', '<']).next()
    }

    /// Finds each function in `text`, with its attributes and its body.
    ///
    /// The scan reads rustfmt layout. A body ends at the first line that
    /// holds only `}` at the indent of the `fn` line. A `fn` line that ends
    /// with `}` or `;` is the whole function.
    fn fns(text: &str) -> Vec<Func> {
        let lines: Vec<&str> = text.lines().collect();
        let mut found = Vec::new();
        let mut attrs: Vec<String> = Vec::new();
        for (at, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("#[") || trimmed.starts_with("//") {
                attrs.push(trimmed.to_owned());
                continue;
            }
            if let Some(name) = fn_name(trimmed) {
                let body = if trimmed.ends_with('}') || trimmed.ends_with(';') {
                    (*line).to_owned()
                } else {
                    let end = format!("{}}}", &line[..line.len() - trimmed.len()]);
                    let body: Vec<&str> = lines[at..]
                        .iter()
                        .take_while(|l| **l != end)
                        .copied()
                        .collect();
                    body.join("\n")
                };
                found.push(Func {
                    name: name.to_owned(),
                    attrs: std::mem::take(&mut attrs),
                    body,
                });
            }
            attrs.clear();
        }
        found
    }

    /// Each `.rs` file under `dir`, at any depth.
    fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    /// The names that read the uid of the worker.
    const ROOT_CHECKS: [&str; 3] = ["geteuid", "getuid", "is_root"];

    /// Tests that check for root, but are not `root_` tests.
    ///
    /// `the_process_limit_holds_without_root` skips as root on purpose. The
    /// non-root job runs it, and a `root_` test covers a root worker.
    const ALLOWED: [&str; 1] = ["the_process_limit_holds_without_root"];

    /// Whether `body` holds `ident` as a whole identifier.
    fn mentions(body: &str, ident: &str) -> bool {
        let is_ident = |c: char| c.is_alphanumeric() || c == '_';
        body.match_indices(ident).any(|(at, _)| {
            let before = body[..at].chars().next_back();
            let after = body[at + ident.len()..].chars().next();
            !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
        })
    }

    /// The helpers in `funcs` that skip for want of root. Each one prints a
    /// `skipped:` line and reads the uid.
    fn root_probes(funcs: &[Func]) -> Vec<String> {
        funcs
            .iter()
            .filter(|f| !f.is_test() && f.body.contains("skipped:"))
            .filter(|f| ROOT_CHECKS.iter().any(|c| mentions(&f.body, c)))
            .map(|f| f.name.clone())
            .collect()
    }

    /// Returns the rule that `test` breaks, if any. A call to a helper in
    /// `probes` counts as a check for root.
    fn root_rule_broken(test: &Func, probes: &[String]) -> Option<&'static str> {
        let ignore = test.attrs.iter().find(|a| a.starts_with("#[ignore"));
        let needs_root = ignore.is_some_and(|a| {
            a.split(|c: char| !c.is_ascii_alphanumeric())
                .any(|word| word == "root")
        });
        let named = test.name.starts_with("root_");
        let checks = ROOT_CHECKS.iter().any(|c| mentions(&test.body, c))
            || probes.iter().any(|p| mentions(&test.body, p));
        if checks && !named && !ALLOWED.contains(&test.name.as_str()) {
            Some("only a `root_` test may check for root")
        } else if needs_root && !named {
            Some("a test that needs root must start with `root_`")
        } else if named && !needs_root {
            Some("a `root_` test must be ignored with a reason that names root")
        } else {
            None
        }
    }

    /// No CI job runs a test as root unless it is an ignored `root_` test.
    /// So a root check in another test hides a skip or a branch that no CI
    /// job runs (issue #2757).
    #[test]
    fn no_sandbox_test_skips_without_root() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sandbox");
        let mut files = Vec::new();
        rust_files(&dir, &mut files);
        // This file holds the patterns as test data, so the scan skips it.
        files.retain(|f| !f.ends_with("isolation_gate.rs"));
        let mut tests = 0;
        let mut broken = Vec::new();
        for file in files {
            let text = std::fs::read_to_string(&file).unwrap();
            let marked = text.lines().filter(|l| is_test_attr(l.trim())).count();
            let funcs = fns(&text);
            let probes = root_probes(&funcs);
            let found: Vec<&Func> = funcs.iter().filter(|f| f.is_test()).collect();
            assert_eq!(
                found.len(),
                marked,
                "the scan must read each test of {}",
                file.display()
            );
            for test in found {
                tests += 1;
                if let Some(rule) = root_rule_broken(test, &probes) {
                    broken.push(format!("{}: {}: {rule}", file.display(), test.name));
                }
            }
        }
        assert!(tests > 100, "the scan found only {tests} tests");
        assert!(broken.is_empty(), "{}", broken.join("\n"));
    }

    fn func(name: &str, attrs: &[&str], body: &str) -> Func {
        Func {
            name: name.to_owned(),
            attrs: attrs.iter().map(|a| (*a).to_owned()).collect(),
            body: body.to_owned(),
        }
    }

    #[test]
    fn the_scan_reads_names_attributes_and_bodies() {
        let text = "\
#[test]
fn plain() {
    if !is_root() {
        return;
    }
}

fn helper() {}

pub(crate) unsafe fn other() {
    x();
}

    #[tokio::test(flavor = \"current_thread\")]
    #[ignore = \"needs root\"]
    async fn root_nested() {
        x();
    }
";
        let found = fns(text);
        let names: Vec<_> = found.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["plain", "helper", "other", "root_nested"]);
        let tests: Vec<_> = found.iter().map(Func::is_test).collect();
        assert_eq!(tests, [true, false, false, true]);
        assert!(found[0].body.contains("!is_root()"));
        assert!(!found[0].body.contains("helper"));
        assert_eq!(found[1].body, "fn helper() {}");
        assert!(found[2].body.ends_with("x();"));
        assert_eq!(
            found[3].attrs,
            [
                "#[tokio::test(flavor = \"current_thread\")]",
                "#[ignore = \"needs root\"]"
            ]
        );
    }

    #[test]
    fn a_test_attribute_is_found_in_each_form() {
        assert!(is_test_attr("#[test]"));
        assert!(is_test_attr("#[tokio::test]"));
        assert!(is_test_attr("#[tokio::test(flavor = \"multi_thread\")]"));
        assert!(is_test_attr("#[test] // a reason"));
        assert!(!is_test_attr("#[cfg(test)]"));
        assert!(!is_test_attr("#[allow(clippy::tests_outside_test_module)]"));
    }

    #[test]
    fn a_skip_helper_is_a_probe() {
        let skip = "fn jailed() -> Option<()> {\n    if unsafe { libc::geteuid() } != 0 {\n        eprintln!(\"skipped: the jailer needs root\");\n        return None;\n    }";
        let branch = "fn run_paused() {\n    let root = is_root();";
        let funcs = [func("jailed", &[], skip), func("run_paused", &[], branch)];
        assert_eq!(root_probes(&funcs), ["jailed"]);
        assert!(mentions("let c = jailed();", "jailed"));
        assert!(!mentions("let c = unjailed(boot);", "jailed"));
        assert!(!mentions("\"is_root_device\": true", "is_root"));
    }

    #[test]
    fn the_root_rules_hold() {
        let ignored = "#[ignore = \"needs root; the Sandbox (KVM) workflow runs it with sudo\"]";
        let kvm = "#[ignore = \"needs /dev/kvm\"]";
        let rootfs = "#[ignore = \"needs a guest rootfs\"]";
        let skip = "if !is_root() { return; }";
        let spelled = "if unsafe { libc::geteuid() } != 0 { return; }";
        let branch = "if unsafe { libc::geteuid() } == 0 { assert!(x()); }";
        let assert = "assert!(is_root(), \"run this test as root\");";
        let probes = ["jailed".to_owned()];
        let broken =
            |name, attrs: &[&str], body| root_rule_broken(&func(name, attrs, body), &probes);
        assert!(broken("root_ok", &["#[test]", ignored], assert).is_none());
        assert!(broken("plain", &["#[test]"], "x();").is_none());
        assert!(broken("guest", &["#[test]", rootfs], "x();").is_none());
        assert!(broken(ALLOWED[0], &["#[test]"], spelled).is_none());
        assert!(broken("skips", &["#[test]"], skip).is_some());
        assert!(broken("spelled", &["#[test]"], spelled).is_some());
        assert!(broken("branch", &["#[test]"], branch).is_some());
        assert!(broken("kvm_skips", &["#[test]", kvm], skip).is_some());
        assert!(broken("probe", &["#[test]", kvm], "let c = jailed();").is_some());
        assert!(broken("root_probe", &["#[test]", ignored], "let c = jailed();").is_none());
        assert!(broken("unnamed", &["#[test]", ignored], assert).is_some());
        assert!(broken("root_run", &["#[test]"], assert).is_some());
        assert!(broken("root_kvm", &["#[test]", kvm], assert).is_some());
    }
}
