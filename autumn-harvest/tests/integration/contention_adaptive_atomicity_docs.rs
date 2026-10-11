//! Guards that keep the issue #2012 R&D report true to the code.
//!
//! `docs/rnd/contention-adaptive-atomicity.md` quotes constants, rule
//! branches, arms and the measurement setup. Each guard reads the fact from
//! the source tree and asserts that the report agrees. The measured numbers
//! and the prose judgement are not pinned. A rerun can change them.
//!
//! The suite needs no database and no feature.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf()
}

/// Read a repository file with `\n` line endings.
fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

fn report() -> String {
    read("docs/rnd/contention-adaptive-atomicity.md")
}

/// The variant names of `pub enum <name>` in `source`.
fn enum_variants(source: &str, name: &str) -> Vec<String> {
    let start = source
        .find(&format!("pub enum {name} {{"))
        .unwrap_or_else(|| panic!("no `pub enum {name}`"));
    let body = &source[start..];
    let end = body.find("\n}").expect("the enum closes");
    body[..end]
        .lines()
        .skip(1)
        .map(str::trim)
        .filter(|line| !line.starts_with("///") && !line.starts_with("#[") && !line.is_empty())
        .map(|line| line.trim_end_matches(',').to_string())
        .collect()
}

#[test]
fn the_report_gives_a_pre_registered_verdict() {
    let report = report();
    assert!(report.contains("issue #2012"), "the report names its issue");
    let verdicts = [
        "**Verdict: Go.**",
        "**Verdict: Go with changes.**",
        "**Verdict: No-go.**",
    ];
    let found = verdicts.iter().filter(|v| report.contains(*v)).count();
    assert_eq!(found, 1, "the report states exactly one verdict line");
    for criterion in ["G1", "G2", "G3", "G4"] {
        assert!(report.contains(criterion), "the report applies {criterion}");
    }
}

#[test]
fn every_rule_branch_is_in_the_report() {
    let rule = read("autumn-harvest/src/atomicity/rule.rs");
    let reasons = enum_variants(&rule, "Reason");
    assert_eq!(reasons.len(), 6, "the rule has six branches: {reasons:?}");
    let report = report();
    for reason in &reasons {
        assert!(
            report.contains(&format!("`{reason}`")),
            "the report omits the rule branch `{reason}`"
        );
    }
}

#[test]
fn the_quoted_thresholds_match_the_code() {
    let rule = read("autumn-harvest/src/atomicity/rule.rs");
    let verdict = read("autumn-harvest/src/atomicity/verdict.rs");
    let report = report();
    let pins = [
        (
            rule.as_str(),
            "pub const MAX_HOLD: Duration = Duration::from_secs(5);",
            "`MAX_HOLD` = 5 s",
        ),
        (
            rule.as_str(),
            "pub const COLD_KEY_CONCURRENCY: f64 = 1.0;",
            "`COLD_KEY_CONCURRENCY` = 1",
        ),
        (
            verdict.as_str(),
            "pub const TIE_BAND: f64 = 0.10;",
            "`TIE_BAND` = 10 %",
        ),
    ];
    for (source, code, quote) in pins {
        assert!(source.contains(code), "the code no longer has `{code}`");
        assert!(report.contains(quote), "the report does not quote {quote}");
    }
}

#[test]
fn the_arms_in_the_report_are_the_arms_in_the_code() {
    let rule = read("autumn-harvest/src/atomicity/rule.rs");
    let arms = enum_variants(&rule, "Atomicity");
    assert_eq!(arms, ["Backout", "Saga", "Hybrid"]);
    let report = report();
    for label in ["`backout`", "`saga`", "`hybrid`"] {
        assert!(report.contains(label), "the report omits the arm {label}");
    }
}

#[test]
fn the_measurement_setup_matches_the_harness() {
    let harness = read("autumn-harvest/src/atomicity/harness.rs");
    let tests = read("autumn-harvest/tests/integration/atomicity_spike_tests.rs");
    let pins = [
        (harness.as_str(), "Self::Low => 1_000,", "1,000 SKUs"),
        (harness.as_str(), "Self::High => 1,", "1 SKU"),
        (
            harness.as_str(),
            "pub const FAIL_RATE: f64 = 0.1;",
            "10 % of orders",
        ),
        (harness.as_str(), "fail_rate: FAIL_RATE,", "10 % of orders"),
        (
            harness.as_str(),
            "for step_work in [Duration::ZERO, Duration::from_millis(20)]",
            "0 ms and 20 ms",
        ),
        (
            tests.as_str(),
            "const REPETITIONS: usize = 3;",
            "3 repetitions",
        ),
        (tests.as_str(), "let clients = 16;", "16 clients"),
        (
            tests.as_str(),
            "harness::matrix(clients, Duration::from_secs(10), 2012)",
            "10 s per run",
        ),
    ];
    let report = report();
    for (source, code, quote) in pins {
        assert!(source.contains(code), "the code no longer has `{code}`");
        assert!(report.contains(quote), "the report does not quote {quote}");
    }
}

#[test]
fn the_reproduce_command_names_a_real_test() {
    let tests = read("autumn-harvest/tests/integration/atomicity_spike_tests.rs");
    let at = tests
        .find("async fn measure_the_full_matrix()")
        .expect("the measurement test exists");
    assert!(
        tests[..at]
            .trim_end()
            .lines()
            .last()
            .unwrap_or("")
            .contains("#[ignore"),
        "the measurement test stays out of the default run"
    );
    assert!(
        report().contains("--features atomicity-spike")
            && report().contains(
                "atomicity_spike_tests::measure_the_full_matrix -- --ignored --nocapture"
            ),
        "the report gives the command that reproduces it"
    );
}

#[test]
fn the_saga_guide_links_the_report() {
    assert!(
        read("docs/saga.md").contains("(rnd/contention-adaptive-atomicity.md)"),
        "docs/saga.md links the report"
    );
}

#[test]
fn the_spike_stays_out_of_the_default_build() {
    let manifest = read("autumn-harvest/Cargo.toml");
    let default = manifest
        .lines()
        .find(|line| line.starts_with("default = "))
        .expect("the crate has default features");
    assert!(!default.contains("atomicity-spike"), "{default}");
    assert!(manifest.contains("atomicity-spike = [\"db\"]"));
    assert!(
        read("autumn-harvest/src/lib.rs")
            .contains("#[cfg(feature = \"atomicity-spike\")]\n#[doc(hidden)]\npub mod atomicity;"),
        "the module is behind the feature"
    );
}
