//! Guards for the journaled host-call spike report (issue #2014).
//!
//! `docs/rnd/agent-code-host-call-journal.md` gives a go / no-go verdict. Its
//! evidence is a list of tests and a set of bounds. These guards read the
//! prototype source and check that the report agrees with it:
//!
//! * The report cites each test of the prototype, and each cited test exists.
//! * Each bound that the report states has the value in the source.
//! * The report states a verdict and cites the issue.
//! * Each prose sentence has 25 words or fewer.
//!
//! The verdict reasoning is argument, not fact, so no guard freezes it.
//!
//! Pure: no database, no async, no feature gate. Runs on every OS.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::docs_guard_support::{check_stanza, issue_refs, prose_sentences, section, step_stanza};

const REPORT: &str = "docs/rnd/agent-code-host-call-journal.md";
const SOURCE: &str = "autumn-harvest/src/wasm_journal.rs";
const FILTER: &str = "--test integration agent_code_journal_docs::";
const MAX_SENTENCE_WORDS: usize = 25;

/// The bounds that the report states, by constant name.
const BOUNDS: &[&str] = &[
    "MAX_HOST_CALL_BYTES",
    "MAX_HOST_CALL_NAME_BYTES",
    "MAX_HOST_CALLS",
    "HOST_CALL_DENIED",
    "HOST_CALL_INVALID",
    "HOST_CALL_LIMIT",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

/// The names of the test functions in `source`.
fn test_fns(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut after_test_attr = false;
    for line in source.lines().map(str::trim) {
        if line == "#[test]" {
            after_test_attr = true;
            continue;
        }
        if after_test_attr && let Some(rest) = line.strip_prefix("fn ") {
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            names.insert(name);
            after_test_attr = false;
        } else if !line.starts_with("#[") {
            after_test_attr = false;
        }
    }
    names
}

/// The backticked snake-case names in the evidence table of the report.
fn cited_tests(report: &str) -> BTreeSet<String> {
    let table = section(report, "## 3. What the tests prove", "\n## ")
        .expect("the report must have a `## 3. What the tests prove` section");
    table
        .lines()
        .filter(|line| line.starts_with('|'))
        .filter_map(|line| line.trim_end_matches('|').rsplit('|').next())
        .filter_map(|cell| cell.trim().strip_prefix('`')?.strip_suffix('`'))
        .filter(|name| {
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
        .map(str::to_owned)
        .collect()
}

/// The literal value of `pub const NAME: TYPE = VALUE;` in `source`.
fn const_value(source: &str, name: &str) -> i64 {
    let line = source
        .lines()
        .find(|line| line.starts_with(&format!("pub const {name}:")))
        .unwrap_or_else(|| panic!("{SOURCE} must define `pub const {name}`"));
    let expr = line
        .split_once('=')
        .and_then(|(_, rhs)| rhs.trim().strip_suffix(';'))
        .unwrap_or_else(|| panic!("cannot parse `{line}`"));
    expr.split('*')
        .map(|factor| {
            factor
                .trim()
                .replace('_', "")
                .parse::<i64>()
                .unwrap_or_else(|err| panic!("cannot parse `{expr}`: {err}"))
        })
        .product()
}

#[test]
fn report_cites_the_issue_and_states_a_verdict() {
    let report = read(REPORT);
    assert!(
        issue_refs(&report).contains(&2014),
        "{REPORT} must cite #2014"
    );
    let verdict = report
        .lines()
        .find(|line| line.starts_with("**Verdict:**"))
        .unwrap_or_else(|| panic!("{REPORT} must have a `**Verdict:**` line"));
    assert!(
        verdict.contains("go"),
        "the verdict must say go or no-go: {verdict}"
    );
}

#[test]
fn report_cites_each_test_of_the_prototype() {
    let tests = test_fns(&read(SOURCE));
    assert!(!tests.is_empty(), "{SOURCE} must hold the prototype tests");
    let cited = cited_tests(&read(REPORT));

    let uncited: Vec<_> = tests.difference(&cited).collect();
    assert!(
        uncited.is_empty(),
        "{REPORT} section 3 must cite each prototype test. Missing: {uncited:?}"
    );
    let unknown: Vec<_> = cited.difference(&tests).collect();
    assert!(
        unknown.is_empty(),
        "{REPORT} section 3 cites tests that {SOURCE} does not define: {unknown:?}"
    );
}

#[test]
fn report_states_each_bound_with_its_source_value() {
    let source = read(SOURCE);
    let report = read(REPORT);
    for name in BOUNDS {
        let stated = format!("`{name}` ({})", const_value(&source, name));
        assert!(
            report.contains(&stated),
            "{REPORT} must state the bound as {stated}"
        );
    }
}

#[test]
fn prose_sentences_stay_short() {
    let long: Vec<String> = prose_sentences(&read(REPORT))
        .into_iter()
        .filter(|sentence| sentence.split_whitespace().count() > MAX_SENTENCE_WORDS)
        .collect();
    assert!(
        long.is_empty(),
        "{REPORT} has prose sentences over {MAX_SENTENCE_WORDS} words. Split each one:\n{}",
        long.join("\n")
    );
}

#[test]
fn report_is_linked_from_the_wasm_spike() {
    let spike = read("docs/rnd/wasm-activities-spike.md");
    assert!(
        spike.contains("agent-code-host-call-journal.md"),
        "docs/rnd/wasm-activities-spike.md must link the journal report"
    );
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define `lint`");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define `test`");
    let stanza = step_stanza(&workflow[lint..test], FILTER).unwrap_or_else(|| {
        panic!(
            "the `lint` job in ci.yml must run these guards. Add this step:\n\
             \x20     - name: Run the agent-code journal report guards (also on docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn helpers_parse_their_inputs() {
    let source = "pub const A: usize = 64 * 1024;\npub const B: i64 = -3;\n\
                  #[test]\n#[ignore = \"x\"]\nfn one() {}\nfn helper() {}\n#[test]\nfn two() {}\n";
    assert_eq!(const_value(source, "A"), 65_536);
    assert_eq!(const_value(source, "B"), -3);
    assert_eq!(
        test_fns(source),
        BTreeSet::from(["one".to_owned(), "two".to_owned()])
    );

    let report = "## 3. What the tests prove\n\n| Claim | Test |\n|---|---|\n\
                  | A. | `one` |\n| B uses `Code`. | `two_words` |\n\n## 4. Next\n| x | `three` |\n";
    assert_eq!(
        cited_tests(report),
        BTreeSet::from(["one".to_owned(), "two_words".to_owned()])
    );
}
