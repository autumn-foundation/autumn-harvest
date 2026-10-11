//! Guards for the safety report (issue #2004).
//!
//! `docs/safety-report.md` states four guarantees: leases, fencing,
//! exactly-once completion and signal ordering. Issue #2004 is done when two
//! things hold:
//!
//! * Every result has a command that reproduces it.
//! * The report states each known limit.
//!
//! These guards make both checks mechanical. A command must name a real test
//! target, test, script or TLA+ config. A cargo filter that matches no test
//! exits 0. So the guard lists the tests that the command compiles, with its
//! features, and each filter must match one of them. The report must restate
//! each limit that a source page records.
//!
//! Pure: no database, no async. Runs on every OS.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use super::docs_guard_support::{check_stanza, issue_refs, prose_sentences, section, step_stanza};

const REPORT: &str = "docs/safety-report.md";
const REPORT_LINK: &str = "safety-report.md";
const MODELS: &str = "formal/tla/models.txt";
const CHAOS: &str = "docs/testing/chaos.md";
const FILTER: &str = "--test integration safety_report_docs::";

/// The top-level sections, in reading order.
const SECTIONS: &[&str] = &[
    "## Summary",
    "## Scope and fault model",
    "## How to reproduce",
    "## Leases",
    "## Fencing",
    "## Exactly-once completion",
    "## Signal ordering",
    "## Bugs these tests found",
    "## Limits of this report",
    "## Related",
];

/// The four guarantees that issue #2004 names.
const GUARANTEES: &[&str] = &[
    "## Leases",
    "## Fencing",
    "## Exactly-once completion",
    "## Signal ordering",
];

/// The parts of each guarantee section, in order.
const PARTS: &[&str] = &["### Claim", "### Tests", "### Results", "### Known limits"];

/// The header of each results table.
const RESULTS_HEADER: &str = "| Check | Result | Reproduce |";

/// A section of the report, and a limit that the section must state.
///
/// Each entry is a known limit from issue #2004 or from the pages that the
/// report cites. The needle is a fixed phrase or an issue reference.
const REQUIRED_LIMITS: &[(&str, &str)] = &[
    ("## Exactly-once completion", "#1871"),
    ("## Limits of this report", "#1818"),
];

/// A page section that lists limits, and how the report restates each one.
///
/// Each limit pairs a phrase from its source bullet with a phrase from the
/// report. A source bullet that is added, removed or reworded fails the
/// guard. So does a report that drops a restatement. Update the pairs here
/// in the same change as the report.
struct LimitSource {
    page: &'static str,
    heading: &'static str,
    link: &'static str,
    limits: &'static [(&'static str, &'static str)],
}

const LIMIT_SOURCES: &[LimitSource] = &[
    LimitSource {
        page: "docs/testing/formal-methods.md",
        heading: "## Not modelled yet",
        link: "testing/formal-methods.md#not-modelled-yet",
        limits: &[
            (
                "Model (d), shard-generation fencing and rebalance.",
                "No model covers shard-generation fencing or the rebalance cutover.",
            ),
            (
                "Trace conformance of `CodecRotation`.",
                "No check compares test traces with the `CodecRotation` model.",
            ),
        ],
    },
    LimitSource {
        page: "docs/testing/simulation.md",
        heading: "## Limits",
        link: "testing/simulation.md#limits",
        limits: &[
            (
                "drives the store statements, not the `worker.rs` loop.",
                "The oracle harness drives store statements, not the `worker.rs` loop.",
            ),
            (
                "A world crash falls between two steps.",
                "A world crash falls between two steps.",
            ),
            (
                "The world workload never fails an activity,",
                "its workload never fails an activity.",
            ),
            (
                "draws actions from fixed weights.",
                "The simulator draws actions from fixed weights.",
            ),
            (
                "does not model the timeout sweeper, the `FAILED` state or the poison-pill \
                 quarantine.",
                "does not model the timeout sweeper, the `FAILED` state or quarantine.",
            ),
        ],
    },
];

/// Pages that must link the report, so an evaluator can find it.
const LINKED_FROM: &[&str] = &["README.md", "docs/comparison.md", "docs/testing/chaos.md"];

/// The longest prose sentence the report may hold, per ASD-STE100.
const MAX_SENTENCE_WORDS: usize = 25;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a repo file with CRLF folded to LF, so a Windows checkout matches.
fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    read_lf(&path)
        .unwrap_or_else(|err| panic!("issue #2004: cannot read {}: {err}", path.display()))
}

/// Read a file with CRLF folded to LF. A Windows checkout writes CRLF, and
/// the manifest and module parsers split on `\n`.
fn read_lf(path: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(path).map(|text| text.replace("\r\n", "\n"))
}

#[test]
fn report_has_every_section_in_order() {
    let report = read(REPORT);
    assert!(
        report.starts_with("# Harvest safety report\n"),
        "{REPORT} must open with its title"
    );
    let mut from = 0;
    for section in SECTIONS {
        let at = report[from..]
            .find(&format!("\n{section}\n"))
            .unwrap_or_else(|| panic!("{REPORT} lacks `{section}`, or it is out of order"));
        from += at + section.len();
    }
}

#[test]
fn each_guarantee_has_a_claim_tests_results_and_limits() {
    let report = read(REPORT);
    for guarantee in GUARANTEES {
        let body = section(&report, &format!("\n{guarantee}\n"), "\n## ")
            .unwrap_or_else(|| panic!("{REPORT} lacks `{guarantee}`"));
        let mut from = 0;
        for part in PARTS {
            let at = body[from..]
                .find(&format!("\n{part}\n"))
                .unwrap_or_else(|| panic!("`{guarantee}` lacks `{part}`, or it is out of order"));
            from += at + part.len();
        }
        for part in PARTS {
            let text = section(body, &format!("\n{part}\n"), "\n### ").unwrap_or_default();
            assert!(
                text.split_whitespace().count() >= 5,
                "`{part}` under `{guarantee}` is empty"
            );
        }
        let limits = section(body, "\n### Known limits\n", "\n### ").unwrap_or_default();
        assert!(
            limits.lines().any(|line| line.starts_with("- ")),
            "`### Known limits` under `{guarantee}` must list each limit as a bullet"
        );
    }
}

/// Every result row carries exactly one command, in one code span.
#[test]
fn every_result_has_one_command() {
    let report = read(REPORT);
    for guarantee in GUARANTEES {
        let body = section(&report, &format!("\n{guarantee}\n"), "\n## ").unwrap_or_default();
        let results = section(body, "\n### Results\n", "\n### ").unwrap_or_default();
        let rows = table_rows(results, RESULTS_HEADER);
        assert!(
            !rows.is_empty(),
            "`### Results` under `{guarantee}` must hold a `{RESULTS_HEADER}` table"
        );
        for row in rows {
            let cells = cells(row);
            assert_eq!(cells.len(), 3, "a results row needs three cells: {row}");
            assert!(
                cells[1].split_whitespace().count() >= 2,
                "a results row must state its result: {row}"
            );
            let spans = code_spans(&cells[2]);
            assert_eq!(
                spans.len(),
                1,
                "the Reproduce cell must hold exactly one command in one code span: {row}"
            );
        }
    }
}

/// Each command names a real target, test, script or TLA+ config.
#[test]
fn every_command_names_something_real() {
    let report = read(REPORT);
    let models = read(MODELS);
    let mut problems = Vec::new();
    for command in report_commands(&report) {
        for problem in command_problems(&command, &models, &repo_root()) {
            problems.push(format!("`{command}`: {problem}"));
        }
    }
    assert!(
        problems.is_empty(),
        "{REPORT} has commands that would not reproduce their result:\n{}",
        problems.join("\n")
    );
}

/// A TLC row states the verdict that `models.txt` expects.
#[test]
fn tla_results_match_the_manifest() {
    let report = read(REPORT);
    let models = read(MODELS);
    let mut seen = 0;
    for row in result_rows(&report) {
        let cells = cells(row);
        let (Some(result), Some(reproduce)) = (cells.get(1), cells.get(2)) else {
            continue;
        };
        let Some(command) = code_spans(reproduce).into_iter().next() else {
            continue;
        };
        let Some((spec, config)) = tlc_target(&command) else {
            continue;
        };
        seen += 1;
        let expect = manifest_expect(&models, &spec, &config)
            .unwrap_or_else(|| panic!("{MODELS} has no row for {spec} {config}"));
        match expect.strip_prefix("violation:") {
            None => assert!(
                result.contains("No error"),
                "{config} must pass, so its result must say `No error`: {row}"
            ),
            Some(invariant) => assert!(
                result.contains(&format!("`{invariant}`")) && result.contains("violat"),
                "{config} must violate `{invariant}`, so its result must say so: {row}"
            ),
        }
    }
    assert!(seen > 0, "{REPORT} must cite at least one TLC result");
}

/// The report cites every bug that the chaos page records.
#[test]
fn every_known_bug_is_listed() {
    let report = read(REPORT);
    let bugs = section(&report, "\n## Bugs these tests found\n", "\n## ").unwrap_or_default();
    let cited = issue_refs(bugs);
    let recorded = chaos_known_bugs(&read(CHAOS));
    assert!(
        !recorded.is_empty(),
        "{CHAOS} must keep its `**Known bugs.**` list"
    );
    let missing: Vec<u32> = recorded.difference(&cited).copied().collect();
    assert!(
        missing.is_empty(),
        "`## Bugs these tests found` must cite each bug in {CHAOS}. Missing: {missing:?}"
    );
}

#[test]
fn every_required_limit_is_stated() {
    let report = read(REPORT);
    for (heading, needle) in REQUIRED_LIMITS {
        let body = section(&report, &format!("\n{heading}\n"), "\n## ")
            .unwrap_or_else(|| panic!("{REPORT} lacks `{heading}`"));
        let limits = section(body, "\n### Known limits\n", "\n### ").unwrap_or(body);
        assert!(
            limits.contains(needle),
            "`{heading}` must state the known limit `{needle}`"
        );
    }
}

/// The report cannot drop a limit that a source page records.
#[test]
fn every_source_limit_is_restated() {
    let report = flat(&read(REPORT));
    for source in LIMIT_SOURCES {
        let (page, heading) = (source.page, source.heading);
        assert!(
            report.contains(source.link),
            "{REPORT} must link the limits in `{page}` as `{}`",
            source.link
        );
        let text = read(page);
        let body = section(&text, &format!("\n{heading}\n"), "\n## ")
            .unwrap_or_else(|| panic!("{page} lacks `{heading}`"));
        let bullets = body
            .lines()
            .filter(|line| {
                ["- ", "* "]
                    .iter()
                    .any(|b| line.trim_start().starts_with(b))
            })
            .count();
        assert_eq!(
            bullets,
            source.limits.len(),
            "`{heading}` in {page} now lists {bullets} limits. Restate each new limit in \
             {REPORT}, then add it to LIMIT_SOURCES"
        );
        let body = flat(body);
        for (in_source, in_report) in source.limits {
            assert!(
                body.contains(in_source),
                "`{heading}` in {page} no longer says `{in_source}`. Check that {REPORT} \
                 still states the limit, then update LIMIT_SOURCES"
            );
            assert!(
                report.contains(in_report),
                "{REPORT} must restate the limit `{in_source}` from {page} as `{in_report}`"
            );
        }
    }
}

/// Squash text to one line, so a phrase can wrap.
fn flat(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn report_is_linked_from_the_corpus() {
    for page in LINKED_FROM {
        let text = read(page);
        assert!(text.contains(REPORT_LINK), "{page} must link {REPORT_LINK}");
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
             \x20     - name: Run docs/safety-report.md guards (also on docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn stanza_checks_catch_each_bypass() {
    let good = "\n      - name: Guard\n        run: \"cargo test --test integration \
                safety_report_docs::\"\n";
    let block = format!("\n      - name: Before\n        run: echo a\n{good}\n  test:\n");
    let stanza = step_stanza(&block, FILTER).expect("the stanza is in the block");
    assert!(stanza.starts_with("\n      - name: Guard\n"), "{stanza}");
    assert_eq!(check_stanza(stanza, FILTER), Ok(()));
    for bad in [
        format!("{good}        if: always()\n"),
        format!("{good}        continue-on-error: true\n"),
        good.replace("docs::\"", "docs::one_test\""),
    ] {
        let stanza = step_stanza(&bad, FILTER).expect("the stanza is in the block");
        assert!(check_stanza(stanza, FILTER).is_err(), "{stanza}");
    }
}

/// The command checker rejects each way a command can fail to reproduce.
#[test]
fn command_checks_catch_each_bad_command() {
    let root = repo_root();
    let models = read(MODELS);
    let ok = [
        "cargo test -p autumn-harvest --test integration safety_report_docs::",
        "cargo test -p autumn-harvest --test integration \
         safety_report_docs::command_checks_catch_each_bad_command -- --exact",
        "PROPTEST_CASES=2000 cargo test -p autumn-harvest --no-default-features --test dst",
        "cargo test -p autumn-harvest --lib merge_wake_events_signal",
        "cargo test --package=autumn-harvest -F chaos --test=integration \
         chaos_tests::oracle_flags_a_duplicate_terminal_event -- --nocapture",
        "cd formal/tla && java -cp \"$TLA2TOOLS_JAR\" tlc2.TLC -metadir /tmp/tlc \
         -config ActivityClaim.cfg ActivityClaim.tla",
        "scripts/check-formal-models.sh",
    ];
    for command in ok {
        let problems = command_problems(command, &models, &root);
        assert!(problems.is_empty(), "`{command}`: {problems:?}");
    }
    let bad = [
        // A module, target, crate or test that does not exist.
        "cargo test -p autumn-harvest --test integration no_such_module::",
        "cargo test -p autumn-harvest --test no_such_target",
        "cargo test -p no-such-crate --test integration",
        "cargo test -p autumn-harvest --test integration safety_report_docs::no_such_test",
        "cargo test -p autumn-harvest --lib no_such_unit_test",
        // No target, an unknown flag, or a test in another target.
        "cargo test -p autumn-harvest safety_report_docs::",
        "cargo test -p autumn-harvest --frobnicate --test integration",
        "cargo test -p autumn-harvest --no-default-features --test dst safety_report_docs::",
        // A feature gate compiles the test out, so cargo runs nothing.
        "cargo test -p autumn-harvest --test integration chaos_tests::oracle_flags",
        "cargo test -p autumn-harvest --no-default-features --test integration signal_tests::",
        // `--exact` needs the whole path, and an ignored test does not run.
        "cargo test -p autumn-harvest --test integration \
         safety_report_docs::command_checks -- --exact",
        "cargo test -p autumn-harvest --test integration audit_log_unexported_idx_write_cost_perf::",
        "cd formal/tla && java -cp x tlc2.TLC -config NoSuch.cfg ActivityClaim.tla",
        "cd formal/tla && java -cp x tlc2.TLC -config ActivityClaim.cfg NoSuch.tla",
        "scripts/no-such-script.sh",
        "make test",
    ];
    for command in bad {
        assert!(
            !command_problems(command, &models, &root).is_empty(),
            "`{command}` must be rejected"
        );
    }
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(
        code_spans("run `a b` then `c`"),
        ["a b".to_string(), "c".to_string()]
    );
    assert_eq!(
        cells("| a | `b \\| c` | d |"),
        ["a".to_string(), "`b | c`".to_string(), "d".to_string()]
    );
    let table = "\n| Check | Result | Reproduce |\n|---|---|---|\n| x | y z | `w` |\n\nAfter.\n";
    assert_eq!(table_rows(table, RESULTS_HEADER), ["| x | y z | `w` |"]);
    let chaos = "**Known bugs.** The tests found three bugs:\n\n- #1871: one.\n  more #9.\n\
                 - #1870: two.\n\n- [#1876](x): three.\nEach test works.\n- #5: not a bug.\n";
    assert_eq!(chaos_known_bugs(chaos), BTreeSet::from([1870, 1871, 1876]));
    assert_eq!(
        manifest_expect("# c\nA  A.cfg  pass\nA  B.cfg  violation:X\n", "A", "B.cfg"),
        Some("violation:X".to_string())
    );
    assert_eq!(
        tlc_target("cd formal/tla && java -cp j tlc2.TLC -config B.cfg A.tla"),
        Some(("A".to_string(), "B.cfg".to_string()))
    );
    assert_eq!(
        issue_refs("(#603) [x](https://github.com/o/r/issues/1817#top) [y](c.md#5-five)"),
        BTreeSet::from([603, 1817])
    );
    let features = BTreeSet::from(["db".to_string()]);
    for (cfg, holds) in [
        ("feature = \"db\")]", true),
        ("feature = \"chaos\")]", false),
        ("all(feature = \"testing\", feature = \"db\"))]", false),
        ("any(feature = \"testing\", feature = \"db\"))]", true),
        ("not(feature = \"db\"))]", false),
        ("unix)]", true),
        ("kani)]", false),
        ("all(test, not(windows)))]", true),
    ] {
        assert_eq!(cfg_holds(cfg, &features), holds, "cfg({cfg}");
    }
    let text = "# T\n\nOne two. Three [four](x.md) five.\n\n```bash\nlet a = b. c;\n```\n\
                \n| a | b |\n|---|---|\n| Six seven. | `x.y()` |\n";
    assert_eq!(
        prose_sentences(text),
        [
            "One two.",
            "Three four five.",
            "a",
            "b",
            "Six seven.",
            "x_y__"
        ]
    );
}

/// The data rows of the table under `header`, trimmed.
fn table_rows<'a>(text: &'a str, header: &str) -> Vec<&'a str> {
    text.lines()
        .skip_while(|line| line.trim() != header)
        .skip(2)
        .take_while(|line| line.trim_start().starts_with('|'))
        .map(str::trim)
        .collect()
}

/// The results rows of every guarantee section.
fn result_rows(report: &str) -> Vec<&str> {
    GUARANTEES
        .iter()
        .filter_map(|guarantee| section(report, &format!("\n{guarantee}\n"), "\n## "))
        .filter_map(|body| section(body, "\n### Results\n", "\n### "))
        .flat_map(|results| table_rows(results, RESULTS_HEADER))
        .collect()
}

/// The command in each results row.
fn report_commands(report: &str) -> Vec<String> {
    result_rows(report)
        .into_iter()
        .filter_map(|row| cells(row).get(2).map(|cell| code_spans(cell)))
        .flatten()
        .collect()
}

/// The cells of a table row. An escaped `\|` stays inside its cell.
fn cells(row: &str) -> Vec<String> {
    let masked = row.trim().replace("\\|", "\u{0}");
    masked
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().replace('\u{0}', "|"))
        .collect()
}

/// The text of each inline code span.
fn code_spans(text: &str) -> Vec<String> {
    text.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// Every problem that stops `command` from running what the report claims.
fn command_problems(command: &str, models: &str, root: &Path) -> Vec<String> {
    let words: Vec<&str> = command
        .split_whitespace()
        .skip_while(|word| is_env_assignment(word))
        .collect();
    match words.as_slice() {
        ["cargo", "test", rest @ ..] => cargo_test_problems(rest, root),
        ["cd", "formal/tla", "&&", "java", ..] => match tlc_target(command) {
            None => vec!["a TLC command needs `-config <file>.cfg <Spec>.tla`".into()],
            Some((spec, config)) => tlc_problems(&spec, &config, models, root),
        },
        [script, ..] if script.starts_with("scripts/") => script_problems(&root.join(script)),
        _ => vec!["the guard knows no such command form".into()],
    }
}

/// A script must exist, and on Unix it must be executable.
fn script_problems(path: &Path) -> Vec<String> {
    let Ok(meta) = std::fs::metadata(path) else {
        return vec![format!("{} does not exist", path.display())];
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 == 0 {
            return vec![format!("{} is not executable", path.display())];
        }
    }
    #[cfg(not(unix))]
    let _ = meta;
    Vec::new()
}

/// `NAME=value` before a command.
fn is_env_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(name, _)| {
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    })
}

/// A test target of a crate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Target {
    Lib,
    Test(String),
}

/// The parts of a `cargo test` command that decide which tests run.
#[derive(Debug, Default)]
struct CargoTest {
    package: String,
    target: Option<Target>,
    features: Vec<String>,
    no_default_features: bool,
    filters: Vec<String>,
    exact: bool,
    ignored: bool,
}

/// A test function, with its path as the test harness prints it.
#[derive(Clone, Debug)]
struct TestFn {
    path: String,
    ignored: bool,
}

/// Parse the arguments of `cargo test`.
///
/// An unknown flag is an error, so a new flag cannot hide a change in what the
/// command runs.
fn parse_cargo_test(args: &[&str]) -> Result<CargoTest, String> {
    let mut parsed = CargoTest {
        package: "autumn-harvest".into(),
        ..CargoTest::default()
    };
    let mut words = args.iter().copied();
    let mut harness = false;
    while let Some(word) = words.next() {
        let (flag, inline) = match word.split_once('=') {
            Some((flag, value)) if flag.starts_with('-') => (flag, Some(value)),
            _ => (word, None),
        };
        let mut value = || {
            inline
                .map(str::to_owned)
                .or_else(|| words.next().map(str::to_owned))
                .ok_or_else(|| format!("`{flag}` needs a value"))
        };
        match (harness, flag) {
            (false, "--") => harness = true,
            (false, "-p" | "--package") => parsed.package = value()?,
            (false, "--test") => parsed.target = Some(Target::Test(value()?)),
            (false, "--lib") => parsed.target = Some(Target::Lib),
            (false, "-F" | "--features") => parsed
                .features
                .extend(value()?.split(',').map(str::to_owned)),
            (false, "--no-default-features") => parsed.no_default_features = true,
            (true, "--exact") => parsed.exact = true,
            (true, "--ignored" | "--include-ignored") => parsed.ignored = true,
            (false, "--release") | (true, "--nocapture") => {}
            (true, "--test-threads" | "--skip") => {
                value()?;
            }
            (_, flag) if flag.starts_with('-') => {
                return Err(format!("the guard does not know the flag `{flag}`"));
            }
            (_, filter) => parsed.filters.push(filter.to_owned()),
        }
    }
    Ok(parsed)
}

/// Problems in the arguments of `cargo test`.
///
/// The guard lists the tests that the command compiles, with the features it
/// turns on. Each filter must then match a test that runs, as cargo matches
/// it: a substring of the test path, or the whole path with `--exact`.
fn cargo_test_problems(args: &[&str], root: &Path) -> Vec<String> {
    let command = match parse_cargo_test(args) {
        Ok(command) => command,
        Err(problem) => return vec![problem],
    };
    let crate_dir = root.join(&command.package);
    let Ok(manifest) = read_lf(&crate_dir.join("Cargo.toml")) else {
        return vec![format!("no crate `{}` in the workspace", command.package)];
    };
    let Some(target) = &command.target else {
        return vec!["name a test target with `--test` or `--lib`".into()];
    };
    let Some(root_file) = target_root(&crate_dir, &manifest, target) else {
        return vec![format!(
            "`{}` has no test target {target:?}",
            command.package
        )];
    };
    let features = enabled_features(&manifest, &command);
    let tests = cached_tests(&root_file, &features);
    let runs = |test: &&TestFn| command.ignored || !test.ignored;
    let mut problems = Vec::new();
    if command.filters.is_empty() && !tests.iter().any(|test| runs(&test)) {
        problems.push(format!("{target:?} runs no test with these features"));
    }
    for filter in &command.filters {
        let hit = tests.iter().filter(runs).any(|test| {
            if command.exact {
                test.path == *filter
            } else {
                test.path.contains(filter.as_str())
            }
        });
        if !hit {
            problems.push(format!(
                "`{filter}` matches no test that {target:?} runs with features {features:?}"
            ));
        }
    }
    problems
}

/// The root source file of a test target.
fn target_root(crate_dir: &Path, manifest: &str, target: &Target) -> Option<PathBuf> {
    let path = match target {
        Target::Lib => crate_dir.join("src/lib.rs"),
        Target::Test(name) => manifest
            .split("[[test]]")
            .skip(1)
            .find_map(|block| {
                let block = block.split("\n[").next().unwrap_or_default();
                let named = block.contains(&format!("name = \"{name}\""));
                let path = block
                    .lines()
                    .find_map(|line| line.trim().strip_prefix("path = \""))?
                    .trim_end_matches('"');
                named.then(|| crate_dir.join(path))
            })
            .unwrap_or_else(|| crate_dir.join(format!("tests/{name}.rs"))),
    };
    path.is_file().then_some(path)
}

/// The features a command turns on, with each feature they imply.
fn enabled_features(manifest: &str, command: &CargoTest) -> BTreeSet<String> {
    let table = manifest
        .split("\n[features]\n")
        .nth(1)
        .and_then(|rest| rest.split("\n[").next())
        .unwrap_or_default();
    let implies = |name: &str| -> Vec<String> {
        let Some(at) = table
            .find(&format!("\n{name} = ["))
            .or_else(|| table.starts_with(&format!("{name} = [")).then_some(0))
        else {
            return Vec::new();
        };
        let list = &table[at..];
        let list = &list[list.find('[').unwrap_or(0)..=list.find(']').unwrap_or(0)];
        list.split('"')
            .skip(1)
            .step_by(2)
            .filter(|item| !item.contains(':') && !item.contains('/'))
            .map(str::to_owned)
            .collect()
    };
    let mut pending = command.features.clone();
    if !command.no_default_features {
        pending.extend(implies("default"));
    }
    let mut enabled = BTreeSet::new();
    while let Some(feature) = pending.pop() {
        if enabled.insert(feature.clone()) {
            pending.extend(implies(&feature));
        }
    }
    enabled
}

/// `collect_tests` for one root file and feature set, read once per run.
fn cached_tests(root_file: &Path, features: &BTreeSet<String>) -> Vec<TestFn> {
    type Cache = HashMap<(PathBuf, BTreeSet<String>), Vec<TestFn>>;
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cache
        .entry((root_file.to_path_buf(), features.clone()))
        .or_insert_with(|| {
            let mut tests = Vec::new();
            collect_tests(root_file, "", true, features, &mut tests);
            tests
        })
        .clone()
}

/// Every test function in the module tree under `file`.
///
/// The walk follows each `mod name;` and inline `mod name {` whose `cfg`
/// holds. It reads rustfmt layout: an inline module ends at a `}` with the
/// indent of its `mod` line. A test is a `fn` under `#[test]` or
/// `#[tokio::test]`.
fn collect_tests(
    file: &Path,
    prefix: &str,
    mod_root: bool,
    features: &BTreeSet<String>,
    tests: &mut Vec<TestFn>,
) {
    let Ok(source) = read_lf(file) else {
        return;
    };
    let inner_cfg_off = source
        .lines()
        .filter(|line| line.starts_with("#![cfg("))
        .any(|line| !cfg_holds(&line["#![cfg(".len()..], features));
    if inner_cfg_off {
        return;
    }
    let dir = if mod_root {
        file.parent().map(Path::to_path_buf).unwrap_or_default()
    } else {
        file.with_extension("")
    };
    let mut attrs: Vec<String> = Vec::new();
    let mut open = String::new();
    let mut inline: Vec<(String, usize, bool)> = Vec::new();
    for raw in source.lines() {
        let line = raw.trim();
        let indent = raw.len() - raw.trim_start().len();
        if inline
            .last()
            .is_some_and(|(_, at, _)| *at == indent && line == "}")
        {
            inline.pop();
            continue;
        }
        if !open.is_empty() || line.starts_with("#[") {
            open.push_str(line);
            if open.matches('[').count() <= open.matches(']').count() {
                attrs.push(std::mem::take(&mut open));
            }
            continue;
        }
        if line.starts_with("//") || line.is_empty() {
            continue;
        }
        let enabled = inline.iter().all(|(_, _, on)| *on)
            && attrs
                .iter()
                .filter_map(|attr| attr.strip_prefix("#[cfg("))
                .all(|cfg| cfg_holds(cfg, features));
        let path = |name: &str| {
            let mut parts: Vec<&str> = Vec::new();
            if !prefix.is_empty() {
                parts.push(prefix);
            }
            parts.extend(inline.iter().map(|(name, _, _)| name.as_str()));
            parts.push(name);
            parts.join("::")
        };
        let item = ["pub(crate) ", "pub(super) ", "pub "]
            .iter()
            .find_map(|vis| line.strip_prefix(vis))
            .unwrap_or(line);
        if let Some(rest) = item.strip_prefix("mod ") {
            let name = ident(rest);
            if rest[name.len()..].starts_with(';') {
                if enabled {
                    let child = child_module(&dir, &name, &attrs);
                    let child_root = child.file_name().is_some_and(|f| f == "mod.rs");
                    collect_tests(&child, &path(&name), child_root, features, tests);
                }
            } else if line.ends_with('{') {
                inline.push((name, indent, enabled));
            }
        } else if let Some(rest) = item
            .strip_prefix("async fn ")
            .or_else(|| item.strip_prefix("fn "))
        {
            let is_test = attrs
                .iter()
                .any(|attr| attr == "#[test]" || attr.starts_with("#[tokio::test"));
            if is_test && enabled {
                tests.push(TestFn {
                    path: path(&ident(rest)),
                    ignored: attrs.iter().any(|attr| attr.starts_with("#[ignore")),
                });
            }
        }
        attrs.clear();
    }
}

/// The identifier at the start of `text`.
fn ident(text: &str) -> String {
    text.chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// The file of `mod name;` in `dir`, or the file that `#[path]` names.
fn child_module(dir: &Path, name: &str, attrs: &[String]) -> PathBuf {
    attrs
        .iter()
        .find_map(|attr| attr.strip_prefix("#[path = \""))
        .map_or_else(
            || {
                let flat = dir.join(format!("{name}.rs"));
                if flat.is_file() {
                    flat
                } else {
                    dir.join(name).join("mod.rs")
                }
            },
            |rel| dir.join(rel.trim_end_matches("\"]")),
        )
}

/// Whether a `cfg` predicate holds for a test build on Linux.
///
/// `text` starts after `cfg(`. A cfg flag that the guard does not know, such
/// as `kani` or `loom`, is off.
fn cfg_holds(text: &str, features: &BTreeSet<String>) -> bool {
    let tokens = cfg_tokens(text.trim_end().trim_end_matches(']'));
    cfg_expr(&tokens, &mut 0, features)
}

/// Split a `cfg` predicate into names, quoted values and marks.
fn cfg_tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if matches!(c, '(' | ')' | ',' | '=') {
            tokens.push(c.to_string());
        } else if c == '"' {
            let value: String = chars.by_ref().take_while(|&c| c != '"').collect();
            tokens.push(format!("\"{value}"));
        } else if c.is_alphanumeric() || c == '_' {
            let mut name = c.to_string();
            while let Some(&next) = chars.peek().filter(|n| n.is_alphanumeric() || **n == '_') {
                name.push(next);
                chars.next();
            }
            tokens.push(name);
        }
    }
    tokens
}

/// Evaluate one `cfg` expression from `tokens[*at]` and move `at` past it.
fn cfg_expr(tokens: &[String], at: &mut usize, features: &BTreeSet<String>) -> bool {
    let token = |i: usize| tokens.get(i).map_or("", String::as_str);
    let head = token(*at).to_owned();
    *at += 1;
    if matches!(head.as_str(), "all" | "any" | "not") && token(*at) == "(" {
        *at += 1;
        let mut values = Vec::new();
        while !matches!(token(*at), ")" | "") {
            values.push(cfg_expr(tokens, at, features));
            if token(*at) == "," {
                *at += 1;
            }
        }
        *at += 1;
        return match head.as_str() {
            "all" => values.iter().all(|value| *value),
            "any" => values.iter().any(|value| *value),
            _ => !values.first().copied().unwrap_or(true),
        };
    }
    if token(*at) == "=" {
        let value = token(*at + 1).trim_start_matches('"').to_owned();
        *at += 2;
        return match head.as_str() {
            "feature" => features.contains(&value),
            "target_os" => value == "linux",
            "target_family" => value == "unix",
            _ => false,
        };
    }
    matches!(head.as_str(), "test" | "unix" | "debug_assertions")
}

/// The spec and config of a TLC command, from `-config X.cfg` and `Y.tla`.
fn tlc_target(command: &str) -> Option<(String, String)> {
    let words: Vec<&str> = command.split_whitespace().collect();
    let config = words
        .windows(2)
        .find(|pair| pair[0] == "-config")
        .map(|pair| pair[1].to_owned())?;
    let spec = words
        .iter()
        .find_map(|word| word.strip_suffix(".tla"))?
        .to_owned();
    Some((spec, config))
}

/// Problems with a TLC spec and config.
fn tlc_problems(spec: &str, config: &str, models: &str, root: &Path) -> Vec<String> {
    let mut problems = Vec::new();
    for file in [format!("{spec}.tla"), config.to_owned()] {
        if !root.join("formal/tla").join(&file).is_file() {
            problems.push(format!("formal/tla/{file} does not exist"));
        }
    }
    if manifest_expect(models, spec, config).is_none() {
        problems.push(format!("{MODELS} has no row for {spec} {config}"));
    }
    problems
}

/// The `expect` column of the `models.txt` row for `spec` and `config`.
fn manifest_expect(models: &str, spec: &str, config: &str) -> Option<String> {
    models.lines().find_map(|line| {
        let columns: Vec<&str> = line.split_whitespace().collect();
        match columns.as_slice() {
            [s, c, expect] if *s == spec && *c == config => Some((*expect).to_owned()),
            _ => None,
        }
    })
}

/// The issue numbers that lead the `**Known bugs.**` list in the chaos page.
///
/// The list starts at the first item after the lead-in. It ends at the first
/// line that is not an item, an indented continuation or a blank line. Each
/// item opens with `- #NNN` or `- [#NNN]`.
fn chaos_known_bugs(chaos: &str) -> BTreeSet<u32> {
    let Some(start) = chaos.find("**Known bugs.**") else {
        return BTreeSet::new();
    };
    chaos[start..]
        .lines()
        .skip(1)
        .skip_while(|line| line.trim().is_empty())
        .take_while(|line| line.is_empty() || line.starts_with("- ") || line.starts_with("  "))
        .filter_map(|line| {
            let rest = line.strip_prefix("- ")?.trim_start_matches('[');
            let digits: String = rest
                .strip_prefix('#')?
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse().ok()
        })
        .collect()
}
