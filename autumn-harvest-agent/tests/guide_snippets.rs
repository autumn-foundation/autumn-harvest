//! Guards the doctest harness of the two agent guides (issue #2482).
//!
//! The harness is `guide_snippets` in `src/lib.rs`. It compiles each Rust
//! fence of `docs/agent-adapter.md` and `docs/agent-mesh.md`. These guards
//! fail when a fence escapes the harness, when the harness goes away, or
//! when the CI step stops running it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The guides that the harness compiles, with the item that includes each.
const GUIDES: [(&str, &str); 2] = [
    ("docs/agent-adapter.md", "struct AgentAdapterGuide;"),
    ("docs/agent-mesh.md", "struct AgentMeshGuide;"),
];

/// The doctest filter of the CI step. It is the name of the harness module.
const FILTER: &str = "--doc guide_snippets";

/// The words that make rustdoc read a fence as Rust, in lower case.
///
/// Rustdoc reads a fence as Rust when its first word is one of these. A
/// word that starts with `ignore` or `edition` counts too.
const RUST_WORDS: [&str; 7] = [
    "rust",
    "rs",
    "ignore",
    "no_run",
    "should_panic",
    "compile_fail",
    "test_harness",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read_normalized(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// `line` with its indent and its block-quote markers removed.
///
/// Rustdoc compiles a fence in a list or in a block quote too.
fn fence_text(line: &str) -> &str {
    let mut rest = line.trim_start();
    while let Some(tail) = rest.strip_prefix('>') {
        rest = tail.trim_start();
    }
    rest
}

/// The info string of each opening fence in `text`, with its line number.
///
/// A fence opens with 3 or more backticks or tildes. Only a bare run of the
/// same character, at least as long, closes it.
fn opening_fences(text: &str) -> Vec<(usize, String)> {
    let mut fences = Vec::new();
    let mut open: Option<(char, usize)> = None;
    for (index, line) in text.lines().enumerate() {
        let rest = fence_text(line);
        let Some(marker) = rest.chars().next().filter(|c| *c == '`' || *c == '~') else {
            continue;
        };
        let run = rest.chars().take_while(|c| *c == marker).count();
        if run < 3 {
            continue;
        }
        // The marker is ASCII, so `run` is also a byte offset.
        let info = rest[run..].trim();
        match open {
            None => {
                fences.push((index + 1, info.to_owned()));
                open = Some((marker, run));
            }
            Some((open_marker, open_run))
                if marker == open_marker && run >= open_run && info.is_empty() =>
            {
                open = None;
            }
            Some(_) => {}
        }
    }
    assert!(open.is_none(), "a fence is not closed");
    fences
}

/// The words of an info string.
fn info_words(info: &str) -> Vec<&str> {
    info.split([',', ' ', '\t'])
        .filter(|word| !word.is_empty())
        .collect()
}

/// True when rustdoc, or a reader, takes `word` for a Rust marker.
fn is_rust_word(word: &str) -> bool {
    let word = word.to_ascii_lowercase();
    RUST_WORDS.contains(&word.as_str()) || word.starts_with("ignore") || word.starts_with("edition")
}

/// Why rustdoc would not compile `info` as a reader copies it.
///
/// Returns `None` for `rust`, for `rust,no_run` and for a fence in another
/// language. Each other fence that holds a Rust marker is refused. An empty
/// info string is Rust to rustdoc, but not to a reader, so it is refused.
fn skipped_reason(info: &str) -> Option<String> {
    let words = info_words(info);
    if words.is_empty() {
        return Some("a fence with no language is Rust to rustdoc; write `rust`".into());
    }
    if !words.iter().copied().any(is_rust_word) || words == ["rust"] || words == ["rust", "no_run"]
    {
        return None;
    }
    Some(format!(
        "write `rust` or `rust,no_run`; rustdoc skips or changes a fence marked `{info}`"
    ))
}

/// Each Rust fence of each guide must compile as written.
///
/// A fence marked `ignore` or `text` drifts from the API with no failure.
/// That drift is the defect of issue #2482.
#[test]
fn every_rust_fence_in_the_guides_compiles() {
    let mut defects = Vec::new();
    for (guide, _) in GUIDES {
        let fences = opening_fences(&read_normalized(guide));
        let compiled = fences
            .iter()
            .filter(|(_, info)| info_words(info).first() == Some(&"rust"))
            .count();
        if compiled == 0 {
            defects.push(format!("{guide}: the guide holds no Rust fence"));
        }
        for (line, info) in fences {
            if let Some(reason) = skipped_reason(&info) {
                defects.push(format!("{guide}:{line}: {reason}"));
            }
        }
    }
    assert!(defects.is_empty(), "{}", defects.join("\n"));
}

/// The byte offset of the harness module in `source`, and the text of its
/// gate, from `#[cfg(all(` to the module.
fn harness_gate(source: &str) -> (usize, &str) {
    let module = source
        .find("mod guide_snippets {")
        .expect("lib.rs must hold `mod guide_snippets`");
    let gate_at = source[..module]
        .rfind("#[cfg(all(")
        .expect("the harness must have a `cfg(all(...))` gate");
    (module, &source[gate_at..module])
}

/// The features of the harness gate.
fn harness_features(gate: &str) -> BTreeSet<String> {
    gate.split("feature = \"")
        .skip(1)
        .filter_map(|tail| tail.split('"').next())
        .map(str::to_owned)
        .collect()
}

/// The harness must include each guide, or the CI filter matches no test.
///
/// `cargo test` exits 0 when a filter matches nothing. A deleted harness, or
/// a gate that never holds in a doctest build, would therefore pass CI. So
/// the gate must be `doctest` and features only, with no other attribute.
#[test]
fn the_harness_includes_both_guides() {
    let source = read_normalized("autumn-harvest-agent/src/lib.rs");
    let (module, gate) = harness_gate(&source);
    let features = harness_features(gate);
    assert!(
        !features.is_empty(),
        "the harness gate must name its features"
    );
    let expected: String = std::iter::once("doctest".to_owned())
        .chain(features.iter().map(|f| format!("feature=\"{f}\"")))
        .collect::<Vec<_>>()
        .join(",");
    let compact: String = gate.chars().filter(|c| !c.is_whitespace()).collect();
    let mut found: Vec<&str> = compact
        .strip_prefix("#[cfg(all(")
        .and_then(|rest| rest.strip_suffix("))]"))
        .unwrap_or_default()
        .split(',')
        .collect();
    found.sort_unstable();
    let mut wanted: Vec<&str> = expected.split(',').collect();
    wanted.sort_unstable();
    assert_eq!(
        found, wanted,
        "the gate must be `cfg(all(doctest, feature = ...))` and nothing else, found: {gate}"
    );

    for (guide, item) in GUIDES {
        let include = format!("#[doc = include_str!(\"../../{guide}\")]");
        let at = source[module..]
            .find(&include)
            .unwrap_or_else(|| panic!("the harness must include {guide}"));
        let next = source[module + at..].lines().nth(1).unwrap_or_default();
        assert!(
            next.trim() == item,
            "the include of {guide} must annotate `{item}`, found: {next}"
        );
    }
}

/// The features that `run_line` enables with `--features`.
fn enabled_features(run_line: &str) -> BTreeSet<String> {
    run_line
        .split_once("--features ")
        .and_then(|(_, tail)| tail.split_whitespace().next())
        .unwrap_or_default()
        .trim_matches('"')
        .split(',')
        .filter(|feature| !feature.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The byte offset of the first job header in `workflow` after `from`.
///
/// A job header is a line of two spaces, a name and a colon. Returns the
/// length of `workflow` when no job follows.
fn next_job_header(workflow: &str, from: usize) -> usize {
    let mut at = from;
    for line in workflow[from..].split_inclusive('\n') {
        let name = line
            .trim_end()
            .strip_prefix("  ")
            .and_then(|l| l.strip_suffix(':'));
        if at > from
            && name.is_some_and(|name| {
                !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            })
        {
            // Point at the newline before the header, as `find` does above.
            return at - 1;
        }
        at += line.len();
    }
    workflow.len()
}

/// The CI step must run the harness in the ungated `doc-snippets` job.
///
/// A docs-only PR skips the `test` matrix. The step must also enable each
/// feature of the harness gate, or the gate removes the harness.
#[test]
fn ci_runs_the_guide_doctests_on_docs_only_changes() {
    let workflow = read_normalized(".github/workflows/ci.yml");
    let step_at = workflow
        .find(FILTER)
        .expect("ci.yml must run the agent guide doctests");
    let line_start = workflow[..step_at].rfind('\n').map_or(0, |i| i + 1);
    let line_end = workflow[step_at..]
        .find('\n')
        .map_or(workflow.len(), |i| step_at + i);
    let run_line = workflow[line_start..line_end].trim_start();
    assert!(
        run_line.starts_with("run:") && run_line.contains("-p autumn-harvest-agent"),
        "the doctest invocation must be a step `run:` line for the agent crate"
    );

    let job_at = workflow
        .find("\n  doc-snippets:")
        .expect("ci.yml must define a `doc-snippets` job");
    let next_job = next_job_header(&workflow, job_at + 1);
    assert!(
        step_at > job_at && step_at < next_job,
        "the doctest step must live in the `doc-snippets` job"
    );
    assert!(
        !workflow[job_at..next_job].contains("needs.changes"),
        "the `doc-snippets` job must not depend on the `changes` filter"
    );

    // A step key can come in any order, so scan the whole step.
    let step_start = workflow[..step_at]
        .rfind("\n      - ")
        .expect("the doctest step must be a list item");
    let step_end = workflow[step_at..next_job]
        .find("\n      - ")
        .map_or(next_job, |i| step_at + i);
    let step = &workflow[step_start..step_end];
    for key in ["\n        if:", "\n        continue-on-error:"] {
        assert!(
            !step.contains(key),
            "the doctest step must not set `{}`",
            key.trim()
        );
    }

    let source = read_normalized("autumn-harvest-agent/src/lib.rs");
    let (_, gate) = harness_gate(&source);
    let missing: Vec<String> = harness_features(gate)
        .difference(&enabled_features(run_line))
        .cloned()
        .collect();
    assert!(
        missing.is_empty(),
        "the CI step must enable each feature of the harness gate, missing: {missing:?}"
    );
}

/// The fence scan refuses each fence that rustdoc skips or changes.
#[test]
fn the_fence_scan_refuses_each_skipped_fence() {
    for info in [
        "rust",
        "rust,no_run",
        "rust no_run",
        "json",
        "sh",
        "text",
        "mermaid",
    ] {
        assert_eq!(skipped_reason(info), None, "`{info}` must pass");
    }
    for info in [
        "",
        "rust,ignore",
        "rust,compile_fail",
        "rust no_run,ignore",
        "ignore",
        "ignore,rust",
        "should_panic",
        "Rust",
        "rs",
        "rust,edition2021",
        "rust\tignore",
    ] {
        assert!(skipped_reason(info).is_some(), "`{info}` must be refused");
    }
    let text = "a\n```rust\nlet x = 1;\n```\n  ```rust,ignore\n  y\n  ```\n\
                ~~~ignore\nz\n~~~\n````rust,ignore\n```\n````\n> ```text\n> w\n> ```\n";
    assert_eq!(
        opening_fences(text),
        vec![
            (2, "rust".to_owned()),
            (5, "rust,ignore".to_owned()),
            (8, "ignore".to_owned()),
            (11, "rust,ignore".to_owned()),
            (14, "text".to_owned()),
        ]
    );
}

/// The gate and feature parsers read exact names, not substrings.
#[test]
fn the_gate_and_feature_parsers_read_exact_names() {
    let gate = "#[cfg(all(\n    doctest,\n    feature = \"sqlite\",\n    feature = \"eval\"\n))]\n";
    assert_eq!(
        harness_features(gate),
        BTreeSet::from(["eval".to_owned(), "sqlite".to_owned()])
    );
    let run = "run: \"cargo test -p x --features sqlite,eval --doc guide_snippets\"";
    assert_eq!(
        enabled_features(run),
        BTreeSet::from(["eval".to_owned(), "sqlite".to_owned()])
    );
    assert!(!enabled_features(run).contains("lite"));
}

/// The job scan ends a job at the next job header, whatever its name.
#[test]
fn the_job_scan_ends_at_the_next_job_header() {
    let workflow = "jobs:\n  doc-snippets:\n    steps:\n      - name: a\n  new_job:\n    x\n";
    let job_at = workflow.find("\n  doc-snippets:").expect("job");
    let end = next_job_header(workflow, job_at + 1);
    assert_eq!(&workflow[end..], "\n  new_job:\n    x\n");
    assert_eq!(
        next_job_header("  only:\n    y\n", 0),
        "  only:\n    y\n".len()
    );
}
