//! Guards the doctest harness of the two agent guides (issue #2482).
//!
//! The harness is `guide_snippets` in `src/lib.rs`. It compiles each Rust
//! fence of `docs/agent-adapter.md` and `docs/agent-mesh.md`. These guards
//! fail when a fence escapes the harness, when the harness goes away, or
//! when the CI step stops running it.

use std::path::{Path, PathBuf};

/// The guides that the harness compiles, with the item that includes each.
const GUIDES: [(&str, &str); 2] = [
    ("docs/agent-adapter.md", "struct AgentAdapterGuide;"),
    ("docs/agent-mesh.md", "struct AgentMeshGuide;"),
];

/// The doctest filter of the CI step.
const FILTER: &str = "--doc guide_snippets";

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

/// The info string of each opening fence in `text`, with its line number.
///
/// A fence can be indented, for example inside a list. Rustdoc compiles an
/// indented Rust fence too, so the scan trims the indent.
fn opening_fences(text: &str) -> Vec<(usize, String)> {
    let mut fences = Vec::new();
    let mut open = false;
    for (index, line) in text.lines().enumerate() {
        let Some(info) = line.trim_start().strip_prefix("```") else {
            continue;
        };
        if !open {
            fences.push((index + 1, info.trim().to_owned()));
        }
        open = !open;
    }
    assert!(!open, "a fence is not closed");
    fences
}

/// Why rustdoc would not compile and run `info` as a reader copies it.
///
/// Returns `None` for a fence that rustdoc compiles, and for a fence in
/// another language. An empty info string is Rust to rustdoc, but not to a
/// reader, so it is refused.
fn skipped_reason(info: &str) -> Option<String> {
    let mut words = info.split([',', ' ']).filter(|word| !word.is_empty());
    let Some(language) = words.next() else {
        return Some("a fence with no language is Rust to rustdoc; write `rust`".into());
    };
    if language != "rust" {
        return None;
    }
    words
        .find(|word| *word != "no_run")
        .map(|word| format!("rustdoc does not compile a fence marked `{word}` as written"))
}

/// Each Rust fence of each guide must compile as written.
///
/// A fence marked `ignore` or `text` drifts from the API with no failure.
/// That drift is the defect of issue #2482.
#[test]
fn every_rust_fence_in_the_guides_compiles() {
    let mut defects = Vec::new();
    let mut rust_fences = 0;
    for (guide, _) in GUIDES {
        for (line, info) in opening_fences(&read_normalized(guide)) {
            if info.starts_with("rust") {
                rust_fences += 1;
            }
            if let Some(reason) = skipped_reason(&info) {
                defects.push(format!("{guide}:{line}: {reason}"));
            }
        }
    }
    assert!(rust_fences > 0, "the guides must hold Rust fences");
    assert!(defects.is_empty(), "{}", defects.join("\n"));
}

/// The harness must include each guide, or the CI filter matches no test.
///
/// `cargo test` exits 0 when a filter matches nothing. A deleted harness
/// would therefore pass CI.
#[test]
fn the_harness_includes_both_guides() {
    let source = read_normalized("autumn-harvest-agent/src/lib.rs");
    let module = source
        .find("mod guide_snippets {")
        .expect("lib.rs must hold `mod guide_snippets`");
    let gate_at = source[..module]
        .rfind("#[cfg(all(")
        .expect("the harness must have a `cfg(all(...))` gate");
    assert!(
        source[gate_at..module].contains("doctest"),
        "the harness must be gated on `cfg(doctest)`"
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

/// The features of the harness gate, in order of the source.
fn harness_features() -> Vec<String> {
    let source = read_normalized("autumn-harvest-agent/src/lib.rs");
    let module = source
        .find("mod guide_snippets {")
        .expect("lib.rs must hold `mod guide_snippets`");
    let gate_at = source[..module]
        .rfind("#[cfg(all(")
        .expect("the harness must have a `cfg(all(...))` gate");
    source[gate_at..module]
        .split("feature = \"")
        .skip(1)
        .filter_map(|tail| tail.split('"').next())
        .map(str::to_owned)
        .collect()
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
    let next_job = workflow[job_at + 1..]
        .find("\n  lint-compile:")
        .map_or(workflow.len(), |i| job_at + 1 + i);
    assert!(
        step_at > job_at && step_at < next_job,
        "the doctest step must live in the `doc-snippets` job"
    );
    let job = &workflow[job_at..next_job];
    assert!(
        !job.contains("needs.changes"),
        "the `doc-snippets` job must not depend on the `changes` filter"
    );

    let step_name = workflow[..step_at]
        .rfind("\n      - name:")
        .expect("the doctest step must have a name");
    assert!(
        !workflow[step_name..step_at].contains("\n        if:"),
        "the doctest step must not have an `if:` condition"
    );

    let features = harness_features();
    assert!(
        !features.is_empty(),
        "the harness gate must name its features"
    );
    for feature in features {
        assert!(
            run_line.contains(&feature),
            "the CI step must enable the `{feature}` feature of the harness gate"
        );
    }
}

/// The fence scan refuses each fence that rustdoc skips.
#[test]
fn the_fence_scan_refuses_each_skipped_fence() {
    for info in ["rust", "rust,no_run", "json", "sh", "text", "mermaid"] {
        assert_eq!(skipped_reason(info), None, "`{info}` must pass");
    }
    for info in ["", "rust,ignore", "rust,compile_fail", "rust no_run,ignore"] {
        assert!(skipped_reason(info).is_some(), "`{info}` must be refused");
    }
    let text = "a\n```rust\nlet x = 1;\n```\n  ```rust,ignore\n  y\n  ```\n";
    assert_eq!(
        opening_fences(text),
        vec![(2, "rust".to_owned()), (5, "rust,ignore".to_owned())]
    );
}
