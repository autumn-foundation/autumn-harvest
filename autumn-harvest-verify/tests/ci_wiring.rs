//! The CI steps that make this crate's numbers mean something (issue #962).
//!
//! Three of the analyzer's four CI steps fail *quietly* rather than loudly:
//!
//! * `examples_metrics` returns `ok` when `HARVEST_VERIFY_EXAMPLES` is unset, so
//!   deleting the step that sets it leaves a green build that measures nothing;
//! * both gates are deliberately non-strict, so deleting either leaves a green
//!   build that verifies nothing;
//! * the clippy step for this crate is the only place `--all-targets` lint
//!   coverage exists for it — the workspace `lint` job does not use
//!   `--workspace`.
//!
//! So the steps are asserted here, the way `determinism_static_analysis_docs`
//! and `sqlite_feasibility_docs` assert theirs. This is a *wiring* guard: it
//! checks that the command is in the file and in the right job, not that it
//! passes. A rename is meant to fail it — read the failure as "update this
//! test too", not as "delete this test".

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-verify has a parent")
        .to_path_buf()
}

/// `.github/workflows/ci.yml` with CRLF normalized away.
fn workflow() -> String {
    let path = repo_root().join(".github/workflows/ci.yml");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The block of `ci.yml` belonging to the top-level job `name`, from its
/// `\n  <name>:` header to the next top-level job header (or the end).
///
/// Job blocks, not the whole file: "the command appears somewhere in ci.yml" is
/// not the property under test — a gate command sitting in a job that never
/// runs on pull requests would satisfy it and protect nothing.
fn job_block<'a>(workflow: &'a str, name: &str) -> &'a str {
    let header = format!("\n  {name}:\n");
    let start = workflow
        .find(&header)
        .unwrap_or_else(|| panic!("ci.yml must define a `{name}` job"));
    let rest = &workflow[start + 1..];
    // The next line that starts a top-level job: two spaces, an identifier, `:`.
    let end = rest
        .match_indices("\n  ")
        .find(|(at, _)| {
            let line = rest[at + 3..].lines().next().unwrap_or_default();
            !line.starts_with(' ')
                && !line.starts_with('#')
                && !line.starts_with('-')
                && line.ends_with(':')
                && line
                    .trim_end_matches(':')
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                && *at > 0
        })
        .map_or(rest.len(), |(at, _)| at);
    &rest[..end]
}

/// Assert that `job` contains every fragment of `needles`, naming what to do
/// about it when it does not.
fn assert_wired(job_name: &str, needles: &[&str], why: &str) {
    let workflow = workflow();
    let block = job_block(&workflow, job_name);
    // Line continuations are cosmetic; the command is one logical string.
    let flattened = block.replace("\\\n", " ");
    let flattened: String = flattened.split_whitespace().collect::<Vec<_>>().join(" ");
    for needle in needles {
        let wanted: String = needle.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            flattened.contains(&wanted),
            "the `{job_name}` job in .github/workflows/ci.yml no longer contains \
             `{wanted}`.\n{why}\nIf the step was deliberately renamed or \
             rewritten, update this test in the same commit — do not delete it.\n\
             --- job block ---\n{block}"
        );
    }
}

#[test]
fn examples_metric_step_is_wired_into_ci() {
    assert_wired(
        "harvest-verify",
        &[
            "HARVEST_VERIFY_EXAMPLES=1",
            "cargo test -p autumn-harvest-verify --test examples_metrics",
        ],
        "`examples_metrics::examples_corpus_allowlist_ratio_within_budget` \
         returns `ok` without measuring anything when HARVEST_VERIFY_EXAMPLES is \
         unset, so this step is the only thing that makes the precision half of \
         issue #962's success metric a real assertion. Without it the test \
         passes vacuously, forever, unnoticed.",
    );
}

#[test]
fn the_examples_gate_is_wired_into_ci() {
    assert_wired(
        "harvest-verify",
        &[
            "harvest-verify -p autumn-harvest --all-examples",
            "--allowlist harvest-verify.allow.toml",
            "--report",
        ],
        "This is AC6's gate over the repo's own `examples/`: `found` fails the \
         job, `unknown` warns. It is also what keeps the checked-in allowlist \
         honest — an entry that stops matching is reported as unused.",
    );
}

#[test]
fn tests_corpus_step_is_wired_into_ci() {
    assert_wired(
        "harvest-verify-tests",
        &[
            "harvest-verify -p autumn-harvest --test integration",
            "--allowlist harvest-verify.allow.toml",
            "--report",
        ],
        "AC6 asks for the analyzer to run over the repo's examples *and* its \
         test workflow corpus. `autumn-harvest/tests/` holds the largest body of \
         `#[workflow]` code in the repository, and this is the only step that \
         analyzes it.",
    );
}

#[test]
fn the_corpus_suite_runs_in_ci() {
    assert_wired(
        "harvest-verify",
        &[
            "- name: Analyzer, corpus and boundary tests",
            "run: cargo test -p autumn-harvest-verify",
        ],
        "The corpus regression suite — the detection ratchet, the expectations \
         oracle and the syntactic-cleanliness premise — runs as part of this \
         crate's ordinary test suite and nowhere else.",
    );
}

#[test]
fn this_crate_is_linted_in_ci() {
    assert_wired(
        "lint",
        &["cargo clippy -p autumn-harvest-verify --all-targets -- -D warnings"],
        "The workspace `lint` job does not use `--workspace`, so this is the \
         only clippy coverage this crate has, and the only step that compiles \
         its test targets outside the `harvest-verify` job.",
    );
}

#[test]
fn the_analyzer_jobs_are_gated_like_the_rest_of_the_workflow() {
    let workflow = workflow();
    for job in ["harvest-verify", "harvest-verify-tests"] {
        let block = job_block(&workflow, job);
        assert!(
            block.contains("needs: changes"),
            "`{job}` must be gated on the `changes` filter like `test`, or a \
             docs-only PR pays for a full MIR build:\n{block}"
        );
        assert!(
            block.contains("needs.changes.outputs.code == 'true'"),
            "`{job}` must skip docs-only pull requests:\n{block}"
        );
        assert!(
            block.contains("github.event.pull_request.draft == false"),
            "`{job}` must carry the draft-skip every other job has:\n{block}"
        );
    }
}

#[test]
fn job_block_stops_at_the_next_job() {
    // Self-test of the helper: without the bound, a needle from a *later* job
    // would satisfy an assertion about an earlier one, and the guard would pass
    // while the step it names has been deleted.
    let workflow = "jobs:\n  first:\n    steps:\n      - run: alpha\n\
                    \n  second:\n    steps:\n      - run: beta\n";
    let first = job_block(workflow, "first");
    assert!(first.contains("alpha"), "{first}");
    assert!(
        !first.contains("beta"),
        "the block must stop at the next job header:\n{first}"
    );
    assert!(job_block(workflow, "second").contains("beta"));
}
