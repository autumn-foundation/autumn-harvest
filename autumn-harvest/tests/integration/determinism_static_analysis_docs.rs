//! Guards that keep the issue-#962 R&D report honest against the tool it audits.
//!
//! `docs/rnd/determinism-static-analysis.md` is AC1's primary deliverable: a
//! leadership decision document about whether a semantic, MIR-level determinism
//! analysis is worth building and keeping. Its load-bearing claims are not
//! prose — they are a boundary set, a rule-subsumption matrix, and three success
//! metrics, each of which is only meaningful if it still matches the code.
//!
//! An R&D report is unusually prone to rotting into a lie precisely because
//! nothing compiles against it. The two precedents in `docs/rnd/` both answered
//! that with a guard suite, and this is the third:
//!
//!   * the boundary table must list every `unknown` reason the tool can emit —
//!     `proven-deterministic` is only honest with its boundary set attached;
//!   * the subsumption matrix must cover all 22 syntactic rules (HVG001–HVG011,
//!     DET001–DET011), because AC8 asks specifically which of them could retire
//!     and a matrix with holes cannot answer that;
//!   * every metric must name a test a reviewer can run, and that test must
//!     exist;
//!   * the report must state the AC5-versus-AC7 resolution, since AC5 suggests a
//!     macro attribute that AC7 forbids and a reader comparing deliverable to
//!     AC list would otherwise read the allowlist file as an unexplained miss.
//!
//! ## Why the boundary list is hard-coded here
//!
//! The authoritative list is `autumn_harvest_verify::BoundaryKind::ALL` (the CLI
//! prints it as `--list-boundaries`). This crate cannot import it:
//! `autumn-harvest-verify` dev-depends on `autumn-harvest`, so a dependency the
//! other way is a cycle. So the guard is split in two and the halves are
//! bidirectional:
//!
//!   * **here** — the report's table contains exactly [`BOUNDARY_NAMES`], so no
//!     row is invented and none is quietly dropped;
//!   * **`autumn-harvest-verify/tests/docs_boundaries.rs`** — every name in
//!     `BoundaryKind::ALL` appears in that table, so no boundary is
//!     undocumented.
//!
//! Neither half alone is enough: this one can be satisfied by a table that
//! matches a stale constant, and that one by a table with extra rows.
//!
//! Deliberately NOT guarded: the report's judgements — the go/no-go reasoning,
//! the cost estimates, the recommendation on scope. Those are argument, not
//! fact, and freezing them would stop the document being revisable.

use std::path::{Path, PathBuf};

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR` (`<repo>/autumn-harvest`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a file with line endings normalised to `\n`.
///
/// Every guard here slices the document by byte offset or matches exact table
/// cells. Under the CRLF checkout of the Windows CI leg, `str::lines()` strips
/// the `\r` while the raw text keeps it, so offsets reconstructed from line
/// lengths under-count one byte per line — and this report contains em dashes,
/// so the resulting shift can land mid-character and panic. Mirrors
/// `sqlite_feasibility_docs::read_normalized`.
fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

fn report_path() -> PathBuf {
    repo_root().join("docs/rnd/determinism-static-analysis.md")
}

fn read_report() -> String {
    let path = report_path();
    let body = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "issue #962 AC1's primary deliverable is missing: could not read {}: {err}\n\
             The feasibility report is the deliverable leadership reads to green-light \
             or kill the direction; the prototype crate is its evidence, not its \
             substitute. Every other guard in this file is downstream of it.",
            path.display()
        )
    });
    body.replace("\r\n", "\n")
}

// ── The section headings this guard requires, by name ───────────────────────
//
// Named as constants rather than inlined so a rename is one edit and the failure
// message can print the exact heading the report is missing.

const SEC_DECISION: &str = "## Decision summary";
const SEC_APPROACHES: &str = "## Candidate approaches";
const SEC_BOUNDARIES: &str = "## Soundness boundaries";
const SEC_BASELINE: &str = "## Relationship to the syntactic baseline";
const SEC_METRICS: &str = "## Success metrics";
const SEC_FOOTPRINT: &str = "## Engine footprint";

/// The escape-hatch section may be titled either way; AC5 calls it an allowlist
/// and the issue's own prose calls it an escape hatch.
const SEC_ESCAPE_ALTERNATIVES: &[&str] = &["## Escape hatch", "## Allowlist"];

/// The go/no-go may be its own section or folded into the decision summary.
const SEC_GO_ALTERNATIVES: &[&str] = &["## Go / no-go", "## Go/no-go", SEC_DECISION];

/// Mirror of `autumn_harvest_verify::BoundaryKind::ALL`'s kebab-case names.
/// See the module docs for why this is a copy and how it is kept honest.
const BOUNDARY_NAMES: &[&str] = &[
    "dyn-dispatch",
    "indirect-call",
    "ffi",
    "unsafe-raw-pointer",
    "inline-asm",
    "external-crate-body",
    "unmodeled-ctx-method",
    "unresolved-generic",
    "recursion",
    "mir-parse",
    "missing-body",
    "drop-glue",
];

/// Slice the section starting at `heading` up to the next `\n## ` (or EOF).
fn section<'a>(report: &'a str, heading: &str) -> &'a str {
    let start = report.find(heading).unwrap_or_else(|| {
        panic!("the report must have a `{heading}` section");
    });
    let rest = &report[start + heading.len()..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    &rest[..end]
}

fn has_any(report: &str, headings: &[&str]) -> bool {
    headings.iter().any(|h| report.contains(h))
}

#[test]
fn report_exists_and_names_the_revision_it_audited() {
    let report = read_report();

    assert!(
        report.starts_with("# ") || report.contains("\n# "),
        "the report must open with a level-1 title"
    );

    // Both `docs/rnd/` precedents open with a status blockquote that states the
    // report's own epistemic footing. Without the audited revision a reader
    // cannot tell whether the inventory below it describes today's code.
    let marker = "**Audited revision:**";
    let at = report.find(marker).unwrap_or_else(|| {
        panic!(
            "the report must carry an `{marker}` line in its opening status \
             blockquote, naming the commit its measurements were taken against. \
             Both `docs/rnd/` precedents do this, and without it every count and \
             every line reference in the report is unfalsifiable."
        )
    });
    let tail = &report[at + marker.len()..];
    let line = tail.lines().next().unwrap_or_default();
    let sha_len = line
        .split_whitespace()
        .map(|tok| tok.trim_matches(|c: char| !c.is_ascii_alphanumeric()))
        .filter(|tok| tok.chars().all(|c| c.is_ascii_hexdigit()))
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(
        sha_len >= 7,
        "`{marker}` must be followed by a git revision of at least 7 hex \
         characters; found {sha_len} hex characters on: {line:?}"
    );

    // The status blockquote itself.
    assert!(
        report
            .lines()
            .take(30)
            .any(|l| l.trim_start().starts_with('>')),
        "the report must open with a status blockquote (the `docs/rnd/` house \
         style: what this document is, what it is not, and when it was audited)"
    );
}

#[test]
fn report_has_a_decision_summary_with_a_question_answer_table() {
    let report = read_report();
    let summary = section(&report, SEC_DECISION);

    assert!(
        summary.contains("| Question | Answer |") || summary.contains("| Question |"),
        "`{SEC_DECISION}` must carry a Question -> Answer table, matching \
         `docs/rnd/sqlite-feasibility.md`. That table is the part a reader who \
         reads nothing else actually reads."
    );

    let rows = summary
        .lines()
        .filter(|l| l.trim_start().starts_with('|') && !l.contains("---"))
        .count();
    assert!(
        rows >= 6,
        "`{SEC_DECISION}`'s table has {rows} row(s); the questions this report \
         has to answer (stable-Rust feasibility, does it work on real code, CI \
         budget, where the difficulty is, cost to keep, verdict honesty, what the \
         syntactic layer misses, engine footprint) do not fit in fewer than 6"
    );

    // A bold one-line verdict, in the precedents' style.
    let has_bold_verdict = summary
        .lines()
        .any(|l| l.trim_start().starts_with("**") && l.trim_end().len() > 10);
    assert!(
        has_bold_verdict,
        "`{SEC_DECISION}` must open with a bold one-line verdict before the \
         table — a reader must not have to infer the recommendation from a table"
    );
}

#[test]
fn report_compares_the_candidate_approaches_that_were_considered() {
    // The substrate decision is the whole feasibility question. A report that
    // presents only the chosen approach is a write-up, not an evaluation.
    const REQUIRED: &[&str] = &[
        "rustc_private",
        "--emit=mir",
        "rustc_public",
        "rust-analyzer",
        "syn",
    ];

    let report = read_report();
    let approaches = section(&report, SEC_APPROACHES);
    let missing: Vec<&str> = REQUIRED
        .iter()
        .copied()
        .filter(|needle| !approaches.contains(needle))
        .collect();
    assert!(
        missing.is_empty(),
        "`{SEC_APPROACHES}` must weigh every substrate that was on the table, so \
         a reader can see the road not taken. Missing: {missing:?}"
    );

    assert!(
        approaches
            .lines()
            .filter(|l| l.trim_start().starts_with('|'))
            .count()
            >= 6,
        "`{SEC_APPROACHES}` must present the comparison as a table (one row per \
         approach plus a header), not as prose"
    );
}

#[test]
fn report_boundary_table_matches_the_code() {
    let report = read_report();
    let boundaries = section(&report, SEC_BOUNDARIES);

    let missing: Vec<&str> = BOUNDARY_NAMES
        .iter()
        .copied()
        .filter(|name| !boundaries.contains(name))
        .collect();
    assert!(
        missing.is_empty(),
        "`{SEC_BOUNDARIES}` must list every `unknown` reason the analyzer can \
         emit. Missing: {missing:?}\n\
         The authoritative list is `autumn_harvest_verify::BoundaryKind::ALL` \
         (printed by `cargo harvest-verify --list-boundaries`); this crate cannot \
         import it, because `autumn-harvest-verify` dev-depends on \
         `autumn-harvest`. The other direction of this guard lives in \
         `autumn-harvest-verify/tests/docs_boundaries.rs`."
    );

    assert!(
        boundaries
            .lines()
            .filter(|l| l.trim_start().starts_with('|'))
            .count()
            >= BOUNDARY_NAMES.len(),
        "`{SEC_BOUNDARIES}` must present the boundary set as a table with a row \
         per boundary (name, what it means, what the tool does), not as a prose list"
    );
}

#[test]
fn report_relates_every_syntactic_rule_to_the_semantic_pass() {
    let report = read_report();
    let baseline = section(&report, SEC_BASELINE);

    let mut missing: Vec<String> = Vec::new();
    for n in 1..=11 {
        for prefix in ["HVG", "DET"] {
            let id = format!("{prefix}{n:03}");
            if !baseline.contains(&id) {
                missing.push(id);
            }
        }
    }
    assert!(
        missing.is_empty(),
        "AC8 asks which existing checks the semantic pass subsumes, which is a \
         rule-by-rule question. `{SEC_BASELINE}` must carry a row for each of the \
         22 syntactic rules. Missing: {missing:?}"
    );

    assert!(
        baseline.contains("retire"),
        "`{SEC_BASELINE}` must state, in words, whether any syntactic rule should \
         be retired — that is AC8's actual question, and a matrix without a \
         recommendation leaves it unanswered"
    );
}

/// Every metric the issue names must cite a test a reviewer can run, and that
/// test must exist. The repo's own precedent (`docs/performance.md`) is explicit
/// that ratios are asserted by tests while timings are published, not asserted —
/// a timing assertion in CI is flaky, and a flaky gate is worse than a table.
#[test]
fn success_metrics_cite_tests_that_exist() {
    const CITED_TESTS: &[&str] = &["corpus::detection_rate_meets_the_success_metric"];

    let report = read_report();
    let metrics = section(&report, SEC_METRICS);

    // Detection rate: "≥ 90%", with or without a thin space.
    assert!(
        metrics.contains("90%") || metrics.contains("90 %"),
        "`{SEC_METRICS}` must state the ≥ 90% detection-rate metric verbatim"
    );
    assert!(
        metrics.contains("10%") || metrics.contains("10 %"),
        "`{SEC_METRICS}` must state the ≤ 10% allowlist/false-positive metric \
         verbatim — it is the one metric produced by code the analyzer's author \
         did not write, and the go/no-go hinges on it"
    );
    assert!(
        metrics.contains("5 min") || metrics.contains("5 minutes"),
        "`{SEC_METRICS}` must state the < 5 min CI-budget metric, and must say \
         whether it is a warm-cache or cold-cache number"
    );

    for cited in CITED_TESTS {
        assert!(
            metrics.contains(cited),
            "`{SEC_METRICS}` must name `{cited}` as the evidence for its metric \
             row. A metric row without a runnable citation is the row a reviewer \
             challenges first."
        );
    }
    assert!(
        metrics.contains("examples_metrics::"),
        "`{SEC_METRICS}` must cite the `examples_metrics::` test that measures \
         the false-positive rate over this repo's real examples"
    );

    // Now check the citations resolve: every `module::fn_name` the metrics
    // section names must exist as a `fn fn_name` in the verify crate's tests.
    let tests_dir = repo_root().join("autumn-harvest-verify/tests");
    let mut sources = String::new();
    let entries = std::fs::read_dir(&tests_dir).unwrap_or_else(|err| {
        panic!("cannot read {}: {err}", tests_dir.display());
    });
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "rs") {
            sources.push_str(&read_normalized(&path));
            sources.push('\n');
        }
    }

    let mut unresolved: Vec<String> = Vec::new();
    for token in metrics.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':')) {
        let Some((module, name)) = token.split_once("::") else {
            continue;
        };
        if module.is_empty() || name.is_empty() || name.contains("::") {
            continue;
        }
        // Only the two test modules the report is allowed to cite as evidence.
        if module != "corpus" && module != "examples_metrics" {
            continue;
        }
        if !sources.contains(&format!("fn {name}")) {
            unresolved.push(token.to_string());
        }
    }
    assert!(
        unresolved.is_empty(),
        "`{SEC_METRICS}` cites {} test(s) that do not exist in \
         `autumn-harvest-verify/tests/`: {unresolved:?}\n\
         A cited-but-absent test is worse than no citation: it reads as evidence \
         and is not.",
        unresolved.len()
    );
}

#[test]
fn report_explains_the_ac5_versus_ac7_resolution() {
    let report = read_report();
    assert!(
        has_any(&report, SEC_ESCAPE_ALTERNATIVES),
        "the report must have a section on the escape hatch (one of \
         {SEC_ESCAPE_ALTERNATIVES:?}). AC5 requires a way to mark a workflow as \
         intentionally unverified."
    );
    let heading = SEC_ESCAPE_ALTERNATIVES
        .iter()
        .copied()
        .find(|h| report.contains(h))
        .expect("located above");
    let escape = section(&report, heading);

    assert!(
        escape.contains("AC7"),
        "`{heading}` must name AC7 explicitly. AC5 suggests a \
         `#[workflow(allow_unverified)]` attribute and AC7 forbids any \
         macro-path change; the checked-in allowlist file is the only reading \
         that satisfies both, and a reviewer comparing the deliverable to the AC \
         list will otherwise score AC5 as an unexplained miss."
    );
    assert!(
        escape.contains("justification"),
        "`{heading}` must state that an allowlist entry requires a \
         justification — an escape hatch without one is an off switch"
    );
}

#[test]
fn report_states_the_zero_engine_footprint_claim() {
    let report = read_report();
    let footprint = section(&report, SEC_FOOTPRINT);

    // AC7's three concrete promises. The PR diffstat is the real evidence; the
    // report has to actually make the claim so the diffstat has something to check.
    assert!(
        footprint.contains("WorkflowEvent"),
        "`{SEC_FOOTPRINT}` must state that no new `WorkflowEvent` variant is added"
    );
    assert!(
        footprint.contains("migration"),
        "`{SEC_FOOTPRINT}` must state that no database migration is added"
    );
    assert!(
        footprint.contains("macro"),
        "`{SEC_FOOTPRINT}` must state that the `#[workflow]` macro path is unchanged"
    );
    assert!(
        footprint.contains("build-time") || footprint.contains("build time"),
        "`{SEC_FOOTPRINT}` must say the tool is build-time only — that is what \
         makes the zero-runtime-footprint claim true rather than merely asserted"
    );
}

#[test]
fn report_reaches_an_explicit_go_or_no_go() {
    let report = read_report();
    assert!(
        has_any(&report, SEC_GO_ALTERNATIVES),
        "the report must reach an explicit go / no-go (one of \
         {SEC_GO_ALTERNATIVES:?}). AC1 asks for a recommendation, not a survey."
    );
    let heading = SEC_GO_ALTERNATIVES
        .iter()
        .copied()
        .find(|h| report.contains(h))
        .expect("located above");
    let verdict = section(&report, heading);
    let lowered = verdict.to_lowercase();
    assert!(
        lowered.contains("go"),
        "`{heading}` must contain the word \"go\" — a conditional go and a no-go \
         are both acceptable answers; an absent one is not"
    );
}

#[test]
fn user_guide_and_cross_links_exist() {
    let guide = repo_root().join("docs/harvest-verify.md");
    assert!(
        guide.is_file(),
        "`docs/harvest-verify.md` (the user guide: how to run the tool, how to \
         read a verdict, how to extend the model, how the allowlist works) must \
         exist. The R&D report explains whether to build it; the guide explains \
         how to use it, and a tool nobody can run scores nothing on AC6."
    );

    let determinism_guide =
        read_normalized(&repo_root().join("docs/workflow-determinism-guide.md"));
    assert!(
        determinism_guide.contains("harvest-verify.md"),
        "`docs/workflow-determinism-guide.md` must link to `docs/harvest-verify.md`. \
         The determinism guide is where an author lands when they hit a \
         non-determinism error; the semantic pass is the second line of defence \
         behind the guardrails that guide already documents, and it is useless if \
         it is not discoverable from there."
    );
    assert!(
        determinism_guide.contains("determinism-static-analysis.md"),
        "`docs/workflow-determinism-guide.md` must link to \
         `docs/rnd/determinism-static-analysis.md`, so a reader who wants to know \
         what the pass can and cannot prove finds the boundary set"
    );
}

#[test]
fn changelog_fragment_exists() {
    let dir = repo_root().join("docs/changelog.d");
    let entries = std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", dir.display()));
    let found = entries.flatten().any(|entry| {
        entry.file_name().to_str().is_some_and(|name| {
            name.starts_with("pr-962-") && Path::new(name).extension().is_some_and(|e| e == "md")
        })
    });
    assert!(
        found,
        "exactly one changelog fragment `docs/changelog.d/pr-962-<slug>.md` must \
         exist. `CHANGELOG.md` itself is generated and must not be edited by hand."
    );
}

// ── Self-guard: these guards must actually run on the change class they exist for ──
//
// Mirrors `sqlite_feasibility_docs::guards_run_on_docs_only_changes`. Docs-only
// PRs skip the entire `test` matrix (every step there is gated on
// `changes.outputs.code`), so a guard suite about a document is exactly the suite
// that would never fire on a change to that document.

/// The **whole** workflow step stanza containing `needle`, from its `- name:`
/// through to the line before the next step's `- name:` (or the end of the block).
///
/// Bounding at the *next* step rather than at `needle` is the load-bearing part:
/// `needle` sits inside the step's `run:` line, so a slice that stops there
/// cannot see an `if:` written below `run:` — and GitHub Actions does not care
/// about key order within a step. See the same helper in
/// `sqlite_feasibility_docs`, where a two-line reordering once evaded this exact
/// guard.
fn workflow_step_stanza<'a>(block: &'a str, needle: &str) -> Option<&'a str> {
    const STEP: &str = "\n      - name:";
    let at = block.find(needle)?;
    let start = block[..at].rfind(STEP).unwrap_or(0);
    let after_marker = start + STEP.len();
    let end = block[after_marker..]
        .find(STEP)
        .map_or(block.len(), |rel| after_marker + rel);
    Some(&block[start..end])
}

#[test]
fn guards_run_on_docs_only_changes() {
    const FILTER: &str = "--test integration determinism_static_analysis_docs::";
    let workflow = read_normalized(&repo_root().join(".github/workflows/ci.yml"));

    let step = workflow
        .lines()
        .find(|line| line.contains(FILTER))
        .unwrap_or_else(|| {
            panic!(
                "ci.yml must run the determinism_static_analysis_docs guards from a \
                 step that is not gated on `changes.outputs.code`, or a docs-only PR \
                 — the change class these guards exist for — skips them entirely. \
                 Add to the `lint` job, beside the other `*_docs::` steps:\n\
                 \x20     - name: Run docs/rnd/determinism-static-analysis.md guards \
                 (also on docs-only PRs)\n\
                 \x20       run: \"cargo test -p autumn-harvest --no-default-features \
                 --features testing {FILTER}\""
            )
        });
    assert!(
        step.trim_start().starts_with("run:"),
        "expected the guard invocation to be a step `run:` line, found: {step}"
    );

    // It must live in `lint`, the ungated job. Every step in `test` is gated on
    // `changes.outputs.code`, so a step there proves nothing for a docs-only PR.
    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let step_at = workflow.find(FILTER).expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the determinism_static_analysis_docs guard step must live in the ungated \
         `lint` job; a step in the `test` matrix is gated on \
         `changes.outputs.code` and so does not run on a docs-only PR"
    );

    let block = &workflow[lint_start..test_start];
    let stanza = workflow_step_stanza(block, FILTER)
        .expect("the guard step is inside the lint block, located above");
    assert!(
        !stanza.contains("\n        if:"),
        "the determinism_static_analysis_docs guard step has acquired an `if:` \
         condition. It must run unconditionally: a condition is how guards like \
         these stopped running on docs-only PRs in the first place. Stanza:\n{stanza}"
    );
}

#[test]
fn step_stanza_covers_keys_written_after_run() {
    // Self-test of the helper above, mirroring `sqlite_feasibility_docs`: an
    // `if:` placed *below* `run:` is valid YAML with identical semantics and is
    // invisible to a slice that stops at the `run:` line.
    let block = "\n      - name: Some earlier step\n        run: echo earlier\n\
                 \n      - name: Guard step\n        run: cargo test GUARD_FILTER\n\
                 \n        if: needs.changes.outputs.code == 'true'\n\
                 \n      - name: A later step\n        run: echo later\n";

    let stanza = workflow_step_stanza(block, "GUARD_FILTER").expect("stanza is present");

    assert!(
        stanza.contains("\n        if:"),
        "the stanza must extend past `run:` to the next step, or an `if:` written \
         below `run:` re-gates the guards undetected. Stanza:\n{stanza}"
    );
    assert!(
        !stanza.contains("A later step"),
        "the stanza must stop at the next step, not swallow it. Stanza:\n{stanza}"
    );
    assert!(
        !stanza.contains("Some earlier step"),
        "the stanza must start at its own `- name:`. Stanza:\n{stanza}"
    );
}
