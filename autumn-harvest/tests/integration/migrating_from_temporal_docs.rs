//! Anti-drift guard for the Temporal migration guide (issue #947).
//!
//! A migration guide that quietly rots is worse than none: a stale mapping
//! sends a real evaluator down a dead end. This test keeps three promises
//! mechanically rather than by review discipline alone:
//!
//! 1. the guide and its worked example both exist, and the guide covers the
//!    required ground — every required section, and at least 25
//!    concept-mapping table rows (issue #947 AC1's literal minimum);
//! 2. every `#NNN` issue citation inside the guide actually appears in
//!    `docs/shipped-work.md` — the repository's own record of what has shipped — so a
//!    claim can never cite a number nobody can verify (issue #947 AC1's
//!    "verifiable in under a minute" bar, and the "no unshipped capability
//!    presented as shipped" bar);
//! 3. the guide is linked from every place a reader would look for it:
//!    `README.md`, `docs/getting-started/README.md`, and — completing the
//!    forward reference `docs/comparison.md` shipped with a `_planned_`
//!    placeholder — `docs/comparison.md` itself (issue #947 AC5).

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const GUIDE_PATH: &str = "docs/migrating-from-temporal.md";
const EXAMPLE_NAME: &str = "temporal_port_subscription_renewal";

const REQUIRED_SECTIONS: &[&str] = &[
    "## Scope and audience",
    "## Non-goals",
    "## Concept mapping",
    "### Workflows and activities",
    "### Signals",
    "### Queries and updates",
    "### Timers and continue-as-new",
    "### Child workflows",
    "### Schedules",
    "### Versioning and determinism",
    "### Worker placement and build routing",
    "### Operational lifecycle",
    "## No equivalent yet",
    "## Workflow-porting checklist",
    "## Dual-run cutover playbook",
    "## Worked example",
    "## Related",
];

/// One literal substring per named primitive the guide must cover. This is
/// a *coverage* check (each of these must be mentioned), not a row-count
/// check — the row count is verified separately by
/// `concept_mapping_table_has_at_least_25_rows` against the actual markdown
/// table structure, so the two checks cannot silently drift apart.
const REQUIRED_CONCEPTS: &[&str] = &[
    "`#[workflow]`",
    "`#[activity]`",
    "SignalWithStart",
    "signal-with-start",
    "`setHandler`",
    "register_signal_handler",
    "`condition(",
    "wait_for_signal",
    "Idempotency-Key",
    "`@workflow.query`",
    "register_query_handler",
    "`@workflow.update`",
    "register_update_handler",
    "UpdateWithStart",
    "update_with_start",
    "`sleep(",
    "ctx.timer(",
    "sleep_until",
    "cancellable",
    "continueAsNew",
    "continue_as_new",
    "cross-type",
    "last_completion_result",
    "executeChild",
    "spawn_child_workflow",
    "ParentClosePolicy",
    "fan-out",
    "Promise.race",
    "ctx.race()",
    "child-or-deadline",
    "Temporal Schedules",
    "OverlapPolicy",
    "Catchup",
    "bounded",
    "Calendar",
    "business-day",
    "jitter",
    "updateSchedule",
    "describeSchedule",
    "GetVersion",
    "Patched",
    "DeprecatePatch",
    "Build ID",
    "build ramp",
    "SideEffect",
    "Local Activity",
    "Worker Sessions",
    "Reset Workflow Execution",
    "Terminate",
    "Cancel",
    "Pause",
    "Search Attributes",
    "WorkflowReplayer",
    "determinism",
    "Nexus",
    "multi-region",
    "non-Rust",
];

#[test]
fn guide_and_example_exist() {
    let guide = workspace_path(GUIDE_PATH);
    assert!(
        guide.is_file(),
        "expected {GUIDE_PATH} to exist (issue #947)"
    );

    let example = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(format!("{EXAMPLE_NAME}.rs"));
    assert!(
        example.is_file(),
        "expected examples/{EXAMPLE_NAME}.rs to exist (issue #947 worked example)"
    );
}

#[test]
fn guide_covers_every_required_section() {
    let guide = read_doc(GUIDE_PATH);
    let missing: Vec<&&str> = REQUIRED_SECTIONS
        .iter()
        .filter(|section| !guide.contains(**section))
        .collect();
    assert!(
        missing.is_empty(),
        "migration guide is missing required section(s): {missing:?}"
    );
}

#[test]
fn guide_covers_every_required_concept() {
    let guide = read_doc(GUIDE_PATH);
    let missing: Vec<&&str> = REQUIRED_CONCEPTS
        .iter()
        .filter(|concept| !guide.contains(**concept))
        .collect();
    assert!(
        missing.is_empty(),
        "migration guide is missing required concept coverage: {missing:?}"
    );
}

/// AC1's literal bar: "a concept-mapping table ... covering at minimum
/// workflows/activities, signals, ... (25+ items)". Counts actual markdown
/// table data rows inside the `## Concept mapping` section (from that
/// heading to the next `## ` heading), excluding header and separator rows
/// from each of the per-category sub-tables.
#[test]
fn concept_mapping_table_has_at_least_25_rows() {
    let guide = read_doc(GUIDE_PATH);
    let section = section_body(&guide, "## Concept mapping");

    let mut data_rows = 0usize;
    for raw_line in section.lines() {
        let line = raw_line.trim();
        if !(line.starts_with('|') && line.ends_with('|')) {
            continue;
        }
        if is_separator_row(line) {
            continue;
        }
        if line.contains("Temporal primitive") {
            // Column header row.
            continue;
        }
        data_rows += 1;
    }

    assert!(
        data_rows >= 25,
        "concept-mapping table must have at least 25 data rows, found {data_rows}"
    );
}

#[test]
fn every_cited_issue_number_appears_in_the_shipped_work_record() {
    let guide = read_doc(GUIDE_PATH);
    let shipped_work = read_doc("docs/shipped-work.md");

    let cited = issue_refs(&guide);
    let known = issue_refs(&shipped_work);

    let unverifiable: Vec<u32> = cited.difference(&known).copied().collect();
    assert!(
        unverifiable.is_empty(),
        "migration guide cites issue number(s) not found anywhere in \
         docs/shipped-work.md, so they cannot be verified against the \
         repository's own shipped-work record: {unverifiable:?}"
    );
    assert!(
        !cited.is_empty(),
        "migration guide cites no issue numbers at all -- every shipped claim must link an \
         issue (issue #947 AC1)"
    );
}

#[test]
fn guide_states_the_history_import_non_goal_with_a_concrete_reason() {
    let guide = read_doc(GUIDE_PATH);
    let non_goals = section_body(&guide, "## Non-goals");

    assert!(
        non_goals.contains("HistoryEvent"),
        "Non-goals must name Temporal's own event type (HistoryEvent) to explain the \
         incompatibility, not just assert unsupported"
    );
    assert!(
        non_goals.contains("WorkflowEvent"),
        "Non-goals must name harvest's own event type (WorkflowEvent) to contrast against \
         Temporal's HistoryEvent"
    );
    assert!(
        non_goals.to_lowercase().contains("drain"),
        "Non-goals must state that in-flight Temporal executions drain rather than migrate"
    );
}

#[test]
fn worked_example_is_referenced_by_name_in_the_guide() {
    let guide = read_doc(GUIDE_PATH);
    assert!(
        guide.contains(EXAMPLE_NAME),
        "the guide's worked-example section must reference the compiling example file by name \
         so a reader can find and run it"
    );
}

#[test]
fn guide_is_linked_from_readme_and_getting_started_index() {
    for (path, label) in [
        ("README.md", "README.md"),
        (
            "docs/getting-started/README.md",
            "docs/getting-started/README.md",
        ),
    ] {
        let doc = read_doc(path);
        assert!(
            doc.contains("migrating-from-temporal.md"),
            "{label} must link to docs/migrating-from-temporal.md"
        );
    }
}

/// Completes the bidirectional link `docs/comparison.md` shipped with a
/// `_planned_` placeholder pointing at issue #947.
#[test]
fn comparison_page_links_back_to_the_migration_guide() {
    let comparison = read_doc("docs/comparison.md");
    assert!(
        comparison.contains("migrating-from-temporal.md"),
        "docs/comparison.md must link to the migration guide, completing the bidirectional \
         cross-reference it shipped as a placeholder for"
    );
    assert!(
        !comparison.contains("Temporal migration guide** — _planned_"),
        "docs/comparison.md still carries the unresolved `_planned_` placeholder for the \
         migration guide -- replace it with a real link now that the guide exists"
    );
}

#[test]
fn ci_workflow_exercises_the_worked_example() {
    let ci = read_doc(".github/workflows/ci.yml");
    assert!(
        ci.contains(&format!("--example {EXAMPLE_NAME}")),
        "the worked example must be exercised by a CI step (`cargo test ... --example \
         {EXAMPLE_NAME}`), not just present on disk"
    );
}

/// The guide embeds the harvest side of the worked example as a Rust code
/// fence, next to the Temporal TypeScript original, so a reader can compare
/// the two without leaving the page. A copy that drifts from the real,
/// compiling file is worse than no copy at all -- it teaches the wrong
/// thing. Keep the embedded snippet byte-identical to the source of truth.
#[test]
fn worked_example_code_block_matches_the_real_file() {
    let guide = read_doc(GUIDE_PATH);
    let example = read_normalized(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join(format!("{EXAMPLE_NAME}.rs")),
    );

    let doc_start = "### Harvest (Rust)\n\n```rust\n";
    let start = guide.find(doc_start).unwrap_or_else(|| {
        panic!("expected to find the '{doc_start:?}' code fence in {GUIDE_PATH}")
    }) + doc_start.len();
    let end = guide[start..]
        .find("\n```\n")
        .unwrap_or_else(|| panic!("expected the Harvest (Rust) code fence to close"));
    let doc_snippet = guide[start..start + end].trim();

    let file_start = "use autumn_harvest::prelude::*;";
    let file_start_idx = example
        .find(file_start)
        .unwrap_or_else(|| panic!("expected {EXAMPLE_NAME}.rs to start with {file_start:?}"));
    let file_end_marker = "\n\nfn main()";
    let file_end_idx = example[file_start_idx..]
        .find(file_end_marker)
        .unwrap_or_else(|| panic!("expected a {file_end_marker:?} marker after the workflow fn"));
    let file_snippet = example[file_start_idx..file_start_idx + file_end_idx].trim();

    assert_eq!(
        doc_snippet, file_snippet,
        "the guide's embedded `### Harvest (Rust)` code block has drifted from the real \
         examples/{EXAMPLE_NAME}.rs file -- keep the two byte-identical"
    );
}

/// `.github/workflows/ci.yml` skips its `test` job's steps entirely on a
/// docs-only PR (every step there is gated on `changes.outputs.code ==
/// 'true'`), and a PR touching only `docs/migrating-from-temporal.md` is
/// exactly a docs-only PR. Without a step outside that gate, these guards
/// -- the ones enforcing every promise this module makes -- would never
/// execute on the change class most likely to break them: a stale link, a
/// drifted citation, an edited worked-example snippet.
///
/// `docs/performance.md` and `docs/rnd/sqlite-feasibility.md` hit this same
/// gap before this module existed and fixed it the same way: a dedicated
/// step in the ungated `lint` job. This asserts that step exists for this
/// module too, lives in `lint` (not the gated `test` matrix), and has not
/// grown an `if:` condition that would put it back behind the same gate.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read_normalized(&workspace_path(".github/workflows/ci.yml"));

    // The step must exist, and must name this module as its filter -- a step
    // that ran some *other* test would satisfy a looser check while leaving
    // these guards just as unexecuted.
    let step = workflow
        .lines()
        .find(|line| line.contains("--test integration migrating_from_temporal_docs::"))
        .expect(
            "ci.yml must run the migrating_from_temporal_docs guards from a step that is \
             not gated on `changes.outputs.code`, or a docs-only PR -- the change class \
             these guards exist for -- skips them entirely",
        );
    assert!(
        step.trim_start().starts_with("run:"),
        "expected the guard invocation to be a step `run:` line, found: {step}"
    );

    // It must live in `lint`, the ungated job. `test` is gated per-step on
    // `changes.outputs.code`, so a step there proves nothing for docs-only PRs.
    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let step_at = workflow
        .find("--test integration migrating_from_temporal_docs::")
        .expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the migrating_from_temporal_docs guard step must live in the ungated `lint` job; \
         a step in the `test` matrix is gated on `changes.outputs.code` and so does not \
         run on a docs-only PR"
    );

    // And it must be unconditional. A step that grew an `if:` is back behind
    // a gate -- which is the exact regression this test exists to prevent.
    let block: &str = &workflow[lint_start..test_start];
    let step_idx = block
        .find("--test integration migrating_from_temporal_docs::")
        .expect("step is inside the lint block");
    let step_line_start = block[..step_idx].rfind("\n      - name:").unwrap_or(0);
    let stanza = &block[step_line_start..step_idx];
    assert!(
        !stanza.contains("\n        if:"),
        "the migrating_from_temporal_docs guard step has acquired an `if:` condition. It \
         must run unconditionally: a condition is how these guards would stop running on \
         docs-only PRs again. Stanza:\n{stanza}"
    );
}

/// Read a file with line endings normalised to `\n`.
///
/// Every needle search in this module -- markdown headers, code-fence
/// markers, table separators, workflow-file structural boundaries such as
/// `"\n  lint:"` -- is anchored on `\n`. A Windows checkout hands
/// `fs::read_to_string` `\r\n` line endings by default, so each needle
/// silently misses and a test fails with a panic that has nothing to do
/// with the file's actual contents. Normalising once here (`read_doc`
/// delegates to this too) keeps every needle search platform-agnostic
/// rather than spreading `\r?` handling across each one.
fn read_normalized(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .replace("\r\n", "\n")
}

fn is_separator_row(line: &str) -> bool {
    let inner = line.trim_matches('|');
    !inner.is_empty()
        && inner.contains('-')
        && inner
            .chars()
            .all(|c| c == '-' || c == ':' || c == '|' || c.is_whitespace())
}

/// Returns the text between `heading` and the next line starting with
/// `"## "` (or end of document).
fn section_body<'a>(doc: &'a str, heading: &str) -> &'a str {
    let start = doc
        .find(heading)
        .unwrap_or_else(|| panic!("expected to find heading {heading:?}"));
    let after_heading = &doc[start + heading.len()..];
    let end = after_heading.find("\n## ").unwrap_or(after_heading.len());
    &after_heading[..end]
}

fn issue_refs(text: &str) -> BTreeSet<u32> {
    let mut refs = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            let digits = end - start;
            if digits > 0
                && digits <= 6
                && let Ok(n) = text[start..end].parse::<u32>()
            {
                refs.insert(n);
            }
            i = end.max(i + 1);
        } else {
            i += 1;
        }
    }
    refs
}

fn read_doc(relative: &str) -> String {
    read_normalized(&workspace_path(relative))
}

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative)
}
