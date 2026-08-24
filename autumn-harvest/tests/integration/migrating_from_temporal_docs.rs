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
//!    `CLAUDE.md` — the repository's own record of what has shipped — so a
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
fn every_cited_issue_number_appears_in_claude_md() {
    let guide = read_doc(GUIDE_PATH);
    let claude_md = read_doc("CLAUDE.md");

    let cited = issue_refs(&guide);
    let known = issue_refs(&claude_md);

    let unverifiable: Vec<u32> = cited.difference(&known).copied().collect();
    assert!(
        unverifiable.is_empty(),
        "migration guide cites issue number(s) not found anywhere in CLAUDE.md, so they \
         cannot be verified against the repository's own shipped-work record: {unverifiable:?}"
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
    fs::read_to_string(workspace_path(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative)
}
