//! Guards the agent-mesh facts of issue #2485 in the guides.
//!
//! The code holds each fact, and a behaviour test proves it. A guard fails
//! when the guide drops the fact. The design is in
//! `docs/design/DESIGN-2485.md`.

use std::path::{Path, PathBuf};

const MESH: &str = "docs/agent-mesh.md";

/// The durable-agents guide.
const ADAPTER: &str = "docs/agent-adapter.md";

/// The child settings that `child_task` sets, beyond memory, follow-ups and
/// delivery. Each pair holds the first cell of a row and a word of its value.
const CHILD_ROWS: [(&str, &str); 9] = [
    ("Workspace", "None"),
    ("Tool guidance, skills", "None"),
    ("Answer check", "Off"),
    ("Kind", "None"),
    ("Flow labels", "shares the label set"),
    ("Failure report", "Off"),
    ("Draw", "keeps the draw id"),
    ("Streaming, parallel-children bound", "As the parent"),
    (
        "Effect allowlist, probe, context reset, mesh",
        "As the parent",
    ),
];

/// The own checks of a durable peer, in the order that `durable_peer` runs
/// them.
const DURABLE_CHECKS: &str = "A durable peer then checks `peer_unavailable`, \
     `not_permitted`, `deadlock` and `invalid_mandate`, in this order.";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The text of the level-2 section that opens with `heading`. The text
/// holds the level-3 subsections of the section.
fn section<'a>(text: &'a str, heading: &str) -> &'a str {
    let start = text
        .find(&format!("\n{heading}\n"))
        .unwrap_or_else(|| panic!("the text lacks `{heading}`"))
        + 1;
    let body = &text[start..];
    let end = body[heading.len()..]
        .find("\n## ")
        .map_or(body.len(), |at| at + heading.len());
    &body[..end]
}

/// Returns `text` with each run of white space as one space. A line wrap then
/// does not hide a phrase.
fn flat(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The table row of `text` whose first cell starts with `name`.
fn row<'a>(text: &'a str, name: &str) -> &'a str {
    text.lines()
        .find(|line| {
            line.strip_prefix('|')
                .and_then(|rest| rest.split('|').next())
                .is_some_and(|cell| cell.trim().starts_with(name))
        })
        .unwrap_or_else(|| panic!("no table row starts with `{name}`"))
}

// DESIGN-2485 item 1: a finished lookup replays from history. Only a lookup in flight
// runs again.
#[test]
fn the_crash_table_reads_a_finished_lookup_from_history() {
    let guide = read(MESH);
    let crash = section(&guide, "## 10. Crash and replay");
    let lookup = row(crash, "A peer lookup");
    assert!(lookup.contains("history"), "{lookup}");
    assert!(lookup.contains("finished lookup"), "{lookup}");
}

// DESIGN-2485 item 2: the guide states the own checks of a durable peer, in
// order. `mesh::tests` checks the order of the table.
#[test]
fn the_refusals_state_the_durable_checks() {
    let guide = read(MESH);
    let refusals = flat(section(&guide, "## 5. Refusals"));
    assert!(
        refusals.contains(DURABLE_CHECKS),
        "section 5 lacks the durable checks in order"
    );
}

// DESIGN-2485 item 3: the child table names each setting that `child_task`
// sets, with its value.
#[test]
fn the_child_table_names_every_child_setting() {
    let guide = read(MESH);
    let child = section(&guide, "## 3. What the child gets");
    for (name, value) in CHILD_ROWS {
        let line = row(child, name);
        assert!(line.contains(value), "{line}");
    }
}

// DESIGN-2485 item 4: the approval helper takes the tree round, not the child step.
#[test]
fn the_guides_name_the_tree_round_of_an_approval() {
    let adapter = flat(&read(ADAPTER));
    assert!(adapter.contains("approval_signal(round, position, call_id)"));
    assert!(!adapter.contains("approval_signal(step"));
    assert!(!adapter.contains("tool_approval:step:"));
    let guide = read(MESH);
    let approvals = flat(section(&guide, "## 8. Approvals in a child"));
    assert!(
        approvals.contains("not the step of the child"),
        "section 8 lacks the warning"
    );
}

// DESIGN-2485 item 5: each delegation mode has its own run-id form.
#[test]
fn the_guide_states_the_run_id_of_each_mode() {
    let guide = read(MESH);
    let child = section(&guide, "## 3. What the child gets");
    let inline = row(child, "Inline child");
    assert!(inline.contains("`{root run id}:d{n}`"), "{inline}");
    let fan_out = row(child, "Fan-out child");
    assert!(fan_out.contains("execution id"), "{fan_out}");
    let durable = row(child, "Durable peer");
    assert!(durable.contains("workflow id"), "{durable}");
}

// DESIGN-2485 item 6: the guide states both roster rules.
#[test]
fn the_guide_states_the_roster_rules() {
    let guide = read(MESH);
    let delegate = flat(section(&guide, "## 2. Delegate to a peer"));
    assert!(
        delegate.contains("keeps the peer added last"),
        "duplicate rule"
    );
    assert!(delegate.contains("invalid name"), "invalid-name rule");
}
