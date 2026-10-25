//! Docs guards for durable routes.
//!
//! The guards read the docs page, the design note and the changelog fragment
//! as data. They keep the status table in step with the code and keep each
//! acceptance criterion tied to a test that exists.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use autumn_harvest_plugin::durable_route::{
    DEFAULT_MAX_CHAIN_STEPS, DEFAULT_MAX_STREAMS, DEFAULT_MAX_WAITERS, DEFAULT_STREAM_FOR,
    MAX_STREAM_FOR, PROBLEM_SLUGS,
};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = workspace_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The problem slugs that the status table of the docs page names.
fn documented_slugs(page: &str) -> BTreeSet<String> {
    let start = page
        .find("## Status table")
        .expect("the page has a status table");
    let table = &page[start..];
    let end = table[3..].find("\n## ").map_or(table.len(), |i| i + 3);
    table[..end]
        .lines()
        .filter(|line| line.starts_with('|'))
        .filter_map(|line| line.split('|').nth(3))
        // A row with no problem starts its cell with an em dash.
        .filter(|cell| !cell.trim_start().starts_with('\u{2014}'))
        // The first code span of the cell is the slug. Later spans name
        // members of the problem body.
        .filter_map(|cell| cell.split('`').nth(1).map(str::to_owned))
        .collect()
}

// AC-44
#[test]
fn status_table_matches_problem_slugs() {
    let page = read("docs/durable-routes.md");
    let documented = documented_slugs(&page);
    let code: BTreeSet<String> = PROBLEM_SLUGS.iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(
        documented, code,
        "the status table must list each problem slug once"
    );
}

// AC-45
#[test]
fn page_is_linked_from_the_idempotency_chapter_and_architecture() {
    for (source, link) in [
        (
            "docs/getting-started/06-idempotency.md",
            "../durable-routes.md",
        ),
        ("docs/architecture.md", "durable-routes.md"),
    ] {
        let text = read(source);
        assert!(
            text.contains(&format!("]({link}")),
            "{source} must link to {link}"
        );
    }
}

/// `text` with each run of whitespace changed to one space, so a needle can
/// span a line break.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

// AC-46
#[test]
fn page_states_layer_timeout_and_key_guidance() {
    let page = one_line(&read("docs/durable-routes.md"));
    for needle in [
        "Retry-After",
        "same `Idempotency-Key`",
        "Send a **random** `Idempotency-Key`",
        "in_flight_ttl_secs",
        "sets its own `timeout_ms` must set `.wait(..)` below it",
        "The wait ends 2 s before Autumn's request deadline",
        "Every read after the start ends 1 s before it",
        "#[intercept]",
        "durable_route_max_waiters",
        "durable_route_max_chain_steps",
        "AppBuilder::layer",
        "idempotency.ttl_secs",
        "is final for this key",
        "Do not turn on `.idempotent()` for such an API",
        "PII erasure does not remove it",
        "do not turn on `.idempotent()` for durable routes",
        "A walk that read only ended runs answers `result-unavailable`",
        "## Known limits",
    ] {
        assert!(
            page.contains(needle),
            "docs/durable-routes.md must mention {needle:?}"
        );
    }
    // The numbers come from the code, so the page cannot drift from it.
    for needle in [
        format!("The default is {DEFAULT_MAX_WAITERS}."),
        format!("The cap is {DEFAULT_MAX_CHAIN_STEPS} steps."),
    ] {
        assert!(
            page.contains(&needle),
            "docs/durable-routes.md must say {needle:?}"
        );
    }
}

/// Every test `fn` name in the plugin sources and tests. A name counts when
/// one of the three lines above it holds a test attribute.
fn plugin_fn_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let root = workspace_root().join("autumn-harvest-plugin");
    let mut stack = vec![root.join("src"), root.join("tests")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("read source");
                let lines: Vec<&str> = text.lines().collect();
                for (i, line) in lines.iter().enumerate() {
                    let is_test = lines[i.saturating_sub(3)..i]
                        .iter()
                        .any(|above| above.contains("#[test]") || above.contains("#[tokio::test"));
                    if !is_test {
                        continue;
                    }
                    if let Some(rest) = line.trim_start().split("fn ").nth(1) {
                        let name: String = rest
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        if !name.is_empty() {
                            names.insert(name);
                        }
                    }
                }
            }
        }
    }
    names
}

/// Check that each test the AC table of `note` names exists. Returns the
/// count of names checked.
fn ac_table_names_existing_tests(note: &str, row_prefix: &str) -> usize {
    let design = read(note);
    let start = design
        .find("## 3. Tests and acceptance criteria")
        .expect("the design note has an AC table");
    let names = plugin_fn_names();
    let mut checked = 0;
    for line in design[start..]
        .lines()
        .filter(|l| l.starts_with(row_prefix))
    {
        let test_cell = line.split('|').nth(3).expect("test cell");
        for candidate in test_cell.split('`').skip(1).step_by(2) {
            let is_fn_name = candidate
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if is_fn_name {
                assert!(
                    names.contains(candidate),
                    "{note}: no test fn named {candidate}"
                );
                checked += 1;
            }
        }
    }
    checked
}

// AC-47
#[test]
fn design_note_ac_table_names_existing_tests() {
    let checked = ac_table_names_existing_tests("DESIGN-durable-routes.md", "| AC-");
    assert!(checked >= 40, "only {checked} test names checked");
}

// SAC-37
#[test]
fn stream_design_note_ac_table_names_existing_tests() {
    let checked = ac_table_names_existing_tests("DESIGN-durable-streaming.md", "| SAC-");
    assert!(checked >= 38, "only {checked} test names checked");
}

/// Count the table rows of `text` that start with `prefix` and a digit.
fn numbered_rows<'a>(text: &'a str, prefix: &str) -> Vec<&'a str> {
    text.lines()
        .filter(|line| {
            line.strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
        })
        .collect()
}

// SAC-37
#[test]
fn stream_design_note_keeps_its_planning_sections() {
    let design = read("DESIGN-durable-streaming.md");
    for heading in [
        "### 0.1 Facts found before the plan",
        "### 0.2 Brainstorm",
        "### 0.3 Reverse brainstorm",
        "### 0.4 Six thinking hats",
        "## 2. Known limits",
        "## 3. Tests and acceptance criteria",
    ] {
        assert!(design.contains(heading), "the design note lost {heading:?}");
    }
    for (prefix, least) in [("| F", 15), ("| B", 20), ("| R", 25)] {
        let rows = numbered_rows(&design, prefix).len();
        assert!(rows >= least, "only {rows} rows that start with {prefix}");
    }
    for row in numbered_rows(&design, "| R") {
        assert!(
            row.split('|').nth(3).is_some_and(|m| !m.trim().is_empty()),
            "a harm lost its mitigation: {row}"
        );
    }
    for hat in ["White", "Red", "Black", "Yellow", "Green", "Blue"] {
        assert!(
            design.contains(&format!("| {hat} |")),
            "the hats lost {hat}"
        );
    }
    let start = design.find("## 2. Known limits").expect("known limits");
    let end = design
        .find("## 3. Tests and acceptance criteria")
        .expect("AC table");
    for limit in [
        "EventSource",
        "at-least-once",
        "operator rerun",
        "durable_route_max_chain_steps",
    ] {
        assert!(
            design[start..end].contains(limit),
            "the known limits lost {limit:?}"
        );
    }
}

// SAC-36
#[test]
fn streaming_section_states_the_client_contract() {
    let page = one_line(&read("docs/durable-routes.md"));
    let start = page
        .find("## Stream the run")
        .expect("the page has a streaming section");
    let section = &page[start..];
    let section = &section[..section[3..].find(" ## ").map_or(section.len(), |i| i + 3)];
    for needle in [
        ".stream()",
        "publish_durable_progress",
        "Last-Event-ID",
        "`<attempt>.<offset>`",
        "event: progress",
        "event: attempt",
        "event: result",
        "event: problem",
        "event: pending",
        "event: activity",
        ".live()",
        "`stream-cursor-invalid`",
        "durable_route_max_streams",
        "EventSource",
        "x-accel-buffering",
        "Do not turn on `.idempotent()`",
        "dedup",
    ] {
        assert!(
            section.contains(needle),
            "the streaming section must mention {needle:?}"
        );
    }
    let secs = |d: std::time::Duration| d.as_secs();
    for needle in [
        format!("The default is {DEFAULT_MAX_STREAMS}."),
        format!(
            "The default budget is {} min",
            secs(DEFAULT_STREAM_FOR) / 60
        ),
        format!("at most {} h", secs(MAX_STREAM_FOR) / 3600),
    ] {
        assert!(
            section.contains(&needle),
            "the streaming section must say {needle:?}"
        );
    }
}

// AC-47
#[test]
fn design_note_keeps_its_planning_sections() {
    let design = read("DESIGN-durable-routes.md");
    for heading in [
        "### 0.1 Facts found before the plan",
        "### 0.2 Brainstorm",
        "### 0.3 Reverse brainstorm",
        "### 0.4 Six thinking hats",
        "## 2. Known limits",
        "## 3. Tests and acceptance criteria",
    ] {
        assert!(design.contains(heading), "the design note lost {heading:?}");
    }
    // Each reverse-brainstorm row keeps its mitigation.
    let harms: Vec<&str> = design
        .lines()
        .filter(|line| {
            line.strip_prefix("| R")
                .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
        })
        .collect();
    assert!(harms.len() >= 25, "only {} harms", harms.len());
    // The facts and the brainstorm keep their rows.
    for (prefix, least) in [("| F", 15), ("| B", 20)] {
        let rows = design
            .lines()
            .filter(|line| {
                line.strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
            })
            .count();
        assert!(rows >= least, "only {rows} rows that start with {prefix}");
    }
    for row in &harms {
        assert!(
            row.split('|').nth(3).is_some_and(|m| !m.trim().is_empty()),
            "a harm lost its mitigation: {row}"
        );
    }
    // The known limits keep their bullets.
    let start = design.find("## 2. Known limits").expect("known limits");
    let end = design
        .find("## 3. Tests and acceptance criteria")
        .expect("AC table");
    let limits = design[start..end]
        .lines()
        .filter(|line| line.starts_with("- "))
        .count();
    assert!(limits >= 20, "only {limits} known limits");
    for limit in [
        "Postgres plugin runtime only",
        "refuses a pool of more than one shard",
        "After an operator rerun",
        "A reset seal",
        "unsalted SHA-256",
        "No in-process single-flight",
    ] {
        assert!(
            design[start..end].contains(limit),
            "the known limits lost {limit:?}"
        );
    }
}

// AC-48
#[test]
fn changelog_fragment_describes_the_feature() {
    let dir = workspace_root().join("docs/changelog.d");
    let fragments: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("changelog.d")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-durable-routes.md"))
        })
        .collect();
    assert_eq!(fragments.len(), 1, "exactly one durable-routes fragment");
    let text = std::fs::read_to_string(&fragments[0]).expect("fragment");
    assert!(
        text.starts_with("## "),
        "the fragment starts with a heading"
    );
    assert!(text.contains("Durable routes"));
    assert!(text.contains("No migration"));
    assert!(text.contains("No new `WorkflowEvent`"));
}

// SAC-38
#[test]
fn stream_changelog_fragment_describes_the_feature() {
    let dir = workspace_root().join("docs/changelog.d");
    let fragments: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("changelog.d")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-durable-streaming.md"))
        })
        .collect();
    assert_eq!(fragments.len(), 1, "exactly one durable-streaming fragment");
    let text = std::fs::read_to_string(&fragments[0]).expect("fragment");
    assert!(
        text.starts_with("## "),
        "the fragment starts with a heading"
    );
    assert!(text.contains("Durable streaming"));
    assert!(text.contains("No migration"));
    assert!(text.contains("No new `WorkflowEvent`"));
}
