//! Docs guards for durable routes.
//!
//! The guards read the docs page, the design note and the changelog fragment
//! as data. They keep the status table in step with the code and keep each
//! acceptance criterion tied to a test that exists.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use autumn_harvest_plugin::durable_route::PROBLEM_SLUGS;

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

// AC-46
#[test]
fn page_states_layer_timeout_and_key_guidance() {
    let page = read("docs/durable-routes.md");
    for needle in [
        "Retry-After",
        "same `Idempotency-Key`",
        "random",
        "in_flight_ttl_secs",
        "timeout_ms",
        "#[intercept]",
        "durable_route_max_waiters",
        "AppBuilder::layer",
        "idempotency.ttl_secs",
        "is final for this key",
        "## Known limits",
    ] {
        assert!(
            page.contains(needle),
            "docs/durable-routes.md must mention {needle:?}"
        );
    }
}

/// Every `fn` name in the plugin sources and tests.
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
                for line in text.lines() {
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

// AC-47
#[test]
fn design_note_ac_table_names_existing_tests() {
    let design = read("DESIGN-durable-routes.md");
    let start = design
        .find("## 3. Tests and acceptance criteria")
        .expect("the design note has an AC table");
    let names = plugin_fn_names();
    let mut checked = 0;
    for line in design[start..].lines().filter(|l| l.starts_with("| AC-")) {
        let test_cell = line.split('|').nth(3).expect("test cell");
        for candidate in test_cell.split('`').skip(1).step_by(2) {
            let is_fn_name = candidate
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if is_fn_name {
                assert!(names.contains(candidate), "no test fn named {candidate}");
                checked += 1;
            }
        }
    }
    assert!(checked >= 40, "only {checked} test names checked");
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
    assert!(text.contains("`WorkflowEvent`"));
}
