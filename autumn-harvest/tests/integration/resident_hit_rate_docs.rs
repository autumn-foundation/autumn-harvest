//! Guards the docs of the resident hit-rate counter (issue #2007).
//!
//! The counter has a closed set of reasons. An operator reads them from
//! `docs/telemetry.md`. These tests fail when a reason is added in code and
//! not in the docs.

use std::path::{Path, PathBuf};

use autumn_harvest::resident::{RESIDENT_HIT_REASON, RESIDENT_MISS_REASONS};
use autumn_harvest::telemetry::METRIC_WORKFLOW_RESIDENT;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

/// The catalogue row of the counter in `docs/telemetry.md`.
fn telemetry_row() -> String {
    let needle = format!("| `{METRIC_WORKFLOW_RESIDENT}` |");
    read("docs/telemetry.md")
        .lines()
        .find(|line| line.starts_with(&needle))
        .unwrap_or_else(|| panic!("docs/telemetry.md has no row for {METRIC_WORKFLOW_RESIDENT}"))
        .to_owned()
}

#[test]
fn the_counter_has_its_documented_name() {
    assert_eq!(METRIC_WORKFLOW_RESIDENT, "harvest.workflow.resident");
}

#[test]
fn the_telemetry_row_names_every_reason() {
    let row = telemetry_row();
    for reason in RESIDENT_MISS_REASONS
        .iter()
        .chain(std::iter::once(&RESIDENT_HIT_REASON))
    {
        assert!(
            row.contains(&format!("`{reason}`")),
            "the {METRIC_WORKFLOW_RESIDENT} row does not name the reason `{reason}`"
        );
    }
}

#[test]
fn the_sticky_routing_page_lists_the_counter() {
    let page = read("docs/sticky-routing.md");
    assert!(
        page.contains(&format!("| `{METRIC_WORKFLOW_RESIDENT}` |")),
        "docs/sticky-routing.md must list {METRIC_WORKFLOW_RESIDENT} in its metrics table"
    );
}
