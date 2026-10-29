//! Guards for the runtime parameter inventory (issue #2391).
//!
//! `docs/design/DESIGN-2391.md` has one row for each field of `WorkflowInfo`,
//! `DagInfo`, `ActivityInfo` and `WorkflowSchedule`. A new field must get a
//! row in the same PR, so its runtime story is decided when it lands.
//!
//! The guards read the four structs from the source at test time. They fail
//! when a field has no row, and when a row names a field that no longer
//! exists. They also check that each row makes one of the three
//! recommendations, and that each override names its child issue.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The structs that the inventory covers, with the file that declares each.
const STRUCTS: [(&str, &str); 4] = [
    ("WorkflowInfo", "autumn-harvest/src/info.rs"),
    ("DagInfo", "autumn-harvest/src/info.rs"),
    ("ActivityInfo", "autumn-harvest/src/info.rs"),
    ("WorkflowSchedule", "autumn-harvest/src/policy.rs"),
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a file with line endings normalised to `\n`.
fn read(relative: &str) -> String {
    std::fs::read_to_string(repo_root().join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
        .replace("\r\n", "\n")
}

/// The public fields of one struct, as `Struct.field`.
fn struct_fields(name: &str, source: &str) -> BTreeSet<String> {
    let header = format!("pub struct {name} {{");
    let start = source
        .find(&header)
        .unwrap_or_else(|| panic!("`{header}` not found"));
    let body = &source[start + header.len()..];
    let end = body.find("\n}").expect("struct body must close");
    body[..end]
        .lines()
        .filter_map(|line| line.strip_prefix("    pub "))
        .filter_map(|rest| rest.split_once(':'))
        .map(|(field, _)| format!("{name}.{field}"))
        .collect()
}

fn source_fields() -> BTreeSet<String> {
    STRUCTS
        .iter()
        .flat_map(|(name, file)| struct_fields(name, &read(file)))
        .collect()
}

/// One inventory row: its field and its recommendation cell.
struct Row {
    field: String,
    recommendation: String,
}

/// The inventory rows, read from the first cell of each table row.
fn inventory_rows() -> Vec<Row> {
    read("docs/design/DESIGN-2391.md")
        .lines()
        .filter_map(|line| {
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
            // A row is `| field | runtime | cost | recommendation |`.
            if cells.len() != 6 {
                return None;
            }
            let field = cells[1].strip_prefix('`')?.strip_suffix('`')?;
            let (owner, _) = field.split_once('.')?;
            STRUCTS.iter().any(|(name, _)| *name == owner).then(|| Row {
                field: field.to_string(),
                recommendation: cells[4].to_string(),
            })
        })
        .collect()
}

#[test]
fn every_field_has_a_row() {
    let documented: BTreeSet<String> = inventory_rows().into_iter().map(|row| row.field).collect();
    let missing: Vec<String> = source_fields()
        .into_iter()
        .filter(|field| !documented.contains(field))
        .collect();
    assert!(
        missing.is_empty(),
        "add a row to docs/design/DESIGN-2391.md for each new field: {missing:?}"
    );
}

#[test]
fn every_row_names_a_field_that_exists() {
    let fields = source_fields();
    let stale: Vec<String> = inventory_rows()
        .into_iter()
        .map(|row| row.field)
        .filter(|field| !fields.contains(field))
        .collect();
    assert!(
        stale.is_empty(),
        "remove or rename the rows of fields that no longer exist: {stale:?}"
    );
}

#[test]
fn each_field_has_exactly_one_row() {
    let mut seen = BTreeSet::new();
    let duplicates: Vec<String> = inventory_rows()
        .into_iter()
        .map(|row| row.field)
        .filter(|field| !seen.insert(field.clone()))
        .collect();
    assert!(duplicates.is_empty(), "duplicate rows: {duplicates:?}");
}

#[test]
fn every_row_makes_one_recommendation() {
    let bad: Vec<String> = inventory_rows()
        .into_iter()
        .filter(|row| {
            let r = row.recommendation.as_str();
            !(r == "already runtime" || r == "deploy-only" || r.starts_with("override (#"))
        })
        .map(|row| format!("{}: {}", row.field, row.recommendation))
        .collect();
    assert!(
        bad.is_empty(),
        "use `override (#issue)`, `already runtime` or `deploy-only`: {bad:?}"
    );
}

#[test]
fn every_override_issue_is_in_the_child_issue_table() {
    let doc = read("docs/design/DESIGN-2391.md");
    let children = doc
        .split_once("## Child issues")
        .map(|(_, rest)| rest)
        .expect("the record has a `## Child issues` section");
    let missing: Vec<String> = inventory_rows()
        .into_iter()
        .filter_map(|row| {
            let issue = row.recommendation.strip_prefix("override (")?;
            let issue = issue.split(')').next()?.to_string();
            (!children.contains(&format!("| {issue} |"))).then(|| format!("{}: {issue}", row.field))
        })
        .collect();
    assert!(
        missing.is_empty(),
        "list each override issue in `## Child issues`: {missing:?}"
    );
}
