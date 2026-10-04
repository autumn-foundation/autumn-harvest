//! Guards the lock-order table in `docs/architecture.md` (issue #1822).
//!
//! The table lists the main lock-ordering rules of the engine. These guards
//! keep it next to the ABBA argument, keep that argument intact, and keep each
//! cited pin test real. A renamed or deleted test fails here, so the table
//! cannot drift from the code that proves it.

use std::path::{Path, PathBuf};

const TABLE_HEADING: &str = "#### Lock-order table";
const ABBA_ANCHOR: &str =
    "**Lock-ordering convention for `materialize_due_child_timeout_deadlines`";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn architecture() -> String {
    let path = repo_root().join("docs/architecture.md");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The table section: from its heading to the next heading.
fn table_section(doc: &str) -> &str {
    let start = doc
        .find(TABLE_HEADING)
        .expect("docs/architecture.md must hold the lock-order table");
    let body = &doc[start + TABLE_HEADING.len()..];
    let end = body.find("\n#").unwrap_or(body.len());
    &body[..end]
}

/// Cells of one Markdown table line, trimmed.
fn cells(line: &str) -> Vec<String> {
    line.trim_matches('|')
        .split(" | ")
        .map(|cell| cell.trim().to_owned())
        .collect()
}

/// A separator line such as `|---|:---:|`.
fn is_separator(line: &str) -> bool {
    line.contains('-') && line.chars().all(|c| matches!(c, '|' | '-' | ':' | ' '))
}

/// Data rows of every Markdown table in `section`.
///
/// Each table must start with the `# | Lock order | ...` header, so a renamed
/// header fails here and does not pass as a data row.
fn table_rows(section: &str) -> Vec<Vec<String>> {
    let lines: Vec<&str> = section
        .lines()
        .filter(|line| line.starts_with('|'))
        .collect();
    let mut rows = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if is_separator(line) {
            continue;
        }
        let row = cells(line);
        if row.first().is_some_and(|c| c == "#") {
            assert_eq!(
                row[1..],
                ["Lock order", "Where", "Why", "Pinned by"],
                "unexpected table header {row:?}"
            );
            assert!(
                lines.get(i + 1).is_some_and(|next| is_separator(next)),
                "a separator line must follow the header"
            );
        } else {
            rows.push(row);
        }
    }
    rows
}

/// Backticked names in `cell`.
fn backticked(cell: &str) -> Vec<&str> {
    cell.split('`').skip(1).step_by(2).collect()
}

/// Whether `text` defines `name` as a test function.
///
/// The test attribute must sit within the five lines above the `fn` line.
fn is_test_fn(text: &str, name: &str) -> bool {
    let lines: Vec<&str> = text.lines().collect();
    let needle = format!("fn {name}(");
    lines.iter().enumerate().any(|(i, line)| {
        line.contains(&needle)
            && lines[i.saturating_sub(5)..i]
                .iter()
                .any(|above| above.contains("#[test]") || above.contains("#[tokio::test"))
    })
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn the_table_follows_the_abba_argument() {
    let doc = architecture();
    let abba = doc
        .find(ABBA_ANCHOR)
        .expect("the ABBA argument must stay in docs/architecture.md");
    let table = doc.find(TABLE_HEADING).expect("table heading");
    assert!(table > abba, "the table must come after the ABBA argument");
    let between = &doc[abba..table];
    assert!(
        !between.contains("\n#"),
        "no other heading may sit between the ABBA argument and the table"
    );
}

#[test]
fn the_abba_rationale_is_intact() {
    let doc = architecture();
    let abba = &doc[doc.find(ABBA_ANCHOR).expect("ABBA argument")..];
    let paragraph = &abba[..abba.find("\n\n").unwrap_or(abba.len())];
    for needle in [
        "parent execution row `FOR UPDATE` FIRST, then the due `harvest_timers` rows `FOR UPDATE`",
        "Had the materializer taken the timer lock first, those two orderings would invert (ABBA)",
        "Postgres would abort a healthy terminal notification",
        "unifies every call site onto execution-row → timer, so no cycle is possible",
        "`materializer_locks_execution_row_before_timers_no_abba`",
    ] {
        assert!(
            paragraph.contains(needle),
            "the ABBA argument lost its rationale: missing {needle:?}"
        );
    }
}

#[test]
fn every_row_names_its_locks_site_and_reason() {
    let doc = architecture();
    let rows = table_rows(table_section(&doc));
    assert!(
        rows.len() >= 12,
        "the table lists the known orders; got {rows:?}"
    );
    for row in &rows {
        assert_eq!(row.len(), 5, "each row has five cells: {row:?}");
        for cell in &row[1..4] {
            assert!(!cell.is_empty(), "empty cell in {row:?}");
        }
    }
}

#[test]
fn every_cited_pin_test_exists() {
    let doc = architecture();
    let rows = table_rows(table_section(&doc));
    let mut sources = Vec::new();
    rust_sources(&repo_root().join("autumn-harvest/src"), &mut sources);
    rust_sources(&repo_root().join("autumn-harvest/tests"), &mut sources);
    let texts: Vec<String> = sources
        .iter()
        .map(|path| std::fs::read_to_string(path).unwrap_or_default())
        .collect();

    let mut cited = 0;
    for row in &rows {
        for name in backticked(&row[4]) {
            cited += 1;
            assert!(
                texts.iter().any(|text| is_test_fn(text, name)),
                "the lock-order table cites `{name}`, but no test has that name"
            );
        }
    }
    assert!(cited >= 5, "most rows cite a pin test; got {cited}");
}

#[test]
fn the_retry_suite_is_cited() {
    let doc = architecture();
    let section = table_section(&doc);
    assert!(
        section.contains("`two_persist_transactions_deadlock_and_both_commit`"),
        "the known-cycle rows must cite the issue #1822 retry test"
    );
    assert!(
        section.contains("harvest.db.transaction_retry"),
        "the table must name the retry metric"
    );
}
