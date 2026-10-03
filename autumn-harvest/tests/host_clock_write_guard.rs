//! Guard for issue #1807: no host-clock write to a column that a timeout scan
//! compares with the database `NOW()`.
//!
//! A worker host that runs behind the database causes false timeouts. A host
//! that runs ahead hides stuck work. These columns must take their value from
//! the database clock, as in `queue::record_heartbeat`.
//!
//! The scan is textual. It finds `.eq(..Utc::now()..)` and `column: ..Utc::now()`
//! in non-test code. It does not follow a value through a local variable.
//!
//! A deliberate host-clock write carries `host-clock-ok: <reason>` on the same
//! line or the line above.

use std::fs;
use std::path::{Path, PathBuf};

/// Columns that timeout scans compare with `NOW()` (see `timeout.rs`).
const SCAN_COLUMNS: &[&str] = &[
    "last_heartbeat_at",
    "scheduled_at",
    "schedule_to_close_at",
];

const MARKER: &str = "host-clock-ok:";

/// Return `(line, text)` for each violating line in `source`.
fn violations(source: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        let test_cfg = trimmed.starts_with("#[cfg(") && trimmed.contains("test");
        if test_cfg
            && lines.get(index + 1).is_some_and(|next| next.trim_start().starts_with("mod "))
        {
            break;
        }
        if line.trim_start().starts_with("//") || !line.contains("Utc::now") {
            continue;
        }
        let allowed = line.contains(MARKER)
            || index.checked_sub(1).is_some_and(|prev| lines[prev].contains(MARKER));
        if allowed {
            continue;
        }
        let hits_column = SCAN_COLUMNS.iter().any(|column| {
            line.contains(&format!("{column}.eq(")) || line.contains(&format!("{column}:"))
        });
        if hits_column {
            found.push((index + 1, (*line).to_string()));
        }
    }
    found
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_host_clock_write_to_timeout_scan_columns() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    let mut report = Vec::new();
    for file in files {
        let source = fs::read_to_string(&file).expect("read source");
        for (line, text) in violations(&source) {
            report.push(format!("{}:{line}: {}", file.display(), text.trim()));
        }
    }
    assert!(
        report.is_empty(),
        "host-clock write to a column compared with NOW() (issue #1807). \
         Use `clock_timestamp()` in SQL, or add `{MARKER} <reason>`:\n{}",
        report.join("\n")
    );
}

#[test]
fn scanner_flags_a_host_clock_heartbeat() {
    let source = "x.set(dsl::last_heartbeat_at.eq(Some(Utc::now())));";
    assert_eq!(violations(source).len(), 1);
}

#[test]
fn scanner_flags_a_struct_field_write() {
    let source = "Row { scheduled_at: Utc::now() - skew, other: 1 }";
    assert_eq!(violations(source).len(), 1);
}

#[test]
fn scanner_ignores_unrelated_columns() {
    let source = "x.set(dsl::completed_at.eq(Some(Utc::now())));";
    assert!(violations(source).is_empty());
}

#[test]
fn scanner_accepts_a_marked_write() {
    let same_line = "dsl::scheduled_at.eq(Utc::now()) // host-clock-ok: demo";
    let line_above = "// host-clock-ok: demo\ndsl::scheduled_at.eq(Utc::now())";
    assert!(violations(same_line).is_empty());
    assert!(violations(line_above).is_empty());
}

#[test]
fn scanner_skips_the_test_module() {
    let source = "#[cfg(test)]\nmod tests {\n    dsl::scheduled_at.eq(Utc::now());\n}";
    assert!(violations(source).is_empty());
}

#[test]
fn scanner_accepts_a_database_clock_write() {
    let source = "dsl::last_heartbeat_at.eq(sql::<Nullable<Timestamptz>>(\"clock_timestamp()\"))";
    assert!(violations(source).is_empty());
}
