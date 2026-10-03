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
const SCAN_COLUMNS: &[&str] = &["last_heartbeat_at", "scheduled_at", "schedule_to_close_at"];

const MARKER: &str = "host-clock-ok:";

/// Upper bound on the bytes that one write expression may span.
const STATEMENT_WINDOW: usize = 300;

/// Blank out `//` comments so a comment cannot hide or fake a write.
fn strip_comments(source: &str) -> String {
    source
        .lines()
        .map(|line| line.find("//").map_or(line, |at| &line[..at]))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Byte offset of the last byte of a raw string that starts at `at`.
fn raw_string_end(bytes: &[u8], at: usize) -> Option<usize> {
    if at > 0 && (bytes[at - 1].is_ascii_alphanumeric() || bytes[at - 1] == b'_') {
        return None;
    }
    let mut cursor = at + 1;
    let mut hashes = 0;
    while bytes.get(cursor) == Some(&b'#') {
        hashes += 1;
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'"') {
        return None;
    }
    let mut closing = vec![b'"'];
    closing.extend(std::iter::repeat_n(b'#', hashes));
    let body = &bytes[cursor + 1..];
    body.windows(closing.len())
        .position(|window| window == closing.as_slice())
        .map(|found| cursor + 1 + found + closing.len() - 1)
}

/// Byte offset of the `}` that closes the block opened at `open`.
///
/// It skips braces inside string, raw string and character literals.
fn matching_brace(code: &str, open: usize) -> Option<usize> {
    let bytes = code.as_bytes();
    let mut depth = 0_usize;
    let mut index = open;
    while index < bytes.len() {
        match bytes[index] {
            b'r' if raw_string_end(bytes, index).is_some() => {
                index = raw_string_end(bytes, index).unwrap_or(index);
            }
            b'"' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
            }
            b'\'' if bytes.get(index + 2) == Some(&b'\'') => index += 2,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// Replace each test-gated module with spaces, keeping line breaks.
///
/// Code after a test module stays visible to the scan.
fn blank_test_modules(code: &str) -> String {
    let lines: Vec<&str> = code.lines().collect();
    let mut starts = Vec::with_capacity(lines.len());
    let mut offset = 0;
    for line in &lines {
        starts.push(offset);
        offset += line.len() + 1;
    }
    let mut out = code.to_string();
    for (index, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("mod ") {
            continue;
        }
        let first = index.saturating_sub(3);
        let gated = lines[first..index].iter().any(|above| {
            let above = above.trim();
            above.starts_with("#[cfg(") && above.contains("test") && !above.contains("not(test")
        });
        if !gated {
            continue;
        }
        let from = starts[index];
        let Some(open) = code[from..].find(['{', ';']).map(|at| from + at) else {
            continue;
        };
        if code.as_bytes()[open] != b'{' {
            continue;
        }
        let close = matching_brace(code, open).unwrap_or(code.len() - 1);
        {
            let blanked: String = code[from..=close]
                .chars()
                .map(|c| {
                    if c == '\n' {
                        "\n".to_string()
                    } else {
                        " ".repeat(c.len_utf8())
                    }
                })
                .collect();
            out.replace_range(from..=close, &blanked);
        }
    }
    out
}

fn line_of(source: &str, offset: usize) -> usize {
    source[..offset].matches('\n').count() + 1
}

/// Whether `column` at `at` is a write target: `.eq(`, `:` or `=`.
fn is_write_target(code: &str, column: &str, at: usize) -> bool {
    let before = code[..at].chars().next_back();
    if before.is_some_and(|c| c.is_alphanumeric() || c == '_') {
        return false;
    }
    let rest = code[at + column.len()..].trim_start();
    rest.starts_with(".eq(")
        || (rest.starts_with(':') && !rest.starts_with("::"))
        || (rest.starts_with('=') && !rest.starts_with("==") && !rest.starts_with("=>"))
}

/// Return `(line, text)` for each violating write in `source`.
///
/// A write is a scan column followed by `.eq(`, `:` or `=`, with `Utc::now`
/// inside the same expression. The expression can span lines.
fn violations(source: &str) -> Vec<(usize, String)> {
    let original: Vec<&str> = source.lines().collect();
    let code = blank_test_modules(&strip_comments(source));
    let mut found = Vec::new();
    for column in SCAN_COLUMNS {
        for (at, _) in code.match_indices(column) {
            if !is_write_target(&code, column, at) {
                continue;
            }
            let window = expression_after(&code, at + column.len());
            if !window.contains("Utc::now") {
                continue;
            }
            let first = line_of(&code, at);
            let last = first + window.matches('\n').count();
            let marked = (first.saturating_sub(1)..=last)
                .filter(|line| *line >= 1)
                .any(|line| {
                    original
                        .get(line - 1)
                        .is_some_and(|text| text.contains(MARKER))
                });
            if !marked {
                found.push((first, original[first - 1].to_string()));
            }
        }
    }
    found.sort_unstable();
    found.dedup();
    found
}

/// The expression that follows a column name.
///
/// It ends at a `,` or `;` outside brackets, at a closing bracket that has no
/// opener, or after `STATEMENT_WINDOW` bytes.
fn expression_after(code: &str, from: usize) -> &str {
    let mut depth = 0_i32;
    for (offset, ch) in code[from..].char_indices() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth < 0 {
                    return &code[from..from + offset];
                }
            }
            ',' | ';' if depth == 0 => return &code[from..from + offset],
            _ => {}
        }
        if offset >= STATEMENT_WINDOW {
            return &code[from..from + offset];
        }
    }
    &code[from..]
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

fn assert_clean(source: &str) {
    assert_eq!(violations(source), Vec::<(usize, String)>::new());
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
    assert_clean(source);
}

#[test]
fn scanner_accepts_a_marked_write() {
    let same_line = "dsl::scheduled_at.eq(Utc::now()) // host-clock-ok: demo";
    let line_above = "// host-clock-ok: demo\ndsl::scheduled_at.eq(Utc::now())";
    assert_clean(same_line);
    assert_clean(line_above);
}

#[test]
fn scanner_skips_the_test_module() {
    let source = "#[cfg(test)]\nmod tests {\n    dsl::scheduled_at.eq(Utc::now());\n}";
    assert_clean(source);
}

#[test]
fn scanner_accepts_a_database_clock_write() {
    let source = "dsl::last_heartbeat_at.eq(sql::<Nullable<Timestamptz>>(\"clock_timestamp()\"))";
    assert_clean(source);
}

#[test]
fn scanner_flags_a_call_split_across_lines() {
    let source = "dsl::scheduled_at\n    .eq(Utc::now()),";
    assert_eq!(violations(source).len(), 1);
}

#[test]
fn scanner_flags_a_qualified_path_and_a_local_binding() {
    assert_eq!(
        violations("x.eq(chrono::Utc::now()); let scheduled_at = 1;").len(),
        0
    );
    assert_eq!(
        violations("dsl::scheduled_at.eq(chrono::Utc::now())").len(),
        1
    );
    assert_eq!(
        violations("let schedule_to_close_at = Utc::now() + d;").len(),
        1
    );
}

#[test]
fn scanner_ignores_a_longer_column_name() {
    assert_clean("let my_scheduled_at = Utc::now();");
    assert_clean("if scheduled_at == Utc::now() {}");
}

#[test]
fn scanner_ignores_a_comment() {
    assert_clean("// scheduled_at: Utc::now() is wrong");
}

#[test]
fn scanner_keeps_scanning_after_a_test_module() {
    let source = "#[cfg(test)]\nmod tests {\n let s = \"}\";\n}\nx.scheduled_at.eq(Utc::now());";
    assert_eq!(violations(source).len(), 1);
}

#[test]
fn scanner_keeps_scanning_after_a_not_test_module() {
    let source = "#[cfg(not(test))]\nmod real {}\nx.scheduled_at.eq(Utc::now());";
    assert_eq!(violations(source).len(), 1);
}

#[test]
fn scanner_skips_a_test_module_with_an_extra_attribute() {
    let source =
        "#[cfg(test)]\n#[allow(clippy::all)]\nmod tests {\n scheduled_at.eq(Utc::now());\n}";
    assert_clean(source);
}

#[test]
fn scanner_accepts_a_marker_above_a_split_call() {
    let source = "// host-clock-ok: demo\ndsl::scheduled_at\n    .eq(Utc::now())";
    assert_clean(source);
}
