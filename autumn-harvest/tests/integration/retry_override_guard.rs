//! Guard: each schedule path and each decode of a stored retry policy keeps
//! its source (issue #2409).
//!
//! An operator override of a workflow retry policy applies only to a policy
//! that the workflow type declares. A schedule's own policy wins. The stored
//! JSON of a run marks a schedule policy with `workflow_retry::SOURCE_KEY`.
//! Three mistakes lose that mark:
//!
//! - a schedule path that picks the schedule policy by hand,
//! - a start of a schedule run that does not take its source from the helper,
//!   and
//! - a decode of the stored JSON with `serde_json::from_value`, which drops
//!   the mark.
//!
//! The scan is textual. It drops a line after its first `//`. The DB tests in
//! `retry_override_db_tests` prove the behaviour of the paths that they
//! drive.

use std::path::{Path, PathBuf};

/// The source trees to scan, from the repository root.
const SOURCE_DIRS: [&str; 4] = [
    "autumn-harvest/src",
    "autumn-harvest-plugin/src",
    "autumn-harvest-macros/src",
    "autumn-harvest-agent/src",
];

/// The files that fire a schedule, with the receiver name of a schedule row.
const SCHEDULE_FILES: [(&str, &str); 3] = [
    ("autumn-harvest/src/scheduler.rs", "schedule"),
    ("autumn-harvest-plugin/src/api.rs", "schedule"),
    ("autumn-harvest-plugin/src/ui.rs", "row"),
];

/// The lines that may decode a stored retry policy by hand, as
/// `(file, line)`. Each is a test that reads `max_attempts` back.
const ALLOWED_DECODES: [(&str, &str); 3] = [
    (
        "autumn-harvest/src/worker.rs",
        "let stored: RetryPolicy = serde_json::from_value(",
    ),
    (
        "autumn-harvest/src/worker.rs",
        r#"serde_json::from_value(under.workflow_retry_policy.expect("policy carried"))"#,
    ),
    (
        "autumn-harvest/src/worker.rs",
        r#"serde_json::from_value(unbounded.workflow_retry_policy.expect("policy carried"))"#,
    ),
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn rust_files(root: &Path, dir: &str, out: &mut Vec<String>) {
    let entries =
        std::fs::read_dir(root.join(dir)).unwrap_or_else(|error| panic!("read {dir}: {error}"));
    for entry in entries {
        let path = entry.expect("dir entry").path();
        let relative = path
            .strip_prefix(root)
            .expect("under the root")
            .to_string_lossy()
            .replace('\\', "/");
        if path.is_dir() {
            rust_files(root, &relative, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(relative);
        }
    }
}

fn scanned_files() -> Vec<String> {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in SOURCE_DIRS {
        rust_files(&root, dir, &mut files);
    }
    files.sort();
    files
}

fn read(file: &str) -> String {
    std::fs::read_to_string(repo_root().join(file))
        .unwrap_or_else(|error| panic!("read {file}: {error}"))
}

/// `source` with each `//` comment removed.
fn code(source: &str) -> String {
    source
        .lines()
        .map(|line| line.split("//").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The line number of byte offset `at`.
fn line_of(code: &str, at: usize) -> usize {
    code[..at].matches('\n').count() + 1
}

/// The trimmed text of the line that holds byte offset `at`.
fn line_text(code: &str, at: usize) -> &str {
    let start = code[..at].rfind('\n').map_or(0, |i| i + 1);
    let end = code[at..].find('\n').map_or(code.len(), |i| at + i);
    code[start..end].trim()
}

/// Whether `c` can sit in a Rust identifier.
fn ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The byte offset of each `<receiver>.retry_policy` in `code`. Space and
/// line breaks may sit around the dot.
fn receiver_reads(code: &str, receiver: &str) -> Vec<usize> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(at) = code[from..].find(receiver) {
        let start = from + at;
        from = start + receiver.len();
        if code[..start].chars().next_back().is_some_and(ident_char) {
            continue;
        }
        let Some(rest) = code[from..].trim_start().strip_prefix('.') else {
            continue;
        };
        let rest = rest.trim_start();
        let field = "retry_policy";
        if rest.starts_with(field) && !rest[field.len()..].chars().next().is_some_and(ident_char) {
            found.push(start);
        }
    }
    found
}

/// Whether the read at `at` is an argument of a `schedule_retry_*` call in
/// the same statement.
fn inside_helper(code: &str, at: usize) -> bool {
    let head = &code[..at];
    head.rfind("schedule_retry_")
        .is_some_and(|call| !head[call..].contains(';'))
}

/// The text from `open`, an opening bracket, to its matching close.
fn bracketed(code: &str, open: usize) -> &str {
    let mut depth = 0_usize;
    for (offset, c) in code[open..].char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    return &code[open..=open + offset];
                }
            }
            _ => {}
        }
    }
    &code[open..]
}

/// The struct literal that encloses byte offset `at`, and the text before it
/// on its line.
fn enclosing_literal(code: &str, at: usize) -> Option<(&str, &str)> {
    let mut depth = 0_usize;
    for (offset, c) in code[..at].char_indices().rev() {
        match c {
            '}' | ')' | ']' => depth += 1,
            '(' | '[' | '{' if depth > 0 => depth -= 1,
            '{' => {
                let head_start = code[..offset].rfind('\n').map_or(0, |i| i + 1);
                return Some((bracketed(code, offset), &code[head_start..offset]));
            }
            _ => {}
        }
    }
    None
}

/// The value expression of field `name` in a struct literal, up to its
/// first top-level comma.
fn field_value<'a>(literal: &'a str, name: &str) -> Option<&'a str> {
    let at = literal.find(&format!("{name}:"))? + name.len() + 1;
    let rest = &literal[at..];
    let mut depth = 0_usize;
    for (offset, c) in rest.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' | ',' if depth == 0 => return Some(rest[..offset].trim()),
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
    }
    Some(rest.trim())
}

/// The leading identifier of `expr`.
fn root_ident(expr: &str) -> &str {
    let end = expr.find(|c: char| !ident_char(c)).unwrap_or(expr.len());
    &expr[..end]
}

/// Whether `source` binds `name` from a call of `helper`, in a `let` alone or
/// in a tuple pattern.
fn bound_from(source: &str, name: &str, helper: &str) -> bool {
    source.split(';').any(|statement| {
        let Some(let_at) = statement.rfind("let ") else {
            return false;
        };
        let statement = &statement[let_at..];
        let Some(eq) = statement.find('=') else {
            return false;
        };
        let pattern = &statement[..eq];
        let binds = pattern
            .split(|c: char| !ident_char(c))
            .any(|word| word == name);
        binds && statement[eq..].contains(&format!("{helper}("))
    })
}

/// The reasons that the schedule start literals of `source` lose the source.
fn schedule_literal_offenders(file: &str, source: &str) -> Vec<String> {
    let mut offenders = Vec::new();
    let mut from = 0;
    while let Some(at) = source[from..].find("schedule_id: Some(") {
        let start = from + at;
        from = start + 1;
        let Some((literal, head)) = enclosing_literal(source, start) else {
            continue;
        };
        let Some(policy) = field_value(literal, "workflow_retry_policy") else {
            continue;
        };
        let line = line_of(source, start);
        if head.contains("DebounceStartOptions") {
            let root = root_ident(policy);
            let from_value = bound_from(source, root, "schedule_retry_value");
            let encoded = policy.contains("encode_stored(")
                && bound_from(source, root, "schedule_retry_policy");
            if !from_value && !encoded {
                offenders.push(format!(
                    "{file}:{line}: the JSON policy `{policy}` does not come from \
                     schedule_retry_value or a marked encode_stored"
                ));
            }
        } else {
            let Some(source_expr) = field_value(literal, "workflow_retry_source") else {
                offenders.push(format!(
                    "{file}:{line}: a schedule start sets workflow_retry_policy \
                     but no workflow_retry_source"
                ));
                continue;
            };
            if !bound_from(source, root_ident(source_expr), "schedule_retry_policy") {
                offenders.push(format!(
                    "{file}:{line}: workflow_retry_source `{source_expr}` does not \
                     come from schedule_retry_policy"
                ));
            }
        }
    }
    offenders
}

/// The byte offset of each hand decode of a stored retry policy in `code`.
///
/// A decode is a `serde_json::from_value` call whose argument names
/// `workflow_retry_policy`, or a `workflow_retry_policy` value whose
/// expression calls it.
fn hand_decodes(code: &str) -> Vec<usize> {
    let mut found = Vec::new();
    let call = "serde_json::from_value";
    let mut from = 0;
    while let Some(at) = code[from..].find(call) {
        let start = from + at;
        from = start + call.len();
        let rest = &code[from..];
        let Some(open) = rest.find('(') else {
            continue;
        };
        if rest[..open].contains(|c: char| !(c == ':' || c == '<' || c == '>' || ident_char(c))) {
            continue;
        }
        if bracketed(code, from + open).contains("workflow_retry_policy") {
            found.push(start);
        }
    }
    let mut from = 0;
    let field = "workflow_retry_policy";
    while let Some(at) = code[from..].find(field) {
        let start = from + at;
        from = start + field.len();
        let end = code[from..]
            .char_indices()
            .scan(0_usize, |depth, (offset, c)| {
                match c {
                    '(' | '[' | '{' => *depth += 1,
                    ')' | ']' | '}' | ',' | ';' if *depth == 0 => return None,
                    ')' | ']' | '}' => *depth -= 1,
                    _ => {}
                }
                Some(offset + c.len_utf8())
            })
            .last()
            .unwrap_or(0);
        if code[from..from + end].contains(call) && !found.contains(&start) {
            found.push(start);
        }
    }
    found
}

#[test]
fn each_schedule_path_picks_its_retry_policy_through_the_helper() {
    let mut offenders = Vec::new();
    for (file, receiver) in SCHEDULE_FILES {
        let source = code(&read(file));
        for at in receiver_reads(&source, receiver) {
            if !inside_helper(&source, at) {
                offenders.push(format!("{file}:{}", line_of(&source, at)));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these lines read a schedule retry_policy by hand. Use \
         `workflow_retry::schedule_retry_policy` or `schedule_retry_value`, \
         so the run stores the schedule source:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn each_schedule_start_takes_its_source_from_the_helper() {
    let mut offenders = Vec::new();
    let mut literals = 0;
    for file in scanned_files() {
        let source = code(&read(&file));
        literals += source.matches("schedule_id: Some(").count();
        offenders.extend(schedule_literal_offenders(&file, &source));
    }
    assert!(literals > 0, "the scan finds the schedule starts");
    assert!(
        offenders.is_empty(),
        "these schedule starts lose the schedule source of their retry \
         policy:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn no_path_decodes_a_stored_retry_policy_by_hand() {
    let mut offenders = Vec::new();
    for file in scanned_files() {
        if file == "autumn-harvest/src/workflow_retry.rs" {
            continue;
        }
        let source = code(&read(&file));
        for at in hand_decodes(&source) {
            let text = line_text(&source, at);
            let allowed = ALLOWED_DECODES
                .iter()
                .any(|(allowed_file, line)| *allowed_file == file && text == *line);
            if !allowed {
                offenders.push(format!("{file}:{}: {text}", line_of(&source, at)));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these lines decode a stored workflow_retry_policy with \
         serde_json::from_value, which drops its source. Use \
         `workflow_retry::decode_stored`:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn each_allowed_decode_matches_exactly_once() {
    for (file, allowed) in ALLOWED_DECODES {
        let count = read(file)
            .lines()
            .filter(|line| line.trim() == allowed)
            .count();
        assert_eq!(count, 1, "{file}: {allowed}");
    }
}

#[test]
fn the_failure_path_resolves_the_override_and_keeps_the_source() {
    let source = code(&read("autumn-harvest/src/worker.rs"));
    let body = source
        .split("pub async fn persist_workflow_failure(")
        .nth(1)
        .and_then(|rest| rest.split("\npub ").next())
        .expect("persist_workflow_failure exists");
    assert!(
        body.contains("current_retry_policy("),
        "the failure path applies the override"
    );
    let successor = field_value(body, "workflow_retry_source").expect("the successor source");
    assert!(
        successor.contains("stored_retry"),
        "the retry successor stores the source of its run: {successor}"
    );
    let policy = field_value(
        &body[body
            .find("workflow_attempt: attempt + 1")
            .expect("the successor")..],
        "workflow_retry_policy",
    )
    .expect("the successor policy");
    assert!(
        policy.contains("stored_retry"),
        "the retry successor stores the stored policy: {policy}"
    );
}

#[test]
fn the_guard_flags_a_bare_read_and_accepts_the_helper() {
    let bare = "let p = schedule\n    .retry_policy\n    .as_ref();";
    assert_eq!(receiver_reads(bare, "schedule"), vec![8]);
    assert!(!inside_helper(bare, 8));
    let helper =
        "let p = schedule_retry_policy(\n    schedule.retry_policy.as_ref(),\n    code,\n);";
    let reads = receiver_reads(helper, "schedule");
    assert_eq!(reads.len(), 1);
    assert!(inside_helper(helper, reads[0]));
    assert_eq!(
        receiver_reads("schedule.retry_policy_x", "schedule"),
        Vec::<usize>::new()
    );
    assert_eq!(
        receiver_reads("my_schedule.retry_policy", "schedule"),
        Vec::<usize>::new()
    );
}

#[test]
fn the_guard_flags_each_shape_of_a_hand_decode() {
    let forward = "let a = Params { workflow_retry_policy: v.and_then(|v| serde_json::from_value(v).ok()), b: 1 };";
    assert_eq!(hand_decodes(forward).len(), 1);
    let argument =
        "let p: RetryPolicy = serde_json::from_value(exec.workflow_retry_policy.clone()).ok()?;";
    assert_eq!(hand_decodes(argument).len(), 1);
    let other =
        "let x = Params { workflow_retry_policy: stored, memo: serde_json::from_value(m).ok() };";
    assert_eq!(
        hand_decodes(other),
        Vec::<usize>::new(),
        "another field may decode"
    );
}

#[test]
fn the_guard_flags_a_schedule_start_that_loses_the_source() {
    let good = "let (p, s) = workflow_retry::schedule_retry_policy(a, b);\n\
                let x = StartWorkflowParams {\n schedule_id: Some(id),\n workflow_retry_policy: p,\n workflow_retry_source: s,\n};";
    assert_eq!(schedule_literal_offenders("f", good), Vec::<String>::new());
    let typed = "let (p, s) = workflow_retry::schedule_retry_policy(a, b);\n\
                 let x = StartWorkflowParams {\n schedule_id: Some(id),\n workflow_retry_policy: p,\n workflow_retry_source: RetryPolicySource::Type,\n};";
    assert_eq!(schedule_literal_offenders("f", typed).len(), 1);
    let missing = "let x = StartWorkflowParams {\n schedule_id: Some(id),\n workflow_retry_policy: p,\n ..StartWorkflowParams::new()\n};";
    assert_eq!(schedule_literal_offenders("f", missing).len(), 1);
    let json = "let r = schedule_retry_value(a, b);\n\
                let o = DebounceStartOptions {\n schedule_id: Some(id),\n workflow_retry_policy: r,\n};";
    assert_eq!(schedule_literal_offenders("f", json), Vec::<String>::new());
    let raw = "let o = DebounceStartOptions {\n schedule_id: Some(id),\n workflow_retry_policy: serde_json::to_value(&p).ok(),\n};";
    assert_eq!(schedule_literal_offenders("f", raw).len(), 1);
}
