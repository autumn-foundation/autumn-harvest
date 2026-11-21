//! Guard: each start literal passes the resolved execution timeout.
//!
//! A start site resolves the timeout of its type, with any operator
//! override. A `StartWorkflowParams` literal that has a base and does not set
//! `execution_timeout`, or sets it to `None`, drops that value. Its run then
//! gets no `deadline_at`. A `DebounceStartOptions` literal with
//! `execution_timeout_secs: None` drops it from a deferred start in the same
//! way. This guard scans the crates that start runs, and fails on each such
//! literal.
//!
//! The guard reads struct literals only. A start from a bare
//! `StartWorkflowParams::new` call, or through `SignalWithStartParams`, is
//! out of its scope.

use std::path::{Path, PathBuf};

/// The source trees to scan, from the repository root.
const SOURCE_DIRS: [&str; 4] = [
    "autumn-harvest/src",
    "autumn-harvest-plugin/src",
    "autumn-harvest-macros/src",
    "autumn-harvest-agent/src",
];

/// The literals that may drop the timeout, as `(file, field entry)`.
///
/// The entry is one top-level field of the literal, with no string in it.
/// The scan blanks each string. A literal here starts no run.
const ALLOWED: [(&str, &str); 1] = [(
    // A unit test of the struct-update syntax.
    "autumn-harvest/src/execution.rs",
    "priority: Priority::High",
)];

/// The struct names that the guard checks.
const PARAMS: &str = "StartWorkflowParams";
const OPTIONS: &str = "DebounceStartOptions";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Every `.rs` file under `dir`, as a path from the repository root.
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

const fn is_ident(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// The offset just past the `"` that closes a plain string. `from` is the
/// first byte inside the string.
const fn plain_string_end(bytes: &[u8], from: usize) -> usize {
    let mut i = from;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// The end of the raw string `r#"…"#` that opens at `at`, if one opens
/// there.
fn raw_string_end(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes[at] != b'r' || (at > 0 && is_ident(bytes[at - 1])) {
        return None;
    }
    let hashes = bytes[at + 1..].iter().take_while(|&&b| b == b'#').count();
    let quote = at + 1 + hashes;
    if bytes.get(quote) != Some(&b'"') {
        return None;
    }
    let mut close = vec![b'"'];
    close.extend(std::iter::repeat_n(b'#', hashes));
    Some(find(&bytes[quote + 1..], &close).map_or(bytes.len(), |n| quote + 1 + n + close.len()))
}

/// The end of the comment, string or char literal that opens at `i`, if
/// one opens there.
fn noise_end(bytes: &[u8], i: usize) -> Option<usize> {
    let rest = &bytes[i..];
    if rest.starts_with(b"//") {
        return Some(find(rest, b"\n").map_or(bytes.len(), |n| i + n));
    }
    if rest.starts_with(b"/*") {
        return Some(find(rest, b"*/").map_or(bytes.len(), |n| i + n + 2));
    }
    if let Some(end) = raw_string_end(bytes, i) {
        return Some(end);
    }
    match rest {
        [b'"', ..] => Some(plain_string_end(bytes, i + 1)),
        // An escaped char, such as '\n' or '\''.
        [b'\'', b'\\', ..] => Some(
            bytes[i + 3..]
                .iter()
                .position(|&b| b == b'\'')
                .map_or(bytes.len(), |n| i + 3 + n + 1),
        ),
        // A plain char, such as '{'. A lifetime has no second quote.
        [b'\'', _, b'\'', ..] => Some(i + 3),
        _ => None,
    }
}

/// The source with each comment, string and char literal replaced by
/// spaces. Newlines stay, so each offset keeps its line.
fn blank_noise(source: &str) -> Vec<u8> {
    let bytes = source.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0;
    while i < bytes.len() {
        let Some(end) = noise_end(bytes, i) else {
            i += 1;
            continue;
        };
        for byte in &mut out[i..end] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
        i = end;
    }
    out
}

/// The top-level field entries of the literal whose `{` sits at `open`.
///
/// Each entry has its white space collapsed. A nested call or literal stays
/// inside its entry.
fn entries(code: &[u8], open: usize) -> Vec<String> {
    let mut depth = 0_usize;
    let mut out = Vec::new();
    let mut current = String::new();
    for &byte in &code[open..] {
        match byte {
            b'{' | b'(' | b'[' => {
                depth += 1;
                if depth == 1 {
                    continue;
                }
            }
            b'}' | b')' | b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    break;
                }
            }
            b',' if depth == 1 => {
                out.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(char::from(byte));
    }
    out.push(current);
    out.into_iter()
        .map(|e| e.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|e| !e.is_empty())
        .collect()
}

/// The field that an entry sets, for `name: value` or the shorthand `name`.
fn field(entry: &str) -> &str {
    entry.split(':').next().unwrap_or_default().trim()
}

/// Whether the literal of `name` with these entries drops the timeout.
fn drops_timeout(name: &str, entries: &[String]) -> bool {
    let set = |f: &str| entries.iter().find(|e| field(e) == f);
    if name == OPTIONS {
        return set("execution_timeout_secs").is_some_and(|e| e.ends_with(": None"));
    }
    // A bare `..` ends a pattern, not a literal.
    if entries.iter().any(|e| e == "..") {
        return false;
    }
    // A literal with no base sets every field, so the compiler checks it.
    let has_base = entries.iter().any(|e| e.starts_with(".."));
    has_base && set("execution_timeout").is_none_or(|e| e.ends_with(": None"))
}

/// Whether the text before `at` on its line opens a definition.
fn is_definition(code: &[u8], at: usize) -> bool {
    let line_start = code[..at]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |n| n + 1);
    let before = String::from_utf8_lossy(&code[line_start..at]);
    before.contains("struct ") || before.contains("impl") || before.contains("->")
}

/// The literals in `source` that drop the timeout. Each item holds the
/// line number and the field entries of the literal.
fn dropped_timeouts(source: &str) -> Vec<(usize, Vec<String>)> {
    let code = blank_noise(source);
    let mut found = Vec::new();
    for name in [PARAMS, OPTIONS] {
        let mut from = 0;
        while let Some(hit) = find(&code[from..], name.as_bytes()) {
            let at = from + hit;
            from = at + name.len();
            let whole_word = (at == 0 || !is_ident(code[at - 1]))
                && !code.get(from).copied().is_some_and(is_ident);
            let gap = code[from..].iter().take_while(|b| **b == b' ').count();
            if !whole_word || code.get(from + gap) != Some(&b'{') || is_definition(&code, at) {
                continue;
            }
            let fields = entries(&code, from + gap);
            if drops_timeout(name, &fields) {
                let line = code[..at].split(|&b| b == b'\n').count();
                found.push((line, fields));
            }
        }
    }
    found.sort();
    found
}

fn allowed(file: &str, fields: &[String]) -> bool {
    ALLOWED
        .iter()
        .any(|(f, entry)| *f == file && fields.iter().any(|e| e == entry))
}

#[test]
fn no_start_site_drops_the_execution_timeout() {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in SOURCE_DIRS {
        rust_files(&root, dir, &mut files);
    }
    assert!(files.len() > 50, "the scan must cover the source trees");
    let mut offenders = Vec::new();
    for file in &files {
        let source = std::fs::read_to_string(root.join(file)).expect("read source");
        for (line, fields) in dropped_timeouts(&source) {
            if !allowed(file, &fields) {
                offenders.push(format!("{file}:{line}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "pass the resolved execution timeout into each start, so the run gets \
         its deadline: {offenders:#?}"
    );
}

/// The guard is live. Each sample from `a` to `g` drops the timeout. Each
/// `ok_*` sample sets it or sets every field. The last lines hold no
/// literal.
#[test]
fn the_guard_flags_a_dropped_timeout_only() {
    let sample = r##"
let a = StartWorkflowParams {
    sla,
    ..StartWorkflowParams::new("https://x", "}", r#"{"k": 1}"#)
};
let b = StartWorkflowParams { sla, ..base };
let c = StartWorkflowParams{
    max_execution_timeout_ceiling: ceiling,
    owner: f(info.name, execution_timeout),
    ..base
};
let d = crate::execution::StartWorkflowParams {
    execution_timeout: None,
    ..base
};
let e = DebounceStartOptions {
    execution_timeout_secs: None,
    sla_secs: None,
};
let f = DebounceStartOptions { execution_timeout_secs: None, ..x };
let g = StartWorkflowParams {
    // execution_timeout,
    ..base
};
let ok_shorthand = StartWorkflowParams { execution_timeout, ..base };
let ok_value = StartWorkflowParams {
    execution_timeout: info.timeout.map(chrono),
    ..StartWorkflowParams::new('{', "x")
};
let ok_full = StartWorkflowParams { sla, owner };
let ok_secs = DebounceStartOptions { execution_timeout_secs: t.map(secs) };
let StartWorkflowParams { sla, .. } = params;
pub struct StartWorkflowParams { sla: u8 }
// let h = StartWorkflowParams { sla, ..base };
let s = "StartWorkflowParams { sla, ..base }";
"##;
    let lines: Vec<usize> = dropped_timeouts(sample)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(lines, [2, 6, 7, 12, 16, 20, 21]);
}

/// Each allowed literal still exists, so the list holds no stale entry.
#[test]
fn each_allowed_literal_still_exists() {
    let root = repo_root();
    for (file, entry) in ALLOWED {
        let source = std::fs::read_to_string(root.join(file)).expect("read source");
        let hits = dropped_timeouts(&source)
            .into_iter()
            .filter(|(_, fields)| fields.iter().any(|e| e == entry))
            .count();
        assert_eq!(hits, 1, "{file}: `{entry}` must name one allowed literal");
    }
}
