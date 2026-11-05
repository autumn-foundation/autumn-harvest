//! Guard: each start path takes the concurrency policy from the override
//! resolver (issue #2407).
//!
//! An operator can override the `limit` of `WorkflowInfo::concurrency` at
//! runtime. `HandlerRegistry::concurrency_policy` and the `apply_concurrency*`
//! helpers apply that override. A start path that reads the declared field
//! itself skips the override. This guard scans every source file of the crates
//! that start runs. It fails on a read of a `.concurrency` field that is not
//! an argument of an open `apply_concurrency*` call. Only the receivers in
//! [`NOT_INFO`] are exempt, so an unknown receiver counts as a read.
//!
//! A limit that a start path copies from a task row or a stored spec is not a
//! read of the field. Unit tests of each such path cover it instead.

use std::path::{Path, PathBuf};

/// The source trees to scan, from the repository root.
const SOURCE_DIRS: [&str; 4] = [
    "autumn-harvest/src",
    "autumn-harvest-plugin/src",
    "autumn-harvest-macros/src",
    "autumn-harvest-agent/src",
];

/// The files that may read the declared value. None of them starts a run.
const ALLOWED_FILES: [&str; 3] = [
    // The declaration, its builder method, its `Debug` output and its tests.
    "autumn-harvest/src/info.rs",
    // The startup check of the declared limit.
    "autumn-harvest/src/builder.rs",
    // The `code` view and the checks of the run-defaults route.
    "autumn-harvest-plugin/src/run_defaults_api.rs",
];

/// The single lines that may read the declared value, as `(file, line)`.
///
/// A line here copies the value and starts no run. A file-level entry would
/// hide a real read in the same file.
const ALLOWED_LINES: [(&str, &str); 3] = [
    // The metadata mirror of the completion-trigger path.
    // `target_concurrency` applies the override when a start fires.
    (
        "autumn-harvest/src/worker.rs",
        "concurrency: w.concurrency,",
    ),
    // The `code_limit` of `GET /admin/concurrency`. `admin_concurrency_limits`
    // applies the override for the `effective_limit` next to it.
    (
        "autumn-harvest-plugin/src/api.rs",
        ".and_then(|info| info.concurrency);",
    ),
    // A unit test of the canary workflow declaration.
    (
        "autumn-harvest-plugin/src/canary.rs",
        "assert!(info.concurrency.is_none());",
    ),
];

/// The receivers whose `.concurrency` field is not a `WorkflowInfo` policy:
/// batch and sampling settings, macro attributes and test traces.
const NOT_INFO: [&str; 9] = [
    "self",
    "config",
    "batch",
    "trace",
    "ask",
    "input",
    "admission",
    "admitted",
    "attrs",
];

/// The prefix of the helpers that apply the override.
const APPLY: &str = "apply_concurrency";

/// How many lines before a read the open call can start.
///
/// `rustfmt` splits a long call over several lines.
const WINDOW: usize = 6;

const FIELD: &str = ".concurrency";

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

/// The code part of a line: no trailing `//` comment, and blanks in place of
/// the text of a `"..."` string.
fn code(line: &str) -> String {
    let line = line.split("//").next().unwrap_or_default();
    let mut out = String::with_capacity(line.len());
    let mut in_string = false;
    let mut escaped = false;
    for c in line.chars() {
        if !in_string {
            in_string = c == '"';
            out.push(c);
        } else if escaped {
            escaped = false;
            out.push(' ');
        } else if c == '\\' {
            escaped = true;
            out.push(' ');
        } else if c == '"' {
            in_string = false;
            out.push(c);
        } else {
            out.push(' ');
        }
    }
    out
}

/// The identifier that ends `text`, after trailing whitespace and one `()`.
///
/// It is empty when `text` ends in another token, such as `?`, `]` or a call
/// with arguments. An empty receiver is unknown, so it counts as a read.
fn receiver(text: &str) -> String {
    let text = text.trim_end();
    let text = text.strip_suffix("()").unwrap_or(text);
    let start = text
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_alphanumeric() || *c == '_')
        .last()
        .map_or(text.len(), |(at, _)| at);
    text[start..].to_string()
}

/// The byte offsets in line `index` that read the field.
///
/// A receiver may sit at the end of the line before, as in a call chain that
/// `rustfmt` splits. An assignment, a method call and a receiver in
/// [`NOT_INFO`] are not reads.
fn reads_in_line(lines: &[String], index: usize) -> Vec<usize> {
    let line = &lines[index];
    line.match_indices(FIELD)
        .map(|(at, _)| at)
        .filter(|&at| {
            let rest = &line[at + FIELD.len()..];
            if rest.starts_with(|c: char| c.is_alphanumeric() || c == '_' || c == '(') {
                return false;
            }
            let trimmed = rest.trim_start();
            if trimmed.starts_with('=') && !trimmed.starts_with("==") {
                return false;
            }
            let before = &line[..at];
            let name = if before.trim().is_empty() && index > 0 {
                receiver(&lines[index - 1])
            } else {
                receiver(before)
            };
            !NOT_INFO.contains(&name.as_str())
        })
        .collect()
}

/// Whether the read at `at` in line `index` is an argument of an
/// `apply_concurrency*` call that is still open there.
fn wrapped(lines: &[String], index: usize, at: usize) -> bool {
    let low = index.saturating_sub(WINDOW);
    let mut text = lines[low..index].join("\n");
    text.push('\n');
    text.push_str(&lines[index][..at]);
    let Some(call) = text.rfind(APPLY) else {
        return false;
    };
    let Some(open) = text[call..].find('(') else {
        return false;
    };
    let args = &text[call + open..];
    args.matches('(').count() > args.matches(')').count()
}

/// The reads of the declared policy that no override wraps.
fn unwrapped_reads(source: &str) -> Vec<(usize, String)> {
    let raw: Vec<&str> = source.lines().collect();
    let lines: Vec<String> = raw.iter().map(|line| code(line)).collect();
    (0..lines.len())
        .filter(|&index| {
            reads_in_line(&lines, index)
                .into_iter()
                .any(|at| !wrapped(&lines, index, at))
        })
        .map(|index| (index + 1, raw[index].trim().to_string()))
        .collect()
}

fn scanned_files() -> Vec<String> {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in SOURCE_DIRS {
        rust_files(&root, dir, &mut files);
    }
    files
}

#[test]
fn no_start_path_reads_the_declared_concurrency_policy() {
    let root = repo_root();
    let files = scanned_files();
    assert!(files.len() > 50, "the scan must cover the source trees");
    let mut offenders = Vec::new();
    for file in files
        .iter()
        .filter(|f| !ALLOWED_FILES.contains(&f.as_str()))
    {
        let source = std::fs::read_to_string(root.join(file)).expect("read source");
        for (line, text) in unwrapped_reads(&source) {
            if ALLOWED_LINES.contains(&(file.as_str(), text.as_str())) {
                continue;
            }
            offenders.push(format!("{file}:{line}: {text}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "read the concurrency policy through HandlerRegistry::concurrency_policy \
         or as an argument of an apply_concurrency call, so the operator \
         override applies: {offenders:#?}"
    );
}

/// The guard is live. It flags a bare read, a split chain, a `Self::info()`
/// read, an unknown receiver and a read after a closed override call. It
/// skips a read inside an open override call, an assignment, a comment, a
/// string, a method call, another field and an exempt receiver.
#[test]
fn the_guard_flags_a_bare_read_only() {
    let sample = "let x = info.concurrency;\n\
                  let y = config.concurrency;\n\
                  // info.concurrency in a comment\n\
                  .and_then(|i| i.concurrency.as_ref())\n\
                  info.concurrency = Some(p);\n\
                  let k = info.concurrency_key;\n\
                  let v = Self::info().concurrency.map(|p| p.limit);\n\
                  let z = info\n\
                      .concurrency\n\
                      .as_ref();\n\
                  let t = map.get(n)?.concurrency;\n\
                  let u = list[0].concurrency;\n\
                  let s = \"harvest.concurrency.superseded\";\n\
                  let d = DrawsInput::new(1).concurrency(0);\n\
                  let a = client.apply_concurrency_run_default(name, None);\n\
                  let b = info.concurrency.map(|p| p.limit);\n";
    let lines: Vec<usize> = unwrapped_reads(sample)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(lines, [1, 4, 7, 9, 11, 12, 16]);
    let wrapped = "registry.apply_concurrency_run_default(name, info.concurrency)\n\
                   let p = apply_concurrency_override(\n\
                       name,\n\
                       meta.get(name)?.concurrency,\n\
                       cache,\n\
                   );\n";
    assert_eq!(unwrapped_reads(wrapped), []);
    let resolver = std::fs::read_to_string(repo_root().join("autumn-harvest/src/worker.rs"))
        .expect("read worker.rs");
    assert!(
        resolver.contains("pub fn concurrency_policy("),
        "the resolver must exist"
    );
}

/// The scan covers the files that hold a start path.
#[test]
fn the_scan_covers_every_start_path_file() {
    let files = scanned_files();
    for file in [
        "autumn-harvest/src/completion_trigger.rs",
        "autumn-harvest/src/dlq.rs",
        "autumn-harvest/src/handle.rs",
        "autumn-harvest/src/reset.rs",
        "autumn-harvest/src/scheduler.rs",
        "autumn-harvest/src/worker.rs",
        "autumn-harvest-plugin/src/api.rs",
        "autumn-harvest-macros/src/update.rs",
        "autumn-harvest-macros/src/workflow.rs",
    ] {
        assert!(files.iter().any(|f| f == file), "{file} must be scanned");
        assert!(!ALLOWED_FILES.contains(&file), "{file} must not be exempt");
    }
}

/// Each allowed line matches once, so a copy of it is not exempt.
#[test]
fn each_allowed_line_matches_exactly_once() {
    let root = repo_root();
    for (file, text) in ALLOWED_LINES {
        let source = std::fs::read_to_string(root.join(file)).expect("read source");
        let hits = unwrapped_reads(&source)
            .iter()
            .filter(|(_, line)| line == text)
            .count();
        assert_eq!(hits, 1, "{file}: `{text}` must match once");
    }
}
