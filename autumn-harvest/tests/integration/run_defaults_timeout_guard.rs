//! Guard: each start site passes the resolved execution timeout.
//!
//! A start site resolves the timeout of its type, with any operator
//! override. A site that builds `StartWorkflowParams` from a base and leaves
//! out `execution_timeout` drops that value, so its run gets no
//! `deadline_at`. A `DebounceStartOptions` with `execution_timeout_secs:
//! None` drops it from a deferred start in the same way. This guard scans the
//! crates that start runs, and fails on each such literal.

use std::path::{Path, PathBuf};

/// The source trees to scan, from the repository root.
const SOURCE_DIRS: [&str; 4] = [
    "autumn-harvest/src",
    "autumn-harvest-plugin/src",
    "autumn-harvest-macros/src",
    "autumn-harvest-agent/src",
];

/// The literals that may leave out the timeout, as `(file, line)`.
///
/// The line is one line inside the literal. A literal here starts no run.
const ALLOWED: [(&str, &str); 1] = [(
    // A unit test of the struct-update syntax.
    "autumn-harvest/src/execution.rs",
    "owner: Some(\"team\"),",
)];

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

/// The code part of a line, without a trailing `//` comment.
fn code(line: &str) -> &str {
    line.split("//").next().unwrap_or_default()
}

/// The lines of the literal that opens at `start`, up to its closing brace.
fn literal<'a>(lines: &'a [&'a str], start: usize) -> &'a [&'a str] {
    let mut depth = 0_i32;
    for (offset, line) in lines[start..].iter().enumerate() {
        for c in code(line).chars() {
            match c {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
        if depth <= 0 {
            return &lines[start..=start + offset];
        }
    }
    &lines[start..]
}

/// Whether a line names the field `field` in a struct literal.
fn names_field(line: &str, field: &str) -> bool {
    let trimmed = code(line).trim();
    trimmed
        .strip_prefix(field)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with([':', ',']))
}

/// Whether `line` opens a struct literal of `name`, not a definition.
fn opens_literal(line: &str, name: &str) -> bool {
    let code = code(line);
    code.contains(&format!("{name} {{"))
        && !code.contains("struct ")
        && !code.contains("impl")
        && !code.contains("->")
}

/// The literals in `source` that drop the timeout, as `(line, first line
/// of the literal)`.
fn dropped_timeouts(source: &str) -> Vec<(usize, Vec<String>)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    for start in 0..lines.len() {
        let params = opens_literal(lines[start], "StartWorkflowParams");
        let options = opens_literal(lines[start], "DebounceStartOptions");
        if !params && !options {
            continue;
        }
        let body = literal(&lines, start);
        let drops = if params {
            // A literal with no base names every field already.
            let has_base = body.iter().any(|l| code(l).trim_start().starts_with(".."));
            has_base && !body.iter().any(|l| names_field(l, "execution_timeout"))
        } else {
            body.iter()
                .any(|l| names_field(l, "execution_timeout_secs") && code(l).contains(": None"))
        };
        if drops {
            let text = body.iter().map(|l| l.trim().to_string()).collect();
            found.push((start + 1, text));
        }
    }
    found
}

fn allowed(file: &str, body: &[String]) -> bool {
    ALLOWED
        .iter()
        .any(|(f, line)| *f == file && body.iter().any(|l| l == line))
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
        for (line, body) in dropped_timeouts(&source) {
            if !allowed(file, &body) {
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

/// The guard is live. It flags a literal that drops the timeout, and it
/// skips a literal that passes it, a full literal and a definition.
#[test]
fn the_guard_flags_a_dropped_timeout_only() {
    let sample = "let a = StartWorkflowParams {\n\
                  \x20   sla,\n\
                  \x20   ..StartWorkflowParams::new()\n\
                  };\n\
                  let b = StartWorkflowParams {\n\
                  \x20   execution_timeout,\n\
                  \x20   ..StartWorkflowParams::new()\n\
                  };\n\
                  let c = StartWorkflowParams {\n\
                  \x20   max_execution_timeout_ceiling: ceiling,\n\
                  \x20   ..StartWorkflowParams::new()\n\
                  };\n\
                  let d = DebounceStartOptions {\n\
                  \x20   execution_timeout_secs: None,\n\
                  };\n\
                  let e = DebounceStartOptions {\n\
                  \x20   execution_timeout_secs: timeout.map(secs),\n\
                  };\n\
                  pub struct StartWorkflowParams {\n\
                  \x20   sla: u8,\n\
                  \x20   ..x\n\
                  }\n";
    let lines: Vec<usize> = dropped_timeouts(sample)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(lines, [1, 9, 13], "a, c and d drop the timeout");
}

/// Each allowed literal still exists, so the list holds no stale entry.
#[test]
fn each_allowed_literal_still_exists() {
    let root = repo_root();
    for (file, line) in ALLOWED {
        let source = std::fs::read_to_string(root.join(file)).expect("read source");
        let hits = dropped_timeouts(&source)
            .into_iter()
            .filter(|(_, body)| body.iter().any(|l| l == line))
            .count();
        assert_eq!(hits, 1, "{file}: `{line}` must name one allowed literal");
    }
}
