//! Guard: each start path takes the chain cap from the registry (issue #2406).
//!
//! An operator can override `chain_execution_timeout` at runtime.
//! `HandlerRegistry::chain_execution_timeout` applies that override. A start
//! path that reads the declared field itself skips the override. This guard
//! scans every source file of the crates that start runs. It fails on a read
//! of the declared field that no `apply_chain*` call wraps.

use std::path::{Path, PathBuf};

/// The source trees to scan, from the repository root.
const SOURCE_DIRS: [&str; 4] = [
    "autumn-harvest/src",
    "autumn-harvest-plugin/src",
    "autumn-harvest-macros/src",
    "autumn-harvest-agent/src",
];

/// The files that may read the declared value. None of them starts a run.
const ALLOWED_FILES: [&str; 4] = [
    // The declaration, its builder method and its `Debug` output.
    "autumn-harvest/src/info.rs",
    // The startup check of the declared durations.
    "autumn-harvest/src/builder.rs",
    // The `code` view of the run-defaults route.
    "autumn-harvest-plugin/src/run_defaults_api.rs",
    // A unit test of the macro token helper.
    "autumn-harvest-macros/src/attr_util.rs",
];

/// The single lines that may read the declared value, as `(file, line)`.
///
/// A line here copies the value and starts no run. A file-level entry would
/// hide a real read in the same file.
const ALLOWED_LINES: [(&str, &str); 1] = [(
    // The metadata mirror of the completion-trigger path (issue #2511).
    // `resolve_target_run_defaults` applies the override when a start fires.
    "autumn-harvest/src/worker.rs",
    "chain_execution_timeout: w.chain_execution_timeout,",
)];

/// The receivers that name a `WorkflowInfo` in the scanned files.
const INFO_NAMES: [&str; 9] = [
    "info",
    "i",
    "w",
    "wf",
    "wf_info",
    "target_info",
    "child_wf_info",
    "workflow_info",
    "meta",
];

/// A read is sanctioned when an `apply_chain*` call wraps it.
const APPLY: &str = "apply_chain";

/// How many lines before or after a read the wrapping call can sit.
///
/// `rustfmt` splits a long call over several lines.
const WINDOW: usize = 3;

const FIELD: &str = ".chain_execution_timeout";

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

/// Whether `code` reads the field of a receiver in [`INFO_NAMES`].
///
/// A receiver may follow a `.`, as in `self.info`. A call such as
/// `Self::info()` is a receiver too (issue #2511). An assignment is not a
/// read.
fn reads_field(code: &str) -> bool {
    code.match_indices(FIELD).any(|(at, _)| {
        let before = code[..at].strip_suffix("()").unwrap_or(&code[..at]);
        let receiver: String = before
            .chars()
            .rev()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let rest = &code[at + FIELD.len()..];
        let whole_token = !rest.starts_with(|c: char| c.is_alphanumeric() || c == '_');
        let trimmed = rest.trim_start();
        let assignment = trimmed.starts_with('=') && !trimmed.starts_with("==");
        whole_token && !assignment && INFO_NAMES.contains(&receiver.as_str())
    })
}

/// The code part of a line, without a trailing `//` comment.
fn code(line: &str) -> &str {
    line.split("//").next().unwrap_or_default()
}

/// The reads of the declared chain cap that no override wraps.
fn unwrapped_reads(source: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    (0..lines.len())
        .filter(|&index| reads_field(code(lines[index])))
        .filter(|&index| {
            let low = index.saturating_sub(WINDOW);
            let high = (index + WINDOW).min(lines.len() - 1);
            !lines[low..=high]
                .iter()
                .any(|line| code(line).contains(APPLY))
        })
        .map(|index| (index + 1, lines[index].trim().to_string()))
        .collect()
}

#[test]
fn no_start_path_reads_the_declared_chain_cap() {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in SOURCE_DIRS {
        rust_files(&root, dir, &mut files);
    }
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
        "read the chain cap through HandlerRegistry::chain_execution_timeout \
         or an apply_chain call, so the operator override applies: {offenders:#?}"
    );
}

/// The guard is live: it flags a bare read, and it skips a wrapped read, an
/// assignment, a comment and a stored row.
#[test]
fn the_guard_flags_a_bare_read_only() {
    let sample = "let x = info.chain_execution_timeout;\n\
                  let y = execution.chain_execution_timeout;\n\
                  // info.chain_execution_timeout in a comment\n\
                  .and_then(|i| i.chain_execution_timeout)\n\
                  info.chain_execution_timeout = Some(d);\n\
                  let z = self.info.chain_execution_timeout;\n\
                  let v = Self::info().chain_execution_timeout;\n";
    let lines: Vec<usize> = unwrapped_reads(sample)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(lines, [1, 4, 6, 7]);
    let wrapped = "cache.apply_chain(name, wf.chain_execution_timeout)\n";
    assert_eq!(unwrapped_reads(wrapped), []);
    let resolver = std::fs::read_to_string(repo_root().join("autumn-harvest/src/worker.rs"))
        .expect("read worker.rs");
    assert!(
        resolver.contains("pub fn chain_execution_timeout(&self, workflow_name: &str)"),
        "the resolver must exist"
    );
}

/// AC3 of issue #2511: the scan covers both files that read the declared
/// value and that no entry of the allow list exempts.
#[test]
fn the_scan_covers_the_completion_trigger_and_update_stub() {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in SOURCE_DIRS {
        rust_files(&root, dir, &mut files);
    }
    for file in [
        "autumn-harvest/src/completion_trigger.rs",
        "autumn-harvest-macros/src/update.rs",
    ] {
        assert!(files.iter().any(|f| f == file), "{file} must be scanned");
        assert!(!ALLOWED_FILES.contains(&file), "{file} must not be exempt");
    }
}
