//! Guard: each quota check takes the caps through the override resolver
//! (issue #2408).
//!
//! An operator can override the three admission caps of
//! `WorkflowInfo::quota` at runtime. `run_defaults::current_quota_policy`
//! applies that override. `enforce_quota_admission` calls it before
//! `check_quota`, and the fork calls it before its stored-history check. A
//! new comparison against a code cap skips the override. This guard fails on
//! a `check_quota` call outside `enforce_quota_admission`, and on a read of a
//! cap field outside the allowed files and lines.

use std::path::{Path, PathBuf};

/// The source trees to scan, from the repository root.
const SOURCE_DIRS: [&str; 4] = [
    "autumn-harvest/src",
    "autumn-harvest-plugin/src",
    "autumn-harvest-macros/src",
    "autumn-harvest-agent/src",
];

/// The cap fields of a `QuotaPolicy`.
const FIELDS: [&str; 3] = [
    ".max_active_executions",
    ".max_history_bytes",
    ".max_dead_letters",
];

/// The files that may read a cap field. None of them compares usage to it.
const ALLOWED_FILES: [&str; 4] = [
    // The policy, `QuotaLimits`, `check_quota` and their tests.
    "autumn-harvest/src/quota.rs",
    // The override fields, the store and their tests.
    "autumn-harvest/src/run_defaults.rs",
    // The copy of the caps into a cross-shard child spec. The target applies
    // its own override by type name in `enforce_quota_admission`.
    "autumn-harvest/src/cross_shard_child.rs",
    // The `code`, `override` and `effective` views of the run-defaults route.
    "autumn-harvest-plugin/src/run_defaults_api.rs",
];

/// The single lines that may read a cap field, as `(file, line)`.
const ALLOWED_LINES: [(&str, &str); 4] = [
    // The registry fingerprint copies the declared caps. It starts no run.
    (
        "autumn-harvest/src/worker.rs",
        "quota.max_active_executions,",
    ),
    ("autumn-harvest/src/worker.rs", "quota.max_history_bytes,"),
    ("autumn-harvest/src/worker.rs", "quota.max_dead_letters,"),
    // The fork returns the cap of the policy that `current_quota_policy`
    // resolved. `the_fork_resolves_the_override` checks that call.
    (
        "autumn-harvest/src/fork.rs",
        "Ok(Some((key, policy.max_history_bytes)))",
    ),
];

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

/// The code part of a line, with no trailing `//` comment.
fn code(line: &str) -> &str {
    line.split("//").next().unwrap_or_default()
}

/// The lines of `source` that read a cap field, as `(line number, text)`.
fn field_reads(source: &str) -> Vec<(usize, String)> {
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| FIELDS.iter().any(|field| code(line).contains(field)))
        .map(|(index, line)| (index + 1, line.trim().to_string()))
        .collect()
}

fn allowed_line(file: &str, line: &str) -> bool {
    ALLOWED_LINES
        .iter()
        .any(|(allowed_file, allowed)| *allowed_file == file && line == *allowed)
}

#[test]
fn no_path_reads_a_code_quota_cap() {
    let mut offenders = Vec::new();
    for file in scanned_files() {
        if ALLOWED_FILES.contains(&file.as_str()) {
            continue;
        }
        for (number, line) in field_reads(&read(&file)) {
            if !allowed_line(&file, &line) {
                offenders.push(format!("{file}:{number}: {line}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these lines read a code quota cap. Resolve the policy with \
         `run_defaults::current_quota_policy`, or use `QuotaLimits`:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn only_enforce_quota_admission_calls_check_quota() {
    let mut callers = Vec::new();
    for file in scanned_files() {
        if file == "autumn-harvest/src/quota.rs" {
            continue;
        }
        for (index, line) in read(&file).lines().enumerate() {
            if code(line).contains("check_quota(") {
                callers.push(format!("{file}:{}", index + 1));
            }
        }
    }
    assert_eq!(
        callers.len(),
        1,
        "exactly one call of check_quota outside quota.rs: {callers:?}"
    );
    assert!(callers[0].starts_with("autumn-harvest/src/execution.rs:"));

    let source = read("autumn-harvest/src/execution.rs");
    let body = source
        .split("pub(crate) async fn enforce_quota_admission(")
        .nth(1)
        .expect("enforce_quota_admission exists");
    let check = body.find("check_quota(").expect("it calls check_quota");
    let resolve = body
        .find("current_quota_policy(")
        .expect("it calls current_quota_policy");
    assert!(
        resolve < check,
        "enforce_quota_admission resolves the override before check_quota"
    );
}

#[test]
fn the_fork_resolves_the_override() {
    let source = read("autumn-harvest/src/fork.rs");
    let resolve = source
        .find("current_quota_policy(")
        .expect("the fork calls current_quota_policy");
    let enforce = source
        .find("crate::execution::enforce_quota_admission(")
        .expect("the fork enforces the quota");
    let cap = source
        .find("Ok(Some((key, policy.max_history_bytes)))")
        .expect("the fork returns its history cap");
    assert!(resolve < enforce && enforce < cap);
}

#[test]
fn the_guard_flags_a_bare_read() {
    let source = "let a = policy.max_dead_letters;\n// policy.max_history_bytes\nlet b = p.with_max_active_executions(1);\n";
    assert_eq!(
        field_reads(source),
        vec![(1, "let a = policy.max_dead_letters;".to_string())]
    );
}

#[test]
fn each_allowed_line_matches_exactly_once() {
    for (file, allowed) in ALLOWED_LINES {
        let count = read(file)
            .lines()
            .filter(|line| line.trim() == allowed)
            .count();
        assert_eq!(count, 1, "{file}: {allowed}");
    }
}
