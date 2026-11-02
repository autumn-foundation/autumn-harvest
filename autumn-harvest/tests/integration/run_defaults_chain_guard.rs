//! Guard: each start path takes the chain cap from the registry (issue #2406).
//!
//! An operator can override `chain_execution_timeout` at runtime.
//! `HandlerRegistry::chain_execution_timeout` applies that override. A start
//! path that reads the declared field itself skips the override. This guard
//! fails on such a read in the files that hold the start paths.

/// The files that start a chain, from the repository root.
const START_PATHS: [&str; 7] = [
    "autumn-harvest-plugin/src/api.rs",
    "autumn-harvest-plugin/src/ui.rs",
    "autumn-harvest-plugin/src/outbox.rs",
    "autumn-harvest/src/scheduler.rs",
    "autumn-harvest/src/handle.rs",
    "autumn-harvest/src/worker.rs",
    "autumn-harvest-macros/src/workflow.rs",
];

fn read(relative: &str) -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent");
    std::fs::read_to_string(root.join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
        .replace("\r\n", "\n")
}

/// The receivers that name a `WorkflowInfo` in these files.
const INFO_NAMES: [&str; 6] = ["info", "w", "wf", "wf_info", "target_info", "child_wf_info"];

/// A read is sanctioned when an `apply_chain*` call wraps it.
const APPLY: &str = "apply_chain";

/// How many lines before or after a read the wrapping call can sit.
///
/// `rustfmt` splits a long call over several lines.
const WINDOW: usize = 5;

/// Whether `code` reads `{name}.chain_execution_timeout` as a whole token.
fn reads_field(code: &str, name: &str) -> bool {
    let needle = format!("{name}.chain_execution_timeout");
    code.match_indices(&needle).any(|(at, _)| {
        let before = code[..at].chars().next_back();
        let after = code[at + needle.len()..].chars().next();
        !before.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '.')
            && !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
    })
}

/// The reads of the declared chain cap that no override wraps.
fn unwrapped_reads(source: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let code = |line: &str| line.split("//").next().unwrap_or_default().to_string();
    (0..lines.len())
        .filter(|&index| {
            let text = code(lines[index]);
            INFO_NAMES.iter().any(|name| reads_field(&text, name))
        })
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
    let offenders: Vec<String> = START_PATHS
        .iter()
        .flat_map(|path| {
            unwrapped_reads(&read(path))
                .into_iter()
                .map(move |(line, text)| format!("{path}:{line}: {text}"))
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "read the chain cap through HandlerRegistry::chain_execution_timeout \
         or an apply_chain call, so the operator override applies: {offenders:#?}"
    );
}

/// The guard is live: it flags a bare read, and it skips a wrapped read, a
/// comment and a stored row.
#[test]
fn the_guard_flags_a_bare_read_only() {
    let sample = "let x = info.chain_execution_timeout;\n\
                  let y = execution.chain_execution_timeout;\n\
                  // info.chain_execution_timeout in a comment\n\
                  .and_then(|info| info.chain_execution_timeout)\n";
    let lines: Vec<usize> = unwrapped_reads(sample)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(lines, [1, 4]);
    let wrapped = "cache.apply_chain(name, wf.chain_execution_timeout)\n";
    assert!(unwrapped_reads(wrapped).is_empty());
    assert!(
        read("autumn-harvest/src/worker.rs")
            .contains("pub fn chain_execution_timeout(&self, workflow_name: &str)"),
        "the resolver must exist"
    );
}
