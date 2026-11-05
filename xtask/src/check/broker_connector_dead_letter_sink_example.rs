//! `cargo xtask check broker-connector-dead-letter-sink-example`: the port of
//! `scripts/check-broker-connector-dead-letter-sink-example.sh`.
//!
//! The guard fails if the "record into harvest" example of
//! `docs/getting-started/13-broker-connectors.md` installs
//! `RecordingDeadLetterSink` instead of `PostgresDeadLetterSink`.
//!
//! Mechanism this guards against: the "Two ways to satisfy it" section of the
//! chapter gives two production-facing options for a directly-constructed
//! `ConnectorRuntime`. The "Testing without a broker" section above it uses
//! `RecordingDeadLetterSink` for its no-Docker unit test, and that use is
//! correct. Option (a), labeled "record into harvest — the default mode",
//! used to read:
//!
//! ```text
//! let runtime = ConnectorRuntime::new(/* ... */)
//!     .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new()));
//! ```
//!
//! The doc comment of `RecordingDeadLetterSink` in
//! `autumn-harvest-plugin/src/connector/dead_letter.rs` says that it "records
//! into memory, exported for tests and local development". Its `write()` only
//! pushes into an in-process `Mutex<Vec<...>>`, and nothing reaches Postgres.
//! The sink that writes `harvest_connector_dead_letters` is
//! `PostgresDeadLetterSink`. `HarvestPlugin` itself installs that sink in
//! `autumn-harvest-plugin/src/plugin.rs`, as the "default mode" that this
//! example claims to reproduce.
//!
//! This section addresses a newcomer who embeds a runtime directly. A copy of
//! option (a) compiles and runs with no error. Poison messages are
//! acknowledged, so the binding makes progress. But they are never durably
//! recorded. An in-process restart loses them all. Every triage query that
//! the chapter documents (`SELECT ... FROM harvest_connector_dead_letters`)
//! returns nothing. That is silent data loss in the one code path whose whole
//! purpose is "a poison message must never be silently lost".

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report};
use crate::util;

const DOC: &str = "docs/getting-started/13-broker-connectors.md";
const LABEL: &str = "// (a) record into harvest";

/// Arguments of `cargo xtask check broker-connector-dead-letter-sink-example`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check broker-connector-dead-letter-sink-example`.
#[allow(
    clippy::unnecessary_wraps,
    reason = "every `check` command keeps the `run` signature that `check/mod.rs` calls"
)]
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root()).emit())
}

/// Returns each line that holds `needle` and the `after` lines that follow.
///
/// This copies `grep -A <after> -F <needle>`, including the `--` line that
/// separates two groups that do not touch.
fn grep_after(text: &str, needle: &str, after: usize) -> String {
    let lines = support::lines(text);
    let mut keep = vec![false; lines.len()];
    for (index, line) in lines.iter().enumerate() {
        if line.contains(needle) {
            for slot in keep.iter_mut().skip(index).take(after + 1) {
                *slot = true;
            }
        }
    }
    let mut out: Vec<&str> = Vec::new();
    let mut last: Option<usize> = None;
    for (index, line) in lines.iter().enumerate() {
        if !keep[index] {
            continue;
        }
        if last.is_some_and(|prev| prev + 1 != index) {
            out.push("--");
        }
        out.push(line);
        last = Some(index);
    }
    support::join_captured(&out)
}

/// Checks the option (a) example of the doc under `root`.
fn evaluate(root: &Path) -> Report {
    let mut report = Report::new();
    let path = root.join(DOC);
    if !path.is_file() {
        report.fail(format!("{DOC}: not found"));
        return report;
    }
    let text = support::read_or_empty(&path);

    // The window is the few lines right after the "(a) record into harvest"
    // label. The example of "Testing without a broker" above it uses
    // RecordingDeadLetterSink correctly, so the guard must not read it.
    let window = grep_after(&text, LABEL, 3);
    if window.is_empty() {
        report.fail(format!(
            "{DOC}: could not find the '{LABEL}' example; has the chapter been restructured? Update this guard to match."
        ));
        return report;
    }

    if window.contains("RecordingDeadLetterSink") {
        report.err(format!(
            "{DOC}: the '(a) record into harvest — the default mode' example installs RecordingDeadLetterSink, an in-memory, test-only sink (see its doc comment in autumn-harvest-plugin/src/connector/dead_letter.rs). It never writes harvest_connector_dead_letters, so a runtime built from this example silently loses every poison message on restart."
        ));
        report.err("");
        report.fail(
            "Fix: use PostgresDeadLetterSink::new(pool) — the sink HarvestPlugin itself installs as the real default (autumn-harvest-plugin/src/plugin.rs).",
        );
        return report;
    }

    if !window.contains("PostgresDeadLetterSink") {
        report.fail(format!(
            "{DOC}: the '(a) record into harvest — the default mode' example no longer names PostgresDeadLetterSink. Update this guard to match the current example."
        ));
        return report;
    }

    // The name must resolve. This example is a fenced block of its own with
    // no `use` statement. A bare `PostgresDeadLetterSink::new(` there is an
    // unresolved name for anyone who assembles the snippet as shown. PR #1499
    // first used the bare name. The shared import block above imports only
    // RecordingDeadLetterSink.
    if window.contains("PostgresDeadLetterSink::new(")
        && !window.contains("autumn_harvest_plugin::connector::PostgresDeadLetterSink")
    {
        report.err(format!(
            "{DOC}: the example calls PostgresDeadLetterSink::new(...) by its bare name, but this fenced block has no 'use' statement importing it (the shared import block in the 'Testing without a broker' example above only imports RecordingDeadLetterSink), so the name does not resolve."
        ));
        report.err("");
        report.fail(
            "Fix: qualify it as autumn_harvest_plugin::connector::PostgresDeadLetterSink::new(...).",
        );
        return report;
    }

    report
        .out("OK: the 'record into harvest' example installs a resolvable PostgresDeadLetterSink.");
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(body: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(DOC);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        std::fs::write(&doc, body).expect("doc");
        dir
    }

    #[test]
    fn a_qualified_postgres_sink_passes() {
        let dir = fixture(
            "use RecordingDeadLetterSink;\n\n```rust\n// (a) record into harvest — the default mode\nlet runtime = ConnectorRuntime::new()\n    .with_dead_letter_sink(Arc::new(autumn_harvest_plugin::connector::PostgresDeadLetterSink::new(pool)));\n```\n",
        );
        let report = evaluate(dir.path());
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn the_recording_sink_fails() {
        let dir = fixture(
            "// (a) record into harvest\n    .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new()));\n",
        );
        let report = evaluate(dir.path());
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("in-memory, test-only sink"));
    }

    #[test]
    fn a_bare_postgres_sink_fails() {
        let dir = fixture("// (a) record into harvest\nPostgresDeadLetterSink::new(pool)\n");
        let report = evaluate(dir.path());
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("by its bare name"));
    }

    #[test]
    fn a_missing_label_and_a_missing_doc_fail() {
        let dir = fixture("nothing\n");
        assert!(evaluate(dir.path()).stderr().contains("could not find"));
        let empty = fs::temp_dir().expect("temp dir");
        assert_eq!(
            evaluate(empty.path()).stderr(),
            format!("{DOC}: not found\n")
        );
    }

    #[test]
    fn grep_after_separates_groups() {
        let text = "x\nA\n1\n2\n3\n4\n5\nA\n6\n";
        assert_eq!(grep_after(text, "A", 3), "A\n1\n2\n3\n--\nA\n6");
        assert_eq!(grep_after("A\nA\n1\n", "A", 1), "A\nA\n1");
    }
}
