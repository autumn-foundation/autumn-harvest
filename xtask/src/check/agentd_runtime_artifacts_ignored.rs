//! `cargo xtask check agentd-runtime-artifacts-ignored`: the port of
//! `scripts/check-agentd-runtime-artifacts-ignored.sh`.
//!
//! The guard fails if the default runtime files of the `claude-agent-daemon`
//! example are not git-ignored at the repository root.
//!
//! Mechanism this guards against: the "Try it in one minute" section of
//! `examples/claude-agent-daemon/README.md` tells the reader to run every
//! `agentd` command "from the repository root". Cargo needs the workspace
//! manifest there. The daemon writes its defaults relative to the current
//! directory:
//!
//! - `agentd.db`, from `examples/claude-agent-daemon/src/main.rs`.
//! - `agentd.sock`, the `DEFAULT_SOCKET` of
//!   `examples/claude-agent-daemon/src/protocol.rs`.
//! - The SQLite WAL sidecars `agentd.db-shm` and `agentd.db-wal`.
//! - The lock file of the socket, `agentd.sock.lock`, from
//!   `examples/claude-agent-daemon/src/guard.rs`.
//!
//! A live run of the `serve` step of the walkthrough, verbatim from the root
//! of a clean checkout, leaves `git status --porcelain` non-empty.
//!
//! That is not just clutter. The doc says that the database "holds every
//! prompt, tool input, and tool result, including the content of each file
//! the agent read". So the same walkthrough against a real
//! `ANTHROPIC_API_KEY` leaves a conversation transcript as an untracked
//! file. One `git add -A` then puts it in version control.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::default_run::support::{self, Report};
use crate::util;

const MAIN_RS: &str = "examples/claude-agent-daemon/src/main.rs";
const PROTOCOL_RS: &str = "examples/claude-agent-daemon/src/protocol.rs";

/// Arguments of `cargo xtask check agentd-runtime-artifacts-ignored`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check agentd-runtime-artifacts-ignored`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let report = evaluate(&root, |name| {
        sh::cmd("git")
            .args(["check-ignore", "-q", "--", name])
            .dir(&root)
            .success()
    })?;
    Ok(report.emit())
}

/// Returns every capture of `pattern` over the lines of `text`.
///
/// This copies `sed -n 's/<pattern>/\1/p'`: each matching line prints its
/// capture, and the substitution joins the lines with newlines.
fn sed_capture(text: &str, pattern: &Regex) -> String {
    let found: Vec<&str> = support::lines(text)
        .into_iter()
        .filter_map(|line| pattern.captures(line))
        .filter_map(|caps| caps.get(1).map(|m| m.as_str()))
        .collect();
    support::join_captured(&found)
}

/// Checks the defaults under `root`. `ignored` answers `git check-ignore`.
fn evaluate(root: &Path, mut ignored: impl FnMut(&str) -> Result<bool>) -> Result<Report> {
    // The guard reads the defaults from source and does not hard-code them.
    // A renamed default then fails this guard. Otherwise the guard checks
    // stale names while the daemon writes to a new, unignored path.
    let db_re = Regex::new(r#".*env = "AGENTD_DB", default_value = "([^"]*)".*"#)?;
    let socket_re = Regex::new(r#".*DEFAULT_SOCKET: &str = "([^"]*)";.*"#)?;
    let db_default = sed_capture(&support::read_or_empty(&root.join(MAIN_RS)), &db_re);
    let socket_default = sed_capture(&support::read_or_empty(&root.join(PROTOCOL_RS)), &socket_re);

    let mut report = Report::new();
    if db_default.is_empty() {
        report.fail(format!(
            "{MAIN_RS}: could not find AGENTD_DB's default_value; has the --db argument been restructured? Update this guard to match."
        ));
        return Ok(report);
    }
    if socket_default.is_empty() {
        report.fail(format!(
            "{PROTOCOL_RS}: could not find DEFAULT_SOCKET; has it been renamed or moved? Update this guard to match."
        ));
        return Ok(report);
    }

    let names = [
        db_default.clone(),
        format!("{db_default}-shm"),
        format!("{db_default}-wal"),
        socket_default.clone(),
        format!("{socket_default}.lock"),
    ];
    let mut missing = Vec::new();
    for name in names {
        if !ignored(&name)? {
            missing.push(name);
        }
    }

    if !missing.is_empty() {
        report.err(
            "The following claude-agent-daemon runtime files are NOT git-ignored at the repository root:",
        );
        report.err(missing.join("\n"));
        report.err("");
        report.fail(
            "Fix: add /agentd.db* and /agentd.sock* to .gitignore, so the documented 'Try it in one minute' walkthrough (run from the repository root) cannot leave the checkout dirty or park a conversation transcript as an untracked file.",
        );
        return Ok(report);
    }
    report.out("OK: the claude-agent-daemon example's default runtime files are git-ignored.");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(main_rs: &str, protocol_rs: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let src = dir.path().join("examples/claude-agent-daemon/src");
        std::fs::create_dir_all(&src).expect("mkdir");
        std::fs::write(src.join("main.rs"), main_rs).expect("main.rs");
        std::fs::write(src.join("protocol.rs"), protocol_rs).expect("protocol.rs");
        dir
    }

    const MAIN: &str = "    #[arg(long, env = \"AGENTD_DB\", default_value = \"agentd.db\")]\n";
    const PROTOCOL: &str = "pub const DEFAULT_SOCKET: &str = \"agentd.sock\";\n";

    #[test]
    fn every_ignored_default_passes() {
        let dir = fixture(MAIN, PROTOCOL);
        let mut asked = Vec::new();
        let report = evaluate(dir.path(), |name| {
            asked.push(name.to_owned());
            Ok(true)
        })
        .expect("evaluate");
        assert_eq!(report.code(), 0);
        assert_eq!(
            asked,
            [
                "agentd.db",
                "agentd.db-shm",
                "agentd.db-wal",
                "agentd.sock",
                "agentd.sock.lock"
            ]
        );
    }

    #[test]
    fn an_unignored_sidecar_is_listed() {
        let dir = fixture(MAIN, PROTOCOL);
        let report = evaluate(dir.path(), |name| Ok(!name.ends_with("-wal"))).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .starts_with("The following claude-agent-daemon runtime files are NOT git-ignored at the repository root:\nagentd.db-wal\n\nFix:"),
            "{}",
            report.stderr()
        );
    }

    #[test]
    fn a_renamed_default_fails_before_git_runs() {
        let dir = fixture("default_value = \"x\"\n", PROTOCOL);
        let report = evaluate(dir.path(), |_| panic!("git must not run")).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("could not find AGENTD_DB's default_value")
        );
    }
}
