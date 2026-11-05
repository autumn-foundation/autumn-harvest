//! `cargo xtask check no-powershell-env-syntax`: the port of
//! `scripts/check-no-powershell-env-syntax.sh`.
//!
//! The guard fails if any ```` ```bash ```` fenced block in the docs or a
//! README uses the `$env:VAR = "value"` env-var assignment of PowerShell.
//!
//! Mechanism this guards against: the "Run" section of
//! `examples/standalone-runner/README.md` fenced its command block as
//! ```` ```bash ````. Every sibling example does the same
//! (`examples/quickstart`, `examples/billing-autumn-web`). But the two lines
//! that set `DATABASE_URL` and `AUTUMN_PROFILE` used the PowerShell form
//! `$env:NAME = "value"`. Bash needs the `NAME=value` shape, and every other
//! example in the repository uses it.
//!
//! A live paste into bash reproduced the failure. Each line prints
//! `bash: line N: :NAME: command not found` and bash skips it silently,
//! because an interactive shell has no `set -e`. The reader sees two red
//! error lines and has no reason to suspect them. The block says bash, and
//! the lines look like env-var assignments. So the reader moves on with
//! neither variable set.
//!
//! A hardcoded default in `examples/standalone-runner/src/server.rs` gives
//! `DATABASE_URL` the same value, which masks that half. But without
//! `AUTUMN_PROFILE=dev` the runner skips the auto-migration branch. So the
//! failure surfaces later and elsewhere. The harvest runner cannot find its
//! tables in a database that no migration ever touched.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::default_run::support::{self, Report};
use crate::util;

/// Arguments of `cargo xtask check no-powershell-env-syntax`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check no-powershell-env-syntax`.
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root())?.emit())
}

/// Returns each PowerShell assignment inside a bash fence of `text`.
///
/// Each entry holds the 1-based line number and the line. A bare file-wide
/// grep would also flag a legitimate ```` ```powershell ```` fence, where the
/// syntax is correct. `docs/plans/2026-07-06-consolidate-integration-tests.md`
/// has two such fences. So the guard tracks the fence language per file. It
/// only checks lines inside a ```` ```bash ```` fence (PR #1453).
fn violations(text: &str) -> Result<Vec<(usize, String)>> {
    let fence = Regex::new(r"^ {0,3}```")?;
    let fence_lang = Regex::new(r"^ {0,3}```[ \t]*")?;
    let assignment = Regex::new(r"^[ \t]*\$env:[A-Za-z_][A-Za-z0-9_]*[ \t]*=")?;
    let mut in_fence = false;
    let mut lang = String::new();
    let mut found = Vec::new();
    for (index, line) in support::lines(text).into_iter().enumerate() {
        if fence.is_match(line) {
            if in_fence {
                in_fence = false;
            } else {
                in_fence = true;
                lang = fence_lang
                    .replace(&line.to_ascii_lowercase(), "")
                    .into_owned();
            }
            continue;
        }
        if in_fence && lang == "bash" && assignment.is_match(line) {
            found.push((index + 1, line.to_owned()));
        }
    }
    Ok(found)
}

/// Scans every markdown file under `root` that opens a bash fence.
fn evaluate(root: &Path) -> Result<Report> {
    let opens_bash = Regex::new(r"^ {0,3}```bash")?;
    let mut report = Report::new();
    let mut count = 0_usize;
    for (shown, path) in support::markdown_files(root) {
        let text = support::read_or_empty(&path);
        if !support::lines(&text)
            .into_iter()
            .any(|line| opens_bash.is_match(line))
        {
            continue;
        }
        for (line_no, line) in violations(&text)? {
            // The old guard read `NR:$0` back with `IFS=: read`, which drops
            // a lone trailing colon from the line.
            let line = support::bash_read_colon_rest(&line);
            report.err(format!(
                "{shown}:{line_no}: PowerShell env-var syntax '{line}' in a bash code block; bash requires 'NAME=value', not '$env:NAME = \"value\"'."
            ));
            count += 1;
        }
    }

    if count > 0 {
        report.err("");
        report.fail(
            "Fix: use the shell's own 'NAME=value' assignment (inline before the command, matching every other example in this repository).",
        );
        return Ok(report);
    }
    report.out("OK: no PowerShell env-var assignment syntax in any documented bash block.");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(files: &[(&str, &str)]) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        for (path, body) in files {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().expect("parent")).expect("mkdir");
            std::fs::write(full, body).expect("write");
        }
        dir
    }

    #[test]
    fn a_powershell_fence_and_a_clean_bash_fence_pass() {
        let dir = fixture(&[
            (
                "a.md",
                "```powershell\n$env:X = \"1\"\n```\n```bash\nX=1 cargo run\n```\n",
            ),
            ("target/b.md", "```bash\n$env:X = \"1\"\n```\n"),
        ]);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn an_assignment_in_a_bash_fence_fails_with_its_line() {
        let dir = fixture(&[(
            "examples/r/README.md",
            "# Run\n\n```BASH\n  $env:DATABASE_URL = \"postgres://x\"\ncargo run\n```\n",
        )]);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(
            report.code(),
            0,
            "an upper-case BASH fence is not opened by grep"
        );

        let dir = fixture(&[(
            "examples/r/README.md",
            "# Run\n\n```bash\n  $env:DATABASE_URL = \"postgres://x\"\n```\n",
        )]);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert_eq!(
            report.stderr(),
            "./examples/r/README.md:4: PowerShell env-var syntax '  $env:DATABASE_URL = \"postgres://x\"' in a bash code block; bash requires 'NAME=value', not '$env:NAME = \"value\"'.\n\nFix: use the shell's own 'NAME=value' assignment (inline before the command, matching every other example in this repository).\n"
        );
    }

    #[test]
    fn a_fence_with_a_title_is_not_bash() {
        let found = violations("```bash title\n$env:X = 1\n```\n```bash\n$env:Y=2\n```\n")
            .expect("violations");
        assert_eq!(found, vec![(5, "$env:Y=2".to_owned())]);
    }
}
