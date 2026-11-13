//! `cargo xtask check dev-runtime-root-caveat`: the port of
//! `scripts/check-dev-runtime-root-caveat.sh`.
//!
//! The guard fails if the "root" caveat for `cargo dev` is missing from either
//! doc that promotes it as the fastest zero-setup path.
//!
//! Mechanism this guards against: two docs promote `cargo dev`. They are the
//! "Quick example" of `README.md` and
//! `docs/getting-started/01-project-skeleton.md`. Both bill it as needing "no
//! database, no Docker, and nothing to configure" or "only the Rust toolchain
//! installed". Neither used to mention that PostgreSQL refuses to run as
//! `root`. So `cargo dev` refuses too, deliberately, per the doc comment of
//! `autumn-harvest-plugin/src/dev/postgres.rs`.
//!
//! That is not a corner case. Docker devcontainers, many CI base images and
//! some cloud sandboxes default to a root shell. A newcomer who follows
//! either doc verbatim there hits an undocumented hard stop on the very first
//! command. A live run against an unmodified checkout reproduced it.
//! `cargo dev` exits immediately with `DevError::RunningAsRoot` ("refusing to
//! provision a cluster as root — PostgreSQL will not run as root, by design
//! ..."). That refusal is deliberate, not a bug. So the fix documents it and
//! does not remove it.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report};
use crate::util;

const DOCS: [&str; 2] = ["README.md", "docs/getting-started/01-project-skeleton.md"];

/// Arguments of `cargo xtask check dev-runtime-root-caveat`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check dev-runtime-root-caveat`.
#[allow(
    clippy::unnecessary_wraps,
    reason = "every `check` command keeps the `run` signature that `check/mod.rs` calls"
)]
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root()).emit())
}

/// Checks both front-door docs under `root` for the root caveat.
fn evaluate(root: &Path) -> Report {
    let missing: Vec<&str> = DOCS
        .into_iter()
        .filter(|doc| !support::read_or_empty(&root.join(doc)).contains("refuses to run as `root`"))
        .collect();
    let mut report = Report::new();
    if missing.is_empty() {
        report.out("OK: the cargo-dev root caveat is documented in both front-door docs.");
        return report;
    }
    report.err("The following docs promote `cargo dev` without the root caveat:");
    report.err(missing.join("\n"));
    report.err("");
    report.err("Fix: document that PostgreSQL — and so `cargo dev` — refuses to run");
    report.err("as root (autumn-harvest-plugin/src/dev/postgres.rs, refuse_to_run_as_root),");
    report.err("and point to the Postgres-in-its-own-container alternatives that are");
    report.fail("unaffected (examples/quickstart, the bring-your-own-Postgres path).");
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(readme: &str, chapter: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        for (path, body) in DOCS.into_iter().zip([readme, chapter]) {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().expect("parent")).expect("mkdir");
            std::fs::write(full, body).expect("write");
        }
        dir
    }

    const CAVEAT: &str = "PostgreSQL refuses to run as `root`.\n";

    #[test]
    fn both_caveats_pass() {
        let dir = fixture(CAVEAT, CAVEAT);
        assert_eq!(evaluate(dir.path()).code(), 0);
    }

    #[test]
    fn each_doc_without_the_caveat_is_listed() {
        let dir = fixture("nothing\n", "nothing\n");
        let report = evaluate(dir.path());
        assert_eq!(report.code(), 1);
        assert!(report.stderr().starts_with(
            "The following docs promote `cargo dev` without the root caveat:\nREADME.md\ndocs/getting-started/01-project-skeleton.md\n\nFix:"
        ));
    }

    #[test]
    fn one_missing_doc_is_listed_alone() {
        let dir = fixture(CAVEAT, "refuses to run as root\n");
        let report = evaluate(dir.path());
        assert!(
            report
                .stderr()
                .contains(":\ndocs/getting-started/01-project-skeleton.md\n\n")
        );
    }
}
