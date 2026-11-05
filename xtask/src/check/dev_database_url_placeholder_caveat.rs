//! `cargo xtask check dev-database-url-placeholder-caveat`: the port of
//! `scripts/check-dev-database-url-placeholder-caveat.sh`.
//!
//! The guard fails if the `HARVEST_DEV_DATABASE_URL` examples of chapter 1
//! promote the literal `postgres://me@localhost:5432/harvest_dev` DSN without
//! a caveat. The caveat says that `me` is a placeholder. The reader must
//! replace it with a role that can authenticate.
//!
//! Mechanism this guards against: `docs/getting-started/01-project-skeleton.md`
//! gives `HARVEST_DEV_DATABASE_URL=postgres://me@localhost:5432/harvest_dev
//! cargo dev` as a directly-runnable command. It does so twice, right after
//! the zero-config `cargo dev` example. Nothing marks `me` as symbolic: no
//! `<user>`-style brackets and no sentence. So it reads as copy-pasteable.
//!
//! A live run against an unmodified checkout reproduced the failure. The
//! verbatim command fails against an ordinary password-authenticated
//! Postgres with `fe_sendauth: no password supplied`. That is the default
//! for an apt or brew install or a fresh `initdb`. The error never mentions
//! `HARVEST_DEV_DATABASE_URL`, never says that `me` needs a replacement, and
//! points nowhere. Real credentials
//! (`postgres://postgres:postgres@localhost:5432/harvest_dev`) against the
//! same checkout succeed end to end. Migrations apply, the worker registers,
//! and the banner prints. So the defect is the undocumented placeholder, not
//! the runtime.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::default_run::support::{self, Report};
use crate::util;

const DOC: &str = "docs/getting-started/01-project-skeleton.md";

/// Arguments of `cargo xtask check dev-database-url-placeholder-caveat`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check dev-database-url-placeholder-caveat`.
#[allow(
    clippy::unnecessary_wraps,
    reason = "every `check` command keeps the `run` signature that `check/mod.rs` calls"
)]
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root()).emit())
}

/// Checks chapter 1 under `root` for the placeholder caveat.
fn evaluate(root: &Path) -> Report {
    let mut report = Report::new();
    if support::read_or_empty(&root.join(DOC)).contains("is a placeholder") {
        report.out("OK: the HARVEST_DEV_DATABASE_URL placeholder caveat is documented.");
        return report;
    }
    report.err(format!(
        "{DOC} promotes the literal `postgres://me@localhost:5432/harvest_dev`"
    ));
    report.err("DSN without explaining that `me` is a placeholder for a role that can");
    report.err("actually authenticate.");
    report.err("");
    report.err("Fix: add a caveat next to the example explaining that `me` must be");
    report.err("replaced with a real, authenticating role (peer, trust, or a password");
    report.err("in the DSN) — copied verbatim against an ordinary password-authenticated");
    report.err("Postgres it fails immediately with Postgres's own \"fe_sendauth: no");
    report.err("password supplied\", which names neither HARVEST_DEV_DATABASE_URL nor");
    report.fail("this doc.");
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(body: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(DOC);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        std::fs::write(doc, body).expect("doc");
        dir
    }

    #[test]
    fn the_caveat_passes() {
        let dir = fixture("`me` is a placeholder for your role.\n");
        assert_eq!(evaluate(dir.path()).code(), 0);
    }

    #[test]
    fn a_missing_caveat_fails_with_the_fix() {
        let dir = fixture("HARVEST_DEV_DATABASE_URL=postgres://me@localhost:5432/harvest_dev\n");
        let report = evaluate(dir.path());
        assert_eq!(report.code(), 1);
        assert!(report.stderr().ends_with("nor\nthis doc.\n"));
    }

    #[test]
    fn a_missing_doc_fails() {
        let dir = fs::temp_dir().expect("temp dir");
        assert_eq!(evaluate(dir.path()).code(), 1);
    }
}
