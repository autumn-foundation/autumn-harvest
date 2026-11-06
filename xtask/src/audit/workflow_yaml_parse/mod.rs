//! `cargo xtask audit workflow-yaml-parse`: every `.github/workflows/*.yml`
//! must parse as YAML.
//!
//! This is the port of `docs/audits/workflow-yaml-parse.py`, a Semaphore CI
//! harness.
//!
//! GitHub does not reject a workflow file that it cannot parse. It runs the
//! file as a zero-job workflow named after its path, and it reports a failed run
//! on every push. The triggers in the file never fire, so a scheduled suite
//! silently never runs. `chaos.yml` was in this state from its first commit.
//!
//! The command parses each workflow as `PyYAML` `SafeLoader` does. It counts each
//! file that does not parse, that repeats a mapping key, or that has no
//! non-empty `jobs` mapping. It exits 1 when it counts one or more.
//!
//! The Python script needed `PyYAML`, and a missing `PyYAML` was a hard failure.
//! A skipped check would report green over an unchecked file. The port
//! compiles its parser in, so it cannot skip.
//!
//! The submodules hold the Python semantics that the audits of this unit
//! share. [`pyscan`] and [`pyparse`] port the pure Python scanner and parser
//! of `PyYAML`. [`pyyaml`] composes and constructs values on top of them. So
//! each audit accepts, rejects and words a YAML error as its script did.
//! [`pytext`] reads text as Python does.

pub mod pyparse;
pub mod pyscan;
pub mod pytext;
pub mod pyyaml;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use smop::prelude::*;

use crate::util;

/// Arguments of `cargo xtask audit workflow-yaml-parse`. It takes none.
#[derive(Debug, Args)]
pub struct Args {}

/// Returns the workflow files under `root`, sorted as Python sorts paths.
fn workflow_files(root: &Path) -> Result<Vec<PathBuf>> {
    let pattern = root.join(".github/workflows/*.y*ml");
    let pattern = pattern
        .to_str()
        .context("the repository path is not valid UTF-8")?;
    fs::glob(pattern)
}

/// Returns the finding for one workflow text, or `None`.
///
/// `PyYAML` keeps the last value of a repeated key silently, and GitHub rejects
/// the workflow. So the load uses the repeated-key check.
fn check(text: &str) -> Option<String> {
    match pyyaml::load(text, true) {
        Err(error) => Some(error.render(text)),
        Ok(doc) if !doc.root_has_nonempty_dict("jobs") => {
            Some("`jobs` is not a non-empty mapping".to_owned())
        }
        Ok(_) => None,
    }
}

/// Checks each workflow under `root`. Returns the output lines and the count
/// of bad files.
fn audit(root: &Path) -> Result<(Vec<String>, usize)> {
    let files = workflow_files(root)?;
    let mut lines = Vec::new();
    let mut bad = 0;
    for path in &files {
        let text = pytext::read_text(path)?.unwrap_or_default();
        let rel = path.strip_prefix(root).unwrap_or(path).display();
        if let Some(finding) = check(&text) {
            lines.push(format!("{rel}: {finding}"));
            bad += 1;
        }
    }
    lines.push(format!(
        "workflow-yaml-parse: {} files, {bad} unparsable",
        files.len()
    ));
    Ok((lines, bad))
}

/// Runs `cargo xtask audit workflow-yaml-parse`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let (lines, bad) = audit(&util::repo_root())?;
    for line in lines {
        println!("{line}");
    }
    Ok(util::gate_exit(bad == 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_workflow_with_jobs_passes() {
        assert_eq!(check("on: push\njobs:\n  a:\n    runs-on: x\n"), None);
    }

    #[test]
    fn a_repeated_key_is_a_finding() {
        let finding = check("on: push\njobs:\n  a: {}\n  a: {}\n").unwrap_or_default();
        assert!(
            finding.starts_with("duplicate mapping key 'a'"),
            "{finding}"
        );
    }

    #[test]
    fn on_and_true_are_distinct_keys() {
        assert_eq!(check("on: push\ntrue: 1\njobs: {a: 1}\n"), None);
    }

    #[test]
    fn missing_or_empty_jobs_is_a_finding() {
        let finding = Some("`jobs` is not a non-empty mapping".to_owned());
        assert_eq!(check("on: push\n"), finding);
        assert_eq!(check("jobs: {}\n"), finding);
        assert_eq!(check("jobs: [a]\n"), finding);
        assert_eq!(check(""), finding);
        assert_eq!(check("- jobs\n"), finding);
    }

    #[test]
    fn jobs_from_a_merge_key_count() {
        assert_eq!(check("base: &b {jobs: {a: 1}}\n<<: *b\n"), None);
    }

    #[test]
    fn an_unparsable_file_is_a_finding() {
        let finding = check("jobs: [unclosed\n").unwrap_or_default();
        assert!(
            finding.contains("while parsing a flow sequence"),
            "{finding}"
        );
    }

    #[test]
    fn the_real_workflows_parse() {
        let (lines, bad) = audit(&util::repo_root()).unwrap_or_default();
        assert_eq!(bad, 0, "{lines:?}");
    }

    #[test]
    fn a_tree_with_one_bad_file_counts_it() {
        let dir = fs::temp_dir().unwrap_or_else(|error| panic!("{error:#}"));
        let workflows = dir.path().join(".github/workflows");
        std::fs::create_dir_all(&workflows).unwrap_or_else(|error| panic!("{error}"));
        std::fs::write(workflows.join("a.yml"), "jobs: {a: 1}\n").unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(workflows.join("b.yaml"), "on: x\n").unwrap_or_else(|e| panic!("{e}"));
        let (lines, bad) = audit(dir.path()).unwrap_or_default();
        assert_eq!(bad, 1);
        assert_eq!(
            lines,
            [
                ".github/workflows/b.yaml: `jobs` is not a non-empty mapping",
                "workflow-yaml-parse: 2 files, 1 unparsable",
            ]
        );
    }
}
