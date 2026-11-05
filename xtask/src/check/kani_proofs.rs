//! `cargo xtask check kani-proofs`: run every Kani proof in `autumn-harvest`
//! and check the count (issue #1819).
//!
//! This is the port of `scripts/check-kani-proofs.sh`.
//!
//! `cargo kani` exits 0 when it finds no proof, and when a `kani::cover!` is
//! unsatisfiable. This command fails in both cases. It also fails when Kani
//! verifies fewer proofs than the source holds, for example when a feature
//! that gates a proof is missing.

use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::formal_models::{StepFailed, finish, grep_lines, shell_code, tee_merged};
use crate::util::repo_root;

/// The Kani run. `formal_models_coverage.rs` reads this line.
///
/// `chaos` turns on the chaos controller, which holds a proof. `-Z stubbing`
/// enables the `mix64` stub of the jitter proofs.
const KANI_RUN: &str =
    "cargo kani -p autumn-harvest --no-default-features --features chaos -Z stubbing";

/// A line that marks a proof: `#[kani::proof]` alone, after white space.
const PROOF_LINE: &str = r"^[[:space:]]*#\[kani::proof\]$";

/// A Kani line that counts the satisfied cover properties.
const COVER_LINE: &str = r"^ \*\* [0-9]+ of [0-9]+ cover properties satisfied";

/// Arguments of `cargo xtask check kani-proofs`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask check kani-proofs`.
pub fn run(_args: Args) -> Result<ExitCode> {
    finish(check_proofs())
}

/// Counts the proof lines of each file under `autumn-harvest/src`.
///
/// This matches `grep -rhxE` on the tree. Symlinks are not followed.
fn count_proofs(src: &std::path::Path) -> Result<usize> {
    let proof = Regex::new(PROOF_LINE)?;
    let mut count = 0;
    for entry in walkdir::WalkDir::new(src) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let bytes = std::fs::read(entry.path())
            .with_context(|| format!("reading {}", entry.path().display()))?;
        let text = String::from_utf8_lossy(&bytes);
        count += grep_lines(&text).filter(|l| proof.is_match(l)).count();
    }
    Ok(count)
}

/// The summary line that Kani prints when it verifies `want` proofs.
fn summary(want: usize) -> String {
    format!("Complete - {want} successfully verified harnesses, 0 failures, {want} total.")
}

/// True when a cover line reports fewer satisfied properties than it has.
///
/// The fields compare as numbers, as `awk` compares them.
fn cover_unsatisfied(log: &str) -> Result<bool> {
    let cover = Regex::new(COVER_LINE)?;
    Ok(grep_lines(log).filter(|l| cover.is_match(l)).any(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let number = |at: usize| {
            fields
                .get(at)
                .map_or("0", |f| f.trim_start_matches('0'))
                .to_string()
        };
        number(1) != number(3)
    }))
}

/// Runs Kani, and returns the exit code of the gate.
fn check_proofs() -> Result<ExitCode> {
    let root = repo_root();
    let want = count_proofs(&root.join("autumn-harvest/src"))?;
    if want == 0 {
        eprintln!("no #[kani::proof] in autumn-harvest/src");
        return Ok(ExitCode::FAILURE);
    }

    let mut words = KANI_RUN.split_whitespace();
    let program = words.next().context("the Kani run names a program")?;
    let mut command = std::process::Command::new(program);
    // `cargo xtask` runs under rustup, which can set RUSTUP_TOOLCHAIN.
    // The script ran with none, so rust-toolchain.toml chose the toolchain.
    command
        .args(words)
        .current_dir(&root)
        .env_remove("RUSTUP_TOOLCHAIN");
    let (status, log) = tee_merged(&mut command)?;
    if !status.success() {
        return Err(StepFailed(shell_code(status)).into());
    }
    let log = String::from_utf8_lossy(&log);

    let summary = summary(want);
    if !grep_lines(&log).any(|l| l == summary) {
        eprintln!("want: {summary}");
        return Ok(ExitCode::FAILURE);
    }
    if cover_unsatisfied(&log)? {
        eprintln!("a kani::cover! property is not satisfied");
        return Ok(ExitCode::FAILURE);
    }
    println!("all {want} Kani proofs verified");
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_lines_match_whole_lines_only() {
        let dir = fs::temp_dir().expect("temp dir");
        std::fs::create_dir(dir.path().join("sub")).expect("mkdir");
        std::fs::write(
            dir.path().join("a.rs"),
            "#[kani::proof]\n    #[kani::proof]\n#[kani::proof] fn x() {}\n// #[kani::proof]\n",
        )
        .expect("write");
        std::fs::write(dir.path().join("sub/b.txt"), "\t#[kani::proof]").expect("write");
        std::fs::write(dir.path().join("sub/c.rs"), "#[kani::proof]\r\n").expect("write");
        assert_eq!(count_proofs(dir.path()).expect("count"), 3);
    }

    #[test]
    fn the_engine_holds_proofs() {
        let src = repo_root().join("autumn-harvest/src");
        assert!(count_proofs(&src).expect("count") >= 3);
    }

    #[test]
    fn the_summary_names_the_count() {
        assert_eq!(
            summary(4),
            "Complete - 4 successfully verified harnesses, 0 failures, 4 total."
        );
    }

    #[test]
    fn cover_lines_compare_their_counts() {
        assert!(!cover_unsatisfied("x\n ** 2 of 2 cover properties satisfied\n").expect("ok"));
        assert!(!cover_unsatisfied(" ** 02 of 2 cover properties satisfied").expect("ok"));
        assert!(cover_unsatisfied(" ** 1 of 2 cover properties satisfied\n").expect("ok"));
        assert!(!cover_unsatisfied("  ** 1 of 2 cover properties satisfied\n").expect("ok"));
        assert!(!cover_unsatisfied("no cover lines\n").expect("ok"));
    }

    #[test]
    fn the_run_keeps_the_flags_of_a_full_run() {
        for flag in ["--features chaos", "-Z stubbing"] {
            assert!(KANI_RUN.contains(flag));
        }
        for flag in ["--harness", "--exact", "--only-codegen"] {
            assert!(!KANI_RUN.contains(flag));
        }
    }
}
