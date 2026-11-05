//! `cargo xtask check verus-proofs`: verify every Verus proof in
//! `formal/verus`.
//!
//! This is the port of `scripts/check-verus-proofs.sh`.
//!
//! Verus checks the crate root `formal/verus/src/lib.rs`. It must report 0
//! errors. The guard suite `verus_proofs_coverage.rs` checks that each
//! mirrored function matches the engine source.
//!
//! `VERUS` names a local verus binary. Without it, the command downloads the
//! pinned release and checks its SHA-256. Pin a weekly release. Verus keeps
//! only the newest rolling release, so a rolling pin returns 404 when the
//! next one ships. The release names the Rust toolchain that it links
//! against, and rustup installs that toolchain.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::formal_models::{finish, grep_lines, step, tee_merged, verify_sha256};
use crate::util::repo_path;

/// The pinned Verus release.
///
/// Pin a weekly release (`release/<version>`), not a rolling one. Verus keeps
/// the assets of only the newest rolling release, so a rolling pin returns
/// 404 within a day. Weekly release assets stay.
const VERUS_RELEASE: &str = "0.2026.10.04.426d8b0";

/// The SHA-256 of the zip of [`VERUS_RELEASE`].
const VERUS_SHA256: &str = "624018662868563a7bec0dd07be5e4dd0c3d61124b4b55105765884bf2d4d1a0";

/// The crate root that Verus checks.
const CRATE_ROOT: &str = "formal/verus/src/lib.rs";

/// The result line of a run that verifies at least one item with no error.
const RESULT_LINE: &str = r"^verification results:: [1-9][0-9]* verified, 0 errors$";

/// Arguments of `cargo xtask check verus-proofs`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask check verus-proofs`.
pub fn run(_args: Args) -> Result<ExitCode> {
    finish(check_proofs())
}

/// Returns the verus binary: `VERUS`, or the pinned release in `work`.
fn verus_binary(work: &Path) -> Result<PathBuf> {
    if let Some(verus) = std::env::var_os("VERUS").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(verus));
    }
    let zip = work.join("verus.zip");
    step(
        sh::cmd("curl")
            .args(["-fsSL", "--retry", "3", "-o"])
            .arg(&zip)
            .arg(format!(
                "https://github.com/verus-lang/verus/releases/download/release%2F{VERUS_RELEASE}/verus-{VERUS_RELEASE}-x86-linux.zip"
            )),
    )?;
    verify_sha256(&zip, VERUS_SHA256)?;
    step(sh::cmd("unzip").arg("-q").arg(&zip).arg("-d").arg(work))?;
    std::fs::remove_file(&zip).with_context(|| format!("removing {}", zip.display()))?;
    Ok(work.join("verus-x86-linux/verus"))
}

/// The toolchain that `version.json` names.
///
/// The file names it as `<name> (<note>)`. The name is the first word.
fn toolchain(version_json: &str) -> Result<String> {
    let doc: serde_json::Value = serde_json::from_str(version_json)?;
    let text = doc["verus"]["toolchain"]
        .as_str()
        .context("version.json has no verus.toolchain string")?;
    text.split_whitespace()
        .next()
        .map(str::to_string)
        .context("verus.toolchain is empty")
}

/// True when the log reports a run with no error.
fn verified(log: &str) -> Result<bool> {
    let result = Regex::new(RESULT_LINE)?;
    Ok(grep_lines(log).any(|l| result.is_match(l)))
}

/// Runs Verus, and returns the exit code of the gate.
fn check_proofs() -> Result<ExitCode> {
    let work = fs::temp_dir()?;
    let verus = verus_binary(work.path())?;

    let dir = verus.parent().unwrap_or_else(|| Path::new(""));
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let version = fs::read_string(dir.join("version.json"))?;
    let toolchain = toolchain(&version)?;
    step(sh::cmd("rustup").args(["toolchain", "install", &toolchain, "--profile", "minimal"]))?;

    let mut command = std::process::Command::new(&verus);
    // `cargo xtask` runs under rustup, which can set RUSTUP_TOOLCHAIN.
    // The script ran with none, so verus chose its own toolchain.
    command
        .arg(repo_path(CRATE_ROOT))
        .arg("--crate-type=lib")
        .env_remove("RUSTUP_TOOLCHAIN");
    let (status, log) = tee_merged(&mut command)?;
    let code = super::formal_models::shell_code(status);
    if code != 0 {
        eprintln!("verus exited {code}");
        return Ok(ExitCode::FAILURE);
    }
    if !verified(&String::from_utf8_lossy(&log))? {
        eprintln!("want: verification results:: <n> verified, 0 errors");
        return Ok(ExitCode::FAILURE);
    }
    println!("all Verus proofs verified");
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_toolchain_is_the_first_word() {
        let json = r#"{"verus": {"toolchain": "1.88.0-x86_64-unknown-linux-gnu (note)"}}"#;
        assert_eq!(
            toolchain(json).expect("parses"),
            "1.88.0-x86_64-unknown-linux-gnu"
        );
        assert!(toolchain(r#"{"verus": {}}"#).is_err());
        assert!(toolchain("not json").is_err());
    }

    #[test]
    fn the_result_line_needs_a_verified_item_and_no_error() {
        let ok = "x\nverification results:: 12 verified, 0 errors\n";
        assert!(verified(ok).expect("ok"));
        assert!(!verified("verification results:: 0 verified, 0 errors\n").expect("ok"));
        assert!(!verified("verification results:: 3 verified, 1 errors\n").expect("ok"));
        assert!(!verified("verification results:: 3 verified, 0 errors \n").expect("ok"));
    }

    #[test]
    fn the_crate_root_exists() {
        assert!(repo_path(CRATE_ROOT).is_file());
    }
}
