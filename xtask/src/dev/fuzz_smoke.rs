//! `cargo xtask dev fuzz-smoke`: the port of `fuzz/smoke.sh`.
//!
//! A short local or manual smoke run of every fuzz target, about 15s each.
//! This is NOT a soak. It checks that each target still builds and runs clean
//! on a quick pass. For a real campaign, run one target with a larger budget:
//!
//! ```text
//! cd fuzz && cargo +nightly-2026-08-14 fuzz run fuzz_det_check_source -- -max_total_time=300
//! ```
//!
//! It needs the pinned nightly toolchain and cargo-fuzz:
//!
//! ```text
//! rustup toolchain install nightly-2026-08-14
//! cargo install cargo-fuzz
//! ```
//!
//! Options, from the environment:
//!
//! - `MAX_TOTAL_TIME=<secs>`: the budget per target (default 15).
//! - `FUZZ_TOOLCHAIN=<toolchain>`: the nightly to fuzz with.
//!
//! `fuzz/` is not a workspace member, and it builds on nightly only. So this
//! command runs `cargo +<nightly> fuzz` from `fuzz/`, through the `cargo` on
//! `PATH`. It does not use `$CARGO`. That variable names the binary of one
//! toolchain, which does not take a `+toolchain` argument.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use smop::prelude::*;

/// Every fuzz target, in the order the smoke run visits them.
///
/// `fuzz_nightly_wiring` in the integration suite checks that this list
/// matches the `[[bin]]` entries of `fuzz/Cargo.toml`.
pub const TARGETS: [&str; 5] = [
    "fuzz_workflow_event_deser",
    "fuzz_det_check_source",
    "fuzz_validate_target_url",
    "fuzz_failure_signature",
    "fuzz_replay",
];

/// The nightly that `.github/workflows/fuzz-nightly.yml` pins.
///
/// `FUZZ_TOOLCHAIN` overrides it. `fuzz_nightly_wiring` checks that the two
/// pins agree.
pub const DEFAULT_TOOLCHAIN: &str = "nightly-2026-08-14";

/// The default budget per target, in seconds.
pub const DEFAULT_MAX_TIME: &str = "15";

/// Returns the corpus directories of one target.
///
/// New inputs go to the first directory. The seeds are read only.
pub fn corpus_dirs(fuzz_dir: &Path, target: &str) -> Vec<PathBuf> {
    let mut dirs = vec![PathBuf::from(format!("corpus/{target}"))];
    let seeds = PathBuf::from(format!("seeds/{target}"));
    if fuzz_dir.join(&seeds).is_dir() {
        dirs.push(seeds);
    }
    dirs
}

/// Returns the `cargo` arguments that fuzz one target.
pub fn fuzz_args(toolchain: &str, target: &str, dirs: &[PathBuf], max_time: &str) -> Vec<String> {
    let mut args = vec![
        format!("+{toolchain}"),
        "fuzz".to_string(),
        "run".to_string(),
        target.to_string(),
    ];
    args.extend(dirs.iter().map(|dir| dir.display().to_string()));
    args.push("--".to_string());
    args.push(format!("-max_total_time={max_time}"));
    args
}

fn non_empty_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Arguments of `cargo xtask dev fuzz-smoke`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask dev fuzz-smoke`.
pub fn run(_args: Args) -> Result<ExitCode> {
    // Run from the fuzz/ crate directory, whatever the cwd of the caller is.
    let fuzz_dir = crate::util::repo_path("fuzz");
    let max_time = non_empty_var("MAX_TOTAL_TIME").unwrap_or_else(|| DEFAULT_MAX_TIME.into());
    let toolchain = non_empty_var("FUZZ_TOOLCHAIN").unwrap_or_else(|| DEFAULT_TOOLCHAIN.into());

    for target in TARGETS {
        println!("=== fuzzing {target} for {max_time}s ===");
        fs::create_dir_all(fuzz_dir.join("corpus").join(target))?;
        let dirs = corpus_dirs(&fuzz_dir, target);
        let status = sh::cmd("cargo")
            .args(fuzz_args(&toolchain, target, &dirs, &max_time))
            .dir(&fuzz_dir)
            .status()?;
        if !status.success() {
            return Ok(exit_code(status));
        }
    }

    println!("smoke run complete: all targets ran clean");
    Ok(ExitCode::SUCCESS)
}

/// Returns the shell exit code of a finished child, `128 + signal` for a signal.
fn exit_code(status: std::process::ExitStatus) -> ExitCode {
    crate::perf::support::exit_code(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzz_args_put_the_toolchain_first_and_the_budget_last() {
        let dirs = vec![PathBuf::from("corpus/t"), PathBuf::from("seeds/t")];
        assert_eq!(
            fuzz_args("nightly-x", "t", &dirs, "15").join(" "),
            "+nightly-x fuzz run t corpus/t seeds/t -- -max_total_time=15"
        );
    }

    #[test]
    fn corpus_dirs_add_the_seeds_only_when_they_exist() {
        let dir = fs::temp_dir().unwrap();
        fs::create_dir_all(dir.path().join("seeds/with")).unwrap();
        assert_eq!(corpus_dirs(dir.path(), "with").len(), 2);
        assert_eq!(
            corpus_dirs(dir.path(), "without"),
            vec![PathBuf::from("corpus/without")]
        );
    }

    #[test]
    fn every_target_has_a_fuzz_binary() {
        let manifest = crate::util::read_repo_file("fuzz/Cargo.toml").unwrap();
        for target in TARGETS {
            assert!(
                manifest.contains(&format!("name = \"{target}\"")),
                "{target}"
            );
        }
    }
}
