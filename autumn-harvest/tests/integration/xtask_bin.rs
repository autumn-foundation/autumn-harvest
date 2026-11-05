//! Runs the workspace `xtask` binary from a guard test.
//!
//! The guards that once ran a shell or Python script now run the xtask port of
//! that script. `xtask()` builds the binary once per test process. Each call
//! then returns a fresh `Command` for it, rooted at the repository.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// Returns the repository root.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate has a parent directory")
        .to_path_buf()
}

/// Returns the path of the built `xtask` binary, and builds it on first use.
pub fn xtask_path() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let status = Command::new(cargo)
            .args(["build", "--quiet", "--package", "xtask"])
            .current_dir(repo_root())
            .status()
            .expect("start cargo build for xtask");
        assert!(status.success(), "cargo build -p xtask failed");
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map_or_else(|| repo_root().join("target"), PathBuf::from);
        let target = if target.is_absolute() {
            target
        } else {
            repo_root().join(target)
        };
        target
            .join("debug")
            .join(format!("xtask{}", std::env::consts::EXE_SUFFIX))
    })
}

/// Returns a `Command` for `xtask`, with the repository root as its directory.
pub fn xtask() -> Command {
    let mut command = Command::new(xtask_path());
    command.current_dir(repo_root());
    command
}
