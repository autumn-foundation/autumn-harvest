//! Helpers that every command shares.
//!
//! Keep this module small. A helper that serves one command belongs in that
//! command's own module.

use std::path::{Path, PathBuf};

use smop::prelude::*;

/// Returns the repository root.
///
/// The root is the parent of this crate's manifest directory. So the result
/// does not depend on the current directory of the caller.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// Returns `path` joined onto the repository root.
pub fn repo_path(path: impl AsRef<Path>) -> PathBuf {
    repo_root().join(path)
}

/// Reads a file under the repository root into a string.
pub fn read_repo_file(path: impl AsRef<Path>) -> Result<String> {
    let full = repo_path(path);
    std::fs::read_to_string(&full).with_context(|| format!("reading {}", full.display()))
}

/// Maps a pass or fail outcome to the process exit code of a gate.
pub fn gate_exit(passed: bool) -> std::process::ExitCode {
    if passed {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}
