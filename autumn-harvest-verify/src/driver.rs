//! Cargo driver: emits MIR for the requested targets into an owned target dir and
//! returns the `.mir` files (located from cargo's JSON artifact messages).

use std::path::PathBuf;

/// What to build.
#[derive(Debug, Clone, Default)]
pub struct BuildRequest {
    pub manifest_path: Option<PathBuf>,
    pub packages: Vec<String>,
    pub lib: bool,
    pub examples: Vec<String>,
    pub all_examples: bool,
    pub bins: Vec<String>,
    pub features: Vec<String>,
    pub no_default_features: bool,
    pub target_dir: Option<PathBuf>,
}

/// One emitted MIR file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedMir {
    pub crate_name: String,
    pub target_kind: String,
    pub path: PathBuf,
}

/// Emit MIR. Refuses optimized builds (`-C opt-level` ≠ 0 would inline helper calls away).
///
/// # Errors
/// When cargo fails, when the profile is optimized, or when no `.mir` file is produced.
pub fn emit_mir(req: &BuildRequest) -> crate::Result<Vec<EmittedMir>> {
    let _ = req;
    todo!("RED phase: implemented in GREEN")
}

/// The workspace root for `manifest_path` (via `cargo locate-project --workspace`).
///
/// # Errors
/// When cargo cannot locate the workspace.
pub fn workspace_root(manifest_path: Option<&std::path::Path>) -> crate::Result<PathBuf> {
    let _ = manifest_path;
    todo!("RED phase: implemented in GREEN")
}
