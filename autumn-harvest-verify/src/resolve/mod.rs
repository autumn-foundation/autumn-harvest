//! Call-target resolution: free fns, `<impl at file:l:c>` bodies (via `syn`), closures,
//! async bodies, generic substitution and RTA-lite devirtualization.

use std::path::PathBuf;

use crate::mir::MirDoc;

/// Index of source files needed to resolve `<impl at file:line:col>` headers.
#[derive(Debug, Clone, Default)]
pub struct SourceRoots {
    /// Directories that `<impl at PATH>` paths are relative to (workspace root first).
    pub roots: Vec<PathBuf>,
}

/// The resolved program: all bodies across all docs plus the lookup tables.
#[derive(Debug, Default)]
pub struct Program {
    pub docs: Vec<MirDoc>,
}

impl Program {
    /// Build the resolution tables. Unresolvable impl headers are kept and surface as
    /// `missing-body` boundaries when called.
    ///
    /// # Errors
    /// Only on i/o failure reading a source root that exists but is unreadable.
    pub fn build(docs: Vec<MirDoc>, sources: &SourceRoots) -> crate::Result<Self> {
        let _ = sources;
        let _ = docs;
        todo!("RED phase: implemented in GREEN")
    }
}
