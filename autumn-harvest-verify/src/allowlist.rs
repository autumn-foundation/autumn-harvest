//! Checked-in allowlist (`harvest-verify.allow.toml`) — the AC5 escape hatch.
//! A file rather than an attribute because AC7 forbids macro-path changes.

use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllowEntry {
    /// Fully-qualified workflow fn path (`crate::module::name`).
    pub workflow: String,
    /// Required, non-blank.
    pub justification: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Allowlist {
    #[serde(default)]
    pub allow: Vec<AllowEntry>,
}

impl Allowlist {
    /// Load and validate. A blank justification is an error, not a warning.
    ///
    /// # Errors
    /// On i/o failure, malformed TOML, duplicate workflows or a blank justification.
    pub fn load(path: &Path) -> crate::Result<Self> {
        let _ = path;
        todo!("RED phase: implemented in GREEN")
    }

    /// Validate an in-memory allowlist (same rules as [`Self::load`]).
    ///
    /// # Errors
    /// On duplicate workflows or a blank justification.
    pub fn validate(&self) -> crate::Result<()> {
        todo!("RED phase: implemented in GREEN")
    }

    /// Justification for `workflow`, if allowlisted.
    #[must_use]
    pub fn justification(&self, workflow: &str) -> Option<&str> {
        let _ = workflow;
        todo!("RED phase: implemented in GREEN")
    }

    /// Entries whose workflow is not in `used` (reported as warnings; errors under `--strict`).
    #[must_use]
    pub fn unused(&self, used: &BTreeSet<String>) -> Vec<&AllowEntry> {
        let _ = used;
        todo!("RED phase: implemented in GREEN")
    }
}
