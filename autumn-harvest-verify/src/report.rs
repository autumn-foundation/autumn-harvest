//! Text and JSON rendering plus the exit-code contract.

use serde::{Deserialize, Serialize};

use crate::verdict::WorkflowVerdict;

/// The whole run's output.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub model_version: String,
    pub rustc_version: String,
    pub workflows: Vec<WorkflowVerdict>,
    /// Allowlist entries that matched no analyzed workflow.
    #[serde(default)]
    pub unused_allowlist: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Counts for the success-metric triple.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub analyzed: usize,
    pub proven: usize,
    pub unknown: usize,
    pub found: usize,
    pub allowed: usize,
}

impl Report {
    #[must_use]
    pub fn summary(&self) -> Summary {
        todo!("RED phase: implemented in GREEN")
    }

    /// Exit code: 0 clean; 1 any `nondeterminism-found` (or, under `strict`, any `unknown`
    /// or unused allowlist entry).
    #[must_use]
    pub fn exit_code(&self, strict: bool) -> i32 {
        let _ = strict;
        todo!("RED phase: implemented in GREEN")
    }

    #[must_use]
    pub fn render_text(&self) -> String {
        todo!("RED phase: implemented in GREEN")
    }

    /// # Errors
    /// Never in practice; kept as `Result` for the serializer contract.
    pub fn render_json(&self) -> crate::Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| crate::Error::Other(e.to_string()))
    }
}
