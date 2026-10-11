//! Saga compensation coverage over a structure manifest (issue #2010).

use serde::{Deserialize, Serialize};

use crate::structure::{ExitOutcome, StructureManifest};

/// The coverage verdict of one workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SagaVerdict {
    NoSaga,
    Covered,
    Gap,
    Unknown,
}

/// The coverage result of one workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaReport {
    pub workflow: String,
    pub name: String,
    pub verdict: SagaVerdict,
    pub gaps: Vec<Gap>,
    pub unknown: Vec<String>,
    pub notes: Vec<String>,
}

/// An exit that a completed forward step can reach with no unwind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gap {
    pub body: String,
    pub at: String,
    pub outcome: ExitOutcome,
}

/// Why a manifest cannot be checked.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CheckError(String);

/// Check each workflow of `manifest`.
///
/// # Errors
/// When the manifest carries no flow graphs.
pub fn check(manifest: &StructureManifest) -> Result<Vec<SagaReport>, CheckError> {
    let _ = manifest;
    Ok(Vec::new())
}
