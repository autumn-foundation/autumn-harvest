//! Workflow entry-point discovery: every `__autumn_workflow_info_X` companion fn the
//! `#[workflow]` macro emits marks `X` (same module) as a workflow; its analyzable body
//! is `X` (sync) or `X::{closure#0}` (async).

use crate::mir::MirDoc;

/// A discovered workflow entry point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub crate_name: String,
    /// `module::name` path of the workflow fn.
    pub workflow: String,
    /// Path of the MIR body to analyze.
    pub body: String,
}

/// Discover entries across the given docs.
#[must_use]
pub fn discover(docs: &[MirDoc]) -> Vec<Entry> {
    let _ = docs;
    todo!("RED phase: implemented in GREEN")
}
