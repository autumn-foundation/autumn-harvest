//! Taint analysis, interprocedural summaries, control dependence and verdicts.

use crate::entry::Entry;
use crate::model::Model;
use crate::resolve::Program;
use crate::verdict::WorkflowVerdict;

/// Analyze every entry and produce one verdict per workflow.
#[must_use]
pub fn analyze(program: &Program, model: &Model, entries: &[Entry]) -> Vec<WorkflowVerdict> {
    let _ = (program, model, entries);
    todo!("RED phase: implemented in GREEN")
}
