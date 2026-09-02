//! Taint analysis, interprocedural summaries, control dependence and verdicts.
//!
//! Entry point: [`analyze`]. One [`summary::Analyzer`] is built per workflow, so
//! every fact's hop chain is rooted at that workflow's own body and the trace a
//! finding carries reads as a path from the entry to the source to the sink.

pub mod control;
pub mod summary;
pub mod taint;
pub mod verdict;

use crate::entry::Entry;
use crate::model::Model;
use crate::resolve::Program;
use crate::verdict::WorkflowVerdict;

use summary::Analyzer;
use taint::TaintSet;

/// Analyze every entry and produce one verdict per workflow.
#[must_use]
pub fn analyze(program: &Program, model: &Model, entries: &[Entry]) -> Vec<WorkflowVerdict> {
    analyze_with_warnings(program, model, entries).0
}

/// [`analyze`], plus the report warnings the run accumulated.
///
/// A warning here is always about an ambiguity the analysis resolved
/// conservatively — two statics or two impl methods that share a printed name —
/// so it never changes a verdict, but a run whose answer turned on a name
/// collision should say which one.
#[must_use]
pub fn analyze_with_warnings(
    program: &Program,
    model: &Model,
    entries: &[Entry],
) -> (Vec<WorkflowVerdict>, Vec<String>) {
    let mut out = Vec::with_capacity(entries.len());
    let mut warnings: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for entry in entries {
        let mut analyzer = Analyzer::new(program, model);
        let args: Vec<TaintSet> = Vec::new();
        // Several analyzed targets routinely define the same trimmed body path
        // (`--all-examples` alone yields five `charge_card`s, one per example);
        // the entry must be the body in this workflow's own crate.
        let body = program.body_id_in(&entry.crate_name, &entry.body);
        analyzer.analyze_body(&body, &crate::resolve::Substitution::new(), &args, &[]);
        out.push(verdict::assemble(
            &entry.workflow,
            &entry.crate_name,
            std::mem::take(&mut analyzer.findings),
            std::mem::take(&mut analyzer.boundaries),
        ));
        warnings.append(&mut analyzer.warnings);
    }
    (out, warnings.into_iter().collect())
}
