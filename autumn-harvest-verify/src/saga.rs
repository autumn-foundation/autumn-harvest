//! Saga compensation coverage over a structure manifest (issue #2010).
//!
//! The check reads the flow graphs of a manifest and no MIR. It asks one
//! question of each workflow: can a completed forward step of a `Saga`
//! reach an error exit with no unwind on the way?
//!
//! For each body with a `saga-new` node, a forward fixpoint carries one flag
//! per node. The flag is set when a forward step may have completed and not
//! been unwound:
//!
//! - The `ok` edges of a tracked `saga-step` set the flag. Its `err` edges
//!   clear it, because a failed step unwinds every earlier step.
//! - Every edge of an untracked `saga-step` sets the flag.
//! - Every edge of a `saga-compensate` clears it.
//!
//! An exit with the outcome `err` or `unknown` and a set flag is a gap.
//!
//! Each of these facts makes the verdict `unknown`:
//!
//! - A saga escapes the body that owns it.
//! - The result of a step is untracked.
//! - One body builds two sagas.
//! - The workflow has a boundary and no saga in sight.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::structure::{
    BodyNode, EdgeLabel, ExitOutcome, FLOW_FORMAT, FlowEvent, FlowGraph, StructureManifest,
    WorkflowStructure,
};

/// The coverage verdict of one workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SagaVerdict {
    /// No body uses a saga, and no boundary can hide one.
    NoSaga,
    /// Each error exit after a completed forward step unwinds first.
    Covered,
    /// An error exit can follow a completed forward step with no unwind.
    Gap,
    /// The graph cannot show the answer. The reasons are in
    /// [`SagaReport::unknown`].
    Unknown,
}

impl SagaVerdict {
    /// The verdict as the text report prints it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::NoSaga => "no-saga",
            Self::Covered => "covered",
            Self::Gap => "gap",
            Self::Unknown => "unknown",
        }
    }
}

/// The coverage result of one workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaReport {
    /// `crate::module::fn`.
    pub workflow: String,
    /// The registered workflow name.
    pub name: String,
    pub verdict: SagaVerdict,
    /// Each exit that a completed forward step reaches with no unwind. With
    /// an `unknown` verdict, these gaps are possible, not proven.
    pub gaps: Vec<Gap>,
    /// Each reason for `unknown`, as `kind: detail`.
    pub unknown: Vec<String>,
    /// Facts that do not change the verdict, as `kind: detail`.
    pub notes: Vec<String>,
}

/// An exit that a completed forward step can reach with no unwind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gap {
    /// The body that holds the exit.
    pub body: String,
    /// The MIR block of the exit.
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
/// When the manifest carries no flow graphs, such as a manifest from a
/// build before issue #2010. Without them every workflow would read as
/// `no-saga`.
pub fn check(manifest: &StructureManifest) -> Result<Vec<SagaReport>, CheckError> {
    if manifest.flow.as_deref() != Some(FLOW_FORMAT) {
        return Err(CheckError(format!(
            "the manifest has no `{FLOW_FORMAT}` flow graphs (flow: {:?}); \
             emit it again with this version of harvest-verify",
            manifest.flow
        )));
    }
    Ok(manifest.workflows.iter().map(check_workflow).collect())
}

/// Check one workflow.
#[must_use]
pub fn check_workflow(workflow: &WorkflowStructure) -> SagaReport {
    let mut gaps = Vec::new();
    let mut unknown: BTreeSet<String> = BTreeSet::new();
    let mut notes: BTreeSet<String> = BTreeSet::new();
    let mut uses_saga = false;

    for body in &workflow.bodies {
        let Some(graph) = &body.flow else {
            unknown.insert(format!("no-flow-graph: {}", body.id));
            continue;
        };
        let owners = count(graph, |e| matches!(e, FlowEvent::SagaNew));
        let operations = count(graph, |e| {
            matches!(e, FlowEvent::SagaStep { .. } | FlowEvent::SagaCompensate)
        });
        if owners == 0 && operations == 0 {
            continue;
        }
        uses_saga = true;
        if owners == 0 {
            unknown.insert(format!(
                "saga-escapes: {} uses a saga it does not own",
                body.id
            ));
            continue;
        }
        if owners > 1 {
            unknown.insert(format!("multiple-sagas: {} builds {owners} sagas", body.id));
        }
        for node in &graph.nodes {
            match &node.event {
                FlowEvent::SagaEscape { to } => {
                    unknown.insert(format!("saga-escapes: {} at {} to {to}", body.id, node.at));
                }
                FlowEvent::SagaStep { tracked: false, .. } => {
                    unknown.insert(format!("saga-result-untracked: {} at {}", body.id, node.at));
                }
                FlowEvent::SagaStep { compensate, .. } => {
                    for start in compensate {
                        if !emits_a_step(workflow, start) {
                            notes.insert(format!("noop-compensation: {start}"));
                        }
                    }
                }
                _ => {}
            }
        }
        gaps.extend(body_gaps(body, graph));
    }

    if !uses_saga {
        for boundary in &workflow.boundaries {
            unknown.insert(format!("boundary: {boundary}"));
        }
    }
    let verdict = if !unknown.is_empty() {
        SagaVerdict::Unknown
    } else if !uses_saga {
        SagaVerdict::NoSaga
    } else if gaps.is_empty() {
        SagaVerdict::Covered
    } else {
        SagaVerdict::Gap
    };
    SagaReport {
        workflow: workflow.workflow.clone(),
        name: workflow.name.clone(),
        verdict,
        gaps,
        unknown: unknown.into_iter().collect(),
        notes: notes.into_iter().collect(),
    }
}

/// One line per workflow, its details indented, and a count line.
#[must_use]
pub fn render_text(reports: &[SagaReport]) -> String {
    let mut out = String::new();
    let mut counts = [0_usize; 4];
    for report in reports {
        let slot = match report.verdict {
            SagaVerdict::Covered => 0,
            SagaVerdict::Gap => 1,
            SagaVerdict::Unknown => 2,
            SagaVerdict::NoSaga => 3,
        };
        if let Some(count) = counts.get_mut(slot) {
            *count = count.saturating_add(1);
        }
        let _ = writeln!(out, "{}  {}", report.verdict.name(), report.workflow);
        for gap in &report.gaps {
            let _ = writeln!(
                out,
                "  gap: {} at {} returns {}",
                gap.body,
                gap.at,
                gap.outcome.name()
            );
        }
        for reason in &report.unknown {
            let _ = writeln!(out, "  unknown: {reason}");
        }
        for note in &report.notes {
            let _ = writeln!(out, "  note: {note}");
        }
    }
    let [covered, gap, unknown, none] = counts;
    let _ = write!(
        out,
        "\nchecked {}: covered {covered}, gap {gap}, unknown {unknown}, no-saga {none}",
        reports.len()
    );
    out
}

fn count(graph: &FlowGraph, pick: impl Fn(&FlowEvent) -> bool) -> usize {
    graph.nodes.iter().filter(|n| pick(&n.event)).count()
}

/// The exits of `body` that a set flag reaches.
fn body_gaps(body: &BodyNode, graph: &FlowGraph) -> Vec<Gap> {
    let size = graph.nodes.len();
    // `reached[n]` holds each flag value seen on entry to node `n`.
    let mut reached: Vec<[bool; 2]> = vec![[false; 2]; size];
    let mut queue: Vec<(usize, bool)> = Vec::new();
    for (id, node) in graph.nodes.iter().enumerate() {
        if matches!(node.event, FlowEvent::Entry) {
            queue.push((id, false));
        }
    }
    while let Some((id, flag)) = queue.pop() {
        let Some(seen) = reached.get_mut(id) else {
            continue;
        };
        let slot = usize::from(flag);
        if seen.get(slot).copied().unwrap_or(true) {
            continue;
        }
        if let Some(bit) = seen.get_mut(slot) {
            *bit = true;
        }
        let event = graph.nodes.get(id).map(|n| &n.event);
        for edge in graph.edges.iter().filter(|e| e.from == id) {
            let out = match (event, edge.label) {
                (Some(FlowEvent::SagaStep { tracked: true, .. }), Some(EdgeLabel::Err))
                | (Some(FlowEvent::SagaCompensate), _) => false,
                (Some(FlowEvent::SagaStep { .. }), _) => true,
                _ => flag,
            };
            queue.push((edge.to, out));
        }
    }
    graph
        .nodes
        .iter()
        .zip(&reached)
        .filter_map(|(node, seen)| match node.event {
            FlowEvent::Exit { outcome }
                if outcome != ExitOutcome::Ok && seen.get(1).copied().unwrap_or(false) =>
            {
                Some(Gap {
                    body: body.id.clone(),
                    at: node.at.clone(),
                    outcome,
                })
            }
            _ => None,
        })
        .collect()
}

/// `start` or a body it calls emits a step.
fn emits_a_step(workflow: &WorkflowStructure, start: &str) -> bool {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut queue: Vec<&str> = vec![start];
    while let Some(id) = queue.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(body) = workflow.bodies.iter().find(|b| b.id == id) else {
            // A body outside the graph may emit a step. Count it as one, so
            // no false note is raised.
            return true;
        };
        if !body.steps.is_empty() {
            return true;
        }
        queue.extend(
            body.calls
                .iter()
                .filter(|c| !c.resume)
                .map(|c| c.callee.as_str()),
        );
    }
    false
}
