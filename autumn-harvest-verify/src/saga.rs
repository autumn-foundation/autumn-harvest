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
    BodyNode, EdgeLabel, ExitOutcome, FLOW_FORMAT, FlowEvent, FlowGraph, STRUCTURE_FORMAT,
    StructureManifest, WorkflowStructure,
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
    check_header(Some(&manifest.format), manifest.flow.as_deref())?;
    Ok(manifest.workflows.iter().map(check_workflow).collect())
}

/// Parse a manifest from JSON text.
///
/// The format fields are read first. So a manifest from another version gets
/// a clear error, not a message about a field it does not know.
///
/// # Errors
/// When the text is not JSON, names another format, or does not parse.
pub fn parse_manifest(text: &str) -> Result<StructureManifest, CheckError> {
    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| CheckError(format!("cannot parse the manifest as JSON: {e}")))?;
    check_header(
        value.get("format").and_then(serde_json::Value::as_str),
        value.get("flow").and_then(serde_json::Value::as_str),
    )?;
    serde_json::from_value(value).map_err(|e| CheckError(format!("cannot parse the manifest: {e}")))
}

fn check_header(format: Option<&str>, flow: Option<&str>) -> Result<(), CheckError> {
    if format != Some(STRUCTURE_FORMAT) {
        return Err(CheckError(format!(
            "the manifest format is {format:?}, not `{STRUCTURE_FORMAT}`"
        )));
    }
    if flow != Some(FLOW_FORMAT) {
        return Err(CheckError(format!(
            "the manifest has no `{FLOW_FORMAT}` flow graphs (flow: {flow:?}); \
             emit it again with this version of harvest-verify"
        )));
    }
    Ok(())
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
            matches!(
                e,
                FlowEvent::SagaStep { .. } | FlowEvent::SagaCompensate { .. }
            )
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
        let (found, reasons) = body_gaps(body, graph, body.id == workflow.root);
        gaps.extend(found);
        unknown.extend(reasons);
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
        // Under `unknown`, a gap is possible, not proven.
        let label = if report.verdict == SagaVerdict::Unknown {
            "possible gap"
        } else {
            "gap"
        };
        for gap in &report.gaps {
            let _ = writeln!(
                out,
                "  {label}: {} at {} returns {}",
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

/// The exits of `body` that a set flag reaches, and the reasons for
/// `unknown` that the fixpoint finds.
///
/// Two states hide a pending step from the check:
///
/// - `saga-recreated`: a new saga starts while a step of the old one is
///   pending. The old saga can no longer unwind it.
/// - `saga-dropped-pending`: a body other than the workflow root exits while
///   a step is pending, even with `Ok`. Its caller cannot unwind that step.
fn body_gaps(body: &BodyNode, graph: &FlowGraph, root: bool) -> (Vec<Gap>, BTreeSet<String>) {
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
                | (Some(FlowEvent::SagaCompensate { tracked: true }), _) => false,
                (Some(FlowEvent::SagaStep { .. }), _) => true,
                _ => flag,
            };
            queue.push((edge.to, out));
        }
    }
    let mut gaps = Vec::new();
    let mut reasons = BTreeSet::new();
    for (node, seen) in graph.nodes.iter().zip(&reached) {
        if !seen.get(1).copied().unwrap_or(false) {
            continue;
        }
        match node.event {
            FlowEvent::SagaNew => {
                reasons.insert(format!("saga-recreated: {} at {}", body.id, node.at));
            }
            FlowEvent::Exit { outcome } if !root => {
                reasons.insert(format!(
                    "saga-dropped-pending: {} at {} returns {}",
                    body.id,
                    node.at,
                    outcome.name()
                ));
            }
            FlowEvent::Exit { outcome } if outcome != ExitOutcome::Ok => gaps.push(Gap {
                body: body.id.clone(),
                at: node.at.clone(),
                outcome,
            }),
            _ => {}
        }
    }
    (gaps, reasons)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structure::{FlowEdge, FlowNode};

    fn node(event: FlowEvent) -> FlowNode {
        FlowNode {
            at: String::new(),
            event,
        }
    }

    fn edge(from: usize, to: usize, label: Option<EdgeLabel>) -> FlowEdge {
        FlowEdge { from, to, label }
    }

    fn workflow(graph: FlowGraph) -> WorkflowStructure {
        WorkflowStructure {
            workflow: "w::wf".to_string(),
            name: "wf".to_string(),
            root: "w::wf::{closure#0}".to_string(),
            bodies: vec![BodyNode {
                id: "w::wf::{closure#0}".to_string(),
                flow: Some(graph),
                ..BodyNode::default()
            }],
            ..WorkflowStructure::default()
        }
    }

    fn step() -> FlowEvent {
        FlowEvent::SagaStep {
            forward: Vec::new(),
            compensate: Vec::new(),
            tracked: true,
        }
    }

    #[test]
    fn a_gap_reached_only_through_a_back_edge_is_found() {
        // entry -> new -> call -> step; step -ok-> call (the loop);
        // call -> err exit. The first pass reaches the exit with the flag
        // clear. Only the back edge carries the set flag to it.
        let graph = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(FlowEvent::Call {
                    callees: Vec::new(),
                }),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, None),
                edge(2, 4, None),
                edge(3, 2, Some(EdgeLabel::Ok)),
                edge(3, 4, Some(EdgeLabel::Err)),
            ],
        };
        let report = check_workflow(&workflow(graph));
        assert_eq!(report.verdict, SagaVerdict::Gap, "{report:#?}");
        assert_eq!(report.gaps.len(), 1);
    }

    #[test]
    fn an_unwind_that_is_not_awaited_clears_nothing() {
        let graph = |tracked: bool| FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::SagaCompensate { tracked }),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Ok)),
                edge(3, 4, None),
            ],
        };
        assert_eq!(
            check_workflow(&workflow(graph(true))).verdict,
            SagaVerdict::Covered
        );
        assert_eq!(
            check_workflow(&workflow(graph(false))).verdict,
            SagaVerdict::Gap
        );
    }

    #[test]
    fn an_ok_exit_of_the_root_with_a_pending_step_is_not_a_gap() {
        let graph = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Ok)),
            ],
        };
        assert_eq!(
            check_workflow(&workflow(graph)).verdict,
            SagaVerdict::Covered
        );
    }
}
